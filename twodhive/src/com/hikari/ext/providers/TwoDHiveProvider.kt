package com.hikari.ext.providers

import android.util.Base64
import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import com.hikari.ext.HikariSubtitle
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import org.json.JSONObject
import java.net.URLEncoder
import javax.crypto.Cipher
import javax.crypto.Mac
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec

/**
 * 2DHive (2dhive.com) — anime grid + summary API + MegaPlay/Baba streams.
 *
 * Faithful native port of KSHITIJ8473/raghav TwoDHive (CloudStream MainAPI),
 * verified live Oct 2026:
 *  - Firebase-hosted domain table (refreshDomain) with https://2dhive.com fallback.
 *  - Rows: /?list=completed and /?list=top (paged; the only two native lists —
 *    unknown list values fall back to the homepage) plus AniList rows
 *    (popular, top rated, movies, airing) keyed by idMal, since every title
 *    links as /anime?anime={malId}.
 *  - Cards (SSR article.lazy-card grid): a[href="/anime?anime={id}"] > img[src]
 *    + h2 title + type badge + EP count + score.
 *  - Search: /?q= (SSR, "N titles found").
 *  - Detail: /anime?anime={malId} (og tags + fact grid) + summary API
 *    /api/anime/summary?malId= -> {anime:{title,imageUrl,episodes,type,score,
 *    status,synopsis,genres,year}}; episodes from data-episode-slug anchors
 *    (else 1..summary.episodes).
 *  - Playback: megaplay.buzz/stream/mal/{id}/{ep}/{sub|dub} (data-id ->
 *    stream/getSourcesNew|getSources, enc via AES seeds, HMAC-signed variant
 *    URLs) + babastream.top embeds via WebView solver capture.
 */
class TwoDHiveProvider : HikariProvider {

    override val id = "twodhive"
    override val name = "2Dhive"
    override val mainUrl = "https://2dhive.com"
    override val description = "Anime from 2dhive.com — completed classics, top rated, popular, movies, sub & dub via MegaPlay."
    override val version = 2
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("completed", "Completed Classics", HikariMediaType.SERIES),
        HikariCatalog("top", "Top Rated Anime", HikariMediaType.SERIES),
        HikariCatalog("popular", "Most Popular", HikariMediaType.SERIES),
        HikariCatalog("top-rated", "Top Scored", HikariMediaType.SERIES),
        HikariCatalog("movies", "Anime Movies", HikariMediaType.MOVIE),
        HikariCatalog("airing", "Top Airing", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        DhApi.refreshDomain()
        val p = page.coerceAtLeast(1)
        return when (catalog.id) {
            "completed", "top" -> DhApi.list(catalog.id, p).mapNotNull { it.toMedia() }
            "popular" -> DhApi.anilistRow(DhApi.AL_POPULAR, p).mapNotNull { it.toMedia() }
            "top-rated" -> DhApi.anilistRow(DhApi.AL_TOP, p).mapNotNull { it.toMedia() }
            "movies" -> DhApi.anilistRow(DhApi.AL_MOVIES, p).mapNotNull { it.toMedia() }
            "airing" -> DhApi.anilistRow(DhApi.AL_AIRING, p).mapNotNull { it.toMedia() }
            else -> emptyList()
        }
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        DhApi.refreshDomain()
        if (page > 1) return emptyList()
        return DhApi.search(q).mapNotNull { it.toMedia() }
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        DhApi.refreshDomain()
        val malId = DhApi.malId(media.id) ?: return media
        val s = DhApi.summary(malId)
        val html = HikariNet.getStringSmart("${DhApi.host()}/anime?anime=$malId", DhApi.pageHeaders())
        val title = Regex("""<meta[^>]+property="og:title"[^>]+content="([^"]+)"""").find(html ?: "")?.groupValues?.get(1)?.substringBefore(" Online Free")?.trim()
            ?: s?.title ?: media.title
        val poster = Regex("""<meta[^>]+property="og:image"[^>]+content="([^"]+)"""").find(html ?: "")?.groupValues?.get(1)?.takeIf { it.startsWith("http") }
            ?: s?.poster ?: media.posterUrl
        return media.copy(
            id = "twodhive:$malId",
            title = title,
            posterUrl = poster,
            backdropUrl = poster,
            overview = s?.plot ?: media.overview,
            genres = s?.genres?.takeIf { it.isNotEmpty() } ?: media.genres,
            year = s?.year ?: media.year,
        )
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        DhApi.refreshDomain()
        val malId = DhApi.malId(media.id) ?: return null
        val html = HikariNet.getStringSmart("${DhApi.host()}/anime?anime=$malId", DhApi.pageHeaders())
        val slugs = html?.let { DhApi.episodeSlugs(it) } ?: emptyList()
        val s = DhApi.summary(malId)
        val count = slugs.size.coerceAtLeast(s?.episodes ?: 0).coerceAtLeast(1).coerceAtMost(3000)
        val hasDub = DhApi.probeDub(malId)
        val out = ArrayList<HikariEpisode>()
        for (n in 1..count) {
            val name = slugs.getOrNull(n - 1)?.second ?: "Episode $n"
            out.add(HikariEpisode(number = n, id = "$malId|$n|sub", name = name))
        }
        if (hasDub) {
            for (n in 1..count) {
                val name = slugs.getOrNull(n - 1)?.second ?: "Episode $n"
                out.add(HikariEpisode(number = n, id = "$malId|$n|dub", name = "$name (Dub)"))
            }
        }
        return out.ifEmpty { null }
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        DhApi.refreshDomain()
        DhMegaCipher.refresh()
        val ref = episode?.id ?: return emptyList()
        val parts = ref.split("|")
        if (parts.size != 3) return emptyList()
        val malId = parts[0].toIntOrNull() ?: return emptyList()
        val ep = parts[1].toIntOrNull() ?: return emptyList()
        val type = if (parts[2] == "dub") "dub" else "sub"
        val epUrl = "${DhApi.host()}/episode?anime=$malId&ep_num=$ep"
        val out = java.util.Collections.synchronizedList(ArrayList<HikariStream>())
        coroutineScope {
            listOf(
                async { out.addAll(DhApi.resolveMegaPlay(malId, ep, type, epUrl)) },
                async { out.addAll(DhApi.resolveBaba(malId, ep, type, epUrl)) },
            ).awaitAll()
        }
        return out.distinctBy { it.url }
    }

    private fun DhItem.toMedia(): HikariMedia? {
        if (malId.isBlank() || title.isBlank()) return null
        return HikariMedia(id = "twodhive:$malId", title = title, type = HikariMediaType.SERIES, posterUrl = poster, year = year)
    }

    private fun DhAlItem.toMedia(): HikariMedia? {
        if (malId.isBlank() || title.isBlank()) return null
        val type = if (format.equals("MOVIE", true)) HikariMediaType.MOVIE else HikariMediaType.SERIES
        return HikariMedia(id = "twodhive:$malId", title = title, type = type, posterUrl = poster, year = year)
    }
}

// ---------------------------------------------------------------------------

private data class DhItem(val malId: String, val title: String, val poster: String?, val year: Int?)
private data class DhAlItem(val malId: String, val title: String, val poster: String?, val year: Int?, val format: String)
private data class DhSummary(val title: String, val poster: String?, val plot: String?, val genres: List<String>, val year: Int?, val episodes: Int?)

private object DhApi {
    const val DEFAULT_HOST = "https://2dhive.com"
    const val MEGA = "https://megaplay.buzz"
    const val ANILIST = "https://graphql.anilist.co"
    const val UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    private const val DOMAIN_URL = "https://cloudstreampluginhelper-default-rtdb.firebaseio.com/.json"

    @Volatile private var base = DEFAULT_HOST
    @Volatile private var lastDomain = 0L

    val MEDIA_CAPTURE = Regex("""(?i)\.(m3u8|mp4)(?:[?#]|$)""")

    fun host(): String = base

    suspend fun refreshDomain() {
        val now = System.currentTimeMillis()
        if (now - lastDomain < 5 * 60 * 1000L) return
        lastDomain = now
        try {
            val txt = HikariNet.getString(DOMAIN_URL) ?: return
            val v = JSONObject(txt).optString("twodhive").ifBlank { JSONObject(txt).optString("twodhive_url") }.trim().removeSuffix("/")
            if (v.startsWith("http")) base = v
        } catch (t: Throwable) { }
    }

    fun pageHeaders(): Map<String, String> = mapOf(
        "User-Agent" to UA,
        "Accept" to "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer" to "$base/",
    )

    fun malId(mediaId: String): String? {
        val tail = mediaId.substringAfterLast("/").substringAfterLast(":").substringBefore("|").substringBefore("?")
        return tail.takeIf { it.isNotBlank() }
    }

    // -- grids ------------------------------------------------------------------
    private val articleRe = Regex("""<article[^>]*>(.*?)</article>""", setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE))
    private val hrefRe = Regex("""<a[^>]+href="(/anime\?anime=\d+[^"]*)"""", RegexOption.IGNORE_CASE)
    private val imgRe = Regex("""<img[^>]+src="([^"]+)"""", RegexOption.IGNORE_CASE)
    private val altRe = Regex("""<img[^>]+alt="([^"]*)"""", RegexOption.IGNORE_CASE)
    private val h2Re = Regex("""<h2[^>]*>(.*?)</h2>""", setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE))
    private val idRe = Regex("""anime=(\d+)""")
    private fun stripTags(s: String): String = s.replace(Regex("<[^>]+>"), " ").replace(Regex("\\s+"), " ").trim()

    private fun parseArticles(html: String): List<DhItem> {
        val out = ArrayList<DhItem>()
        val seen = HashSet<String>()
        for (m in articleRe.findAll(html)) {
            val block = m.groupValues[1]
            val href = hrefRe.find(block)?.groupValues?.get(1) ?: continue
            val id = idRe.find(href)?.groupValues?.get(1) ?: continue
            if (!seen.add(id)) continue
            val title = h2Re.find(block)?.groupValues?.get(1)?.let { stripTags(it) }?.takeIf { it.length >= 2 }
                ?: altRe.find(block)?.groupValues?.get(1)?.takeIf { it.length >= 2 } ?: continue
            val poster = imgRe.find(block)?.groupValues?.get(1)?.takeIf { it.startsWith("http") }
            out.add(DhItem(id, title, poster, null))
        }
        return out
    }

    suspend fun list(kind: String, page: Int): List<DhItem> {
        val url = if (page > 1) "$base/?list=$kind&page=$page" else "$base/?list=$kind"
        val html = HikariNet.getStringSmart(url, pageHeaders()) ?: return emptyList()
        return parseArticles(html)
    }

    suspend fun search(q: String): List<DhItem> {
        val html = HikariNet.getStringSmart("$base/?q=${URLEncoder.encode(q, "UTF-8")}", pageHeaders()) ?: return emptyList()
        return parseArticles(html)
    }

    // -- anilist rows (idMal keyed) -----------------------------------------------
    const val AL_POPULAR = "POPULARITY_DESC|"
    const val AL_TOP = "SCORE_DESC|"
    const val AL_MOVIES = "POPULARITY_DESC|MOVIE"
    const val AL_AIRING = "TRENDING_DESC|RELEASING"

    suspend fun anilistRow(kind: String, page: Int): List<DhAlItem> {
        val parts = kind.split("|")
        val sort = parts[0]
        val extra = parts.getOrNull(1) ?: ""
        val format = if (extra == "MOVIE") ", format: MOVIE" else ""
        val status = if (extra == "RELEASING") ", status: RELEASING" else ""
        val query = """query (${'$'}page: Int, ${'$'}perPage: Int) { Page(page: ${'$'}page, perPage: ${'$'}perPage) { media(type: ANIME, sort: $sort$format$status, idMal_not: null) { idMal title { romaji english } coverImage { large extraLarge } format seasonYear } } }"""
        val body = JSONObject().put("query", query)
            .put("variables", JSONObject().put("page", page).put("perPage", 24)).toString()
        val resp = HikariNet.postString(ANILIST, body, mapOf("Content-Type" to "application/json", "User-Agent" to UA, "Referer" to "https://anilist.co/")) ?: return emptyList()
        return try {
            val arr = JSONObject(resp).optJSONObject("data")?.optJSONObject("Page")?.optJSONArray("media") ?: return emptyList()
            buildList {
                for (i in 0 until arr.length()) {
                    val m = arr.optJSONObject(i) ?: continue
                    val mal = m.optInt("idMal", 0).takeIf { it != 0 }?.toString() ?: continue
                    val t = m.optJSONObject("title")
                    val title = t?.optString("english")?.takeIf { it.isNotBlank() } ?: t?.optString("romaji")?.takeIf { it.isNotBlank() } ?: continue
                    val c = m.optJSONObject("coverImage")
                    add(DhAlItem(mal, title,
                        c?.optString("extraLarge")?.takeIf { it.startsWith("http") } ?: c?.optString("large")?.takeIf { it.startsWith("http") },
                        m.optInt("seasonYear", 0).takeIf { it > 0 }, m.optString("format")))
                }
            }
        } catch (t: Throwable) { emptyList() }
    }

    // -- detail ---------------------------------------------------------------------
    suspend fun summary(malId: String): DhSummary? {
        val body = HikariNet.getStringSmart("$base/api/anime/summary?malId=$malId", mapOf("User-Agent" to UA, "Accept" to "application/json", "Referer" to "$base/")) ?: return null
        return try {
            val a = JSONObject(body).optJSONObject("anime") ?: return null
            DhSummary(
                title = a.optString("englishTitle").takeIf { it.isNotBlank() } ?: a.optString("title"),
                poster = a.optString("coverImageUrl").takeIf { it.startsWith("http") } ?: a.optString("imageUrl").takeIf { it.startsWith("http") },
                plot = a.optString("synopsis").takeIf { it.isNotBlank() },
                genres = a.optJSONArray("genres")?.let { g -> List(g.length()) { g.optString(it) } } ?: emptyList(),
                year = a.optInt("year", 0).takeIf { it > 0 },
                episodes = a.optInt("episodes", 0).takeIf { it > 0 },
            )
        } catch (t: Throwable) { null }
    }

    private val slugRe = Regex("""data-episode-slug="([^"]+)"""")
    private val epLinkRe = Regex("""/episode\?anime=\d+[^"]*?ep_num=(\d+)""")

    fun episodeSlugs(html: String): List<Pair<Int, String?>> {
        val slugs = slugRe.findAll(html).map { it.groupValues[1] }.toList()
        if (slugs.isNotEmpty()) {
            return slugs.mapIndexedNotNull { i, s ->
                val n = Regex("""(?:ep|e)[-_]?(\d+)""", RegexOption.IGNORE_CASE).find(s)?.groupValues?.get(1)?.toIntOrNull() ?: (i + 1)
                n to s.replace(Regex("[-_]"), " ").trim().takeIf { it.isNotBlank() }
            }.sortedBy { it.first }
        }
        val nums = epLinkRe.findAll(html).mapNotNull { it.groupValues[1].toIntOrNull() }.distinct().sorted().toList()
        return nums.map { it to null }
    }

    suspend fun probeDub(malId: String): Boolean {
        return try {
            val html = HikariNet.getString("$MEGA/stream/mal/$malId/1/dub", mapOf("User-Agent" to UA, "Referer" to "$base/"))
            html != null && (html.contains("data-id=") || html.contains("data-realid="))
        } catch (t: Throwable) { false }
    }

    // -- playback ---------------------------------------------------------------------
    suspend fun resolveMegaPlay(malId: Int, ep: Int, type: String, epUrl: String): List<HikariStream> {
        val playerUrl = "$MEGA/stream/mal/$malId/$ep/$type"
        val html = HikariNet.getStringSmart(playerUrl, mapOf("User-Agent" to UA, "Referer" to epUrl)) ?: return emptyList()
        val playerId = Regex("""data-id=["'](\d+)["']""").find(html)?.groupValues?.get(1)
            ?: Regex("""data-realid=["'](\d+)["']""").find(html)?.groupValues?.get(1) ?: return emptyList()
        val ajax = mapOf("User-Agent" to UA, "X-Requested-With" to "XMLHttpRequest", "Referer" to playerUrl)
        var root: JSONObject? = null
        for (endpoint in listOf("getSourcesNew", "getSources")) {
            val body = HikariNet.getStringSmart("$MEGA/stream/$endpoint?id=$playerId&type=$type", ajax) ?: continue
            root = try { JSONObject(body) } catch (t: Throwable) { continue }
            if (root != null) break
        }
        root ?: return emptyList()
        val raw = extractMpUrl(root) ?: return emptyList()
        val origin = Regex("""https?://[^/]+""").find(raw)?.value
        val m3u8 = migrate(raw, origin)
        val playHeaders = mapOf("User-Agent" to UA, "Referer" to "$MEGA/")
        val subs = ArrayList<HikariSubtitle>()
        root.optJSONArray("tracks")?.let { arr ->
            for (i in 0 until arr.length()) {
                val t = arr.optJSONObject(i) ?: continue
                val f = t.optString("file")
                if (f.isBlank()) continue
                subs.add(HikariSubtitle(t.optString("label").ifBlank { "English" }, migrate(f, origin)))
            }
        }
        val label = if (type == "dub") "2DHive MegaPlay Dub" else "2DHive MegaPlay Sub"
        val signedMaster = signUrl(m3u8)
        val masterText = HikariNet.getString(signedMaster, playHeaders)
        val variants = masterText?.let { parseVariants(m3u8, it) } ?: emptyList()
        if (variants.isNotEmpty()) {
            return variants.map { v ->
                val suffix = v.second?.let { " $it" } ?: ""
                HikariStream(name = "$label$suffix", url = signUrl(v.first), headers = playHeaders, subtitles = subs, isM3u8 = true)
            }
        }
        return listOf(HikariStream(name = label, url = signedMaster, headers = playHeaders, subtitles = subs, isM3u8 = true))
    }

    private fun extractMpUrl(root: JSONObject): String? {
        val sources = root.opt("sources")
        val legacy = when {
            sources == null || sources == JSONObject.NULL -> null
            sources is org.json.JSONArray && sources.length() > 0 -> sources.optJSONObject(0)?.optString("file")?.takeIf { it.isNotBlank() }
            sources is JSONObject -> sources.optString("file").takeIf { it.isNotBlank() }
            else -> null
        }
        if (!legacy.isNullOrBlank()) return legacy
        val enc = root.optString("enc").takeIf { it.isNotBlank() } ?: return null
        return DhMegaCipher.resolveEncSync(enc)
    }

    private fun migrate(url: String, origin: String?): String {
        if (!url.contains("https://cdn.imgnex.top/anime")) return url
        return url.replace("https://cdn.imgnex.top/anime", origin ?: "https://megap.norami.top")
    }

    private fun parseVariants(masterUrl: String, masterText: String): List<Pair<String, Int?>> {
        val base = masterUrl.substringBefore('?').substringBeforeLast('/') + "/"
        val out = ArrayList<Pair<String, Int?>>()
        val lines = masterText.lines()
        var i = 0
        while (i < lines.size) {
            if (lines[i].trim().startsWith("#EXT-X-STREAM-INF:")) {
                val q = Regex("""RESOLUTION=(\d+)x(\d+)""").find(lines[i])?.groupValues?.get(2)?.toIntOrNull()
                var j = i + 1
                while (j < lines.size && (lines[j].isBlank() || lines[j].startsWith("#"))) j++
                if (j < lines.size) {
                    val uri = lines[j].trim()
                    if (uri.isNotEmpty()) out.add((if (uri.startsWith("http")) uri else base + uri) to q)
                    i = j
                }
            }
            i++
        }
        return out
    }

    private fun b64url(bytes: ByteArray): String =
        Base64.encodeToString(bytes, Base64.URL_SAFE or Base64.NO_WRAP or Base64.NO_PADDING)

    fun signUrl(url: String): String {
        val m = Regex("""/([a-f0-9]{32})/([a-f0-9]{32})/""", RegexOption.IGNORE_CASE).find(url) ?: return url
        val expires = System.currentTimeMillis() / 1000L + 7L * 24 * 60 * 60
        val payload = "$expires|${m.groupValues[1].lowercase()}/${m.groupValues[2].lowercase()}"
        return try {
            val mac = Mac.getInstance("HmacSHA256")
            mac.init(SecretKeySpec("MpCdnT0k3n!9f2K#xQ7vL5mR8wN1pY4s".toByteArray(Charsets.UTF_8), "HmacSHA256"))
            val sig = mac.doFinal(payload.toByteArray(Charsets.UTF_8))
            "$url${if (url.contains('?')) "&" else "?"}token=${b64url(payload.toByteArray(Charsets.UTF_8))}.${b64url(sig)}"
        } catch (t: Throwable) { url }
    }

    private val babaSolver = """
(function () {
    if (window.__babaShell) return;
    window.__babaShell = 1;
    var tries = 0;
    function poll() {
        var t = "";
        try { t = document.title || ""; } catch (e) {}
        if (t === "video.mp4") { rebuild(); return; }
        if (t === "Just a moment...") return;
        if (tries++ < 300) setTimeout(poll, 10);
    }
    function rebuild() {
        document.open();
        document.write('<!doctype html><html><head><meta charset="utf-8"><title>baba</title>' + '<script src="https://cdn.jsdelivr.net/npm/cap-widget@0.1.57"><\/script>' + '</head><body><script>(' + driver.toString() + ')();<\/script></body></html>');
        document.close();
    }
    function driver() {
        function b64d(s) { var b = atob(s), u = new Uint8Array(b.length); for (var i = 0; i < b.length; i++) u[i] = b.charCodeAt(i); return u; }
        function b64e(u) { var s = ""; for (var i = 0; i < u.length; i++) s += String.fromCharCode(u[i]); return btoa(s); }
        var keyPromise = null;
        function key() { if (!keyPromise) { keyPromise = crypto.subtle.importKey("raw", b64d(CFG.pk), { name: "AES-GCM" }, false, ["encrypt", "decrypt"]); } return keyPromise; }
        async function seal(str) { var iv = crypto.getRandomValues(new Uint8Array(12)); var ct = await crypto.subtle.encrypt({ name: "AES-GCM", iv: iv }, await key(), new TextEncoder().encode(str)); var out = new Uint8Array(iv.length + ct.byteLength); out.set(iv, 0); out.set(new Uint8Array(ct), iv.length); return b64e(out); }
        async function open(b64) { var d = b64d(b64), iv = d.slice(0, 12), ct = d.slice(12); var pt = await crypto.subtle.decrypt({ name: "AES-GCM", iv: iv }, await key(), ct); return new TextDecoder().decode(pt); }
        async function call(route, payload) { var body = { s: CFG.sid, d: await seal(JSON.stringify(payload)) }; var r = await fetch(route, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) }); if (!r.ok) throw new Error("http " + r.status + " on " + route); return JSON.parse(await open((await r.json()).d)); }
        var attempts = 0;
        async function run() {
            var html = await (await fetch(location.href, { credentials: "same-origin" })).text();
            var m = html.match(/var CFG = (\{[^;]+\});/);
            if (!m) throw new Error("page config missing");
            window.CFG = JSON.parse(m[1]);
            var r = await call("/api/resolve", { ts: Date.now() });
            if (r.t === "error" && r.m === "verify") {
                if (!window.Cap) throw new Error("cap widget missing");
                var solved = await new window.Cap({ apiEndpoint: CFG.cap }).solve();
                var v = await call("/api/cap-verify", { ts: Date.now(), token: solved.token, mode: "invisible" });
                if (v.t !== "ok") throw new Error("cap rejected");
                r = await call("/api/resolve", { ts: Date.now() });
            }
            if (!r.u) throw new Error(r.m || "no stream url");
            var url = new URL(r.u, location.origin).href;
            if (/\.(m3u8|mp4)([?#]|$)/i.test(url)) { fetch(url, { mode: "no-cors" }).catch(function () {}); }
            else { location.href = url; }
        }
        (function attempt() { attempts += 1; if (attempts > 3) return; run().catch(function () { setTimeout(attempt, 5000); }); })();
    }
    poll();
})();
""".trimIndent()

    suspend fun resolveBaba(malId: Int, ep: Int, type: String, epUrl: String): List<HikariStream> {
        val embedUrl = "https://babastream.top/embed/$malId/$ep/$type"
        return try {
            HikariNet.resolveWithWebView(embedUrl, MEDIA_CAPTURE, timeoutMs = 90_000, script = babaSolver).map {
                HikariStream(
                    name = "2DHive BabaStream",
                    url = it.url,
                    headers = it.headers.ifEmpty { mapOf("User-Agent" to UA, "Referer" to "https://babastream.top/") },
                    isM3u8 = it.url.contains(".m3u8"),
                )
            }
        } catch (t: Throwable) { emptyList() }
    }
}

// MegaPlay AES seeds (enc payloads); seed refresh needs suspend so the cipher
// object exposes a sync decrypt with cached/fallback seeds and an async refresh.
private object DhMegaCipher {
    private const val FALLBACK_KEY_SEED = "i?LMTAx0Q6,:}50U"
    private const val FALLBACK_IV_SEED = "W0;27ToaUpl_P%'c"
    private val keyPairRe = Regex("""[A-Za-z]\w*="([^"]{16})",[A-Za-z]\w*="([^"]{16})"""")
    private val fileRe = Regex(""""file"\s*:\s*"([^"]+)"""")
    @Volatile private var cached: Pair<String, String>? = null

    suspend fun refresh() {
        try {
            HikariNet.getString("https://megaplay.buzz/lib/newclient.min.js", mapOf("User-Agent" to DhApi.UA))
                ?.let { keyPairRe.find(it)?.groupValues?.let { g -> cached = g[1] to g[2] } }
        } catch (t: Throwable) { }
    }

    fun resolveEncSync(enc: String): String? {
        val seeds = listOfNotNull(cached, FALLBACK_KEY_SEED to FALLBACK_IV_SEED)
        for ((k, iv) in seeds) {
            decrypt(enc, k, iv)?.let { plain ->
                fileRe.find(plain)?.groupValues?.get(1)?.let { return it }
            }
        }
        return null
    }

    private fun decrypt(enc: String, keySeed: String, ivSeed: String): String? {
        return try {
            var b64 = enc.replace('-', '+').replace('_', '/')
            while (b64.length % 4 != 0) b64 += "="
            val cipherBytes = Base64.decode(b64, Base64.DEFAULT)
            val keyBytes = ByteArray(32)
            val sb = keySeed.toByteArray(Charsets.UTF_8)
            System.arraycopy(sb, 0, keyBytes, 0, minOf(32, sb.size))
            val ivBytes = ByteArray(16)
            val ib = ivSeed.toByteArray(Charsets.UTF_8)
            System.arraycopy(ib, 0, ivBytes, 0, minOf(16, ib.size))
            val cipher = Cipher.getInstance("AES/CBC/PKCS5Padding")
            cipher.init(Cipher.DECRYPT_MODE, SecretKeySpec(keyBytes, "AES"), IvParameterSpec(ivBytes))
            String(cipher.doFinal(cipherBytes), Charsets.UTF_8)
        } catch (t: Throwable) { null }
    }
}
