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
import org.json.JSONArray
import org.json.JSONObject
import java.net.URLEncoder
import javax.crypto.Cipher
import javax.crypto.Mac
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec

/**
 * AniPM (ani.pm) — anime via JSON API + Settlar embeds + MegaPlay backup.
 *
 * Faithful native port of KSHITIJ8473/raghav AniPM (CloudStream MainAPI),
 * verified live Oct 2026:
 *  - Firebase-hosted domain table (refreshDomain) with https://ani.pm fallback.
 *  - Browse: GET {host}/api/anime/browse?sort=trending|popular|score&page=&limit=30[&format=Movie|OVA]
 *    -> {items:[{id,title,poster(/api/anime/cover?key=... relative),year,score,
 *       status,type,genres,subCount,dubCount,synopsis,malId,anilistId}],hasNextPage}
 *  - Latest: GET {host}/api/anime/latest-episodes?page= -> {items:[{id,title,
 *    poster,sub,dub}],hasNextPage}
 *  - Search: GET {host}/api/anime/search?q= (min 2 chars) -> {items:[...]}
 *  - Series: GET {host}/api/anime/series/{id}?routes=e3 -> detail + episodes[]
 *    [{number,title,thumbnail,description,sub,dub}].
 *  - Packages: GET .../anipm-server/_packages?anilistId= -> hardsub map;
 *    filler: GET .../filler?anilistId=&title= -> {ranges:{filler,mixed}}.
 *  - Playback: playback-bootstrap/settlar/{id}?ep=&lang=&backup=1 ->
 *    {settlarSelection,backupEmbed{available,url},anipmPackages} ->
 *    settlar/session?selection=&provider=anipm&ep=&channel= -> {embedUrl} ->
 *    https://embed.settlar.io/api/embed/session?t= -> {source,subtitles[]};
 *    backup via MegaPlay stream/ URLs (getSourcesNew/getSources + enc + signUrl).
 */
class AniPMProvider : HikariProvider {

    override val id = "anipm"
    override val name = "AniPM"
    override val mainUrl = "https://ani.pm"
    override val description = "Anime from ani.pm — latest episodes, trending, popular, top rated, movies and OVA, sub & dub via Settlar."
    override val version = 2
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("latest", "Latest Episodes", HikariMediaType.SERIES),
        HikariCatalog("trending", "Trending", HikariMediaType.SERIES),
        HikariCatalog("popular", "Popular", HikariMediaType.SERIES),
        HikariCatalog("top", "Top Rated", HikariMediaType.SERIES),
        HikariCatalog("movies", "Movies", HikariMediaType.MOVIE),
        HikariCatalog("ova", "OVA / Specials", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        PmApi.refreshDomain()
        val p = page.coerceAtLeast(1)
        return when (catalog.id) {
            "latest" -> PmApi.latest(p).mapNotNull { it.toMedia() }
            "movies" -> PmApi.browse("popular", p, "Movie").mapNotNull { it.toMedia() }
            "ova" -> PmApi.browse("popular", p, "OVA").mapNotNull { it.toMedia() }
            "popular" -> PmApi.browse("popular", p, null).mapNotNull { it.toMedia() }
            "top" -> PmApi.browse("score", p, null).mapNotNull { it.toMedia() }
            else -> PmApi.browse("trending", p, null).mapNotNull { it.toMedia() }
        }
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.length < 2 || page > 1) return emptyList()
        PmApi.refreshDomain()
        return PmApi.search(q).mapNotNull { it.toMedia() }
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        PmApi.refreshDomain()
        val sid = media.id.substringAfterLast("/").substringAfterLast("|").substringBefore("?").toIntOrNull() ?: return media
        val s = PmApi.series(sid) ?: return media
        val type = when (s.type.lowercase()) {
            "movie" -> if (s.hasDub) HikariMediaType.SERIES else HikariMediaType.MOVIE
            else -> HikariMediaType.SERIES
        }
        return media.copy(
            id = "anipm:$sid",
            title = s.title,
            type = type,
            posterUrl = s.poster ?: media.posterUrl,
            backdropUrl = s.banner ?: media.backdropUrl,
            overview = s.plot ?: media.overview,
            genres = s.genres.takeIf { it.isNotEmpty() } ?: media.genres,
            year = s.year ?: media.year,
        )
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        PmApi.refreshDomain()
        val sid = media.id.substringAfterLast("/").substringAfterLast("|").substringAfterLast(":").substringBefore("?").toIntOrNull() ?: return null
        val s = PmApi.series(sid) ?: return null
        val fillerNums = PmApi.expandRanges(PmApi.filler(s.anilistId, s.title)?.filler)
        val mixedNums = PmApi.expandRanges(PmApi.filler(s.anilistId, s.title)?.mixed)
        val pkgs = PmApi.packages(s.anilistId)
        val out = ArrayList<HikariEpisode>()
        for (ep in s.episodes) {
            if (ep.number < 1) continue
            val mark = when (ep.number) {
                in fillerNums -> " (Filler)"
                in mixedNums -> " (Mixed)" else -> ""
            }
            val name = ep.title.takeIf { it.isNotBlank() && !PmApi.GENERIC_TITLE.matches(it) }?.let { "$it$mark" } ?: "Episode ${ep.number}$mark"
            if (ep.sub) {
                val hard = pkgs?.get(ep.number.toString())?.subhard == true
                out.add(HikariEpisode(number = ep.number, id = "$sid|${ep.number}|sub|${if (hard) "hs" else ""}|${s.anilistId}|${s.malId}", name = name, image = ep.thumb))
            }
            if (ep.dub) {
                val hard = pkgs?.get(ep.number.toString())?.dubhard == true
                out.add(HikariEpisode(number = ep.number, id = "$sid|${ep.number}|dub|${if (hard) "hs" else ""}|${s.anilistId}|${s.malId}", name = "$name (Dub)", image = ep.thumb))
            }
        }
        return out.ifEmpty { null }
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        PmApi.refreshDomain()
        val ref = episode?.id ?: return emptyList()
        val parts = ref.split("|")
        if (parts.size < 3) return emptyList()
        val seriesId = parts[0].substringAfterLast(":").toIntOrNull() ?: return emptyList()
        val ep = parts[1].toIntOrNull() ?: return emptyList()
        val channel = if (parts.getOrNull(2) == "dub") "dub" else "sub"
        val hardAvailable = parts.getOrNull(3) == "hs"
        val anilistId = parts.getOrNull(4).orEmpty()
        val malId = parts.getOrNull(5).orEmpty()
        val boot = PmApi.bootstrap(seriesId, ep, channel)
        val selection = boot?.settlarSelection?.takeIf { it.isNotBlank() }
        val out = java.util.Collections.synchronizedList(ArrayList<HikariStream>())
        val seenLinks = java.util.Collections.synchronizedSet(HashSet<String>())
        val seenSubs = java.util.Collections.synchronizedSet(HashSet<String>())
        coroutineScope {
            val tasks = ArrayList<suspend () -> Unit>()
            tasks.add { emitSettlar(selection, ep, channel, "AniPM Settlar", out, seenLinks, seenSubs) }
            if (hardAvailable) {
                tasks.add { emitSettlar(selection, ep, "${channel}hard", "AniPM Settlar Hardsub", out, seenLinks, seenSubs) }
            }
            tasks.add { emitBackup(boot?.backupEmbed, anilistId, malId, ep, channel, out, seenLinks, seenSubs) }
            tasks.map { async { it() } }.awaitAll()
        }
        return out.distinctBy { it.url }
    }

    private suspend fun emitSettlar(
        selection: String?, episode: Int, channel: String, label: String,
        out: MutableList<HikariStream>, seenLinks: MutableSet<String>, seenSubs: MutableSet<String>,
    ) {
        if (selection == null) return
        val embedUrl = PmApi.settlarSession(selection, episode, channel) ?: return
        val stream = PmApi.settlarResolve(embedUrl) ?: return
        val master = stream.source.takeIf { it.startsWith("http") } ?: return
        val playHeaders = mapOf("User-Agent" to PmApi.UA, "Referer" to "https://embed.settlar.io/")
        val subs = stream.subtitles.filter { it.second.startsWith("http") && seenSubs.add(it.second) }
            .map { HikariSubtitle(it.first.ifBlank { "Subtitle" }, it.second) }
        if (seenLinks.add(master)) {
            out.add(HikariStream(name = "$label (${channel.uppercase()})", url = master, headers = playHeaders, subtitles = subs, isM3u8 = true))
        }
    }

    private suspend fun emitBackup(
        backup: PmBackupEmbed?, anilistId: String, malId: String, episode: Int, channel: String,
        out: MutableList<HikariStream>, seenLinks: MutableSet<String>, seenSubs: MutableSet<String>,
    ) {
        val candidates = ArrayList<String>()
        if (backup?.available == true) backup.url?.takeIf { it.startsWith("http") }?.let { candidates.add(it) }
        if (anilistId.isNotBlank()) candidates.add("https://megaplay.buzz/stream/ani/$anilistId/$episode/$channel")
        if (malId.isNotBlank()) candidates.add("https://megaplay.buzz/stream/mal/$malId/$episode/$channel")
        val playHeaders = mapOf("User-Agent" to PmApi.UA, "Referer" to "https://megaplay.buzz/")
        for (embedUrl in candidates) {
            val stream = PmMega.resolveStream(embedUrl, "${PmApi.host()}/") ?: continue
            val subs = stream.subtitles.filter { it.second.startsWith("http") && seenSubs.add(it.second) }
                .map { HikariSubtitle(it.first.ifBlank { "English" }, it.second) }
            val variants = PmMega.variants(stream.m3u8, playHeaders)
            if (variants.isEmpty()) {
                val signed = PmMega.signUrl(stream.m3u8)
                if (seenLinks.add(signed)) {
                    out.add(HikariStream(name = "AniPM MegaPlay", url = signed, headers = playHeaders, subtitles = subs, isM3u8 = true))
                }
            } else {
                for (v in variants) {
                    val signed = PmMega.signUrl(v.url)
                    if (!seenLinks.add(signed)) continue
                    val suffix = v.quality?.let { " $it" } ?: ""
                    out.add(HikariStream(name = "AniPM MegaPlay$suffix", url = signed, headers = playHeaders, subtitles = subs, isM3u8 = true))
                }
            }
            return
        }
    }

    private fun PmTitle.toMedia(): HikariMedia? {
        if (id <= 0 || title.isBlank()) return null
        val type = if (format.equals("movie", true)) HikariMediaType.MOVIE else HikariMediaType.SERIES
        return HikariMedia(id = "anipm:$id", title = title, type = type, posterUrl = poster, year = year)
    }

    private fun PmLatest.toMedia(): HikariMedia? {
        if (id <= 0 || title.isBlank()) return null
        return HikariMedia(id = "anipm:$id", title = title, type = HikariMediaType.SERIES, posterUrl = poster)
    }
}

// ---------------------------------------------------------------------------

private data class PmTitle(val id: Int, val title: String, val poster: String?, val year: Int?, val format: String)
private data class PmLatest(val id: Int, val title: String, val poster: String?)
private data class PmEp(val number: Int, val title: String, val thumb: String?, val desc: String?, val sub: Boolean, val dub: Boolean)
private data class PmSeries(
    val title: String, val poster: String?, val banner: String?, val year: Int?,
    val plot: String?, val genres: List<String>, val type: String,
    val anilistId: String, val malId: String, val hasDub: Boolean, val episodes: List<PmEp>,
)
private data class PmPkg(val subhard: Boolean, val dubhard: Boolean)
private data class PmRanges(val filler: List<List<Int>>?, val mixed: List<List<Int>>?)
private data class PmBackupEmbed(val available: Boolean, val url: String?)
private data class PmBoot(val settlarSelection: String?, val backupEmbed: PmBackupEmbed?)
private data class PmSettlarStream(val source: String, val subtitles: List<Pair<String, String>>)

private object PmApi {
    const val DEFAULT_HOST = "https://ani.pm"
    const val SETTLAR_EMBED = "https://embed.settlar.io"
    const val UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    private const val DOMAIN_URL = "https://cloudstreampluginhelper-default-rtdb.firebaseio.com/.json"
    val GENERIC_TITLE = Regex("""^Episode \d+(\.\d+)?$""")

    @Volatile private var site = DEFAULT_HOST
    @Volatile private var lastDomainFetch = 0L

    fun host(): String = site

    suspend fun refreshDomain() {
        val now = System.currentTimeMillis()
        if (now - lastDomainFetch < 5 * 60 * 1000L) return
        lastDomainFetch = now
        try {
            val txt = HikariNet.getString(DOMAIN_URL) ?: return
            val v = JSONObject(txt).optString("anipm").ifBlank { JSONObject(txt).optString("anipm_url") }.trim().removeSuffix("/")
            if (v.startsWith("http")) site = v
        } catch (t: Throwable) { }
    }

    private fun headers(referer: String? = null): Map<String, String> = mapOf(
        "User-Agent" to UA, "Accept" to "application/json", "Referer" to (referer ?: "$site/"),
    )

    fun abs(url: String?): String? {
        if (url.isNullOrBlank()) return null
        return if (url.startsWith("http")) url else "$site$url"
    }

    private fun parseTitle(o: JSONObject): PmTitle? {
        val id = o.optInt("id", 0).takeIf { it > 0 } ?: return null
        val title = o.optString("title").takeIf { it.isNotBlank() } ?: return null
        return PmTitle(id, title, abs(o.optString("poster")), o.optInt("year", 0).takeIf { it > 0 }, o.optString("type"))
    }

    suspend fun browse(sort: String, page: Int, format: String?): List<PmTitle> {
        val sb = StringBuilder("$site/api/anime/browse?sort=$sort&page=$page&limit=30")
        if (format != null) sb.append("&format=$format")
        val body = HikariNet.getStringSmart(sb.toString(), headers()) ?: return emptyList()
        return try {
            val arr = JSONObject(body).optJSONArray("items") ?: return emptyList()
            buildList { for (i in 0 until arr.length()) parseTitle(arr.optJSONObject(i) ?: continue)?.let { add(it) } }
        } catch (t: Throwable) { emptyList() }
    }

    suspend fun latest(page: Int): List<PmLatest> {
        val body = HikariNet.getStringSmart("$site/api/anime/latest-episodes?page=$page", headers()) ?: return emptyList()
        return try {
            val arr = JSONObject(body).optJSONArray("items") ?: return emptyList()
            buildList {
                for (i in 0 until arr.length()) {
                    val e = arr.optJSONObject(i) ?: continue
                    val id = e.optInt("id", 0).takeIf { it > 0 } ?: continue
                    val title = e.optString("title").takeIf { it.isNotBlank() } ?: continue
                    add(PmLatest(id, title, abs(e.optString("poster"))))
                }
            }
        } catch (t: Throwable) { emptyList() }
    }

    suspend fun search(q: String): List<PmTitle> {
        val body = HikariNet.getStringSmart("$site/api/anime/search?q=${URLEncoder.encode(q, "UTF-8")}", headers()) ?: return emptyList()
        return try {
            val arr = JSONObject(body).optJSONArray("items") ?: return emptyList()
            buildList { for (i in 0 until arr.length()) parseTitle(arr.optJSONObject(i) ?: continue)?.let { add(it) } }
        } catch (t: Throwable) { emptyList() }
    }

    suspend fun series(id: Int): PmSeries? {
        val body = HikariNet.getStringSmart("$site/api/anime/series/$id?routes=e3", headers()) ?: return null
        return try {
            val o = JSONObject(body)
            val s = if (o.has("series")) o.getJSONObject("series") else o
            val title = s.optString("title").takeIf { it.isNotBlank() } ?: return null
            val eps = ArrayList<PmEp>()
            s.optJSONArray("episodes")?.let { arr ->
                for (i in 0 until arr.length()) {
                    val e = arr.optJSONObject(i) ?: continue
                    val num = e.optInt("number", e.optInt("episode", -1))
                    if (num < 1) continue
                    eps.add(PmEp(num, e.optString("title"), abs(e.optString("thumbnail")), e.optString("description").takeIf { it.isNotBlank() }, e.optBoolean("sub", false), e.optBoolean("dub", false)))
                }
            }
            PmSeries(
                title = title,
                poster = abs(s.optString("poster")),
                banner = abs(s.optString("banner")),
                year = s.optInt("year", 0).takeIf { it > 0 },
                plot = s.optString("synopsis").takeIf { it.isNotBlank() }?.replace(Regex("<[^>]*>"), ""),
                genres = s.optJSONArray("genres")?.let { g -> List(g.length()) { g.optString(it) } } ?: emptyList(),
                type = s.optString("type"),
                anilistId = s.optString("anilistId"),
                malId = s.optString("malId"),
                hasDub = eps.any { it.dub },
                episodes = eps,
            )
        } catch (t: Throwable) { null }
    }

    suspend fun packages(anilistId: String): Map<String, PmPkg>? {
        if (anilistId.isBlank()) return null
        val body = HikariNet.getStringSmart("$site/api/anime/anipm-server/_packages?anilistId=$anilistId", headers()) ?: return null
        return try {
            val eps = JSONObject(body).optJSONObject("episodes") ?: return null
            buildMap {
                val keys = eps.keys()
                while (keys.hasNext()) {
                    val k = keys.next()
                    val e = eps.optJSONObject(k) ?: continue
                    put(k, PmPkg(e.optBoolean("subhard", false), e.optBoolean("dubhard", false)))
                }
            }
        } catch (t: Throwable) { null }
    }

    suspend fun filler(anilistId: String, title: String): PmRanges? {
        if (anilistId.isBlank()) return null
        val body = HikariNet.getStringSmart("$site/api/anime/filler?anilistId=$anilistId&title=${URLEncoder.encode(title, "UTF-8")}", headers()) ?: return null
        return try {
            val r = JSONObject(body).optJSONObject("ranges") ?: return null
            fun ranges(a: JSONArray?): List<List<Int>>? {
                a ?: return null
                return List(a.length()) { i ->
                    val r2 = a.optJSONArray(i) ?: return@List emptyList()
                    List(r2.length()) { j -> r2.optInt(j) }
                }
            }
            PmRanges(ranges(r.optJSONArray("filler")), ranges(r.optJSONArray("mixed")))
        } catch (t: Throwable) { null }
    }

    fun expandRanges(ranges: List<List<Int>>?): Set<Int> {
        if (ranges.isNullOrEmpty()) return emptySet()
        val out = mutableSetOf<Int>()
        for (r in ranges) {
            if (r.isEmpty()) continue
            val start = r[0]
            val end = r.getOrElse(1) { start }
            if (start > 0 && end >= start) out.addAll(start..end)
        }
        return out
    }

    suspend fun bootstrap(id: Int, episode: Int, lang: String): PmBoot? {
        val body = HikariNet.getStringSmart("$site/api/anime/playback-bootstrap/settlar/$id?ep=$episode&lang=$lang&backup=1", headers()) ?: return null
        return try {
            val o = JSONObject(body)
            val b = o.optJSONObject("backupEmbed")
            PmBoot(
                settlarSelection = o.optString("settlarSelection").takeIf { it.isNotBlank() },
                backupEmbed = PmBackupEmbed(b?.optBoolean("available", false) == true, b?.optString("url")?.takeIf { it.startsWith("http") }),
            )
        } catch (t: Throwable) { null }
    }

    suspend fun settlarSession(selection: String, episode: Int, channel: String): String? {
        val body = HikariNet.getStringSmart("$site/api/anime/settlar/session?selection=${URLEncoder.encode(selection, "UTF-8")}&provider=anipm&ep=$episode&channel=$channel&telemetry=0", headers()) ?: return null
        return try {
            JSONObject(body).optString("embedUrl").takeIf { it.isNotBlank() }
        } catch (t: Throwable) { null }
    }

    suspend fun settlarResolve(embedUrl: String): PmSettlarStream? {
        val token = Regex("t=([^&]+)").find(embedUrl)?.groupValues?.get(1) ?: return null
        val body = HikariNet.getStringSmart("$SETTLAR_EMBED/api/embed/session?t=$token", headers("$SETTLAR_EMBED/embed/v1")) ?: return null
        return try {
            val o = JSONObject(body)
            val subs = ArrayList<Pair<String, String>>()
            o.optJSONArray("subtitles")?.let { arr ->
                for (i in 0 until arr.length()) {
                    val s = arr.optJSONObject(i) ?: continue
                    val u = s.optString("url")
                    if (u.startsWith("http")) subs.add((s.optString("label").ifBlank { "Subtitle" }) to u)
                }
            }
            val src = o.optString("source").takeIf { it.isNotBlank() } ?: return null
            PmSettlarStream(src, subs)
        } catch (t: Throwable) { null }
    }
}

// MegaPlay backup (ported from MegaPlayBackup: cipher seeds, enc streams,
// HMAC-signed variant URLs). okhttp -> HikariNet.
private object PmMega {
    private const val FALLBACK_KEY_SEED = "i?LMTAx0Q6,:}50U"
    private const val FALLBACK_IV_SEED = "W0;27ToaUpl_P%'c"
    private const val TOKEN_KEY = "MpCdnT0k3n!9f2K#xQ7vL5mR8wN1pY4s"
    private const val TOKEN_LIFETIME = 7L * 24L * 60L * 60L
    private val keyPairRe = Regex("""[A-Za-z]\w*="([^"]{16})",[A-Za-z]\w*="([^"]{16})"""")
    private val fileRe = Regex(""""file"\s*:\s*"([^"]+)"""")
    private val hexIdsRe = Regex("""/([a-f0-9]{32})/([a-f0-9]{32})/""", RegexOption.IGNORE_CASE)
    private val resRe = Regex("""RESOLUTION=(\d+)x(\d+)""")
    data class Variant(val url: String, val quality: Int?)
    class Stream(val m3u8: String, val subtitles: List<Pair<String, String>>)

    @Volatile private var cachedSeeds: Pair<String, String>? = null

    private suspend fun seeds(base: String): List<Pair<String, String>> {
        cachedSeeds?.let { return listOf(it, FALLBACK_KEY_SEED to FALLBACK_IV_SEED) }
        val dynamic = try {
            val js = HikariNet.getString("$base/lib/newclient.min.js", mapOf("User-Agent" to PmApi.UA)) ?: return listOf(FALLBACK_KEY_SEED to FALLBACK_IV_SEED)
            keyPairRe.find(js)?.groupValues?.let { g -> (g[1] to g[2]).also { cachedSeeds = it } }
        } catch (t: Throwable) { null }
        return listOfNotNull(dynamic, FALLBACK_KEY_SEED to FALLBACK_IV_SEED)
    }

    private fun decryptToken(enc: String, keySeed: String, ivSeed: String): String? {
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

    suspend fun resolveEnc(enc: String, base: String): String? {
        for ((k, iv) in seeds(base)) {
            val plain = decryptToken(enc, k, iv) ?: continue
            fileRe.find(plain)?.groupValues?.get(1)?.let { return it }
        }
        return null
    }

    suspend fun resolveStream(embedUrl: String, referer: String?): Stream? {
        val host = Regex("""https?://([^/]+)""").find(embedUrl)?.groupValues?.get(1) ?: return null
        val pageHeaders = mapOf("User-Agent" to PmApi.UA, "Referer" to (referer ?: "https://$host/"))
        val html = HikariNet.getStringSmart(embedUrl, pageHeaders) ?: return null
        val streamId = Regex("""data-id=["'](\d+)["']""").find(html)?.groupValues?.get(1)
            ?: Regex("""data-realid=["'](\d+)["']""").find(html)?.groupValues?.get(1)
            ?: Regex("""/stream/s-\d+/(\d+)/""").find(embedUrl)?.groupValues?.get(1)
            ?: return null
        val audio = Regex("""/(dub|sub)(?:[/?#]|$)""").find(embedUrl)?.groupValues?.get(1)
            ?: Regex("""type\s*:\s*['"](dub|sub)['"]""").find(html)?.groupValues?.get(1)
            ?: "sub"
        val altHost = Regex("""data-domain=["']([^"']+)["']""").find(html)?.groupValues?.get(1)
        val hosts = listOfNotNull(host, altHost?.takeIf { it != host }).distinct()
        for (apiHost in hosts) {
            val base = "https://$apiHost"
            val ajax = mapOf("User-Agent" to PmApi.UA, "Accept" to "*/*", "X-Requested-With" to "XMLHttpRequest", "Origin" to base, "Referer" to embedUrl)
            for (endpoint in listOf("getSourcesNew", "getSources")) {
                val body = HikariNet.getStringSmart("$base/stream/$endpoint?id=$streamId&type=$audio", ajax) ?: continue
                val root = try { JSONObject(body) } catch (t: Throwable) { continue }
                val streamUrl = extractStream(root, base) ?: continue
                val subs = ArrayList<Pair<String, String>>()
                root.optJSONArray("tracks")?.let { arr ->
                    for (i in 0 until arr.length()) {
                        val e = arr.optJSONObject(i) ?: continue
                        val kind = e.optString("kind")
                        if (kind != "captions" && kind != "subtitles") continue
                        val f = e.optString("file")
                        if (f.isBlank()) continue
                        subs.add((e.optString("label").ifBlank { "English" }) to f)
                    }
                }
                return Stream(streamUrl, subs)
            }
        }
        return null
    }

    private suspend fun extractStream(root: JSONObject, base: String): String? {
        val sources = root.opt("sources")
        val plain = when {
            sources == null || sources == JSONObject.NULL -> null
            sources is JSONObject -> sources.optString("file").takeIf { it.isNotBlank() }
            sources is JSONArray && sources.length() > 0 -> sources.optJSONObject(0)?.optString("file")?.takeIf { it.isNotBlank() }
            else -> null
        }
        if (!plain.isNullOrBlank()) return plain
        val enc = root.optString("enc").takeIf { it.isNotBlank() } ?: return null
        return resolveEnc(enc, base)
    }

    private fun b64url(bytes: ByteArray): String =
        Base64.encodeToString(bytes, Base64.URL_SAFE or Base64.NO_WRAP or Base64.NO_PADDING)

    fun signUrl(url: String): String {
        val m = hexIdsRe.find(url) ?: return url
        val expires = System.currentTimeMillis() / 1000L + TOKEN_LIFETIME
        val payload = "$expires|${m.groupValues[1].lowercase()}/${m.groupValues[2].lowercase()}"
        val mac = Mac.getInstance("HmacSHA256")
        mac.init(SecretKeySpec(TOKEN_KEY.toByteArray(Charsets.UTF_8), "HmacSHA256"))
        val sig = mac.doFinal(payload.toByteArray(Charsets.UTF_8))
        val token = "${b64url(payload.toByteArray(Charsets.UTF_8))}.${b64url(sig)}"
        return "$url${if (url.contains('?')) "&" else "?"}token=$token"
    }

    suspend fun variants(m3u8: String, headers: Map<String, String>): List<Variant> {
        val text = HikariNet.getString(signUrl(m3u8), headers) ?: return emptyList()
        val base = m3u8.substringBefore('?').substringBeforeLast('/') + "/"
        val out = ArrayList<Variant>()
        val lines = text.lines()
        var i = 0
        while (i < lines.size) {
            val line = lines[i].trim()
            if (line.startsWith("#EXT-X-STREAM-INF:")) {
                val q = resRe.find(line)?.groupValues?.get(2)?.toIntOrNull()
                var j = i + 1
                while (j < lines.size && (lines[j].isBlank() || lines[j].startsWith("#"))) j++
                if (j < lines.size) {
                    val uri = lines[j].trim()
                    if (uri.isNotEmpty()) out.add(Variant(if (uri.startsWith("http")) uri else base + uri, q))
                    i = j
                }
            }
            i++
        }
        return out
    }
}
