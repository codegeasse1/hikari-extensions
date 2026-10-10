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
import java.text.SimpleDateFormat
import java.util.Locale
import java.util.TimeZone
import javax.crypto.Mac
import javax.crypto.Cipher
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.SecretKeySpec

/**
 * AniChan (anichan.to) — anime via JSON API + signed watch servers.
 *
 * Faithful native port of KSHITIJ8473/raghav AniChan (CloudStream MainAPI):
 *  - Firebase-hosted domain table (refreshDomain) with https://anichan.to fallback.
 *  - Catalogs:  GET {host}/api/catalog/trending?page=, /api/catalog/airing?page=
 *    -> {results:[{id,title,titleRomaji,poster,banner,format,status,episodes,
 *       score,genres,startDate:{year},description,selfhost:{cached_eps,
 *       cached_dub,count,ep_meta:{title,image,airDate,overview}}}]}
 *    Plus AniList GraphQL rows (popular, top rated, movies, upcoming) — items
 *    are keyed by AniList id so episodes/streams resolve identically.
 *  - Search: GET {host}/api/suggest?q= -> same envelope.
 *  - Detail: GET {host}/api/catalog/anime/{id} (single object).
 *  - Episodes: GET {host}/api/watch/episodes?anilistId={id}
 *    -> {episodes,dubAvailable,sources}; unaired eps filtered via ep_meta airDate.
 *  - Servers: signed flow from AniChanWebView — POST /api/watch/session to mint
 *    a session, GET /api/watch/servers?anilistId=&ep=&category=&tier= with the
 *    X-Wk HMAC header, AES-GCM decrypt the {v:1,i,d} envelope. Keys default to
 *    the shipped triple and refresh by scanning the site's JS bundles.
 *  - Embeds: Vidhawk race/play natively (vidhawk.buzz), Kiwi/voe/generic via
 *    WebView m3u8 capture (the Hikari equivalent of loadExtractor).
 */
class AniChanProvider : HikariProvider {

    override val id = "anichan"
    override val name = "AniChan"
    override val mainUrl = "https://anichan.to"
    override val description = "Anime from anichan.to — trending, airing, popular, top rated, movies and upcoming, sub & dub."
    override val version = 2
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("trending", "Trending Now", HikariMediaType.SERIES),
        HikariCatalog("airing", "Airing This Season", HikariMediaType.SERIES),
        HikariCatalog("popular", "Most Popular", HikariMediaType.SERIES),
        HikariCatalog("top-rated", "Top Rated", HikariMediaType.SERIES),
        HikariCatalog("movies", "Anime Movies", HikariMediaType.MOVIE),
        HikariCatalog("upcoming", "Upcoming", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        AcApi.refreshDomain()
        val p = page.coerceAtLeast(1)
        return when (catalog.id) {
            "trending" -> AcApi.trending(p).mapNotNull { it.toMedia() }
            "airing" -> AcApi.airing(p).mapNotNull { it.toMedia() }
            "popular" -> AcApi.anilistRow(AcApi.AL_POPULAR, p).mapNotNull { it.toMedia() }
            "top-rated" -> AcApi.anilistRow(AcApi.AL_TOP, p).mapNotNull { it.toMedia() }
            "movies" -> AcApi.anilistRow(AcApi.AL_MOVIES, p).mapNotNull { it.toMedia() }
            "upcoming" -> AcApi.anilistRow(AcApi.AL_UPCOMING, p).mapNotNull { it.toMedia() }
            else -> emptyList()
        }
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank() || page > 1) return emptyList()
        AcApi.refreshDomain()
        return AcApi.suggest(q).mapNotNull { it.toMedia() }
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        AcApi.refreshDomain()
        val aid = AcApi.seriesId(media.id) ?: return media
        val d = AcApi.animeDetail(aid) ?: return media
        val title = d.title ?: d.titleRomaji ?: media.title
        val type = if (d.format.equals("MOVIE", true)) HikariMediaType.MOVIE else HikariMediaType.SERIES
        return media.copy(
            id = "anichan:$aid",
            title = title,
            type = type,
            posterUrl = d.poster ?: media.posterUrl,
            backdropUrl = d.banner ?: media.backdropUrl,
            overview = d.description?.let { AcApi.stripHtml(it) } ?: media.overview,
            genres = d.genres?.takeIf { it.isNotEmpty() } ?: media.genres,
            year = d.year ?: media.year,
        )
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        AcApi.refreshDomain()
        val aid = AcApi.seriesId(media.id) ?: return null
        val anime = AcApi.animeDetail(aid)
        val info = AcApi.watchInfo(aid)
        val numbers = AcApi.collectEpisodeNumbers(anime, info)
        if (numbers.isEmpty()) return null
        val dubAvailable = info?.dubAvailable == true ||
            anime?.selfhostDub?.isNotEmpty() == true
        val single = numbers.size == 1
        val out = ArrayList<HikariEpisode>(numbers.size * 2)
        for (n in numbers) {
            val meta = anime?.epMeta?.get(n.toString())
            out.add(HikariEpisode(number = n, id = "$aid|sub|$n", name = meta?.title?.takeIf { it.isNotBlank() } ?: if (single) "Sub" else "Episode $n"))
        }
        if (dubAvailable) {
            for (n in numbers) {
                val meta = anime?.epMeta?.get(n.toString())
                out.add(HikariEpisode(number = n, id = "$aid|dub|$n", name = if (single) "Dub" else (meta?.title?.takeIf { it.isNotBlank() } ?: "Episode $n")))
            }
        }
        return out
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        AcApi.refreshDomain()
        val ref = episode?.id ?: return emptyList()
        val parts = ref.split("|")
        if (parts.size != 3) return emptyList()
        val aid = parts[0].toIntOrNull() ?: return emptyList()
        val category = if (parts[1] == "dub") "dub" else "sub"
        val ep = parts[2].toIntOrNull() ?: return emptyList()
        val out = ArrayList<HikariStream>()
        val seenSubs = HashSet<String>()
        val servers = AcApi.fetchServers(aid, ep, category)
        if (servers.isEmpty()) {
            // Last resort: WebView capture straight off the watch page.
            return webViewFallback(aid, ep, category)
        }
        val embeds = ArrayList<AcServer>()
        for (sv in servers) {
            val embed = sv.embed
            if (sv.type.equals("embed", true) && !embed.isNullOrBlank()) {
                embeds.add(sv)
                continue
            }
            var stream = sv.stream?.takeIf { it.startsWith("http") }
                ?: sv.stream?.takeIf { it.startsWith("/") }?.let { "${AcApi.host()}$it" }
                ?: continue
            out.add(
                HikariStream(
                    name = "AniChan ${AcApi.serverLabel(sv.name, sv.label)}",
                    url = stream,
                    headers = AcApi.playHeaders(),
                    isM3u8 = stream.contains(".m3u8") || stream.contains("master.txt"),
                )
            )
            for (sub in sv.subtitles) {
                if (sub.url.startsWith("http") && seenSubs.add(sub.lang)) {
                    // collect; attached below per-stream below via copy
                }
            }
        }
        // Attach collected subtitles to direct streams.
        val subs = servers.flatMap { it.subtitles }
            .filter { it.url.startsWith("http") }
            .distinctBy { it.url }
            .map { HikariSubtitle(it.lang.ifBlank { "English" }, it.url) }
        val withSubs = if (subs.isNotEmpty()) out.map { it.copy(subtitles = subs) } else out
        out.clear()
        out.addAll(withSubs)
        if (embeds.isNotEmpty()) {
            coroutineScope {
                embeds.map { sv ->
                    async { resolveEmbed(sv, aid, ep, category) }
                }.awaitAll().forEach { out.addAll(it) }
            }
        }
        if (out.isEmpty()) return webViewFallback(aid, ep, category)
        // Direct streams first, embeds after.
        return out.sortedByDescending { !it.url.contains("vidhawk") && !it.name.contains("embed", true) }
    }

    private suspend fun resolveEmbed(sv: AcServer, aid: Int, ep: Int, category: String): List<HikariStream> {
        val embed = sv.embed ?: return emptyList()
        val label = "AniChan ${AcApi.serverLabel(sv.name, sv.label)}"
        if (embed.contains("vidhawk")) {
            val vidServer = Regex("[?&]server=([^&]+)").find(embed)?.groupValues?.get(1) ?: "kari"
            val embedAudio = Regex("/embed/ani/\\d+/\\d+/([a-z]+)/").find(embed)?.groupValues?.get(1) ?: category
            return AcApi.vidhawkResolve(aid, ep, embedAudio, vidServer)
        }
        // Kiwi / voe / generic embeds: capture the file via WebView.
        val captured = HikariNet.resolveWithWebView(embed, AcApi.MEDIA_CAPTURE, timeoutMs = 45_000)
        return captured.map {
            HikariStream(
                name = label,
                url = it.url,
                headers = it.headers.ifEmpty { AcApi.playHeaders() },
                isM3u8 = it.url.contains(".m3u8") || it.url.contains("master.txt"),
            )
        }
    }

    private suspend fun webViewFallback(aid: Int, ep: Int, category: String): List<HikariStream> {
        val host = AcApi.host()
        for (watchUrl in listOf("$host/watch/$aid/$ep", "$host/anime/$aid")) {
            val html = HikariNet.getStringSmart(watchUrl, AcApi.pageHeaders()) ?: continue
            val direct = AcApi.MEDIA_CAPTURE.findAll(html).map { it.value }.distinct().toList()
            if (direct.isNotEmpty()) {
                return direct.map {
                    HikariStream(
                        name = "AniChan ${if (category == "dub") "Dub" else "Sub"}",
                        url = it,
                        headers = AcApi.playHeaders(),
                        isM3u8 = it.contains(".m3u8") || it.contains("master.txt"),
                    )
                }
            }
        }
        return emptyList()
    }

    private fun AcItem.toMedia(): HikariMedia? {
        val aid = this.id ?: return null
        val title = this.title ?: this.titleRomaji ?: return null
        val type = if (this.format.equals("MOVIE", true)) HikariMediaType.MOVIE else HikariMediaType.SERIES
        return HikariMedia(
            id = "anichan:$aid",
            title = title,
            type = type,
            posterUrl = poster,
            backdropUrl = banner,
            overview = description?.let { AcApi.stripHtml(it) },
            genres = genres ?: emptyList(),
            year = year,
        )
    }
}

// ---------------------------------------------------------------------------
// API + crypto (ported from AniChanApi / AniChanWebView / resolvers)
// ---------------------------------------------------------------------------

private data class AcItem(
    val id: Int?,
    val title: String?,
    val titleRomaji: String?,
    val poster: String?,
    val banner: String?,
    val format: String?,
    val status: String?,
    val episodes: Int?,
    val score: Int?,
    val genres: List<String>?,
    val year: Int?,
    val description: String?,
    val duration: Int?,
    val selfhostEps: List<Int>?,
    val selfhostDub: List<Int>?,
    val selfhostCount: Int?,
    val epMeta: Map<String, AcEpMeta>?,
)

private data class AcEpMeta(val title: String?, val image: String?, val airDate: String?, val overview: String?)
private data class AcWatchInfo(val episodes: Int?, val dubAvailable: Boolean?)
private data class AcSub(val lang: String, val url: String)
private data class AcServer(
    val name: String?,
    val label: String?,
    val type: String?,
    val stream: String?,
    val embed: String?,
    val subType: String?,
    val subtitles: List<AcSub>,
)

private object AcApi {
    const val DEFAULT_HOST = "https://anichan.to"
    const val UA = "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36"
    private const val DOMAIN_URL = "https://cloudstreampluginhelper-default-rtdb.firebaseio.com/.json"
    private const val VIDHAWK = "https://vidhawk.buzz"
    private const val ANILIST = "https://graphql.anilist.co"

    // Shipped key triple (site rotates; refreshed from JS bundles on failure).
    @Volatile private var wKey = "6be19f72"
    @Volatile private var k1 = "wuEoOR48/o2oD14z71nB9MG2eC3T5q+uayZj5fb3Xsk="
    @Volatile private var k2 = "0Avpb+a3t0ihtp6DLmBkfB3wStHog0zOAhKALhnfeC4="

    @Volatile private var host = DEFAULT_HOST
    @Volatile private var lastDomainFetch = 0L

    @Volatile private var sessionN: String? = null
    @Volatile private var sessionExp = 0L

    val MEDIA_CAPTURE = Regex("""https?://[^"' \s]+\.m3u8[^"' \s]*|https?://[^"' \s]*master\.txt[^"' \s]*|https?://[^"' \s]+\.mp4[^"' \s]*""")

    fun host(): String = host
    fun playHeaders(): Map<String, String> = mapOf("User-Agent" to UA, "Referer" to "$host/")
    fun pageHeaders(): Map<String, String> = mapOf(
        "User-Agent" to UA,
        "Accept" to "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer" to "$host/",
    )
    private fun apiHeaders(): Map<String, String> = mapOf("User-Agent" to UA, "Accept" to "application/json")

    suspend fun refreshDomain() {
        val now = System.currentTimeMillis()
        if (now - lastDomainFetch < 5 * 60 * 1000L) return
        lastDomainFetch = now
        try {
            val txt = HikariNet.getString(DOMAIN_URL) ?: return
            val o = JSONObject(txt)
            val v = o.optString("anichan").ifBlank { o.optString("anichan_url") }.trim().removeSuffix("/")
            if (v.startsWith("http")) host = v
        } catch (t: Throwable) { }
    }

    fun seriesId(mediaId: String): Int? {
        val tail = mediaId.substringAfterLast("/").substringAfterLast(":")
        return tail.substringBefore("|").substringBefore("?").toIntOrNull()
    }

    // -- catalog ------------------------------------------------------------
    suspend fun trending(page: Int): List<AcItem> {
        val body = HikariNet.getStringSmart("$host/api/catalog/trending?page=$page", apiHeaders()) ?: return emptyList()
        return parseEnvelope(body)
    }

    suspend fun airing(page: Int): List<AcItem> {
        val body = HikariNet.getStringSmart("$host/api/catalog/airing?page=$page", apiHeaders()) ?: return emptyList()
        return parseEnvelope(body)
    }

    suspend fun suggest(q: String): List<AcItem> {
        val body = HikariNet.getStringSmart("$host/api/suggest?q=${URLEncoder.encode(q, "UTF-8")}", apiHeaders()) ?: return emptyList()
        return parseEnvelope(body)
    }

    suspend fun animeDetail(aid: Int): AcItem? {
        val body = HikariNet.getStringSmart("$host/api/catalog/anime/$aid", apiHeaders()) ?: return null
        return try {
            if (body.trimStart().startsWith("[")) parseEnvelope(body).firstOrNull()
            else parseItem(JSONObject(body))
        } catch (t: Throwable) { null }
    }

    suspend fun watchInfo(aid: Int): AcWatchInfo? {
        val body = HikariNet.getStringSmart("$host/api/watch/episodes?anilistId=$aid", apiHeaders()) ?: return null
        return try {
            val o = JSONObject(body)
            AcWatchInfo(
                episodes = o.optInt("episodes", 0).takeIf { it > 0 },
                dubAvailable = o.optBoolean("dubAvailable", false) || o.optBoolean("dub", false),
            )
        } catch (t: Throwable) { null }
    }

    private fun parseEnvelope(body: String): List<AcItem> {
        return try {
            val t = body.trimStart()
            if (t.startsWith("[")) {
                val arr = JSONArray(t)
                buildList { for (i in 0 until arr.length()) parseItem(arr.optJSONObject(i))?.let { add(it) } }
            } else {
                val o = JSONObject(t)
                val arr = o.optJSONArray("results") ?: o.optJSONArray("data") ?: o.optJSONArray("items") ?: return emptyList()
                buildList { for (i in 0 until arr.length()) parseItem(arr.optJSONObject(i))?.let { add(it) } }
            }
        } catch (t: Throwable) { emptyList() }
    }

    private fun parseItem(o: JSONObject?): AcItem? {
        o ?: return null
        val id = if (o.has("id")) o.optInt("id", 0).takeIf { it != 0 } else null
        val sd = o.optJSONObject("startDate")
        val self = o.optJSONObject("selfhost")
        val meta = self?.optJSONObject("ep_meta") ?: self?.optJSONObject("epMeta")
        val epMeta = meta?.let { m ->
            buildMap {
                val keys = m.keys()
                while (keys.hasNext()) {
                    val k = keys.next()
                    val e = m.optJSONObject(k) ?: continue
                    put(k, AcEpMeta(
                        title = e.optString("title").takeIf { it.isNotBlank() },
                        image = e.optString("image").takeIf { it.startsWith("http") },
                        airDate = e.optString("airDate").takeIf { it.isNotBlank() },
                        overview = e.optString("overview").takeIf { it.isNotBlank() },
                    ))
                }
            }
        }
        return AcItem(
            id = id,
            title = o.optString("title").takeIf { it.isNotBlank() },
            titleRomaji = o.optString("titleRomaji").takeIf { it.isNotBlank() },
            poster = o.optString("poster").takeIf { it.startsWith("http") },
            banner = o.optString("banner").takeIf { it.startsWith("http") },
            format = o.optString("format").takeIf { it.isNotBlank() },
            status = o.optString("status").takeIf { it.isNotBlank() },
            episodes = o.optInt("episodes", 0).takeIf { it > 0 },
            score = o.optInt("score", 0).takeIf { it > 0 },
            genres = o.optJSONArray("genres")?.let { arr -> buildList { for (i in 0 until arr.length()) arr.optString(i).takeIf { it.isNotBlank() }?.let { add(it) } } },
            year = sd?.optInt("year", 0)?.takeIf { it > 0 },
            description = o.optString("description").takeIf { it.isNotBlank() },
            duration = o.optInt("duration", 0).takeIf { it > 0 },
            selfhostEps = self?.optJSONArray("cached_eps")?.let { arr -> List(arr.length()) { arr.optInt(it) } },
            selfhostDub = self?.optJSONArray("cached_dub")?.let { arr -> List(arr.length()) { arr.optInt(it) } },
            selfhostCount = self?.optInt("count", 0)?.takeIf { it > 0 },
            epMeta = epMeta,
        )
    }

    fun collectEpisodeNumbers(anime: AcItem?, info: AcWatchInfo?): List<Int> {
        val today = SimpleDateFormat("yyyy-MM-dd", Locale.US).apply { timeZone = TimeZone.getTimeZone("UTC") }.format(java.util.Date(0).let { java.util.Date(System.currentTimeMillis()) })
        val eps = sortedSetOf<Int>()
        anime?.selfhostEps?.forEach { if (it > 0) eps.add(it) }
        anime?.epMeta?.forEach { (key, meta) ->
            val num = key.toIntOrNull() ?: return@forEach
            if (num <= 0) return@forEach
            val air = meta.airDate
            if (air != null && air <= today) eps.add(num)
        }
        if (eps.isEmpty()) {
            val count = info?.episodes ?: anime?.episodes ?: anime?.selfhostCount ?: 0
            if (count > 0) eps.addAll(1..minOf(count, 3000))
        }
        return eps.toList()
    }

    fun stripHtml(s: String): String = s.replace(Regex("<br\\s*/?>"), "\n").replace(Regex("<[^>]+>"), "").replace("&amp;", "&").trim()

    fun serverLabel(name: String?, label: String?): String {
        val n = name?.takeIf { it.isNotBlank() } ?: "Server"
        return if (!label.isNullOrBlank() && !label.equals(n, true)) "$n $label" else n
    }

    // -- AniList rows -------------------------------------------------------
    const val AL_POPULAR = "POPULARITY_DESC|"
    const val AL_TOP = "SCORE_DESC|"
    const val AL_MOVIES = "POPULARITY_DESC|MOVIE"
    const val AL_UPCOMING = "POPULARITY_DESC|UPCOMING"

    suspend fun anilistRow(kind: String, page: Int): List<AcItem> {
        val parts = kind.split("|")
        val sort = parts[0]
        val extra = parts.getOrNull(1) ?: ""
        val format = if (extra == "MOVIE") ", format: MOVIE" else ""
        val status = if (extra == "UPCOMING") ", status: NOT_YET_RELEASED" else ""
        val query = """query (${'$'}page: Int, ${'$'}perPage: Int) { Page(page: ${'$'}page, perPage: ${'$'}perPage) { media(type: ANIME, sort: $sort$format$status) { id title { romaji english } coverImage { large extraLarge } format status episodes seasonYear averageScore genres description(asHtml: false) } } }"""
        val body = JSONObject().put("query", query)
            .put("variables", JSONObject().put("page", page).put("perPage", 24)).toString()
        val resp = HikariNet.postString(ANILIST, body, mapOf("Content-Type" to "application/json", "User-Agent" to UA, "Referer" to "https://anilist.co/")) ?: return emptyList()
        return try {
            val arr = JSONObject(resp).optJSONObject("data")?.optJSONObject("Page")?.optJSONArray("media") ?: return emptyList()
            buildList {
                for (i in 0 until arr.length()) {
                    val m = arr.optJSONObject(i) ?: continue
                    val t = m.optJSONObject("title")
                    val c = m.optJSONObject("coverImage")
                    add(AcItem(
                        id = m.optInt("id", 0).takeIf { it != 0 },
                        title = t?.optString("english")?.takeIf { it.isNotBlank() } ?: t?.optString("romaji"),
                        titleRomaji = t?.optString("romaji")?.takeIf { it.isNotBlank() },
                        poster = c?.optString("extraLarge")?.takeIf { it.startsWith("http") } ?: c?.optString("large")?.takeIf { it.startsWith("http") },
                        banner = null,
                        format = m.optString("format").takeIf { it.isNotBlank() },
                        status = m.optString("status").takeIf { it.isNotBlank() },
                        episodes = m.optInt("episodes", 0).takeIf { it > 0 },
                        score = m.optInt("averageScore", 0).takeIf { it > 0 },
                        genres = m.optJSONArray("genres")?.let { g -> List(g.length()) { g.optString(it) } },
                        year = m.optInt("seasonYear", 0).takeIf { it > 0 },
                        description = m.optString("description").takeIf { it.isNotBlank() },
                        duration = null, selfhostEps = null, selfhostDub = null, selfhostCount = null, epMeta = null,
                    ))
                }
            }
        } catch (t: Throwable) { emptyList() }
    }

    // -- signed servers -----------------------------------------------------
    private fun hmacKey(): SecretKeySpec {
        val a = Base64.decode(k1, Base64.DEFAULT)
        val b = Base64.decode(k2, Base64.DEFAULT)
        val x = ByteArray(a.size) { i -> (a[i].toInt() xor b[i % b.size].toInt()).toByte() }
        return SecretKeySpec(x, "HmacSHA256")
    }

    private suspend fun session(): String? {
        val now = System.currentTimeMillis()
        sessionN?.let { if (now < sessionExp - 60_000) return it }
        return try {
            val resp = HikariNet.postString(
                "$host/api/watch/session", """{"token":""}""",
                mapOf("Content-Type" to "application/json", "User-Agent" to UA, "Referer" to "$host/"),
            ) ?: return null
            val o = JSONObject(resp)
            val n = o.optString("n").takeIf { it.isNotBlank() } ?: return null
            val exp = o.optLong("exp", 0)
            sessionN = n
            sessionExp = if (exp > 0) exp * 1000 else now + 3600_000
            n
        } catch (t: Throwable) { null }
    }

    private fun decryptSealed(obj: JSONObject, nonce: String): JSONObject? {
        return try {
            val mac = Mac.getInstance("HmacSHA256")
            mac.init(hmacKey())
            val ak = mac.doFinal(nonce.toByteArray(Charsets.UTF_8))
            val cipher = Cipher.getInstance("AES/GCM/NoPadding")
            cipher.init(Cipher.DECRYPT_MODE, SecretKeySpec(ak, "AES"), GCMParameterSpec(128, Base64.decode(obj.optString("i"), Base64.DEFAULT)))
            JSONObject(String(cipher.doFinal(Base64.decode(obj.optString("d"), Base64.DEFAULT)), Charsets.UTF_8))
        } catch (t: Throwable) { null }
    }

    private suspend fun loadTier(aid: Int, ep: Int, cat: String, tier: String): List<AcServer> {
        val nonce = session() ?: return emptyList()
        val url = "$host/api/watch/servers?anilistId=$aid&ep=$ep&category=$cat&tier=$tier"
        val body = HikariNet.getString(url, apiHeaders() + ("X-Wk" to wKey)) ?: return emptyList()
        return try {
            val o = JSONObject(body)
            val opened = if (o.optInt("v", 0) == 1) decryptSealed(o, nonce) ?: return emptyList() else o
            parseServers(opened.optJSONArray("servers"))
        } catch (t: Throwable) { emptyList() }
    }

    private fun parseServers(arr: JSONArray?): List<AcServer> {
        arr ?: return emptyList()
        return buildList {
            for (i in 0 until arr.length()) {
                val s = arr.optJSONObject(i) ?: continue
                val subs = ArrayList<AcSub>()
                s.optJSONArray("subtitles")?.let { sa ->
                    for (j in 0 until sa.length()) {
                        val su = sa.optJSONObject(j) ?: continue
                        val u = su.optString("url")
                        if (u.startsWith("http")) subs.add(AcSub(su.optString("lang").ifBlank { "English" }, u))
                    }
                }
                if (s.optString("type").equals("embed", true) && s.optString("embed").isBlank() &&
                    s.optString("stream").isBlank()) continue
                add(AcServer(
                    name = s.optString("name").takeIf { it.isNotBlank() },
                    label = s.optString("label").takeIf { it.isNotBlank() },
                    type = s.optString("type").takeIf { it.isNotBlank() },
                    stream = s.optString("stream").takeIf { it.isNotBlank() },
                    embed = s.optString("embed").takeIf { it.isNotBlank() },
                    subType = s.optString("subType").takeIf { it.isNotBlank() },
                    subtitles = subs,
                ))
            }
        }
    }

    suspend fun fetchServers(aid: Int, ep: Int, category: String): List<AcServer> {
        val cats = if (category == "dub") listOf("dub") else listOf("sub", "hsub")
        val out = ArrayList<AcServer>()
        val seen = HashSet<String>()
        var emptyRounds = 0
        for (cat in cats) {
            for (tier in listOf("fast", "rest")) {
                val list = loadTier(aid, ep, cat, tier)
                if (list.isEmpty()) {
                    emptyRounds++
                    continue
                }
                for (sv in list) {
                    val key = if (sv.type.equals("embed", true)) "${sv.name}|${sv.label}|${sv.embed}" else "${sv.name}|${sv.label}|${sv.subType}"
                    if (seen.add(key)) {
                        val fixed = if (cat == "hsub" && sv.subType == null && sv.type == "hls") sv.copy(subType = "hard") else sv
                        out.add(fixed)
                    }
                }
            }
        }
        if (out.isEmpty() && emptyRounds >= 3) {
            // Possibly a key rotation: rescan bundles once, reset session, retry.
            if (refreshKeys()) {
                sessionN = null
                for (cat in cats) {
                    for (tier in listOf("fast", "rest")) {
                        val list = loadTier(aid, ep, cat, tier)
                        for (sv in list) {
                            val key = if (sv.type.equals("embed", true)) "${sv.name}|${sv.label}|${sv.embed}" else "${sv.name}|${sv.label}|${sv.subType}"
                            if (seen.add(key)) out.add(sv)
                        }
                    }
                }
            }
        }
        // Kiwi download mirrors ride along under the sub tab.
        if (category != "dub") {
            try {
                val dj = HikariNet.getString("$host/api/watch/ext-downloads?anilistId=$aid&ep=$ep", apiHeaders()) ?: return out
                val bucket = JSONObject(dj).optJSONObject("kiwi")?.optJSONObject("sub")
                bucket?.let { b ->
                    val keys = b.keys()
                    while (keys.hasNext()) {
                        val q = keys.next()
                        val u = b.optString(q)
                        if (u.startsWith("http")) {
                            out.add(AcServer(name = "kiwi", label = "Kiwi $q", type = "embed", stream = null, embed = u, subType = "soft", subtitles = emptyList()))
                        }
                    }
                }
            } catch (t: Throwable) { }
        }
        return out
    }

    private val tripleRe = Regex("""["']([0-9a-f]{8})["'],[A-Za-z_${'$'}][\w${'$'}]*=["']([A-Za-z0-9+/=]{44})["'],[A-Za-z_${'$'}][\w${'$'}]*=["']([A-Za-z0-9+/=]{44})["']""")
    private val chunkRe = Regex("""src="(/_next/static/chunks/[^"]+\.js)"""")

    suspend fun refreshKeys(): Boolean {
        return try {
            val html = HikariNet.getStringSmart("$host/", pageHeaders()) ?: return false
            val chunks = chunkRe.findAll(html).map { "$host${it.groupValues[1]}" }.distinct().take(12).toList()
            for (u in chunks) {
                val js = HikariNet.getString(u, mapOf("User-Agent" to UA)) ?: continue
                val m = tripleRe.find(js) ?: continue
                val (w, a, b) = m.destructured
                if (w != wKey || a != k1 || b != k2) {
                    wKey = w; k1 = a; k2 = b
                    return true
                }
                return false
            }
            false
        } catch (t: Throwable) { false }
    }

    // -- vidhawk ------------------------------------------------------------
    suspend fun vidhawkResolve(aid: Int, ep: Int, audio: String, server: String): List<HikariStream> {
        val out = ArrayList<HikariStream>()
        try {
            val parentHost = try { java.net.URI(host).host ?: "anichan.to" } catch (t: Throwable) { "anichan.to" }
            val headers = mapOf("User-Agent" to UA, "Referer" to "$host/")
            val raceUrl = "$VIDHAWK/api/stream/race?episode=$ep&audio=$audio&server=$server&anilistId=$aid&parentHost=$parentHost"
            val raceBody = HikariNet.getString(raceUrl, headers) ?: return emptyList()
            val race = JSONObject(raceBody)
            val tickets = ArrayList<Pair<String?, String?>>()
            race.optJSONArray("servers")?.let { arr ->
                for (i in 0 until arr.length()) {
                    val s = arr.optJSONObject(i) ?: continue
                    val t = s.optString("ticket").takeIf { it.isNotBlank() } ?: continue
                    tickets.add((s.optString("label").takeIf { it.isNotBlank() } ?: s.optString("id").takeIf { it.isNotBlank() }) to t)
                }
            }
            if (tickets.isEmpty()) {
                race.optString("ticket").takeIf { it.isNotBlank() }?.let { tickets.add(null to it) }
            }
            val wanted = if (audio.equals("dub", true)) setOf("dub", "hin") else setOf("sub")
            for ((tag, ticket) in tickets) {
                val playBody = HikariNet.getString("$VIDHAWK/api/play?t=${URLEncoder.encode(ticket!!, "UTF-8")}", headers) ?: continue
                val tracks = JSONObject(playBody).optJSONArray("tracks") ?: continue
                for (i in 0 until tracks.length()) {
                    val tr = tracks.optJSONObject(i) ?: continue
                    val id = tr.optString("id").lowercase()
                    if (id !in wanted) continue
                    val src = tr.optString("src").takeIf { it.startsWith("http") } ?: continue
                    val alive = HikariNet.fetch(src, headers + ("Range" to "bytes=0-1023"))?.status in 200..399
                    if (!alive) continue
                    val suffix = if (id == "hin") " Hindi" else ""
                    out.add(HikariStream(
                        name = "AniChan vidHawk ${(tag ?: "").trim()}${suffix}".trim(),
                        url = src,
                        headers = headers,
                        isM3u8 = src.contains(".m3u8"),
                    ))
                }
            }
        } catch (t: Throwable) { }
        return out
    }
}
