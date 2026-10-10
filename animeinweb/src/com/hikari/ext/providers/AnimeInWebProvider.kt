package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.json.JSONArray
import org.json.JSONObject
import java.net.URLEncoder
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Locale
import java.util.TimeZone
import java.util.concurrent.ConcurrentHashMap

/**
 * AnimeInWeb (animeinweb.com, Indonesian) — signed proxy JSON API.
 *
 * Faithful native port of KSHITIJ8473/raghav AnimeInWebProvider:
 *  - All calls go through $mainUrl/api/proxy/3/2{path} with the
 *    x-proxy-secret header (direct proxy access is 403 without it, verified).
 *  - 6 rows: Terbaru (home/data "new" list), Populer (explore sort=views),
 *    Jadwal Hari Ini (schedule/data?day=<Indonesian day>), Hot, Akan Tayang,
 *    Acak. Explore pages are 0-based; schedule/home rows are single-page.
 *  - Posters resolve through kitsu.app (best title match, permanent CDN URLs
 *    cached) with the site URL as fallback.
 *  - Detail + 30-per-page episode batches (parallel, newest-first, reversed).
 *  - Playback: "direct" servers stream natively; blogger/gdplayer legacy
 *    hosts fall back to WebView capture.
 */
class AnimeInWebProvider : HikariProvider {

    override val id = "animeinweb"
    override val name = "AnimeInWeb"
    override val mainUrl = "https://animeinweb.com"
    override val description = "Indonesian anime from AnimeInWeb — terbaru, populer, jadwal, hot."
    override val version = 2
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private companion object {
        const val UA =
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        const val PROXY_SECRET = "animein-secure-proxy-key-123"
        const val IMG_HOST = "https://xyz-api.animein.net"
        const val KITSU = "https://kitsu.app/api/edge"
        const val EXPLORE_PAGE_SIZE = 60
        const val EPISODES_PER_PAGE = 30
        const val PAGE_BATCH = 6
        const val MAX_EPISODE_PAGES = 60
    }

    private val apiHeaders = mapOf(
        "x-proxy-secret" to PROXY_SECRET,
        "Accept" to "application/json",
        "User-Agent" to UA,
    )

    @Volatile
    private var base: String? = null

    private suspend fun domain(): String {
        base?.let { return it }
        val remote = try {
            HikariNet.getString("https://cloudstreampluginhelper-default-rtdb.firebaseio.com/.json")
                ?.let { JSONObject(it).optString("animeinweb") }
                ?.takeIf { it.startsWith("http") }?.trimEnd('/')
        } catch (_: Exception) {
            null
        }
        val d = remote ?: mainUrl
        base = d
        return d
    }

    private fun apiUrl(path: String, d: String): String = "$d/api/proxy/3/2$path"

    private suspend fun fetchJson(url: String): JSONObject {
        var last = ""
        repeat(2) { attempt ->
            try {
                val text = HikariNet.getString(url, apiHeaders) ?: throw IllegalStateException("empty")
                return JSONObject(text)
            } catch (e: Exception) {
                last = e.message.orEmpty()
                kotlinx.coroutines.delay(800L * (attempt + 1))
            }
        }
        throw IllegalStateException("fetch failed: $last")
    }

    private fun moviesOf(root: JSONObject): List<JSONObject> {
        val out = ArrayList<JSONObject>()
        try {
            val arr = root.optJSONObject("data")?.optJSONArray("movie") ?: return out
            for (i in 0 until arr.length()) arr.optJSONObject(i)?.let { out.add(it) }
        } catch (_: Exception) {
        }
        return out
    }

    private fun fixPoster(url: String?): String? {
        val v = url?.takeIf { it.isNotBlank() } ?: return null
        return if (v.startsWith("http")) v else IMG_HOST + v
    }

    private fun normalizeTitle(v: String): String = v.lowercase().filter { it.isLetterOrDigit() }

    private val posterCache = ConcurrentHashMap<String, String>()
    private val posterGate = Semaphore(8)

    private suspend fun posterFor(title: String?, siteUrl: String?): String? {
        val fallback = fixPoster(siteUrl) ?: return null
        val key = title?.trim()?.lowercase()?.takeIf { it.isNotEmpty() } ?: return fallback
        posterCache[key]?.let { return it.takeIf { it.isNotEmpty() } ?: fallback }
        return try {
            var found: String? = null
            posterGate.withPermit { found = kitsuPoster(key) }
            if (found != null && !found.contains("X-Amz-")) posterCache[key] = found
            else if (found == null) posterCache[key] = ""
            found ?: fallback
        } catch (_: Exception) {
            fallback
        }
    }

    private suspend fun kitsuPoster(key: String): String? {
        val q = URLEncoder.encode(key, "UTF-8")
        val text = HikariNet.getString(
            "$KITSU/anime?filter[text]=$q&page%5Blimit%5D=5",
            mapOf("Accept" to "application/vnd.api+json", "User-Agent" to UA),
        ) ?: return null
        val site = normalizeTitle(key)
        var best = 0
        var bestUrl: String? = null
        try {
            val arr = JSONObject(text).optJSONArray("data") ?: return null
            for (i in 0 until arr.length()) {
                val attrs = arr.optJSONObject(i)?.optJSONObject("attributes") ?: continue
                val pi = attrs.optJSONObject("posterImage") ?: continue
                val url = pi.optString("large").ifBlank { pi.optString("medium") }
                    .ifBlank { pi.optString("small") }.ifBlank { continue }
                val titles = attrs.optJSONObject("titles")
                val cands = listOfNotNull(
                    attrs.optString("canonicalTitle").ifBlank { null },
                    titles?.optString("en")?.ifBlank { null },
                    titles?.optString("en_jp")?.ifBlank { null },
                ).distinct()
                for (c in cands) {
                    val s = titleMatchScore(site, normalizeTitle(c))
                    if (s > best) {
                        best = s
                        bestUrl = url
                    }
                }
            }
        } catch (_: Exception) {
        }
        return bestUrl
    }

    private fun titleMatchScore(site: String, candidate: String): Int {
        if (site.isEmpty() || candidate.isEmpty()) return 0
        if (site == candidate) return 3
        if (minOf(site.length, candidate.length) >= 6 &&
            (candidate.startsWith(site) || site.startsWith(candidate))
        ) return 2
        val sep = Regex("[^a-z0-9]+")
        val st = site.split(sep).filter { it.isNotEmpty() }.toSet()
        val ct = candidate.split(sep).filter { it.isNotEmpty() }.toSet()
        val shared = st.intersect(ct).size
        val smallest = minOf(st.size, ct.size)
        return if (shared >= 2 && shared * 10 >= smallest * 6) 1 else 0
    }

    private fun todayDay(): String {
        val cal = Calendar.getInstance(TimeZone.getTimeZone("Asia/Jakarta"))
        val names = arrayOf("MINGGU", "SENIN", "SELASA", "RABU", "KAMIS", "JUMAT", "SABTU")
        return names[cal.get(Calendar.DAY_OF_WEEK) - 1]
    }

    private suspend fun toMedia(o: JSONObject, d: String): HikariMedia? {
        val id = o.optString("id")
        if (id.isBlank()) return null
        val title = o.optString("title").ifBlank { "Unknown" }
        val type = when (o.optString("type").uppercase()) {
            "MOVIE" -> HikariMediaType.MOVIE
            else -> HikariMediaType.SERIES
        }
        val poster = posterFor(o.optString("title").ifBlank { null }, o.optString("image_poster").ifBlank { null })
        return HikariMedia(
            id = "$d/anime/$id", title = title, type = type, posterUrl = poster,
            year = o.optString("year").toIntOrNull(),
            overview = o.optString("synopsis").ifBlank { null },
            genres = o.optString("genre").split(",").map { it.trim() }.filter { it.isNotEmpty() },
        )
    }

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("terbaru", "Terbaru", HikariMediaType.SERIES, rawType = "home:new"),
        HikariCatalog("populer", "Populer", HikariMediaType.SERIES, rawType = "views"),
        HikariCatalog("jadwal", "Jadwal Hari Ini", HikariMediaType.SERIES, rawType = "schedule"),
        HikariCatalog("hot", "Hot", HikariMediaType.SERIES, rawType = "home:hot"),
        HikariCatalog("tayang", "Akan Tayang", HikariMediaType.SERIES, rawType = "home:waiting"),
        HikariCatalog("acak", "Acak", HikariMediaType.SERIES, rawType = "home:random"),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val d = domain()
        return try {
            when {
                catalog.rawType == "views" -> {
                    val root = fetchJson(apiUrl("/explore/movie", d) + "?page=${page.coerceAtLeast(1) - 1}&sort=views&keyword=")
                    val items = moviesOf(root)
                    coroutineScope {
                        items.map { m -> async(Dispatchers.IO) { toMedia(m, d) } }
                            .mapNotNull { runCatching { it.await() }.getOrNull() }
                    }
                }
                catalog.rawType == "schedule" -> {
                    if (page > 1) return emptyList()
                    val root = fetchJson(apiUrl("/schedule/data", d) + "?day=${todayDay()}")
                    coroutineScope {
                        moviesOf(root).map { m -> async(Dispatchers.IO) { toMedia(m, d) } }
                            .mapNotNull { runCatching { it.await() }.getOrNull() }
                    }
                }
                catalog.rawType.startsWith("home:") -> {
                    if (page > 1) return emptyList()
                    val key = catalog.rawType.removePrefix("home:")
                    val root = fetchJson(apiUrl("/home/data", d) + "?day=${todayDay()}&limit=16")
                    val data = root.optJSONObject("data") ?: return emptyList()
                    val arr = data.optJSONArray(key) ?: return emptyList()
                    val items = (0 until arr.length()).mapNotNull { arr.optJSONObject(it) }
                    coroutineScope {
                        items.map { m -> async(Dispatchers.IO) { toMedia(m, d) } }
                            .mapNotNull { runCatching { it.await() }.getOrNull() }
                    }
                }
                else -> emptyList()
            }
        } catch (_: Exception) {
            emptyList()
        }
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank()) return emptyList()
        val d = domain()
        return try {
            val enc = URLEncoder.encode(query.trim(), "UTF-8")
            val root = fetchJson(apiUrl("/explore/movie", d) + "?page=${page.coerceAtLeast(1) - 1}&sort=&keyword=$enc")
            coroutineScope {
                moviesOf(root).map { m -> async(Dispatchers.IO) { toMedia(m, d) } }
                    .mapNotNull { runCatching { it.await() }.getOrNull() }
            }
        } catch (_: Exception) {
            emptyList()
        }
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val d = domain()
        val id = media.id.substringAfterLast("/").takeIf { it.isNotBlank() } ?: return media
        return try {
            val movie = fetchJson(apiUrl("/movie/detail", d) + "/$id")
                .optJSONObject("data")?.optJSONObject("movie") ?: return media
            val title = movie.optString("title").ifBlank { return media }
            val type = if (movie.optString("type").uppercase() == "MOVIE") HikariMediaType.MOVIE
            else HikariMediaType.SERIES
            media.copy(
                title = title, type = type,
                posterUrl = posterFor(title, movie.optString("image_poster").ifBlank { null })
                    ?: media.posterUrl,
                overview = movie.optString("synopsis").ifBlank { null } ?: media.overview,
                year = movie.optString("year").toIntOrNull() ?: media.year,
                genres = movie.optString("genre").split(",").map { it.trim() }
                    .filter { it.isNotEmpty() }.ifEmpty { media.genres },
            )
        } catch (_: Exception) {
            media
        }
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val d = domain()
        val id = media.id.substringAfterLast("/").takeIf { it.isNotBlank() } ?: return null
        return try {
            val first = epPage(d, id, 0)
            val all = ArrayList(first)
            if (first.size >= EPISODES_PER_PAGE) {
                coroutineScope {
                    var start = 1
                    while (start <= MAX_EPISODE_PAGES) {
                        val range = start until start + PAGE_BATCH
                        val pages = range.map { p ->
                            async(Dispatchers.IO) { runCatching { epPage(d, id, p) }.getOrDefault(emptyList()) }
                        }.map { runCatching { it.await() }.getOrDefault(emptyList()) }
                        pages.forEach { all.addAll(it) }
                        if (pages.any { it.size < EPISODES_PER_PAGE }) break
                        start += PAGE_BATCH
                    }
                }
            }
            all.reversed().distinctBy { it.first }.mapIndexed { idx, (eid, eindex, etitle) ->
                val num = eindex.toIntOrNull() ?: (idx + 1)
                HikariEpisode(
                    number = num, id = eid,
                    name = etitle.takeIf { it.isNotBlank() } ?: "Episode $num",
                    image = media.posterUrl,
                )
            }.ifEmpty { null }
        } catch (_: Exception) {
            null
        }
    }

    private suspend fun epPage(d: String, id: String, page: Int): List<Triple<String, String, String>> {
        val out = ArrayList<Triple<String, String, String>>()
        try {
            val root = fetchJson(apiUrl("/movie/episode", d) + "/$id?page=$page")
            val arr = root.optJSONObject("data")?.optJSONArray("episode") ?: return out
            for (i in 0 until arr.length()) {
                val e = arr.optJSONObject(i) ?: continue
                val eid = e.optString("id")
                if (eid.isBlank()) continue
                out.add(Triple(eid, e.optString("index"), e.optString("title")))
            }
        } catch (_: Exception) {
        }
        return out
    }

    private val capture = Regex("""https?://[^"' \s]+\.m3u8[^"' \s]*|https?://[^"' \s]+\.mp4[^"' \s]*""")

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val d = domain()
        val epArg = episode?.id ?: media.id
        val epId = epArg.trimEnd('/').substringAfterLast("/").takeIf { it.isNotBlank() } ?: return emptyList()
        return try {
            val root = fetchJson(apiUrl("/episode/streamnew", d) + "/$epId")
            val arr = root.optJSONObject("data")?.optJSONArray("server") ?: return emptyList()
            val out = ArrayList<HikariStream>()
            for (i in 0 until arr.length()) {
                val s = arr.optJSONObject(i) ?: continue
                val link = s.optString("link")
                if (link.isBlank()) continue
                if (s.optString("type") == "direct") {
                    val label = s.optString("name").ifBlank { "RAPSODI" }
                    val q = s.optString("quality")
                    val tag = when {
                        q.contains("1080") -> "1080p"
                        q.contains("720") -> "720p"
                        q.contains("480") -> "480p"
                        q.contains("360") -> "360p"
                        else -> ""
                    }
                    out.add(
                        HikariStream(
                            name = "[AnimeInWeb] $label" + (if (tag.isNotBlank()) " · $tag" else ""),
                            url = link, headers = mapOf("Referer" to "$d/"),
                        ),
                    )
                } else {
                    val host = try {
                        java.net.URI(link).host.orEmpty()
                    } catch (_: Exception) {
                        ""
                    }
                    if (host == "www.blogger.com" || host == "gdplayer.to") {
                        try {
                            for (r in HikariNet.resolveWithWebView(link, capture, timeoutMs = 45_000)) {
                                out.add(
                                    HikariStream(
                                        name = "[AnimeInWeb] ${s.optString("name").ifBlank { host }}",
                                        url = r.url, headers = r.headers,
                                        isM3u8 = r.url.contains("m3u8"),
                                    ),
                                )
                            }
                        } catch (_: Exception) {
                        }
                    }
                }
            }
            out
        } catch (_: Exception) {
            emptyList()
        }
    }
}
