package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import org.json.JSONArray
import org.json.JSONObject
import java.net.URLEncoder

/**
 * Hotstar via NetMirror backend (net52.cc / net22.cc rotation), verified Oct 2026:
 *  - Backend status: DEAD on both hosts (same NetMirror shell as DisneyPlus).
 *    Full endpoint map retained with rotation + TMDB fallback so rows load.
 *  - Endpoints: /verify.php, /mobile/verify2.php, /newtv/otp.php,
 *    /mobile/home?app=1, /mobile/hs/search.php?s=, /mobile/hs/post.php?id=,
 *    /mobile/hs/episodes.php?s=, /mobile/hs/playlist.php?id=,
 *    /newtv/player.php?id=, userver https://userver.net52.cc/?hee5=.
 *  - Posters: https://imgcdn.kim/{hs/h,hs/v,hsepimg/150,poster/v}/ + TMDB.
 */
class HotstarProvider : HikariProvider {

    override val id = "hotstar"
    override val name = "Hotstar"
    override val mainUrl = "https://net52.cc"
    override val description = "Hotstar movies & series via NetMirror — trending, top rated, search (backend rotation + TMDB fallback)."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val UA =
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.0 /OS.GatuNewTV v1.0"

    companion object {
        val BASES = listOf("https://net52.cc", "https://net22.cc")
        var base = BASES[0]
        const val TMDB = "https://api.themoviedb.org/3"
        const val TMDB_KEY = "1865f43a0549ca50d341dd9ab8b29f49"
        const val TMDB_KEY_ALT = "98ae14df2b8d8f8f8136499daf79f0e0"
        const val IMG = "https://image.tmdb.org/t/p/w500"
        const val PROVIDER = 122
    }

    private fun appH(extra: Map<String, String> = emptyMap()): Map<String, String> {
        val m = HashMap<String, String>()
        m["User-Agent"] = UA
        m["Accept"] = "application/json"
        m["Connection"] = "Keep-Alive"
        m.putAll(extra)
        return m
    }

    private fun tmdbH(): Map<String, String> = mapOf("User-Agent" to UA, "Accept" to "application/json")

    private suspend fun tmdbGet(path: String, query: String): JSONObject? {
        val t1 = HikariNet.getString("$TMDB$path?api_key=$TMDB_KEY&$query", tmdbH())
        if (t1 != null) {
            val o = runCatching { JSONObject(t1) }.getOrNull()
            if (o != null) return o
        }
        val t2 = HikariNet.getString("$TMDB$path?api_key=$TMDB_KEY_ALT&$query", tmdbH()) ?: return null
        return runCatching { JSONObject(t2) }.getOrNull()
    }

    private suspend fun nmGet(path: String): String? {
        for (b in BASES) {
            val t = HikariNet.getString("$b$path", appH(mapOf("Referer" to "$b/")))
            if (t != null && !t.contains("Page Not Found!")) {
                base = b
                return t
            }
        }
        return null
    }

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("nm-home", "NetMirror Home", HikariMediaType.SERIES),
        HikariCatalog("nm-movies", "NetMirror Movies", HikariMediaType.MOVIE),
        HikariCatalog("nm-series", "NetMirror Series", HikariMediaType.SERIES),
        HikariCatalog("tmdb-trend", "Trending", HikariMediaType.SERIES),
        HikariCatalog("tmdb-topmov", "Top Movies", HikariMediaType.MOVIE),
        HikariCatalog("tmdb-toptv", "Top Series", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val p = page.coerceAtLeast(1)
        when (catalog.id) {
            "nm-home", "nm-movies", "nm-series" -> {
                val html = nmGet("/mobile/home?app=1") ?: nmGet("/") ?: ""
                val items = parseNmHome(html, catalog.id)
                if (items.isNotEmpty()) return items
                return tmdbDiscover(catalog.id, p)
            }
            "tmdb-topmov" -> return tmdbList("/movie/top_rated", "language=en-US&page=$p", HikariMediaType.MOVIE)
            "tmdb-toptv" -> return tmdbList("/tv/top_rated", "language=en-US&page=$p", HikariMediaType.SERIES)
            else -> return tmdbList("/trending/all/week", "page=$p", HikariMediaType.SERIES)
        }
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        val p = page.coerceAtLeast(1)
        val nm = nmGet("/mobile/hs/search.php?s=${URLEncoder.encode(q, "UTF-8")}")
        if (nm != null) {
            val items = parseNmSearch(nm)
            if (items.isNotEmpty()) return items
        }
        val root = tmdbGet("/search/multi", "query=${URLEncoder.encode(q, "UTF-8")}&page=$p&include_adult=false") ?: return emptyList()
        return tmdbResultsToMedia(root.optJSONArray("results"))
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        if (media.id.startsWith("nm:")) {
            val sid = media.id.substringAfter("nm:")
            val txt = nmGet("/mobile/hs/post.php?id=${URLEncoder.encode(sid, "UTF-8")}") ?: return media
            val o = runCatching { JSONObject(txt) }.getOrNull()
            val d = o?.optJSONObject("data") ?: o ?: return media
            return media.copy(
                title = d.optString("title").ifBlank { d.optString("name") }.ifBlank { media.title },
                posterUrl = fixPoster(d.optString("poster").ifBlank { d.optString("image") }) ?: media.posterUrl,
                backdropUrl = fixPoster(d.optString("backdrop").ifBlank { d.optString("fanart") }) ?: media.backdropUrl,
                overview = d.optString("description").ifBlank { d.optString("overview") }.takeIf { it.isNotBlank() } ?: media.overview,
            )
        }
        return media
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        if (media.id.startsWith("nm:")) {
            val sid = media.id.substringAfter("nm:")
            val txt = nmGet("/mobile/hs/episodes.php?s=${URLEncoder.encode(sid, "UTF-8")}")
            if (txt != null) {
                val eps = parseNmEpisodes(txt, media)
                if (eps.isNotEmpty()) return eps
            }
            return listOf(HikariEpisode(number = 1, id = "${media.id}#ep1", name = media.title, image = media.posterUrl))
        }
        if (media.type == HikariMediaType.MOVIE) {
            return listOf(HikariEpisode(number = 1, id = "${media.id}#ep1", name = media.title, image = media.posterUrl))
        }
        return listOf(HikariEpisode(number = 1, season = 1, id = "${media.id}#s1e1", name = media.title, image = media.posterUrl))
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val ref = episode?.id ?: media.id
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        val nmId = ref.substringAfter("nm:", "").substringBefore("#").substringBefore(":")
        if (nmId.isNotBlank()) {
            for (b in BASES) {
                val txt = HikariNet.getString("$b/mobile/hs/playlist.php?id=${URLEncoder.encode(nmId, "UTF-8")}", appH(mapOf("Referer" to "$b/")))
                    ?: continue
                extractStreamsJson(txt, b, out, seen)
                if (out.isNotEmpty()) {
                    base = b
                    return out
                }
                val p2 = HikariNet.getString("$b/newtv/player.php?id=${URLEncoder.encode(nmId, "UTF-8")}", appH(mapOf("Referer" to "$b/")))
                if (p2 != null) extractStreamsJson(p2, b, out, seen)
                if (out.isNotEmpty()) {
                    base = b
                    return out
                }
            }
        }
        if (nmId.isNotBlank()) {
            val uv = HikariNet.getString("https://userver.net52.cc/?hee5=${URLEncoder.encode(nmId, "UTF-8")}", appH())
            if (uv != null) extractStreamsJson(uv, base, out, seen)
        }
        return out
    }

    private suspend fun tmdbDiscover(row: String, page: Int): List<HikariMedia> {
        val q = "page=$page&watch_region=US&with_watch_providers=$PROVIDER&sort_by=popularity.desc"
        val path = if (row == "nm-movies") "/discover/movie" else "/discover/tv"
        val root = tmdbGet(path, q) ?: tmdbGet("/trending/all/week", "page=$page") ?: return emptyList()
        val type = if (row == "nm-movies") HikariMediaType.MOVIE else HikariMediaType.SERIES
        return tmdbResultsToMedia(root.optJSONArray("results"), type)
    }

    private suspend fun tmdbList(path: String, query: String, type: HikariMediaType): List<HikariMedia> {
        val root = tmdbGet(path, query) ?: return emptyList()
        return tmdbResultsToMedia(root.optJSONArray("results"), type)
    }

    private fun tmdbResultsToMedia(arr: JSONArray?, force: HikariMediaType? = null): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        if (arr == null) return out
        for (i in 0 until arr.length()) {
            val o = arr.optJSONObject(i) ?: continue
            val mt = o.optString("media_type")
            val isMovie = force == HikariMediaType.MOVIE || (force == null && mt == "movie")
            val isTv = force == HikariMediaType.SERIES || (force == null && mt == "tv")
            if (!isMovie && !isTv && force == null) continue
            val id = o.optInt("id", 0)
            if (id == 0) continue
            val title = o.optString("title").ifBlank { o.optString("name") }.trim()
            if (title.isBlank()) continue
            out.add(
                HikariMedia(
                    id = "tmdb:$id:${if (isMovie) "movie" else "tv"}",
                    title = title,
                    type = if (isMovie) HikariMediaType.MOVIE else HikariMediaType.SERIES,
                    posterUrl = o.optString("poster_path").takeIf { it.isNotBlank() }?.let { IMG + it },
                    backdropUrl = o.optString("backdrop_path").takeIf { it.isNotBlank() }?.let { IMG + it },
                    overview = o.optString("overview").takeIf { it.isNotBlank() },
                    year = o.optString("release_date").ifBlank { o.optString("first_air_date") }.take(4).toIntOrNull(),
                    rawType = if (isMovie) "movie" else "tv",
                )
            )
        }
        return out
    }

    fun parseNmHome(txt: String, row: String): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        if (txt.isBlank() || txt.length < 50) return out
        val root = runCatching { JSONObject(txt) }.getOrNull()
        if (root != null) {
            val arr = root.optJSONArray("data") ?: root.optJSONArray("list") ?: root.optJSONArray("results") ?: root.optJSONArray("home")
            if (arr != null) {
                for (i in 0 until arr.length()) {
                    val o = arr.optJSONObject(i) ?: continue
                    val id = o.optString("id").ifBlank { o.optInt("id", 0).takeIf { it != 0 }?.toString() ?: "" }
                    val title = o.optString("title").ifBlank { o.optString("name") }.trim()
                    if (id.isBlank() || title.isBlank()) continue
                    val kind = o.optString("type").lowercase()
                    val isMovie = row == "nm-movies" || kind.contains("movie")
                    out.add(HikariMedia(id = "nm:$id", title = title, type = if (isMovie) HikariMediaType.MOVIE else HikariMediaType.SERIES, posterUrl = fixPoster(o.optString("poster").ifBlank { o.optString("image") })))
                }
                return out
            }
        }
        return out
    }

    fun parseNmSearch(txt: String): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        val root = runCatching { JSONObject(txt) }.getOrNull() ?: return out
        val arr = root.optJSONArray("data") ?: root.optJSONArray("results") ?: root.optJSONArray("list") ?: return out
        for (i in 0 until arr.length()) {
            val o = arr.optJSONObject(i) ?: continue
            val id = o.optString("id").ifBlank { continue }
            val title = o.optString("title").ifBlank { o.optString("name") }.trim().ifBlank { continue }
            out.add(HikariMedia(id = "nm:$id", title = title, type = HikariMediaType.SERIES, posterUrl = fixPoster(o.optString("poster"))))
        }
        return out
    }

    fun parseNmEpisodes(txt: String, media: HikariMedia): List<HikariEpisode> {
        val out = ArrayList<HikariEpisode>()
        val root = runCatching { JSONObject(txt) }.getOrNull() ?: return out
        val arr = root.optJSONArray("data") ?: root.optJSONArray("episodes") ?: root.optJSONArray("list") ?: return out
        for (i in 0 until arr.length()) {
            val o = arr.optJSONObject(i) ?: continue
            val n = o.optInt("episode", o.optInt("ep", i + 1))
            val s = o.optInt("season", 1)
            val title = o.optString("title").ifBlank { "S${s} E$n" }
            val eid = o.optString("id").ifBlank { "${media.id}#s${s}e$n" }
            out.add(HikariEpisode(number = n, season = s, id = "${media.id}#${eid}", name = title, image = media.posterUrl))
        }
        return out
    }

    private fun extractStreamsJson(txt: String, referer: String, out: MutableList<HikariStream>, seen: HashSet<String>) {
        for (m in Regex("https?://[^\\s\"'<>\\\\]+\\.(?:m3u8|mp4)[^\\s\"'<>\\\\]*").findAll(txt)) {
            val u = m.value
            if (u.startsWith("http") && seen.add(u)) {
                out.add(HikariStream(name = "Hotstar", url = u, headers = mapOf("User-Agent" to UA, "Referer" to referer), isM3u8 = u.contains(".m3u8")))
            }
        }
    }

    private fun fixPoster(p: String): String? {
        if (p.isBlank()) return null
        if (p.startsWith("http")) return p
        if (p.startsWith("//")) return "https:$p"
        if (p.startsWith("/")) return base + p
        return "https://imgcdn.kim/poster/v/$p"
    }
}
