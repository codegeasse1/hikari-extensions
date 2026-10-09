package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import org.json.JSONObject
import java.net.URLEncoder

/**
 * Kdesa (kdesa.stream) — TMDB meta + kdesa watch resolvers.
 * Ported from KSHITIJ8473/raghav Kdesa to native Hikari.
 * Catalogs/search/meta/episodes via TMDB (source's public v3 key);
 * streams resolve from the kdesa watch pages via WebView capture.
 */
class KdesaProvider : HikariProvider {

    override val id = "kdesa"
    override val name = "KDesa"
    override val mainUrl = "https://kdesa.stream"
    override val description = "Movies & shows from KDesa — TMDB browsing with direct streams."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val tmdbApi = "https://api.themoviedb.org/3"
    private val tmdbKey = "1865f43a0549ca50d341dd9ab8b29f49"
    private val tmdbImg = "https://image.tmdb.org/t/p/w500"
    private val pageHeaders = mapOf(
        "User-Agent" to "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36",
        "Accept" to "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer" to "$mainUrl/",
    )
    private val capture = Regex("""https?://[^"' \s]+\.m3u8[^"' \s]*|https?://[^"' \s]+\.mp4[^"' \s]*""")

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("trending-movie", "Trending Movies", HikariMediaType.MOVIE),
        HikariCatalog("trending-tv", "Trending Shows", HikariMediaType.SERIES),
        HikariCatalog("top-movie", "Top Movies", HikariMediaType.MOVIE),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val p = page.coerceAtLeast(1)
        val path = when (catalog.id) {
            "top-movie" -> "/movie/top_rated"
            "trending-tv" -> "/trending/tv/week"
            else -> "/trending/movie/week"
        }
        val defType = if (catalog.id.contains("tv")) "tv" else "movie"
        val body = HikariNet.getString("$tmdbApi$path?api_key=$tmdbKey&language=en-US&page=$p") ?: return emptyList()
        return parseTmdbList(body, defType)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank()) return emptyList()
        val body = HikariNet.getString("$tmdbApi/search/multi?query=${URLEncoder.encode(query.trim(), "UTF-8")}&page=${page.coerceAtLeast(1)}&include_adult=false&api_key=$tmdbKey&language=en-US")
            ?: return emptyList()
        return parseTmdbList(body, null)
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val (kind, tmdbId) = splitRef(media.id) ?: return media
        val body = HikariNet.getString("$tmdbApi/$kind/$tmdbId?api_key=$tmdbKey&language=en-US") ?: return media
        return try {
            val o = JSONObject(body)
            val title = o.optString("title").ifBlank { o.optString("name") }.ifBlank { media.title }
            val poster = o.optString("poster_path").takeIf { it.isNotBlank() }?.let { "$tmdbImg$it" } ?: media.posterUrl
            media.copy(title = title, posterUrl = poster, backdropUrl = poster, overview = o.optString("overview").takeIf { it.isNotBlank() } ?: media.overview)
        } catch (t: Throwable) { media }
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val (kind, tmdbId) = splitRef(media.id) ?: return null
        if (kind == "movie") return listOf(HikariEpisode(number = 1, id = "${media.id}|1|1", name = media.title, image = media.posterUrl))
        val body = HikariNet.getString("$tmdbApi/tv/$tmdbId?api_key=$tmdbKey&language=en-US") ?: return null
        return try {
            val seasons = JSONObject(body).optJSONArray("seasons") ?: return null
            val out = ArrayList<HikariEpisode>()
            for (i in 0 until seasons.length()) {
                val s = seasons.optJSONObject(i) ?: continue
                val sn = s.optInt("season_number", -1)
                if (sn <= 0) continue
                for (e in 1..s.optInt("episode_count", 0).coerceAtMost(200)) {
                    out.add(HikariEpisode(number = e, season = sn, id = "${media.id}|$sn|$e", name = "S${sn} E$e", image = media.posterUrl))
                }
            }
            out.ifEmpty { null }
        } catch (t: Throwable) { null }
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        val q = URLEncoder.encode(media.title, "UTF-8")
        val candidates = listOf("$mainUrl/search?q=$q", "$mainUrl/?s=$q")
        for (c in candidates) {
            val html = HikariNet.getStringSmart(c, pageHeaders) ?: continue
            val links = Regex("""<a[^>]+href="([^"]+)"[^>]*>""").findAll(html).map { it.groupValues[1] }
                .filter { it.contains("/") && !it.contains("/search") && !it.contains("?s=") }.take(3).toList()
            for (l in links) {
                val pageUrl = if (l.startsWith("http")) l else "$mainUrl$l"
                val ph = HikariNet.getStringSmart(pageUrl, pageHeaders) ?: continue
                for (m in capture.findAll(ph)) {
                    if (!seen.add(m.value)) continue
                    out.add(HikariStream(name = "KDesa", url = m.value, isM3u8 = m.value.contains("m3u8")))
                }
                if (out.isNotEmpty()) return out
                for (m in Regex("""<iframe[^>]+src="([^"]+)"""").findAll(ph)) {
                    val src = m.groupValues[1]
                    if (!src.startsWith("http")) continue
                    for (r in HikariNet.resolveWithWebView(src, capture, timeoutMs = 45_000)) {
                        if (!seen.add(r.url)) continue
                        out.add(HikariStream(name = "KDesa", url = r.url, headers = r.headers, isM3u8 = r.url.contains("m3u8")))
                    }
                    if (out.isNotEmpty()) return out
                }
            }
        }
        return out
    }

    private fun parseTmdbList(body: String, defType: String?): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        try {
            val arr = JSONObject(body).optJSONArray("results") ?: return emptyList()
            for (i in 0 until arr.length()) {
                val e = arr.optJSONObject(i) ?: continue
                val id = e.optInt("id", 0)
                if (id == 0) continue
                val kind = if ((e.optString("media_type").ifBlank { defType }).orEmpty().contains("tv")) "tv" else "movie"
                val title = e.optString("title").ifBlank { e.optString("name") }.ifBlank { continue }
                val poster = e.optString("poster_path").takeIf { it.isNotBlank() }?.let { "$tmdbImg$it" }
                out.add(HikariMedia(id = "kdesa:$kind:$id", title = title, type = if (kind == "tv") HikariMediaType.SERIES else HikariMediaType.MOVIE, posterUrl = poster))
            }
        } catch (t: Throwable) { }
        return out
    }

    private fun splitRef(id: String): Pair<String, String>? {
        val parts = id.split(":")
        if (parts.size < 3 || parts[0] != "kdesa") return null
        return parts[1] to parts[2]
    }
}
