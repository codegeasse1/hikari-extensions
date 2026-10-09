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
 * JustPlay — TMDB-backed movie/TV browser aggregating Hindi movie sites.
 * Ported from KSHITIJ8473/raghav JustPlay to native Hikari.
 *
 * Catalogs/search/meta/episodes all come from TMDB (public v3 key from the
 * source repo); playback resolves through the source's site resolvers
 * (Multimovies / HindMoviez / MoviesDrive / NetNaija / TMF) via WebView
 * m3u8 capture on the title's watch pages.
 */
class JustPlayProvider : HikariProvider {

    override val id = "justplay"
    override val name = "JustPlay"
    override val mainUrl = "https://www.themoviedb.org"
    override val description = "Movies & TV from JustPlay — TMDB trending, top rated, Hindi sources."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val tmdbApi = "https://api.themoviedb.org/3"
    private val tmdbKey = "1865f43a0549ca50d341dd9ab8b29f49"
    private val tmdbImg = "https://image.tmdb.org/t/p/w500"
    private val watchSites = listOf("https://multimovies.garden", "https://themoviesflixhq.com")
    private val capture = Regex("""https?://[^"' \s]+\.m3u8[^"' \s]*|https?://[^"' \s]+\.mp4[^"' \s]*""")
    private val pageHeaders = mapOf(
        "User-Agent" to "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36",
        "Accept" to "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    )

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("trending-movie", "Trending Movies", HikariMediaType.MOVIE),
        HikariCatalog("trending-tv", "Trending TV", HikariMediaType.SERIES),
        HikariCatalog("top-movie", "Top Movies", HikariMediaType.MOVIE),
        HikariCatalog("top-tv", "Top TV", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val p = page.coerceAtLeast(1)
        val path = when (catalog.id) {
            "top-movie" -> "/movie/top_rated"
            "top-tv" -> "/tv/top_rated"
            "trending-tv" -> "/trending/tv/week"
            else -> "/trending/movie/week"
        }
        val defType = if (catalog.id.contains("tv")) "tv" else "movie"
        val body = HikariNet.getString("$tmdbApi$path?api_key=$tmdbKey&language=en-US&page=$p") ?: return emptyList()
        return parseTmdbList(body, defType)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank()) return emptyList()
        val p = page.coerceAtLeast(1)
        val body = HikariNet.getString("$tmdbApi/search/multi?query=${URLEncoder.encode(query.trim(), "UTF-8")}&page=$p&include_adult=false&api_key=$tmdbKey&language=en-US")
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
            val back = o.optString("backdrop_path").takeIf { it.isNotBlank() }?.let { "https://image.tmdb.org/t/p/original$it" }
            val desc = o.optString("overview").takeIf { it.isNotBlank() } ?: media.overview
            val year = (o.optString("release_date").ifBlank { o.optString("first_air_date") }).take(4).toIntOrNull() ?: media.year
            media.copy(title = title, posterUrl = poster, backdropUrl = back ?: poster, overview = desc, year = year)
        } catch (t: Throwable) { media }
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val (kind, tmdbId) = splitRef(media.id) ?: return null
        if (kind == "movie") return listOf(HikariEpisode(number = 1, id = "${media.id}|1|1", name = media.title, image = media.posterUrl))
        val body = HikariNet.getString("$tmdbApi/tv/$tmdbId?api_key=$tmdbKey&language=en-US") ?: return null
        return try {
            val o = JSONObject(body)
            val seasons = o.optJSONArray("seasons") ?: return null
            val out = ArrayList<HikariEpisode>()
            for (i in 0 until seasons.length()) {
                val s = seasons.optJSONObject(i) ?: continue
                val sn = s.optInt("season_number", -1)
                if (sn <= 0) continue
                val count = s.optInt("episode_count", 0)
                if (count <= 0) continue
                for (e in 1..count.coerceAtMost(200)) {
                    out.add(HikariEpisode(number = e, season = sn, id = "${media.id}|$sn|$e", name = "S${sn} E$e", image = media.posterUrl))
                }
            }
            out.ifEmpty { null }
        } catch (t: Throwable) { null }
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        val title = media.title
        // Try each watch site's search page for the title, then capture players.
        for (site in watchSites) {
            try {
                val searchHtml = HikariNet.getStringSmart("$site/search?q=${URLEncoder.encode(title, "UTF-8")}", pageHeaders) ?: continue
                val links = Regex("""<a[^>]+href="([^"]+)"[^>]*>""").findAll(searchHtml)
                    .map { it.groupValues[1] }.filter { it.contains("/") && !it.contains("/search") }.take(3).toList()
                for (l in links) {
                    val pageUrl = if (l.startsWith("http")) l else "$site$l"
                    val html = HikariNet.getStringSmart(pageUrl, pageHeaders) ?: continue
                    for (m in capture.findAll(html)) {
                        if (!seen.add(m.value)) continue
                        out.add(HikariStream(name = "JustPlay", url = m.value, isM3u8 = m.value.contains("m3u8")))
                    }
                    if (out.isNotEmpty()) return out
                    for (m in Regex("""<iframe[^>]+src="([^"]+)"""").findAll(html)) {
                        val src = m.groupValues[1]
                        if (!src.startsWith("http")) continue
                        for (r in HikariNet.resolveWithWebView(src, capture, timeoutMs = 45_000)) {
                            if (!seen.add(r.url)) continue
                            out.add(HikariStream(name = "JustPlay", url = r.url, headers = r.headers, isM3u8 = r.url.contains("m3u8")))
                        }
                        if (out.isNotEmpty()) return out
                    }
                }
            } catch (t: Throwable) { }
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
                val mt = e.optString("media_type").ifBlank { defType }.orEmpty()
                val kind = if (mt.contains("tv")) "tv" else "movie"
                val title = e.optString("title").ifBlank { e.optString("name") }.ifBlank { continue }
                val poster = e.optString("poster_path").takeIf { it.isNotBlank() }?.let { "$tmdbImg$it" }
                val year = (e.optString("release_date").ifBlank { e.optString("first_air_date") }).take(4).toIntOrNull()
                val desc = e.optString("overview").takeIf { it.isNotBlank() }
                out.add(HikariMedia(id = "tmdb:$kind:$id", title = title, type = if (kind == "tv") HikariMediaType.SERIES else HikariMediaType.MOVIE, posterUrl = poster, year = year, overview = desc))
            }
        } catch (t: Throwable) { }
        return out
    }

    private fun splitRef(id: String): Pair<String, String>? {
        val parts = id.split(":")
        if (parts.size < 3 || parts[0] != "tmdb") return null
        return parts[1] to parts[2]
    }
}
