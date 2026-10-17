package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import com.hikari.ext.HikariSubtitle
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import org.json.JSONObject
import java.net.URLEncoder

/**
 * KDesa (kdesa.stream) — TMDB front-end with a 9-source resolver fleet.
 *
 * Faithful native port of KSHITIJ8473/raghav KdesaProvider:
 *  - 9 TMDB rows (bearer token from the site's config.js, public v3 key
 *    fallback): Trending, Popular Movies/TV, Top Movies/TV, Anime
 *    Movies/Series, Airing Now, In Theaters.
 *  - Media ids ride the source's payload scheme (movie|id|title and
 *    tv|id|season|episode|title); episodes expand per TMDB season.
 *  - Playback fans out to KdSources (CornClick, 7Movies, 1Embed, VixSrc,
 *    Anidap, TQQ anime, Cuevana3, Nova, FSOnline).
 */
class KdesaProvider : HikariProvider {

    override val id = "kdesa"
    override val name = "KDesa"
    override val mainUrl = "https://kdesa.stream"
    override val description = "Movies, TV and anime from KDesa — TMDB catalogs, 9-source resolvers."
    override val version = 2
    override val tvTypes = setOf(HikariMediaType.MOVIE, HikariMediaType.SERIES)

    private companion object {
        const val TMDB_API = "https://api.themoviedb.org/3"
        const val TMDB_BEARER =
            "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJjYTg3NmZkYmVhMjNhMzI3ODY0ZjRjN2U5MzMwZTYxNiIsIm5iZiI6MTc4MjIwOTQ0NC45OTksInN1YiI6IjZhM2E1YmE0ZmMzZGFiNGNmYzMzNjIxMCIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.WlSOswQDdxdbKu0jARJoruV6PlteoTXB1Oj4gRaibBI"
        const val TMDB_KEY = "1865f43a0549ca50d341dd9ab8b29f49"
        const val TMDB_IMG = "https://image.tmdb.org/t/p/w500"
        const val TMDB_BACK = "https://image.tmdb.org/t/p/original"
        const val UA =
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    }

    private suspend fun tmdbGet(path: String): JSONObject? {
        val sep = if (path.contains("?")) "&" else "?"
        return try {
            val text = HikariNet.getString(
                "$TMDB_API$path${sep}language=en-US",
                mapOf(
                    "Authorization" to "Bearer $TMDB_BEARER",
                    "Accept" to "application/json",
                    "User-Agent" to UA,
                ),
            )
            if (text != null && !text.contains("Invalid API key")) return JSONObject(text)
            val fb = HikariNet.getString(
                "$TMDB_API$path${sep}api_key=$TMDB_KEY&language=en-US",
                mapOf("Accept" to "application/json", "User-Agent" to UA),
            ) ?: return null
            JSONObject(fb)
        } catch (_: Exception) {
            null
        }
    }

    private fun itemToMedia(o: JSONObject, defType: String?): HikariMedia? {
        val type = o.optString("media_type").ifBlank { defType } ?: return null
        if (type != "movie" && type != "tv") return null
        val id = o.optInt("id", 0)
        if (id == 0) return null
        val t = if (type == "movie") o.optString("title").ifBlank { o.optString("name") }
        else o.optString("name").ifBlank { o.optString("title") }
        if (t.isBlank()) return null
        val poster = o.optString("poster_path").takeIf { it.isNotBlank() }?.let { TMDB_IMG + it }
        val year = (o.optString("release_date").ifBlank { o.optString("first_air_date") })
            .take(4).toIntOrNull()
        val pid = if (type == "movie") "movie|$id|$t" else "tv|$id|$t"
        return HikariMedia(
            id = pid, title = t,
            type = if (type == "movie") HikariMediaType.MOVIE else HikariMediaType.SERIES,
            posterUrl = poster, year = year,
        )
    }

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("trending", "Trending", HikariMediaType.MOVIE, rawType = "tmdb_trending"),
        HikariCatalog("pop-movie", "Popular Movies", HikariMediaType.MOVIE, rawType = "tmdb_popular_movies"),
        HikariCatalog("pop-tv", "Popular TV Shows", HikariMediaType.SERIES, rawType = "tmdb_popular_tv"),
        HikariCatalog("top-movie", "Top Rated Movies", HikariMediaType.MOVIE, rawType = "tmdb_top_movies"),
        HikariCatalog("top-tv", "Top Rated TV Shows", HikariMediaType.SERIES, rawType = "tmdb_top_tv"),
        HikariCatalog("anime-movie", "Anime Movies", HikariMediaType.MOVIE, rawType = "tmdb_anime_movies"),
        HikariCatalog("anime-tv", "Anime Series", HikariMediaType.SERIES, rawType = "tmdb_anime_tv"),
        HikariCatalog("airing", "Airing Now", HikariMediaType.SERIES, rawType = "tmdb_airing_tv"),
        HikariCatalog("theaters", "In Theaters", HikariMediaType.MOVIE, rawType = "tmdb_now_playing"),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val p = page.coerceAtLeast(1)
        var path: String
        var defType: String? = null
        when (catalog.rawType.ifBlank { catalog.id }) {
            "tmdb_trending" -> path = "/trending/all/day?page=$p"
            "tmdb_popular_movies" -> {
                path = "/movie/popular?page=$p"; defType = "movie"
            }
            "tmdb_popular_tv" -> {
                path = "/tv/popular?page=$p"; defType = "tv"
            }
            "tmdb_top_movies" -> {
                path = "/movie/top_rated?page=$p"; defType = "movie"
            }
            "tmdb_top_tv" -> {
                path = "/tv/top_rated?page=$p"; defType = "tv"
            }
            "tmdb_anime_movies" -> {
                path = "/discover/movie?page=$p&with_genres=16&sort_by=popularity.desc&include_adult=false"
                defType = "movie"
            }
            "tmdb_anime_tv" -> {
                path = "/discover/tv?page=$p&with_genres=16&sort_by=popularity.desc&include_adult=false"
                defType = "tv"
            }
            "tmdb_airing_tv" -> {
                path = "/tv/on_the_air?page=$p"; defType = "tv"
            }
            "tmdb_now_playing" -> {
                path = "/movie/now_playing?page=$p"; defType = "movie"
            }
            else -> return emptyList()
        }
        val root = tmdbGet(path) ?: return emptyList()
        val arr = root.optJSONArray("results") ?: return emptyList()
        val out = ArrayList<HikariMedia>()
        for (i in 0 until arr.length()) {
            arr.optJSONObject(i)?.let { itemToMedia(it, defType)?.let { m -> out.add(m) } }
        }
        return out
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank()) return emptyList()
        val p = page.coerceAtLeast(1)
        val root = tmdbGet("/search/multi?query=${URLEncoder.encode(query.trim(), "UTF-8")}&page=$p&include_adult=false")
            ?: return emptyList()
        val arr = root.optJSONArray("results") ?: return emptyList()
        val out = ArrayList<HikariMedia>()
        for (i in 0 until arr.length()) {
            arr.optJSONObject(i)?.let { itemToMedia(it, null)?.let { m -> out.add(m) } }
        }
        return out
    }

    private fun parts(raw: String): List<String> {
        var s = raw.trim()
        if (s.startsWith("$mainUrl/")) s = s.removePrefix("$mainUrl/")
        return s.split("|")
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val p = parts(media.id)
        if (p.size < 3) return media
        val type = p[0]
        val tmdbId = p[1].toIntOrNull() ?: return media
        val fallback = p.drop(2).joinToString("|")
        return try {
            val d = tmdbGet("/$type/$tmdbId") ?: return media
            val title = d.optString("title").ifBlank { d.optString("name") }.ifBlank { fallback }
            val year = (d.optString("release_date").ifBlank { d.optString("first_air_date") })
                .take(4).toIntOrNull() ?: media.year
            val poster = d.optString("poster_path").takeIf { it.isNotBlank() }?.let { TMDB_IMG + it }
                ?: media.posterUrl
            val back = d.optString("backdrop_path").takeIf { it.isNotBlank() }?.let { TMDB_BACK + it }
                ?: poster
            val genres = mutableListOf<String>()
            d.optJSONArray("genres")?.let { arr ->
                for (i in 0 until arr.length()) {
                    arr.optJSONObject(i)?.optString("name")?.takeIf { it.isNotBlank() }?.let { genres.add(it) }
                }
            }
            media.copy(
                title = title, posterUrl = poster, backdropUrl = back,
                overview = d.optString("overview").ifBlank { null } ?: media.overview,
                year = year, genres = genres.ifEmpty { media.genres },
            )
        } catch (_: Exception) {
            media
        }
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val p = parts(media.id)
        if (p.size < 2) return null
        val type = p[0]
        val tmdbId = p[1].toIntOrNull() ?: return null
        if (type == "movie") {
            return listOf(HikariEpisode(number = 1, id = media.id, name = media.title, image = media.posterUrl))
        }
        return try {
            val d = tmdbGet("/tv/$tmdbId") ?: return null
            val seasons = ArrayList<Int>()
            d.optJSONArray("seasons")?.let { arr ->
                for (i in 0 until arr.length()) {
                    val s = arr.optJSONObject(i) ?: continue
                    val sn = s.optInt("season_number", -1)
                    if (sn > 0 && s.optInt("episode_count", 0) > 0) seasons.add(sn)
                }
            }
            if (seasons.isEmpty()) return null
            val out = ArrayList<HikariEpisode>()
            coroutineScope {
                seasons.map { sn ->
                    async(Dispatchers.IO) {
                        try {
                            val sd = tmdbGet("/tv/$tmdbId/season/$sn") ?: return@async emptyList<HikariEpisode>()
                            val eps = sd.optJSONArray("episodes") ?: return@async emptyList<HikariEpisode>()
                            (0 until eps.length()).mapNotNull { i ->
                                val ep = eps.optJSONObject(i) ?: return@mapNotNull null
                                val num = ep.optInt("episode_number", 0)
                                if (num <= 0) return@mapNotNull null
                                val title = p.drop(2).joinToString("|")
                                HikariEpisode(
                                    number = num, season = sn,
                                    id = "tv|$tmdbId|$sn|$num|$title",
                                    name = ep.optString("name").ifBlank { "Episode $num" },
                                    image = ep.optString("still_path").takeIf { it.isNotBlank() }
                                        ?.let { TMDB_IMG + it } ?: media.posterUrl,
                                )
                            }
                        } catch (_: Exception) {
                            emptyList()
                        }
                    }
                }.forEach { def -> runCatching { out.addAll(def.await()) } }
            }
            out.sortBy { it.season * 10000 + it.number }
            out.ifEmpty { null }
        } catch (_: Exception) {
            null
        }
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val arg = episode?.id ?: media.id
        val p = parts(arg)
        val type = p.getOrNull(0) ?: return emptyList()
        val tmdbId = p.getOrNull(1)?.toIntOrNull() ?: return emptyList()
        val resolver = KdSources()
        val out = ArrayList<HikariStream>()
        val subs = ArrayList<HikariSubtitle>()
        val subCb: (HikariSubtitle) -> Unit = { subs.add(it) }
        val cb: (HikariStream) -> Unit = { out.add(it) }
        try {
            if (type == "movie") {
                val title = p.drop(2).joinToString("|")
                resolver.resolveMovie(tmdbId, title, subCb, cb)
            } else {
                val season = p.getOrNull(2)?.toIntOrNull() ?: return emptyList()
                val ep = p.getOrNull(3)?.toIntOrNull() ?: return emptyList()
                val title = p.drop(4).joinToString("|")
                resolver.resolveShow(tmdbId, title, season, ep, subCb, cb)
            }
        } catch (_: Exception) {
        }
        if (subs.isNotEmpty() && out.isNotEmpty()) {
            return out.map { it.copy(subtitles = it.subtitles + subs) }
        }
        return out
    }
}
