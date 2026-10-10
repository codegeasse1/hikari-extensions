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
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.json.JSONObject
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * JustPlay — TMDB-backed movie/TV browser aggregating 10 Hindi movie sites.
 *
 * Faithful native port of KSHITIJ8473/raghav JustPlay (CloudStream MainAPI):
 *  - 12 TMDB catalog rows (trending / popular / top rated / airing / Netflix /
 *    Prime / Indian movies + series / Korean / anime), two API keys with
 *    fallback, paged catalog + multi search.
 *  - Detail + per-season episode expansion from TMDB (credits/genres kept).
 *  - Playback fans out to all 10 site backends (NetNaija / VegaMovies /
 *    HDHub4u / 4KHDHub / TheMoviesFlix / Multimovies / Movies4u / MoviesDrive /
 *    HindMoviez / NetMirror) behind a 6-permit gate, with shared-URL dedupe
 *    and a 12-links-per-site cap. Download-only sources are skipped (the
 *    source's settings UI is not portable, so streamOnly=true is hardcoded).
 */
class JustPlayProvider : HikariProvider {

    override val id = "justplay"
    override val name = "JustPlay"
    override val mainUrl = "https://www.themoviedb.org"
    override val description = "Movies & TV from JustPlay — TMDB catalogs with Hindi sources."
    override val version = 2
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    companion object {
        private const val TMDB = "https://api.themoviedb.org/3"
        private const val TMDB_KEY = "1865f43a0549ca50d341dd9ab8b29f49"
        private const val TMDB_KEY_ALT = "98ae14df2b8d8f8f8136499daf79f0e0"
        private const val LINKS_PER_SOURCE = 12

        private fun tmdbImageUrl(path: String?): String? {
            if (path.isNullOrBlank()) return null
            return if (path.startsWith("http")) path
            else "https://image.tmdb.org/t/p/original$path"
        }
    }

    private suspend fun tmdbGet(path: String, params: Map<String, String> = emptyMap()): JSONObject? {
        val query = params.entries.joinToString("&") { "${it.key}=${JpNet.urlEncode(it.value)}" }
        return try {
            HikariNet.getString("$TMDB$path?api_key=$TMDB_KEY&$query")?.let { JSONObject(it) }
        } catch (_: Exception) {
            null
        } ?: try {
            HikariNet.getString("$TMDB$path?api_key=$TMDB_KEY_ALT&$query")?.let { JSONObject(it) }
        } catch (_: Exception) {
            null
        }
    }

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("trending", "Trending Now", HikariMediaType.UNKNOWN, rawType = "/trending/all/day"),
        HikariCatalog("popular-movies", "Popular Movies", HikariMediaType.MOVIE, rawType = "/movie/popular"),
        HikariCatalog("popular-tv", "Popular TV Shows", HikariMediaType.SERIES, rawType = "/tv/popular"),
        HikariCatalog("top-movies", "Top Rated Movies", HikariMediaType.MOVIE, rawType = "/movie/top_rated"),
        HikariCatalog("top-tv", "Top Rated TV Shows", HikariMediaType.SERIES, rawType = "/tv/top_rated"),
        HikariCatalog("airing", "Airing Today", HikariMediaType.SERIES, rawType = "/tv/airing_today"),
        HikariCatalog("netflix", "Netflix", HikariMediaType.SERIES, rawType = "/discover/tv&with_networks=213"),
        HikariCatalog("prime", "Amazon Prime", HikariMediaType.SERIES, rawType = "/discover/tv&with_networks=1024"),
        HikariCatalog(
            "indian-movies", "Indian Movies", HikariMediaType.MOVIE,
            rawType = "/discover/movie&with_origin_country=IN&sort_by=popularity.desc",
        ),
        HikariCatalog(
            "indian-series", "Indian Web Series", HikariMediaType.SERIES,
            rawType = "/discover/tv&with_origin_country=IN&sort_by=popularity.desc",
        ),
        HikariCatalog(
            "korean", "Korean Shows", HikariMediaType.SERIES,
            rawType = "/discover/tv&with_original_language=ko&sort_by=popularity.desc",
        ),
        HikariCatalog(
            "anime", "Anime Series", HikariMediaType.SERIES,
            rawType = "/discover/tv&with_genres=16&with_original_language=ja&sort_by=popularity.desc",
        ),
    )

    private fun toMedia(item: JSONObject, fallbackType: String): HikariMedia? {
        val id = item.optInt("id", 0)
        if (id == 0) return null
        val mediaType = item.optString("media_type").ifBlank { fallbackType }
        if (mediaType != "movie" && mediaType != "tv") return null
        val title = item.optString("title").ifBlank { item.optString("name") }
        if (title.isBlank()) return null
        val poster = tmdbImageUrl(item.optString("poster_path").ifBlank { null })
        val year = item.optString("release_date").ifBlank { item.optString("first_air_date") }
            .take(4).toIntOrNull()
        val desc = item.optString("overview").takeIf { it.isNotBlank() }
        return HikariMedia(
            id = "tmdb:$mediaType:$id",
            title = title,
            type = if (mediaType == "movie") HikariMediaType.MOVIE else HikariMediaType.SERIES,
            posterUrl = poster,
            year = year,
            overview = desc,
        )
    }

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val p = page.coerceAtLeast(1)
        val parts = catalog.rawType.ifBlank { return emptyList() }.split("&", limit = 2)
        val path = parts[0]
        val extra = parts.getOrNull(1)?.split("&")?.mapNotNull {
            val kv = it.split("=", limit = 2)
            if (kv.size == 2) kv[0] to kv[1] else null
        } ?: emptyList()
        val type = if (catalog.rawType.contains("/movie")) "movie" else "tv"
        val params = (extra + listOf("language" to "en-US", "page" to "$p")).toMap()
        val json = tmdbGet(path, params) ?: return emptyList()
        val results = json.optJSONArray("results") ?: return emptyList()
        val out = ArrayList<HikariMedia>()
        for (i in 0 until results.length()) {
            val item = results.optJSONObject(i) ?: continue
            toMedia(item, if (path.contains("trending")) "" else type)?.let { out.add(it) }
        }
        return out
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank() || page > 1) return emptyList()
        val json = tmdbGet(
            "/search/multi",
            mapOf("query" to query.trim(), "include_adult" to "false", "page" to "1", "language" to "en-US"),
        ) ?: return emptyList()
        val results = json.optJSONArray("results") ?: return emptyList()
        val out = ArrayList<HikariMedia>()
        for (i in 0 until results.length()) {
            val item = results.optJSONObject(i) ?: continue
            toMedia(item, "")?.let { out.add(it) }
        }
        return out
    }

    private fun splitRef(id: String): Pair<String, Int>? {
        val parts = id.split(":")
        if (parts.size < 3 || parts[0] != "tmdb") return null
        val tmdbId = parts[2].toIntOrNull() ?: return null
        return parts[1] to tmdbId
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val (kind, tmdbId) = splitRef(media.id) ?: return media
        val detail = tmdbGet(
            "/$kind/$tmdbId",
            mapOf("language" to "en-US", "append_to_response" to "external_ids"),
        ) ?: return media
        return try {
            val title = detail.optString("title").ifBlank { detail.optString("name") }.ifBlank { media.title }
            val poster = tmdbImageUrl(detail.optString("poster_path").ifBlank { null }) ?: media.posterUrl
            val back = tmdbImageUrl(detail.optString("backdrop_path").ifBlank { null }) ?: poster
            val desc = detail.optString("overview").takeIf { it.isNotBlank() } ?: media.overview
            val year = detail.optString("release_date").ifBlank { detail.optString("first_air_date") }
                .take(4).toIntOrNull() ?: media.year
            val genres = mutableListOf<String>()
            detail.optJSONArray("genres")?.let { arr ->
                for (i in 0 until arr.length()) {
                    arr.optJSONObject(i)?.optString("name")?.takeIf { it.isNotBlank() }?.let { genres.add(it) }
                }
            }
            media.copy(
                title = title, posterUrl = poster, backdropUrl = back,
                overview = desc, year = year,
                genres = genres.ifEmpty { media.genres },
            )
        } catch (_: Exception) {
            media
        }
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val (kind, tmdbId) = splitRef(media.id) ?: return null
        if (kind == "movie") {
            return listOf(
                HikariEpisode(number = 1, id = "${media.id}|1|1", name = media.title, image = media.posterUrl),
            )
        }
        val detail = tmdbGet("/tv/$tmdbId", mapOf("language" to "en-US")) ?: return null
        val seasonsArr = detail.optJSONArray("seasons") ?: return null
        val seasons = mutableListOf<Int>()
        for (i in 0 until seasonsArr.length()) {
            val s = seasonsArr.optJSONObject(i) ?: continue
            val num = s.optInt("season_number", -1)
            if (num > 0) seasons.add(num)
        }
        if (seasons.isEmpty()) seasons.add(1)
        val episodes = mutableListOf<HikariEpisode>()
        coroutineScope {
            val gate = Semaphore(8)
            val deferred = seasons.map { sn ->
                async(Dispatchers.IO) {
                    gate.withPermit {
                        try {
                            val seasonJson = tmdbGet("/tv/$tmdbId/season/$sn", mapOf("language" to "en-US"))
                                ?: return@withPermit emptyList()
                            val epsArr = seasonJson.optJSONArray("episodes") ?: return@withPermit emptyList()
                            (0 until epsArr.length()).mapNotNull { i ->
                                val ep = epsArr.optJSONObject(i) ?: return@mapNotNull null
                                val epNum = ep.optInt("episode_number", 0)
                                if (epNum <= 0) return@mapNotNull null
                                HikariEpisode(
                                    number = epNum,
                                    season = sn,
                                    id = "${media.id}|$sn|$epNum",
                                    name = ep.optString("name").ifBlank { "Episode $epNum" },
                                    image = tmdbImageUrl(ep.optString("still_path").ifBlank { null })
                                        ?: media.posterUrl,
                                )
                            }
                        } catch (_: Exception) {
                            emptyList()
                        }
                    }
                }
            }
            deferred.forEach { d -> runCatching { episodes.addAll(d.await()) } }
        }
        episodes.sortBy { it.season * 10000 + it.number }
        return episodes.ifEmpty { null }
    }

    private data class SiteEntry(
        val id: String,
        val invoke: suspend (JpPlayLink, (HikariSubtitle) -> Unit, (HikariStream) -> Unit) -> Unit,
    )

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val (kind, tmdbId) = splitRef(media.id) ?: return emptyList()
        val isMovie = kind == "movie"
        val season = if (isMovie) null else episode?.season?.takeIf { it > 0 } ?: 1
        val epNum = if (isMovie) null else episode?.number?.takeIf { it > 0 } ?: 1

        // one detail call for the canonical title + imdb id + year
        val detail = tmdbGet(
            "/$kind/$tmdbId",
            mapOf("language" to "en-US", "append_to_response" to "external_ids"),
        )
        val title = detail?.optString("title")?.ifBlank { detail.optString("name") }
            ?.takeIf { it.isNotBlank() } ?: media.title
        val orgTitle = detail?.optString("original_title")
            ?.ifBlank { detail.optString("original_name") }?.takeIf { it.isNotBlank() }
        val year = detail?.optString("release_date")?.ifBlank { detail.optString("first_air_date") }
            ?.take(4)?.toIntOrNull() ?: media.year
        val imdbId = detail?.optJSONObject("external_ids")?.optString("imdb_id")?.takeIf { it.isNotBlank() }

        val res = JpPlayLink(
            id = tmdbId,
            imdbId = imdbId,
            type = kind,
            season = season,
            episode = epNum,
            title = title,
            orgTitle = orgTitle,
            year = year,
            showYear = year,
            isMovie = isMovie,
        )

        val allSites = listOf(
            SiteEntry("netnaija") { r, s, c -> JpNetNaija.invoke(r, s, c) },
            SiteEntry("vegamovies") { r, s, c -> JpVegaMovies.invoke(r, s, c) },
            SiteEntry("hdhub4u") { r, s, c -> JpHdHub4u.invoke(r, s, c) },
            SiteEntry("4khdhub") { r, s, c -> Jp4kHub.invoke(r, s, c) },
            SiteEntry("themoviesflix") { r, s, c -> JpTmfSite.invoke(r, s, c) },
            SiteEntry("multimovies") { r, s, c -> JpMmSite.invoke(r, s, c) },
            SiteEntry("movies4u") { r, s, c -> JpMovies4u.invoke(r, s, c) },
            SiteEntry("moviesdrive") { r, s, c -> JpMoviesDrive.invoke(r, s, c) },
            SiteEntry("hindmoviez") { r, s, c -> JpHindMoviez.invoke(r, s, c) },
            SiteEntry("netmirror") { r, s, c -> JpNetMirror.invoke(r, s, c) },
        )

        val out = Collections.synchronizedList(mutableListOf<HikariStream>())
        val subs = Collections.synchronizedList(mutableListOf<HikariSubtitle>())
        val seenUrls = ConcurrentHashMap.newKeySet<String>()
        val sourceCounts = ConcurrentHashMap<String, AtomicInteger>()
        val subCb: (HikariSubtitle) -> Unit = { subs.add(it) }
        val guarded: (HikariStream) -> Unit = { link ->
            // stream-only: download sources have no player path in Hikari
            if (!JpFilter.isDownloadOnlyName(link.name)) {
                if (seenUrls.add(link.url)) {
                    val siteKey = link.name.substringBefore("]").trim('[', ' ', '-')
                    val emitted = sourceCounts.computeIfAbsent(siteKey) { AtomicInteger() }
                    if (emitted.incrementAndGet() <= LINKS_PER_SOURCE) out.add(link)
                }
            }
        }
        coroutineScope {
            val gate = Semaphore(6)
            allSites.forEach { site ->
                async(Dispatchers.IO) {
                    gate.withPermit {
                        try {
                            site.invoke(res, subCb, guarded)
                        } catch (_: Exception) {
                        }
                    }
                }
            }
        }
        if (subs.isNotEmpty() && out.isNotEmpty()) {
            val withSubs = out.map { it.copy(subtitles = it.subtitles + subs) }
            out.clear()
            out.addAll(withSubs)
        }
        return out.toList()
    }
}
