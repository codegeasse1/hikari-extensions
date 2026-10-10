package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import java.net.URLEncoder
import org.json.JSONArray
import org.json.JSONObject

/**
 * TorrentsV1 — anime + movies/TV torrent aggregator.
 *
 * Native port of KSHITIJ8473/raghav TorrentsV1: 10 AniList rows (AniList
 * GraphQL) + 10 TMDB rows, AniZip episode mapping, and a 3-backend stream
 * fleet — Torrentio + TorrentsDB (per-episode Stremio stream URLs) +
 * Animetosho (AniDB-eid feed + title search, pack filtering so only releases
 * matching the picked show+episode reach the player). Torrent streams emit
 * infoHash+trackers for the built-in TorrServer; Animetosho magnets emit as
 * torrent links too.
 *
 * NOTE: the source reads its TMDB key from BuildConfig (compile-time secret,
 * not in the repo). TMDB rows reuse the public demo v3 key shipped in this
 * repo's Kdesa port; AniList rows need no key.
 */
class TorrentsV1Provider : HikariProvider {

    override val id = "torrentsv1"
    override val name = "TorrentsV1"
    override val mainUrl = "https://graphql.anilist.co"
    override val description = "Anime & TMDB catalogs with Torrentio / TorrentsDB / Animetosho cached torrents."
    override val version = 2
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private companion object {
        const val ANILIST = "https://graphql.anilist.co"
        const val ANIZIP = "https://api.ani.zip"
        const val TMDB_API = "https://api.themoviedb.org/3"
        const val TMDB_IMG = "https://image.tmdb.org/t/p/w500"
        // Public demo key vendored in this repo's Kdesa port (source repo's own
        // public key); the TorrentsV1 source keeps its key in BuildConfig.
        const val TMDB_KEY = "1865f43a0549ca50d341dd9ab8b29f49"
        const val TORRENTIO = "https://torrentio.strem.fun"
        const val TORRENTSDB = "https://torrentsdb.com"
        const val TORRENTSDB_CFG = "eyJsaW1pdCI6IjMiLCJkZWJyaWRvcHRpb25zIjpbIm5vZG93bmxvYWRsaW5rcyJdfQ=="
        const val ANIMETOSHO = "https://feed.animetosho.net"
        const val UA =
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    }

    private val anilistHeaders = mapOf(
        "User-Agent" to UA, "Accept" to "application/json",
        "Content-Type" to "application/json",
        "Origin" to "https://anilist.co", "Referer" to "https://anilist.co/",
    )
    private val trackers = listOf(
        "udp://tracker.opentrackr.org:1337/announce",
        "udp://open.stealth.si:80/announce",
        "udp://exodus.desync.com:6969/announce",
        "udp://tracker.torrent.eu.org:451/announce",
    )

    private val homeQuery = """
        query (${"$"}page: Int, ${"$"}perPage: Int, ${"$"}sort: [MediaSort], ${"$"}genreIn: [String], ${"$"}format: MediaFormat) {
          Page(page: ${"$"}page, perPage: ${"$"}perPage) {
            media(type: ANIME, sort: ${"$"}sort, genre_in: ${"$"}genreIn, format: ${"$"}format) {
              id title { romaji english } coverImage { large extraLarge }
              episodes seasonYear averageScore genres format status
            }
          }
        }
    """.trimIndent()

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("anilist_trending", "Anime: Trending", HikariMediaType.SERIES),
        HikariCatalog("anilist_popular", "Anime: Popular", HikariMediaType.SERIES),
        HikariCatalog("anilist_top", "Anime: Top Rated", HikariMediaType.SERIES),
        HikariCatalog("anilist_action", "Anime: Action", HikariMediaType.SERIES),
        HikariCatalog("anilist_fantasy", "Anime: Fantasy", HikariMediaType.SERIES),
        HikariCatalog("anilist_comedy", "Anime: Comedy", HikariMediaType.SERIES),
        HikariCatalog("anilist_drama", "Anime: Drama", HikariMediaType.SERIES),
        HikariCatalog("anilist_scifi", "Anime: Sci-Fi", HikariMediaType.SERIES),
        HikariCatalog("anilist_romance", "Anime: Romance", HikariMediaType.SERIES),
        HikariCatalog("anilist_movies", "Anime: Movies", HikariMediaType.MOVIE),
        HikariCatalog("tmdb_trending", "Trending Movies & TV", HikariMediaType.MOVIE),
        HikariCatalog("tmdb_popular_movies", "Popular Movies", HikariMediaType.MOVIE),
        HikariCatalog("tmdb_popular_tv", "Popular TV Shows", HikariMediaType.SERIES),
        HikariCatalog("tmdb_top_movies", "Top Rated Movies", HikariMediaType.MOVIE),
        HikariCatalog("tmdb_top_tv", "Top Rated TV Shows", HikariMediaType.SERIES),
        HikariCatalog("tmdb_netflix", "Netflix", HikariMediaType.SERIES),
        HikariCatalog("tmdb_amazon", "Amazon Prime", HikariMediaType.SERIES),
        HikariCatalog("tmdb_disney", "Disney+", HikariMediaType.SERIES),
        HikariCatalog("tmdb_hbo", "HBO", HikariMediaType.SERIES),
        HikariCatalog("tmdb_korean", "Korean Shows", HikariMediaType.SERIES),
    )

    // -------------------------------------------------------------- catalogs

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val p = page.coerceAtLeast(1)
        return try {
            if (catalog.id.startsWith("anilist_")) anilistHome(catalog.id, p)
            else tmdbHome(catalog.id, p)
        } catch (_: Exception) {
            emptyList()
        }
    }

    private suspend fun anilistHome(data: String, page: Int): List<HikariMedia> {
        val vars = JSONObject().put("page", page).put("perPage", 20)
        when (data) {
            "anilist_trending" -> vars.put("sort", JSONArray().put("TRENDING_DESC").put("POPULARITY_DESC"))
            "anilist_popular" -> vars.put("sort", JSONArray().put("POPULARITY_DESC"))
            "anilist_top" -> vars.put("sort", JSONArray().put("SCORE_DESC"))
            "anilist_movies" -> {
                vars.put("sort", JSONArray().put("POPULARITY_DESC"))
                vars.put("format", "MOVIE")
            }
            "anilist_action" -> {
                vars.put("sort", JSONArray().put("TRENDING_DESC"))
                vars.put("genreIn", JSONArray().put("Action"))
            }
            "anilist_fantasy" -> {
                vars.put("sort", JSONArray().put("TRENDING_DESC"))
                vars.put("genreIn", JSONArray().put("Fantasy"))
            }
            "anilist_comedy" -> {
                vars.put("sort", JSONArray().put("TRENDING_DESC"))
                vars.put("genreIn", JSONArray().put("Comedy"))
            }
            "anilist_drama" -> {
                vars.put("sort", JSONArray().put("TRENDING_DESC"))
                vars.put("genreIn", JSONArray().put("Drama"))
            }
            "anilist_scifi" -> {
                vars.put("sort", JSONArray().put("TRENDING_DESC"))
                vars.put("genreIn", JSONArray().put("Sci-Fi"))
            }
            "anilist_romance" -> {
                vars.put("sort", JSONArray().put("TRENDING_DESC"))
                vars.put("genreIn", JSONArray().put("Romance"))
            }
            else -> return emptyList()
        }
        val res = HikariNet.postJson(ANILIST, JSONObject().put("query", homeQuery).put("variables", vars), anilistHeaders)
            ?: return emptyList()
        val arr = res.optJSONObject("data")?.optJSONObject("Page")?.optJSONArray("media")
            ?: return emptyList()
        val out = ArrayList<HikariMedia>()
        for (i in 0 until arr.length()) {
            val m = arr.optJSONObject(i) ?: continue
            val id = m.optInt("id", 0)
            if (id == 0) continue
            val t = m.optJSONObject("title")
            val title = t?.optString("english")?.takeIf { it.isNotBlank() }
                ?: t?.optString("romaji") ?: continue
            val poster = (m.optJSONObject("coverImage")?.optString("extraLarge")
                ?: m.optJSONObject("coverImage")?.optString("large"))?.takeIf { it.startsWith("http") }
            val movie = m.optString("format").equals("MOVIE", true)
            val payload = JSONObject().put("source", "anilist").put("anilistId", id)
                .put("title", title).put("format", if (movie) "MOVIE" else "TV")
            out.add(
                HikariMedia(
                    id = "tv1:" + URLEncoder.encode(payload.toString(), "UTF-8"),
                    title = title,
                    type = if (movie) HikariMediaType.MOVIE else HikariMediaType.SERIES,
                    posterUrl = poster, year = m.optInt("seasonYear", 0).takeIf { it > 0 },
                ),
            )
        }
        return out
    }

    private suspend fun tmdbHome(data: String, page: Int): List<HikariMedia> {
        val path = when (data) {
            "tmdb_trending" -> "/trending/all/day?page=$page"
            "tmdb_popular_movies" -> "/movie/popular?page=$page"
            "tmdb_popular_tv" -> "/tv/popular?page=$page"
            "tmdb_top_movies" -> "/movie/top_rated?page=$page"
            "tmdb_top_tv" -> "/tv/top_rated?page=$page"
            "tmdb_netflix" -> "/discover/tv?page=$page&with_networks=213"
            "tmdb_amazon" -> "/discover/tv?page=$page&with_networks=1024"
            "tmdb_disney" -> "/discover/tv?page=$page&with_networks=2739"
            "tmdb_hbo" -> "/discover/tv?page=$page&with_networks=49"
            "tmdb_korean" -> "/discover/tv?page=$page&with_original_language=ko&sort_by=popularity.desc"
            else -> return emptyList()
        }
        val txt = HikariNet.getString(
            "$TMDB_API$path&api_key=$TMDB_KEY&language=en-US",
            mapOf("Accept" to "application/json", "User-Agent" to UA),
        ) ?: return emptyList()
        val arr = JSONObject(txt).optJSONArray("results") ?: return emptyList()
        val out = ArrayList<HikariMedia>()
        for (i in 0 until arr.length()) {
            val o = arr.optJSONObject(i) ?: continue
            val type = o.optString("media_type").ifBlank {
                if (data.contains("tv") || data == "tmdb_trending") "" else if (data.contains("movie")) "movie" else ""
            }
            val kind = when {
                type == "movie" || (type.isBlank() && data.contains("movie")) -> "movie"
                type == "tv" || (type.isBlank() && (data.contains("tv") || data == "tmdb_netflix" || data == "tmdb_amazon" || data == "tmdb_disney" || data == "tmdb_hbo" || data == "tmdb_korean")) -> "tv"
                data == "tmdb_trending" -> o.optString("media_type").takeIf { it == "movie" || it == "tv" } ?: continue
                else -> continue
            }
            val id = o.optInt("id", 0)
            if (id == 0) continue
            val title = if (kind == "movie") o.optString("title").ifBlank { o.optString("name") }
            else o.optString("name").ifBlank { o.optString("title") }
            if (title.isBlank()) continue
            val poster = o.optString("poster_path").takeIf { it.isNotBlank() }?.let { TMDB_IMG + it }
            val payload = JSONObject().put("source", "tmdb").put("tmdbId", id)
                .put("tmdbType", kind).put("title", title)
                .put("format", if (kind == "movie") "MOVIE" else "TV")
            out.add(
                HikariMedia(
                    id = "tv1:" + URLEncoder.encode(payload.toString(), "UTF-8"),
                    title = title,
                    type = if (kind == "movie") HikariMediaType.MOVIE else HikariMediaType.SERIES,
                    posterUrl = poster,
                    year = (o.optString("release_date").ifBlank { o.optString("first_air_date") }).take(4).toIntOrNull(),
                ),
            )
        }
        return out
    }

    // ---------------------------------------------------------------- search

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank()) return emptyList()
        val out = ArrayList<HikariMedia>()
        try {
            out.addAll(anilistSearch(query.trim(), page.coerceAtLeast(1)))
        } catch (_: Exception) {
        }
        if (out.size < 8) {
            try {
                out.addAll(tmdbSearch(query.trim(), 1))
            } catch (_: Exception) {
            }
        }
        return out
    }

    private suspend fun anilistSearch(q: String, page: Int): List<HikariMedia> {
        val query = """
            query (${"$"}search: String, ${"$"}page: Int, ${"$"}perPage: Int) {
              Page(page: ${"$"}page, perPage: ${"$"}perPage) {
                media(type: ANIME, search: ${"$"}search, sort: POPULARITY_DESC) {
                  id title { romaji english } coverImage { large extraLarge }
                  episodes seasonYear averageScore genres format status
                }
              }
            }
        """.trimIndent()
        val vars = JSONObject().put("search", q).put("page", page).put("perPage", 20)
        val res = HikariNet.postJson(ANILIST, JSONObject().put("query", query).put("variables", vars), anilistHeaders)
            ?: return emptyList()
        val arr = res.optJSONObject("data")?.optJSONObject("Page")?.optJSONArray("media")
            ?: return emptyList()
        val out = ArrayList<HikariMedia>()
        for (i in 0 until arr.length()) {
            val m = arr.optJSONObject(i) ?: continue
            val id = m.optInt("id", 0)
            if (id == 0) continue
            val t = m.optJSONObject("title")
            val title = t?.optString("english")?.takeIf { it.isNotBlank() }
                ?: t?.optString("romaji") ?: continue
            val poster = (m.optJSONObject("coverImage")?.optString("extraLarge")
                ?: m.optJSONObject("coverImage")?.optString("large"))?.takeIf { it.startsWith("http") }
            val movie = m.optString("format").equals("MOVIE", true)
            val payload = JSONObject().put("source", "anilist").put("anilistId", id)
                .put("title", title).put("format", if (movie) "MOVIE" else "TV")
            out.add(
                HikariMedia(
                    id = "tv1:" + URLEncoder.encode(payload.toString(), "UTF-8"),
                    title = title,
                    type = if (movie) HikariMediaType.MOVIE else HikariMediaType.SERIES,
                    posterUrl = poster, year = m.optInt("seasonYear", 0).takeIf { it > 0 },
                ),
            )
        }
        return out
    }

    private suspend fun tmdbSearch(q: String, page: Int): List<HikariMedia> {
        val txt = HikariNet.getString(
            "$TMDB_API/search/multi?query=${URLEncoder.encode(q, "UTF-8")}&page=$page&include_adult=false&api_key=$TMDB_KEY&language=en-US",
            mapOf("Accept" to "application/json", "User-Agent" to UA),
        ) ?: return emptyList()
        val arr = JSONObject(txt).optJSONArray("results") ?: return emptyList()
        val out = ArrayList<HikariMedia>()
        for (i in 0 until arr.length()) {
            val o = arr.optJSONObject(i) ?: continue
            val kind = o.optString("media_type").takeIf { it == "movie" || it == "tv" } ?: continue
            val id = o.optInt("id", 0)
            if (id == 0) continue
            val title = (if (kind == "movie") o.optString("title") else o.optString("name")).ifBlank { continue }
            val poster = o.optString("poster_path").takeIf { it.isNotBlank() }?.let { TMDB_IMG + it }
            val payload = JSONObject().put("source", "tmdb").put("tmdbId", id)
                .put("tmdbType", kind).put("title", title)
                .put("format", if (kind == "movie") "MOVIE" else "TV")
            out.add(
                HikariMedia(
                    id = "tv1:" + URLEncoder.encode(payload.toString(), "UTF-8"),
                    title = title,
                    type = if (kind == "movie") HikariMediaType.MOVIE else HikariMediaType.SERIES,
                    posterUrl = poster,
                ),
            )
        }
        return out
    }

    // -------------------------------------------------------------- episodes

    private fun payloadOf(media: HikariMedia): JSONObject? = try {
        JSONObject(java.net.URLDecoder.decode(media.id.removePrefix("tv1:"), "UTF-8"))
    } catch (_: Exception) {
        null
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val p = payloadOf(media) ?: return null
        return try {
            if (p.optString("source") == "tmdb") {
                val kind = p.optString("tmdbType", "movie")
                val tmdbId = p.optInt("tmdbId", 0)
                if (tmdbId == 0) return null
                val title = p.optString("title")
                if (kind == "movie") {
                    val eid = "tv1:" + URLEncoder.encode(
                        JSONObject().put("source", "tmdb").put("tmdbId", tmdbId)
                            .put("tmdbType", "movie").put("title", title)
                            .put("format", "MOVIE").put("episode", 1).toString(), "UTF-8",
                    )
                    return listOf(HikariEpisode(number = 1, id = eid, name = title, image = media.posterUrl))
                }
                val txt = HikariNet.getString(
                    "$TMDB_API/tv/$tmdbId?api_key=$TMDB_KEY&language=en-US",
                    mapOf("Accept" to "application/json", "User-Agent" to UA),
                ) ?: return null
                val seasons = JSONObject(txt).optJSONArray("seasons") ?: return null
                val out = ArrayList<HikariEpisode>()
                for (i in 0 until seasons.length()) {
                    val s = seasons.optJSONObject(i) ?: continue
                    val sn = s.optInt("season_number", -1)
                    val count = s.optInt("episode_count", 0)
                    if (sn <= 0 || count <= 0) continue
                    for (ep in 1..count) {
                        val eid = "tv1:" + URLEncoder.encode(
                            JSONObject().put("source", "tmdb").put("tmdbId", tmdbId)
                                .put("tmdbType", "tv").put("title", title)
                                .put("format", "TV").put("season", sn).put("episode", ep).toString(), "UTF-8",
                        )
                        out.add(HikariEpisode(number = ep, season = sn, id = eid, name = "Episode $ep", image = media.posterUrl))
                    }
                }
                out.sortWith(compareBy({ it.season }, { it.number }))
                out.ifEmpty { null }
            } else {
                val anilistId = p.optInt("anilistId", 0)
                if (anilistId == 0) return null
                val title = p.optString("title")
                val movie = p.optString("format") == "MOVIE"
                val total = anilistEpisodeCount(anilistId)
                if (movie || total <= 1) {
                    val eid = "tv1:" + URLEncoder.encode(
                        JSONObject().put("source", "anilist").put("anilistId", anilistId)
                            .put("title", title).put("format", "MOVIE").put("episode", 1).toString(), "UTF-8",
                    )
                    return listOf(HikariEpisode(number = 1, id = eid, name = title, image = media.posterUrl))
                }
                (1..total.coerceAtMost(2000)).map { n ->
                    val eid = "tv1:" + URLEncoder.encode(
                        JSONObject().put("source", "anilist").put("anilistId", anilistId)
                            .put("title", title).put("format", "TV").put("episode", n).toString(), "UTF-8",
                    )
                    HikariEpisode(number = n, id = eid, name = "Episode $n", image = media.posterUrl)
                }
            }
        } catch (_: Exception) {
            null
        }
    }

    private suspend fun anilistEpisodeCount(anilistId: Int): Int {
        return try {
            val q = "query (${"$"}id: Int) { Media(id: ${"$"}id, type: ANIME) { episodes } }"
            HikariNet.postJson(ANILIST, JSONObject().put("query", q).put("variables", JSONObject().put("id", anilistId)), anilistHeaders)
                ?.optJSONObject("data")?.optJSONObject("Media")?.optInt("episodes", 0) ?: 0
        } catch (_: Exception) {
            0
        }
    }

    // --------------------------------------------------------------- streams

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val ref = episode?.id ?: return emptyList()
        if (!ref.startsWith("tv1:")) return emptyList()
        val ep = try {
            JSONObject(java.net.URLDecoder.decode(ref.removePrefix("tv1:"), "UTF-8"))
        } catch (_: Exception) {
            return emptyList()
        }
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        try {
            // AniZip enriches AniList items with kitsu/imdb/anidb ids.
            var kitsuId: Int? = null
            var imdbId: String? = null
            var anidbEid: Int? = null
            val anilistId = ep.optInt("anilistId", 0)
            if (anilistId > 0) {
                try {
                    val zip = HikariNet.getJson("$ANIZIP/mappings?anilist_id=$anilistId") ?: JSONObject()
                    kitsuId = zip.optJSONObject("mappings")?.optInt("kitsu_id", 0)?.takeIf { it > 0 }
                    imdbId = zip.optString("imdb_id").ifBlank { null }
                        ?: zip.optJSONObject("mappings")?.optString("imdb_id")?.ifBlank { null }
                    val epsMap = zip.optJSONObject("episodes")
                    val epNum = ep.optInt("episode", 1)
                    val node = epsMap?.optJSONObject(epNum.toString())
                    anidbEid = node?.optInt("anidb_eid", 0)?.takeIf { it > 0 }
                    if (ep.optString("title").isBlank()) {
                        // keep original title
                    }
                } catch (_: Exception) {
                }
            }
            val title = (media.title.ifBlank { null } ?: ep.optString("title")).ifBlank { return emptyList() }
            val epNum = ep.optInt("episode", 1)
            val season = ep.optInt("season", 1)
            val isMovie = ep.optString("format") == "MOVIE"
            val stremioId: String? = when {
                isMovie -> imdbId ?: tmdbImdb(ep)
                kitsuId != null -> "kitsu:$kitsuId"
                imdbId != null -> imdbId
                else -> tmdbImdb(ep)
            }
            if (stremioId != null) {
                fetchStremio("$TORRENTIO/limit=4", stremioId, epNum, season, isMovie, "Torrentio", out, seen)
                fetchStremio("$TORRENTSDB/$TORRENTSDB_CFG", stremioId, epNum, season, isMovie, "TorrentsDB", out, seen)
            }
            if (anilistId > 0) {
                animetosho(anilistId, anidbEid, title, epNum, out, seen)
            }
        } catch (_: Exception) {
        }
        return out
    }

    private suspend fun tmdbImdb(ep: JSONObject): String? {
        // TMDB detail carries imdb_id; one extra call at stream time.
        return try {
            val tmdbId = ep.optInt("tmdbId", 0)
            if (tmdbId == 0) return null
            val kind = ep.optString("tmdbType", "movie")
            val txt = HikariNet.getString(
                "$TMDB_API/$kind/$tmdbId?api_key=$TMDB_KEY&language=en-US",
                mapOf("Accept" to "application/json", "User-Agent" to UA),
            ) ?: return null
            JSONObject(txt).optString("imdb_id").ifBlank { null }
        } catch (_: Exception) {
            null
        }
    }

    private suspend fun fetchStremio(
        base: String, id: String, ep: Int, season: Int, isMovie: Boolean,
        label: String, out: MutableList<HikariStream>, seen: HashSet<String>,
    ) {
        val url = if (isMovie) "$base/stream/movie/$id.json"
        else "$base/stream/series/$id:$season:$ep.json"
        // Anime entries addressed by kitsu id take plain episode form.
        val urls = if (!isMovie && id.startsWith("kitsu:")) {
            listOf("$base/stream/series/$id:$ep.json", url)
        } else listOf(url)
        for (u in urls) {
            try {
                val root = HikariNet.getJson(u) ?: continue
                val arr = root.optJSONArray("streams") ?: continue
                for (i in 0 until arr.length()) {
                    val s = arr.optJSONObject(i) ?: continue
                    val hash = s.optString("infoHash").ifBlank { continue }
                    if (!seen.add("h$hash")) continue
                    val st = s.optString("title").ifBlank { s.optString("name") }
                    val q = Regex("(2160p|1080p|720p|480p)", RegexOption.IGNORE_CASE)
                        .find(st)?.groupValues?.get(1)?.let { " $it" }.orEmpty()
                    out.add(
                        HikariStream(
                            name = "$label$q — ${st.take(60)}",
                            isTorrent = true, infoHash = hash, trackers = trackers,
                        ),
                    )
                }
                if (out.isNotEmpty()) return
            } catch (_: Exception) {
            }
        }
    }

    private suspend fun animetosho(
        anilistId: Int, anidbEid: Int?, title: String, epNum: Int,
        out: MutableList<HikariStream>, seen: HashSet<String>,
    ) {
        // Episode feed picks up batches/specials of the same franchise; only
        // releases fitting the picked show+episode reach the player.
        if (anidbEid != null) {
            try {
                val root = HikariNet.getJson("$ANIMETOSHO/json/v1/episodes/$anidbEid") ?: JSONObject()
                val items = root.optJSONObject("data")?.optJSONArray("releases") ?: JSONArray()
                var added = 0
                for (i in 0 until items.length()) {
                    val r = items.optJSONObject(i) ?: continue
                    if (r.optString("magnet").isBlank() || r.optBoolean("is_batch")) continue
                    if ((r.optInt("seeders", 0)) <= 0) continue
                    if (!matchesEpisode(r.optString("title"), title, epNum)) continue
                    val magnet = r.optString("magnet")
                    if (!seen.add("m$magnet")) continue
                    val q = qualityOf(r.optString("title"))
                    out.add(HikariStream(name = "Animetosho$q | ${r.optInt("seeders")} | ${r.optString("title").take(50)}", url = magnet, isTorrent = true, trackers = trackers))
                    added++
                }
                if (added > 0) return
            } catch (_: Exception) {
            }
        }
        for (q in listOf(title, compactSeasonTitle(title)).distinct()) {
            if (q.isBlank()) continue
            try {
                val root = HikariNet.getJson(
                    "$ANIMETOSHO/json/v1/releases?q=${URLEncoder.encode(q, "UTF-8")}",
                    mapOf("User-Agent" to UA),
                ) ?: continue
                val items = root.optJSONArray("data") ?: continue
                for (i in 0 until items.length()) {
                    val r = items.optJSONObject(i) ?: continue
                    if (r.optString("magnet").isBlank() || r.optBoolean("is_batch")) continue
                    if (!matchesEpisode(r.optString("title"), title, epNum)) continue
                    val magnet = r.optString("magnet")
                    if (!seen.add("m$magnet")) continue
                    val ql = qualityOf(r.optString("title"))
                    out.add(HikariStream(name = "Animetosho$ql | ${r.optInt("seeders")} | ${r.optString("title").take(50)}", url = magnet, isTorrent = true, trackers = trackers))
                    if (out.size >= 40) return
                }
                if (out.isNotEmpty()) return
            } catch (_: Exception) {
            }
        }
    }

    private fun compactSeasonTitle(t: String): String =
        t.replace(Regex("(?i)\\s+season\\s+(\\d+)"), " S$1")
            .replace(Regex("(?i)\\s+(second|third|fourth|fifth)\\s+season"), "")

    private fun qualityOf(t: String): String =
        Regex("(2160p|1080p|720p|480p|360p)", RegexOption.IGNORE_CASE)
            .find(t)?.groupValues?.get(1)?.let { " $it" }.orEmpty()

    private fun matchesEpisode(releaseTitle: String, show: String, want: Int): Boolean {
        if (releaseTitle.isBlank()) return false
        // Skip packs/batches/ranges.
        if (Regex("\\bbatch\\b|\\bcomplete\\b|E\\d+\\s*-\\s*E?\\d+|\\b\\d+\\s*-\\s*\\d+\\b", RegexOption.IGNORE_CASE)
                .containsMatchIn(releaseTitle)
        ) return false
        for (marker in listOf(
            Regex("\\bE(\\d{1,4})\\b", RegexOption.IGNORE_CASE),
            Regex("\\bEP(\\d{1,4})\\b", RegexOption.IGNORE_CASE),
            Regex("\\bS\\d{1,2}\\s*E(\\d{1,4})\\b", RegexOption.IGNORE_CASE),
            Regex("(?i)(?:episode\\s+|#)(\\d{1,4})"),
            Regex("\\b(\\d{1,4})\\b"),
        )) {
            val m = marker.find(releaseTitle) ?: continue
            val n = m.groupValues[1].toIntOrNull() ?: continue
            // The number must not be part of the show title itself.
            val head = show.take(8).lowercase()
            if (head.isNotBlank() && releaseTitle.lowercase().startsWith(head)) {
                val showEnd = releaseTitle.lowercase().indexOf(head) + show.length / 2
                if (m.range.first < showEnd && releaseTitle.length > 24) continue
            }
            return n == want
        }
        return false
    }
}
