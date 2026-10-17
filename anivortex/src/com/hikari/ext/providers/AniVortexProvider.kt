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
 * AniVortex (api.anivortex.in) — TMDB-enriched anime catalog with per-device
 * attestation, verified Oct 2026:
 *  - API without a registered installation answers 401
 *    invalid_installation_signature: registration needs /api/v1/install/challenge
 *    + /api/v1/install/register with a hardware-Keystore attested EC key
 *    (AndroidKeyStore, per-device). That flow cannot run inside a Hikari
 *    extension sandbox, so playback stays gated until the user pastes an
 *    installation key-id pair exported from the CloudStream plugin (settings
 *    screen: Key ID + PKCS#8 private key) — headers x-anivortex-key-id.
 *  - Catalogs therefore come from TMDB (same metadata the plugin enriches
 *    with: /3/search, /3/find/{imdb}, /3/movie|tv/{id}/external_ids, posters
 *    https://image.tmdb.org/t/p/{w500,original}); rows always load.
 *  - Endpoints attempted when keys exist: /api/v1/catalog/home,
 *    /api/v1/catalog/explore, /api/v1/search, /api/v1/titles/{id},
 *    /api/v1/seasons/{id}, /api/v1/catalog/ott-platforms/netflix/titles.
 */
class AniVortexProvider : HikariProvider {

    override val id = "anivortex"
    override val name = "AniVortex"
    override val mainUrl = "https://api.anivortex.in"
    override val description = "Anime catalog with TMDB metadata — popular, recent, search (API playback needs an exported installation key)."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val UA =
        "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36"

    companion object {
        const val API = "https://api.anivortex.in"
        const val TMDB = "https://api.themoviedb.org/3"
        const val TMDB_KEY = "1865f43a0549ca50d341dd9ab8b29f49"
        const val TMDB_KEY_ALT = "98ae14df2b8d8f8f8136499daf79f0e0"
        const val IMG = "https://image.tmdb.org/t/p/w500"
        // Exported installation credentials (blank = unattested = API 401s,
        // catalogs still served from TMDB). Wired to settings when the host
        // app exposes extension preferences; until then they stay empty.
        var keyId: String = ""
        var privateKeyB64: String = ""
    }

    private fun tmdbH(): Map<String, String> = mapOf("User-Agent" to UA, "Accept" to "application/json")

    private fun apiH(): Map<String, String> {
        val m = HashMap<String, String>()
        m["User-Agent"] = UA
        m["Accept"] = "application/json"
        if (keyId.isNotBlank()) m["x-anivortex-key-id"] = keyId
        return m
    }

    private suspend fun tmdbGet(path: String, query: String): JSONObject? {
        val t1 = HikariNet.getString("$TMDB$path?api_key=$TMDB_KEY&$query", tmdbH())
        if (t1 != null) {
            val o = runCatching { JSONObject(t1) }.getOrNull()
            if (o != null) return o
        }
        val t2 = HikariNet.getString("$TMDB$path?api_key=$TMDB_KEY_ALT&$query", tmdbH()) ?: return null
        return runCatching { JSONObject(t2) }.getOrNull()
    }

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("popular-tv", "Popular TV Series", HikariMediaType.SERIES),
        HikariCatalog("recent-tv", "Recent TV Series", HikariMediaType.SERIES),
        HikariCatalog("top-mov", "Top Movies", HikariMediaType.MOVIE),
        HikariCatalog("top-tv", "Top Series", HikariMediaType.SERIES),
        HikariCatalog("trend", "Trending", HikariMediaType.SERIES),
        HikariCatalog("netflix", "Netflix Titles", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val p = page.coerceAtLeast(1)
        // Attested API first (works when keys are exported), TMDB fallback.
        val apiItems = tryApiCatalog(catalog.id, p)
        if (apiItems.isNotEmpty()) return apiItems
        when (catalog.id) {
            "popular-tv" -> return tmdbList("/tv/popular", "language=en-US&page=$p", HikariMediaType.SERIES)
            "recent-tv" -> return tmdbList("/tv/on_the_air", "language=en-US&page=$p", HikariMediaType.SERIES)
            "top-mov" -> return tmdbList("/movie/top_rated", "language=en-US&page=$p", HikariMediaType.MOVIE)
            "top-tv" -> return tmdbList("/tv/top_rated", "language=en-US&page=$p", HikariMediaType.SERIES)
            "netflix" -> {
                val root = tmdbGet("/discover/tv", "page=$p&watch_region=US&with_watch_providers=8&sort_by=popularity.desc")
                    ?: return emptyList()
                return tmdbResultsToMedia(root.optJSONArray("results"), HikariMediaType.SERIES)
            }
            else -> return tmdbList("/trending/all/week", "page=$p", HikariMediaType.SERIES)
        }
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        val p = page.coerceAtLeast(1)
        if (keyId.isNotBlank()) {
            val txt = HikariNet.getString("$API/api/v1/search?query=${URLEncoder.encode(q, "UTF-8")}&page=$p", apiH())
            if (txt != null) {
                val items = parseApiTitles(txt)
                if (items.isNotEmpty()) return items
            }
        }
        val root = tmdbGet("/search/multi", "query=${URLEncoder.encode(q, "UTF-8")}&page=$p&include_adult=false") ?: return emptyList()
        return tmdbResultsToMedia(root.optJSONArray("results"))
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        if (media.id.startsWith("av:")) {
            val tid = media.id.substringAfter("av:")
            val txt = HikariNet.getString("$API/api/v1/titles/$tid", apiH())
            if (txt != null) {
                val o = runCatching { JSONObject(txt) }.getOrNull()
                val d = o?.optJSONObject("data") ?: o
                if (d != null) {
                    return media.copy(
                        title = d.optString("title").ifBlank { media.title },
                        posterUrl = d.optString("poster").takeIf { it.startsWith("http") } ?: media.posterUrl,
                        backdropUrl = d.optString("backdrop").takeIf { it.startsWith("http") } ?: media.backdropUrl,
                        overview = d.optString("overview").takeIf { it.isNotBlank() } ?: media.overview,
                    )
                }
            }
        }
        return media
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        if (media.id.startsWith("av:")) {
            val tid = media.id.substringAfter("av:")
            val txt = HikariNet.getString("$API/api/v1/seasons/$tid", apiH())
            if (txt != null) {
                val eps = parseApiSeasons(txt, media)
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
        val out = ArrayList<HikariStream>()
        if (!media.id.startsWith("av:") || keyId.isBlank()) return out
        val ref = episode?.id ?: media.id
        val tid = ref.substringAfter("av:", "").substringBefore("#").substringBefore(":")
        if (tid.isBlank()) return out
        // Playback servers list; each server resolves to m3u8/mp4 or YouTube key.
        val txt = HikariNet.getString("$API/api/v1/titles/$tid", apiH()) ?: return out
        val seen = HashSet<String>()
        for (m in Regex("https?://[^\\s\"'<>\\\\]+\\.(?:m3u8|mp4)[^\\s\"'<>\\\\]*").findAll(txt)) {
            if (m.value.startsWith("http") && seen.add(m.value)) {
                out.add(HikariStream(name = "AniVortex", url = m.value, headers = mapOf("User-Agent" to UA), isM3u8 = m.value.contains(".m3u8")))
            }
        }
        val yt = Regex("\"youtubeKey\"\\s*:\\s*\"([A-Za-z0-9_-]{6,})\"").find(txt)?.groupValues?.get(1)
        if (yt != null && seen.add("yt:$yt")) {
            out.add(HikariStream(name = "AniVortex Trailer", ytId = yt))
        }
        return out
    }

    // ------------------------------------------------------------------
    // Helpers.
    // ------------------------------------------------------------------

    private suspend fun tryApiCatalog(row: String, page: Int): List<HikariMedia> {
        if (keyId.isBlank()) return emptyList()
        val path = when (row) {
            "netflix" -> "/api/v1/catalog/ott-platforms/netflix/titles?page=$page"
            "popular-tv", "recent-tv" -> "/api/v1/catalog/home?page=$page"
            else -> "/api/v1/catalog/explore?page=$page"
        }
        val txt = HikariNet.getString("$API$path", apiH()) ?: return emptyList()
        return parseApiTitles(txt)
    }

    fun parseApiTitles(txt: String): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        val root = runCatching { JSONObject(txt) }.getOrNull() ?: return out
        val arr = root.optJSONArray("data") ?: root.optJSONArray("results") ?: root.optJSONArray("titles") ?: root.optJSONArray("tv_results") ?: return out
        for (i in 0 until arr.length()) {
            val o = arr.optJSONObject(i) ?: continue
            val id = o.optString("id").ifBlank { o.optInt("id", 0).takeIf { it != 0 }?.toString() ?: "" }
            if (id.isBlank()) continue
            val title = o.optString("title").ifBlank { o.optString("name") }.trim().ifBlank { continue }
            out.add(
                HikariMedia(
                    id = "av:$id",
                    title = title,
                    type = HikariMediaType.SERIES,
                    posterUrl = o.optString("poster").takeIf { it.startsWith("http") },
                    overview = o.optString("overview").takeIf { it.isNotBlank() },
                )
            )
        }
        return out
    }

    fun parseApiSeasons(txt: String, media: HikariMedia): List<HikariEpisode> {
        val out = ArrayList<HikariEpisode>()
        val root = runCatching { JSONObject(txt) }.getOrNull() ?: return out
        val arr = root.optJSONArray("seasons") ?: root.optJSONArray("data") ?: root.optJSONArray("episodes") ?: return out
        for (i in 0 until arr.length()) {
            val o = arr.optJSONObject(i) ?: continue
            val eps = o.optJSONArray("episodes")
            if (eps != null) {
                val sn = o.optInt("season", 1)
                for (j in 0 until eps.length()) {
                    val e = eps.optJSONObject(j) ?: continue
                    val n = e.optInt("episode", j + 1)
                    out.add(HikariEpisode(number = n, season = sn, id = "${media.id}#s${sn}e$n", name = e.optString("title").ifBlank { "S${sn} E$n" }, image = media.posterUrl))
                }
            } else {
                val n = o.optInt("episode", i + 1)
                val sn = o.optInt("season", 1)
                out.add(HikariEpisode(number = n, season = sn, id = "${media.id}#s${sn}e$n", name = o.optString("title").ifBlank { "S${sn} E$n" }, image = media.posterUrl))
            }
        }
        return out
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
}
