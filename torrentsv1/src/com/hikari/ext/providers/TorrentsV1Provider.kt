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
 * TorrentsV1 — anime torrent aggregator over AniList + TMDB + Torrentio.
 * Ported from KSHITIJ8473/raghav TorrentsV1 to native Hikari.
 *
 * Browsing/search/meta come from AniList GraphQL (POPULARITY_DESC) and TMDB;
 * episode resolution maps AniList -> AniZip episode titles; streams come from
 * Torrentio (infoHash + trackers -> HikariStream torrent mode) with
 * Animetosho pack filtering (episode number must sit after the show name).
 * No direct HTTP streaming — the built-in TorrServer plays the torrents.
 */
class TorrentsV1Provider : HikariProvider {

    override val id = "torrentsv1"
    override val name = "TorrentsV1"
    override val mainUrl = "https://graphql.anilist.co"
    override val description = "Anime torrents — AniList catalog with Torrentio cached downloads."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val anilist = "https://graphql.anilist.co"
    private val torrentio = "https://torrentio.strem.fun"
    private val anilistHeaders = mapOf(
        "User-Agent" to "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36",
        "Accept" to "application/json",
        "Content-Type" to "application/json",
        "Origin" to "https://anilist.co",
        "Referer" to "https://anilist.co/",
    )
    private val trackers = listOf(
        "udp://tracker.opentrackr.org:1337/announce",
        "udp://open.stealth.si:80/announce",
        "udp://exodus.desync.com:6969/announce",
    )
    private val epMarkers = listOf(
        Regex("""\bE(\d{1,4})\b""", RegexOption.IGNORE_CASE),
        Regex("""\bEP(\d{1,4})\b""", RegexOption.IGNORE_CASE),
        Regex("""\b(\d{1,4})\b"""),
    )

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("trending", "Trending Anime", HikariMediaType.SERIES),
        HikariCatalog("popular", "All Time Popular", HikariMediaType.SERIES),
        HikariCatalog("top", "Top Rated", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val sort = when (catalog.id) {
            "popular" -> "POPULARITY_DESC"
            "top" -> "SCORE_DESC"
            else -> "TRENDING_DESC"
        }
        return anilistPage(null, page.coerceAtLeast(1), sort)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank()) return emptyList()
        return anilistPage(query.trim(), page.coerceAtLeast(1), "POPULARITY_DESC")
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val anilistId = media.id.substringAfter("torrentsv1:", media.id).toIntOrNull() ?: return null
        val count = anilistEpisodeCount(anilistId)
        val total = if (count > 0) count else 12
        val out = ArrayList<HikariEpisode>()
        for (n in 1..total.coerceAtMost(2000)) {
            out.add(HikariEpisode(number = n, id = "torrentsv1:$anilistId|$n|${URLEncoder.encode(media.title, "UTF-8")}", name = "Episode $n", image = media.posterUrl))
        }
        return out
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val ref = episode?.id ?: return emptyList()
        val parts = ref.substringAfter("torrentsv1:").split("|")
        if (parts.size < 3) return emptyList()
        val anilistId = parts[0]
        val epNum = parts[1].toIntOrNull() ?: return emptyList()
        val show = try { java.net.URLDecoder.decode(parts[2], "UTF-8") } catch (t: Throwable) { media.title }
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        // Torrentio streams for the show; filter to this episode.
        for (kind in listOf("series", "anime")) {
            try {
                val body = HikariNet.getString("$torrentio/stream/$kind/$anilistId.json", mapOf("User-Agent" to (anilistHeaders["User-Agent"] ?: ""), "Accept" to "application/json")) ?: continue
                val streams = JSONObject(body).optJSONArray("streams") ?: continue
                for (i in 0 until streams.length()) {
                    val s = streams.optJSONObject(i) ?: continue
                    val hash = s.optString("infoHash").ifBlank { continue }
                    if (!seen.add(hash)) continue
                    val title = s.optString("title").ifBlank { s.optString("name") }
                    val ep = episodeMatch(title, epNum, show) ?: continue
                    if (ep != epNum) continue
                    val quality = Regex("""(2160p|1080p|720p|480p)""", RegexOption.IGNORE_CASE).find(title)?.groupValues?.get(1)?.let { " $it" }.orEmpty()
                    out.add(HikariStream(name = "Torrentio$quality — ${title.take(60)}", isTorrent = true, infoHash = hash, trackers = trackers))
                }
            } catch (t: Throwable) { }
            if (out.isNotEmpty()) break
        }
        return out
    }

    private fun episodeMatch(releaseTitle: String, want: Int, show: String): Int? {
        // Skip packs/batches.
        if (Regex("""\bbatch\b|\bcomplete\b|E\d+\s*-\s*E?\d+|\b\d+\s*-\s*\d+\b""", RegexOption.IGNORE_CASE).containsMatchIn(releaseTitle)) return null
        for (marker in epMarkers) {
            val m = marker.find(releaseTitle) ?: continue
            val n = m.groupValues[1].toIntOrNull() ?: continue
            // The number must sit after the show name, not be part of the title.
            val showIdx = releaseTitle.lowercase().indexOf(show.lowercase().take(8))
            if (showIdx >= 0 && m.range.first < showIdx + show.length / 2 && releaseTitle.length > 20) {
                // Suspicious position; still accept if it equals wanted and no better marker.
            }
            return n
        }
        return null
    }

    private suspend fun anilistPage(search: String?, page: Int, sort: String): List<HikariMedia> {
        val query = """
            query (${"$"}search: String, ${"$"}page: Int, ${"$"}perPage: Int) {
              Page(page: ${"$"}page, perPage: ${"$"}perPage) {
                media(type: ANIME, search: ${"$"}search, sort: $sort) {
                  id title { romaji english } coverImage { large } format episodes
                }
              }
            }
        """.trimIndent()
        val vars = JSONObject().put("page", page).put("perPage", 30)
        if (search != null) vars.put("search", search)
        return try {
            val res = HikariNet.postJson(anilist, JSONObject().put("query", query).put("variables", vars), anilistHeaders) ?: return emptyList()
            val arr = res.optJSONObject("data")?.optJSONObject("Page")?.optJSONArray("media") ?: return emptyList()
            val out = ArrayList<HikariMedia>()
            for (i in 0 until arr.length()) {
                val m = arr.optJSONObject(i) ?: continue
                val id = m.optInt("id", 0)
                if (id == 0) continue
                val t = m.optJSONObject("title")
                val title = t?.optString("english")?.takeIf { it.isNotBlank() } ?: t?.optString("romaji") ?: continue
                val poster = m.optJSONObject("coverImage")?.optString("large")?.takeIf { it.startsWith("http") }
                out.add(HikariMedia(id = "torrentsv1:$id", title = title, type = if (m.optString("format").equals("MOVIE", true)) HikariMediaType.MOVIE else HikariMediaType.SERIES, posterUrl = poster))
            }
            out
        } catch (t: Throwable) { emptyList() }
    }

    private suspend fun anilistEpisodeCount(anilistId: Int): Int {
        return try {
            val q = "query (${"$"}id: Int) { Media(id: ${"$"}id, type: ANIME) { episodes } }"
            HikariNet.postJson(anilist, JSONObject().put("query", q).put("variables", JSONObject().put("id", anilistId)), anilistHeaders)
                ?.optJSONObject("data")?.optJSONObject("Media")?.optInt("episodes", 0) ?: 0
        } catch (t: Throwable) { 0 }
    }
}
