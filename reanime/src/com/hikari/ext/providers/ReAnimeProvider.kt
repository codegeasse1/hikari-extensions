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
 * ReAnime (reanime.to) — anime via /api/v1 + FlixCloud servers.
 * Ported from KSHITIJ8473/raghav ReAnime to native Hikari.
 *
 *  GET /api/v1/search?q=&limit=&offset=[&sort=popularity_desc]
 *  GET /api/v1/home/{section}?limit=  home rows (latest-aired, etc.)
 *  GET /api/v1/anime/{slug}           detail
 *  GET /api/v1/anime/{slug}/episodes?limit=2000  episodes (episodeNumber, subbed, dubbed)
 *  GET /api/flix/{anilistId}/{ep} (or /api/flix/0/{ep}?tmdb=&season=) -> servers
 * Embed base: https://flixcloud.cc — resolved via WebView capture.
 */
class ReAnimeProvider : HikariProvider {

    override val id = "reanime"
    override val name = "Re:ANIME"
    override val mainUrl = "https://reanime.to"
    override val description = "Anime from ReAnime — latest aired, popular, movies, sub & dub."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val flixBase = "https://flixcloud.cc"
    private val apiHeaders = mapOf(
        "User-Agent" to "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36",
        "Accept" to "application/json, text/plain, */*",
        "Referer" to "$mainUrl/",
    )
    private val capture = Regex("""https?://[^"' \s]+\.m3u8[^"' \s]*|https?://[^"' \s]+\.mp4[^"' \s]*""")

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("latest-aired", "Latest Episodes", HikariMediaType.SERIES),
        HikariCatalog("trending", "Trending", HikariMediaType.SERIES),
        HikariCatalog("popular", "Most Popular", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val p = page.coerceAtLeast(1)
        val offset = (p - 1) * 40
        if (catalog.id == "popular") {
            val body = HikariNet.getString("$mainUrl/api/v1/search?limit=40&offset=$offset&sort=popularity_desc", apiHeaders) ?: return emptyList()
            return parseSearchBody(body)
        }
        val body = HikariNet.getString("$mainUrl/api/v1/home/${catalog.id}?limit=40&offset=$offset", apiHeaders) ?: return emptyList()
        return try {
            val o = JSONObject(body)
            val arr = o.optJSONArray("results") ?: o.optJSONArray("data") ?: return emptyList()
            parseItems(arr)
        } catch (t: Throwable) { emptyList() }
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank()) return emptyList()
        val offset = (page.coerceAtLeast(1) - 1) * 40
        val body = HikariNet.getString("$mainUrl/api/v1/search?q=${URLEncoder.encode(query.trim(), "UTF-8")}&limit=40&offset=$offset", apiHeaders)
            ?: return emptyList()
        return parseSearchBody(body)
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val slug = media.id.substringAfterLast("/").ifBlank { return media }
        val body = HikariNet.getString("$mainUrl/api/v1/anime/$slug", apiHeaders) ?: return media
        return try {
            val o = JSONObject(body)
            val d = if (o.has("data")) o.getJSONObject("data") else o
            val title = d.optString("title").ifBlank { media.title }
            val poster = d.optString("poster").ifBlank { d.optString("image") }.takeIf { it.startsWith("http") } ?: media.posterUrl
            media.copy(title = title, posterUrl = poster, backdropUrl = poster, overview = d.optString("description").takeIf { it.isNotBlank() } ?: media.overview)
        } catch (t: Throwable) { media }
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val slug = media.id.substringAfterLast("/").ifBlank { return null }
        var anilistId = ""
        var tmdbId = ""
        var season = 1
        try {
            val detail = HikariNet.getString("$mainUrl/api/v1/anime/$slug", apiHeaders)
            if (detail != null) {
                val d = JSONObject(detail).let { if (it.has("data")) it.getJSONObject("data") else it }
                anilistId = d.optString("anilistId").ifBlank { d.optString("anilist_id") }
                tmdbId = d.optString("tmdbId").ifBlank { d.optString("tmdb_id") }
                season = d.optInt("season", 1).takeIf { it > 0 } ?: 1
            }
        } catch (t: Throwable) { }
        val body = HikariNet.getString("$mainUrl/api/v1/anime/$slug/episodes?limit=2000", apiHeaders) ?: return null
        return try {
            val arr = JSONObject(body).optJSONArray("data") ?: return null
            val sub = ArrayList<HikariEpisode>()
            val dub = ArrayList<HikariEpisode>()
            var sawFlags = false
            for (i in 0 until arr.length()) {
                val e = arr.optJSONObject(i) ?: continue
                val num = e.optInt("episodeNumber", e.optInt("number", 0)).takeIf { it > 0 } ?: continue
                val title = e.optString("title").takeIf { it.isNotBlank() && !it.startsWith("Episode ") }
                val thumb = e.optString("thumbnail").takeIf { it.startsWith("http") }
                val isSub = e.optBoolean("subbed", false)
                val isDub = e.optBoolean("dubbed", false)
                if (e.has("subbed") || e.has("dubbed")) sawFlags = true
                val ref = "$slug|$anilistId|$tmdbId|$season|$num"
                if (isSub || !sawFlags) sub.add(HikariEpisode(number = num, id = "$ref|sub", name = title ?: "Episode $num", image = thumb ?: media.posterUrl))
                if (isDub || !sawFlags) dub.add(HikariEpisode(number = num, id = "$ref|dub", name = "${title ?: "Episode $num"} (Dub)", image = thumb ?: media.posterUrl))
            }
            (sub.sortedBy { it.number } + dub.sortedBy { it.number }).ifEmpty { null }
        } catch (t: Throwable) { null }
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val ref = episode?.id ?: return emptyList()
        val parts = ref.split("|")
        if (parts.size < 6) return emptyList()
        val anilistId = parts[1]
        val tmdbId = parts[2]
        val season = parts[3]
        val ep = parts[4]
        val lang = parts[5]
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        val flixUrls = buildList {
            if (anilistId.isNotBlank()) add("$mainUrl/api/flix/$anilistId/$ep")
            if (tmdbId.isNotBlank()) add("$mainUrl/api/flix/0/$ep?tmdb=$tmdbId&season=$season")
        }
        for (fu in flixUrls) {
            try {
                val body = HikariNet.getString(fu, apiHeaders) ?: continue
                val servers = JSONObject(body).optJSONArray("servers") ?: continue
                for (i in 0 until servers.length()) {
                    val s = servers.optJSONObject(i) ?: continue
                    var url = s.optString("url").ifBlank { s.optString("embed") }
                    if (url.isBlank()) continue
                    if (url.startsWith("/")) url = flixBase + url
                    if (!url.startsWith("http")) continue
                    for (r in HikariNet.resolveWithWebView(url, capture, timeoutMs = 45_000)) {
                        if (!seen.add(r.url)) continue
                        out.add(HikariStream(name = "ReAnime ${lang.uppercase()}", url = r.url, headers = r.headers, isM3u8 = r.url.contains("m3u8")))
                    }
                    if (out.size >= 4) return out
                }
            } catch (t: Throwable) { }
        }
        return out
    }

    private fun parseSearchBody(body: String): List<HikariMedia> {
        return try {
            val o = JSONObject(body)
            val arr = o.optJSONArray("results") ?: o.optJSONArray("data") ?: return emptyList()
            parseItems(arr)
        } catch (t: Throwable) { emptyList() }
    }

    private fun parseItems(arr: org.json.JSONArray): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        for (i in 0 until arr.length()) {
            val e = arr.optJSONObject(i) ?: continue
            val slug = e.optString("slug").ifBlank { e.optString("id") }.ifBlank { continue }
            val title = e.optString("title").ifBlank { continue }
            val poster = e.optString("poster").ifBlank { e.optString("image") }.takeIf { it.startsWith("http") }
            out.add(HikariMedia(id = "$mainUrl/anime/$slug", title = title, type = HikariMediaType.SERIES, posterUrl = poster))
        }
        return out
    }
}
