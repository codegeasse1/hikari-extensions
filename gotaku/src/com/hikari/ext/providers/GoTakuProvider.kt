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
 * GoTaku (gotaku.to) — anime via /api/v1 titles + episode embeds.
 * Ported from KSHITIJ8473/raghav GoTaku to native Hikari.
 *
 *  GET /api/v1/titles?{q,limit,page,sort}  catalog + search
 *  GET /api/v1/titles/{id}                 detail
 *  GET /api/v1/titles/{id}/episodes        episode list (number, label, is_filler)
 *  GET /api/v1/episodes/{epId}/embed?type=hard_sub|dub  -> embed page URL
 * Embed pages carry a manifest token; streams are multi-quality HLS
 * ({proxy}/m/{index}/master.m3u8). Resolution: direct manifest probe first,
 * WebView capture fallback.
 */
class GoTakuProvider : HikariProvider {

    override val id = "gotaku"
    override val name = "GoTaku"
    override val mainUrl = "https://gotaku.to"
    override val description = "Anime from GoTaku — trending, latest, movies, sub & dub, multi-quality HLS."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val browserHeaders = mapOf(
        "User-Agent" to "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36",
        "Accept" to "application/json, text/plain, */*",
        "Referer" to "$mainUrl/",
    )
    private val capture = Regex("""https?://[^"' \s]+\.m3u8[^"' \s]*|https?://[^"' \s]+\.mp4[^"' \s]*""")

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("trending", "Trending", HikariMediaType.SERIES),
        HikariCatalog("latest", "Latest Updated", HikariMediaType.SERIES),
        HikariCatalog("top", "Top Rated", HikariMediaType.SERIES),
        HikariCatalog("movies", "Movies", HikariMediaType.MOVIE),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val p = page.coerceAtLeast(1)
        val params = when (catalog.id) {
            "latest" -> "sort=latest&page=$p&limit=28"
            "top" -> "sort=score&page=$p&limit=28"
            "movies" -> "type=movie&page=$p&limit=28"
            else -> "sort=trending&page=$p&limit=28"
        }
        return fetchTitles(params)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank() || page > 1) return emptyList()
        return fetchTitles("q=${URLEncoder.encode(query.trim(), "UTF-8")}&limit=28")
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val titleId = media.id.substringAfterLast("/").ifBlank { return media }
        val body = HikariNet.getString("$mainUrl/api/v1/titles/$titleId", browserHeaders) ?: return media
        return try {
            val o = JSONObject(body)
            val d = if (o.has("data")) o.getJSONObject("data") else o
            val title = d.optString("title").ifBlank { media.title }
            val poster = d.optString("poster").ifBlank { d.optString("image") }.takeIf { it.startsWith("http") } ?: media.posterUrl
            val desc = d.optString("synopsis").ifBlank { d.optString("description") }.takeIf { it.isNotBlank() } ?: media.overview
            media.copy(title = title, posterUrl = poster, backdropUrl = poster, overview = desc)
        } catch (t: Throwable) { media }
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val titleId = media.id.substringAfterLast("/").ifBlank { return null }
        val body = HikariNet.getString("$mainUrl/api/v1/titles/$titleId/episodes", browserHeaders) ?: return null
        return try {
            val o = JSONObject(body)
            val arr = o.optJSONArray("data") ?: return null
            val out = ArrayList<HikariEpisode>()
            for (i in 0 until arr.length()) {
                val e = arr.optJSONObject(i) ?: continue
                val epid = e.optString("id").ifBlank { continue }
                val num = e.optInt("number", 0).takeIf { it > 0 }
                    ?: e.optString("label").let { Regex("""(\d+)""").find(it)?.groupValues?.get(1)?.toIntOrNull() }
                    ?: (i + 1)
                var name = e.optString("label").takeIf { it.isNotBlank() && !it.equals("Episode $num", true) } ?: "Episode $num"
                if (e.optBoolean("is_filler", false)) name += " (Filler)"
                out.add(HikariEpisode(number = num, id = "$titleId|$epid|hard_sub", name = name, image = media.posterUrl))
                out.add(HikariEpisode(number = num, id = "$titleId|$epid|dub", name = "$name (Dub)", image = media.posterUrl))
            }
            out.sortedBy { it.number }.ifEmpty { null }
        } catch (t: Throwable) { null }
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val ref = episode?.id ?: return emptyList()
        val parts = ref.split("|")
        if (parts.size < 3) return emptyList()
        val epId = parts[1]
        val track = parts.getOrNull(2) ?: "hard_sub"
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        val embedBody = HikariNet.getString("$mainUrl/api/v1/episodes/$epId/embed?type=$track", browserHeaders)
        val embedUrl = try {
            if (embedBody == null) null
            else {
                val o = JSONObject(embedBody)
                val d = if (o.has("data")) o.getJSONObject("data") else o
                d.optString("url").ifBlank { d.optString("embed") }.takeIf { it.isNotBlank() }
            }
        } catch (t: Throwable) { null } ?: return emptyList()
        val full = if (embedUrl.startsWith("http")) embedUrl else "$mainUrl$embedUrl"
        // Direct manifest links exposed on the embed page.
        try {
            val html = HikariNet.getString(full, browserHeaders + mapOf("Referer" to "$mainUrl/"))
            if (html != null) {
                for (m in Regex("""https?://[^"' \s]+/m/\d+/master\.m3u8[^"' \s]*""").findAll(html)) {
                    if (!seen.add(m.value)) continue
                    out.add(HikariStream(name = "GoTaku ${if (track == "dub") "Dub" else "Sub"}", url = m.value, isM3u8 = true))
                }
                if (out.isNotEmpty()) return out
            }
        } catch (t: Throwable) { }
        for (r in HikariNet.resolveWithWebView(full, capture, timeoutMs = 60_000)) {
            if (!seen.add(r.url)) continue
            out.add(HikariStream(name = "GoTaku ${if (track == "dub") "Dub" else "Sub"}", url = r.url, headers = r.headers, isM3u8 = r.url.contains("m3u8")))
        }
        return out
    }

    private suspend fun fetchTitles(query: String): List<HikariMedia> {
        val body = HikariNet.getString("$mainUrl/api/v1/titles?$query", browserHeaders) ?: return emptyList()
        val out = ArrayList<HikariMedia>()
        try {
            val o = JSONObject(body)
            val arr = o.optJSONArray("data") ?: return emptyList()
            for (i in 0 until arr.length()) {
                val e = arr.optJSONObject(i) ?: continue
                val id = e.optString("id").ifBlank { continue }
                val title = e.optString("title").ifBlank { continue }
                val poster = e.optString("poster").ifBlank { e.optString("image") }.takeIf { it.startsWith("http") }
                val format = e.optString("type").ifBlank { e.optString("format") }
                out.add(HikariMedia(id = "$mainUrl/titles/$id", title = title, type = if (format.contains("movie", true)) HikariMediaType.MOVIE else HikariMediaType.SERIES, posterUrl = poster))
            }
        } catch (t: Throwable) { }
        return out
    }
}
