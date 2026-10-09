package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import com.hikari.ext.HikariSubtitle
import org.json.JSONArray
import org.json.JSONObject
import java.net.URLEncoder

/**
 * AniChan (anichan.to) — anime via JSON API + WebView server resolver.
 * Ported from KSHITIJ8473/raghav AniChan (CloudStream MainAPI) to native Hikari.
 *
 * API (all GET, JSON):
 *  /api/suggest?q=            search suggestions
 *  /api/catalog/trending?page= home row
 *  /api/catalog/airing?page=   home row
 *  /api/catalog/anime/{id}     detail
 *  /api/watch/episodes?anilistId={id}  episode counts / dub availability
 * Servers come from the site's watch page JS (AniChanWebView.fetchServers in
 * source); here episode streams are resolved by scraping the watch page
 * iframes/direct files first, then WebView m3u8 capture as fallback.
 */
class AniChanProvider : HikariProvider {

    override val id = "anichan"
    override val name = "AniChan"
    override val mainUrl = "https://anichan.to"
    override val description = "Anime from anichan.to — trending, airing, AniList-backed catalog, sub & dub."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val apiHeaders = mapOf(
        "User-Agent" to "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36",
        "Accept" to "application/json, text/plain, */*",
        "Referer" to "$mainUrl/",
    )
    private val pageHeaders = mapOf(
        "User-Agent" to "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36",
        "Accept" to "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer" to "$mainUrl/",
    )
    private val m3u8Capture = Regex("""https?://[^"' \s]+\.m3u8[^"' \s]*|https?://[^"' \s]*master\.txt[^"' \s]*|https?://[^"' \s]+\.mp4[^"' \s]*""")
    private val iframeRe = Regex("""<iframe[^>]+src="([^"]+)"""", RegexOption.IGNORE_CASE)

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("trending", "Trending", HikariMediaType.SERIES),
        HikariCatalog("airing", "Airing Now", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val p = page.coerceAtLeast(1)
        val url = when (catalog.id) {
            "airing" -> "$mainUrl/api/catalog/airing?page=$p"
            else -> "$mainUrl/api/catalog/trending?page=$p"
        }
        val body = HikariNet.getString(url, apiHeaders) ?: return emptyList()
        return parseCatalogArray(body)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        if (page > 1) return emptyList()
        val body = HikariNet.getString("$mainUrl/api/suggest?q=${URLEncoder.encode(q, "UTF-8")}", apiHeaders)
            ?: return emptyList()
        return parseCatalogArray(body)
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val anilistId = media.id.substringAfterLast("/").substringBefore("|").trim()
        if (anilistId.isBlank() || anilistId == media.id) return media
        val body = HikariNet.getString("$mainUrl/api/catalog/anime/$anilistId", apiHeaders) ?: return media
        return try {
            val o = if (body.trimStart().startsWith("[")) null else JSONObject(body)
            if (o == null) return media
            val title = o.optString("title_english").ifBlank { o.optString("title") }.ifBlank { media.title }
            val poster = o.optString("cover").ifBlank { o.optString("poster") }.takeIf { it.startsWith("http") }
                ?: media.posterUrl
            val desc = o.optString("description").takeIf { it.isNotBlank() } ?: media.overview
            val year = o.optInt("year", 0).takeIf { it > 0 } ?: media.year
            val genres = o.optJSONArray("genres")?.let { arr ->
                buildList { for (i in 0 until arr.length()) arr.optString(i).takeIf { it.isNotBlank() }?.let { add(it) } }
            } ?: media.genres
            val type = if (o.optString("format").equals("movie", true)) HikariMediaType.MOVIE else HikariMediaType.SERIES
            media.copy(title = title, posterUrl = poster, overview = desc, year = year, genres = genres, type = type)
        } catch (t: Throwable) { media }
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val anilistId = media.id.substringAfterLast("/").substringBefore("|").trim()
        if (anilistId.isBlank()) return null
        var total = 0
        var dubAvailable = false
        try {
            val info = HikariNet.getString("$mainUrl/api/watch/episodes?anilistId=$anilistId", apiHeaders)
            if (info != null) {
                val o = JSONObject(info)
                total = o.optInt("episodes", 0)
                if (total <= 0) total = o.optInt("count", 0)
                dubAvailable = o.optBoolean("dub", false) || o.optBoolean("hasDub", false)
            }
        } catch (t: Throwable) { }
        if (total <= 0) {
            try {
                val detail = HikariNet.getString("$mainUrl/api/catalog/anime/$anilistId", apiHeaders)
                if (detail != null) total = JSONObject(detail).optInt("episodes", 0)
            } catch (t: Throwable) { }
        }
        if (total <= 0) total = 12
        if (total > 3000) total = 3000
        val out = ArrayList<HikariEpisode>(total * 2)
        for (n in 1..total) {
            out.add(HikariEpisode(number = n, id = "$anilistId|sub|$n", name = "Episode $n"))
        }
        if (dubAvailable) {
            for (n in 1..total) {
                out.add(HikariEpisode(number = n, id = "$anilistId|dub|$n", name = "Episode $n (Dub)"))
            }
        }
        return out
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val ref = episode?.id ?: return emptyList()
        val parts = ref.split("|")
        if (parts.size < 3) return emptyList()
        val anilistId = parts[0]
        val category = parts.getOrNull(1) ?: "sub"
        val ep = parts.getOrNull(2) ?: "1"
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        // Watch page iframes / direct files.
        for (watchUrl in listOf("$mainUrl/watch/$anilistId/$ep", "$mainUrl/anime/$anilistId")) {
            val html = HikariNet.getStringSmart(watchUrl, pageHeaders) ?: continue
            for (m in iframeRe.findAll(html)) {
                val src = m.groupValues[1]
                if (!src.startsWith("http")) continue
                val label = if (category == "dub") "Dub embed" else "Sub embed"
                val captured = HikariNet.resolveWithWebView(src, m3u8Capture, timeoutMs = 45_000)
                for (r in captured) {
                    if (!seen.add(r.url)) continue
                    out.add(HikariStream(name = "AniChan $label", url = r.url, headers = r.headers, isM3u8 = r.url.contains("m3u8") || r.url.contains("master.txt")))
                }
                if (out.isNotEmpty()) return out
            }
            for (m in m3u8Capture.findAll(html)) {
                val u = m.value
                if (!seen.add(u)) continue
                out.add(HikariStream(name = "AniChan ${if (category == "dub") "Dub" else "Sub"}", url = u, isM3u8 = u.contains("m3u8") || u.contains("master.txt")))
            }
            if (out.isNotEmpty()) return out
        }
        return out
    }

    private fun parseCatalogArray(body: String): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        try {
            val arr: JSONArray = if (body.trimStart().startsWith("[")) JSONArray(body)
            else JSONObject(body).let { o ->
                o.optJSONArray("data") ?: o.optJSONArray("results") ?: o.optJSONArray("items") ?: return emptyList()
            }
            for (i in 0 until arr.length()) {
                val o = arr.optJSONObject(i) ?: continue
                val id = o.optString("anilistId").ifBlank { o.optString("id") }
                if (id.isBlank()) continue
                val title = o.optString("title_english").ifBlank { o.optString("title") }.ifBlank { continue }
                val poster = o.optString("cover").ifBlank { o.optString("poster") }.takeIf { it.startsWith("http") }
                val year = o.optInt("year", 0).takeIf { it > 0 }
                out.add(HikariMedia(id = "$mainUrl/anime/$id", title = title, type = HikariMediaType.SERIES, posterUrl = poster, year = year))
            }
        } catch (t: Throwable) { }
        return out
    }
}
