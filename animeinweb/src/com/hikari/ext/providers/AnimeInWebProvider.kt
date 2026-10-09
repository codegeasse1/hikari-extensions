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
 * AnimeInWeb (animeinweb.com) — Indonesian anime API proxy.
 * Ported from KSHITIJ8473/raghav AnimeInWeb to native Hikari.
 *
 * JSON proxy (GET):
 *  /api/proxy/3/2/home/data?day=&limit=16   home rows
 *  /api/proxy/3/2/explore/movie?page=&sort=views&keyword=  catalog + search (0-based page)
 *  /api/proxy/3/2/movie/detail/{id}          detail
 *  episode pages: 30 per page, newest first (paged until short page)
 * Streams: episode sources resolved via WebView m3u8 capture on the episode page.
 */
class AnimeInWebProvider : HikariProvider {

    override val id = "animeinweb"
    override val name = "AnimeInWeb"
    override val mainUrl = "https://animeinweb.com"
    override val description = "Anime from animeinweb.com — schedule, explore, subbed episodes."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val apiHeaders = mapOf(
        "User-Agent" to "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36",
        "Accept" to "application/json, text/plain, */*",
        "Referer" to "$mainUrl/",
    )
    private val capture = Regex("""https?://[^"' \s]+\.m3u8[^"' \s]*|https?://[^"' \s]+\.mp4[^"' \s]*""")

    private fun api(path: String) = "$mainUrl/api/proxy/3/2$path"

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("latest", "Terbaru (Latest)", HikariMediaType.SERIES),
        HikariCatalog("popular", "Populer (Views)", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val p = (page.coerceAtLeast(1)) - 1
        val sort = if (catalog.id == "popular") "views" else ""
        val body = HikariNet.getString(api("/explore/movie?page=$p&sort=$sort&keyword="), apiHeaders) ?: return emptyList()
        return parseExplore(body)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank()) return emptyList()
        val p = (page.coerceAtLeast(1)) - 1
        val body = HikariNet.getString(api("/explore/movie?page=$p&sort=&keyword=${URLEncoder.encode(query.trim(), "UTF-8")}"), apiHeaders)
            ?: return emptyList()
        return parseExplore(body)
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val id = media.id.substringAfterLast("/").ifBlank { return media }
        val body = HikariNet.getString(api("/movie/detail/$id"), apiHeaders) ?: return media
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
        val id = media.id.substringAfterLast("/").ifBlank { return null }
        val all = ArrayList<HikariEpisode>()
        var start = 0
        while (start <= 300) {
            val body = HikariNet.getString(api("/movie/episodes/$id?start=$start&limit=30"), apiHeaders)
                ?: HikariNet.getString(api("/episode/list/$id?start=$start&limit=30"), apiHeaders)
                ?: break
            val batch = parseEpisodeBatch(body, media)
            if (batch.isEmpty()) break
            all.addAll(batch)
            if (batch.size < 30) break
            start += 30
        }
        if (all.isEmpty()) {
            val detail = HikariNet.getString(api("/movie/detail/$id"), apiHeaders)
            if (detail != null) {
                try {
                    val o = JSONObject(detail)
                    val d = if (o.has("data")) o.getJSONObject("data") else o
                    val count = d.optInt("episodeCount", d.optInt("episodes", 0))
                    if (count > 0) {
                        for (n in 1..count.coerceAtMost(3000)) all.add(HikariEpisode(number = n, id = "$id|$n", name = "Episode $n", image = media.posterUrl))
                    }
                } catch (t: Throwable) { }
            }
        }
        // API is newest-first; oldest-first for playback order.
        return all.sortedBy { it.number }.ifEmpty { null }
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val ref = episode?.id ?: return emptyList()
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        val watchUrls = listOf("$mainUrl/watch/$ref", "$mainUrl/episode/$ref", media.id)
        for (w in watchUrls) {
            if (!w.startsWith("http")) continue
            val html = HikariNet.getStringSmart(w, apiHeaders) ?: continue
            for (m in Regex("""<iframe[^>]+src="([^"]+)"""").findAll(html)) {
                val src = m.groupValues[1]
                if (!src.startsWith("http")) continue
                val captured = HikariNet.resolveWithWebView(src, capture, timeoutMs = 45_000)
                for (r in captured) {
                    if (!seen.add(r.url)) continue
                    out.add(HikariStream(name = "AnimeInWeb", url = r.url, headers = r.headers, isM3u8 = r.url.contains("m3u8")))
                }
                if (out.isNotEmpty()) return out
            }
            for (m in capture.findAll(html)) {
                if (!seen.add(m.value)) continue
                out.add(HikariStream(name = "AnimeInWeb", url = m.value, isM3u8 = m.value.contains("m3u8")))
            }
            if (out.isNotEmpty()) return out
        }
        return out
    }

    private fun parseExplore(body: String): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        try {
            val o = JSONObject(body)
            val d = if (o.has("data")) o.getJSONObject("data") else o
            val arr = d.optJSONArray("movie") ?: d.optJSONArray("data") ?: d.optJSONArray("items") ?: return emptyList()
            for (i in 0 until arr.length()) {
                val e = arr.optJSONObject(i) ?: continue
                val id = e.optString("id").ifBlank { e.optString("slug") }.ifBlank { continue }
                val title = e.optString("title").ifBlank { continue }
                val poster = e.optString("poster").ifBlank { e.optString("image") }.takeIf { it.startsWith("http") }
                val type = if (e.optString("type").contains("movie", true)) HikariMediaType.MOVIE else HikariMediaType.SERIES
                out.add(HikariMedia(id = "$mainUrl/movie/$id", title = title, type = type, posterUrl = poster))
            }
        } catch (t: Throwable) { }
        return out
    }

    private fun parseEpisodeBatch(body: String, media: HikariMedia): List<HikariEpisode> {
        val out = ArrayList<HikariEpisode>()
        try {
            val o = JSONObject(body)
            val d = if (o.has("data")) o.getJSONObject("data") else o
            val arr = d.optJSONArray("episodes") ?: d.optJSONArray("data") ?: d.optJSONArray("list") ?: return emptyList()
            for (i in 0 until arr.length()) {
                val e = arr.optJSONObject(i) ?: continue
                val eid = e.optString("id").ifBlank { continue }
                val num = e.optInt("index", 0).takeIf { it > 0 }
                    ?: e.optString("index").toIntOrNull()
                    ?: Regex("""(\d+)""").find(e.optString("title"))?.groupValues?.get(1)?.toIntOrNull()
                    ?: (i + 1)
                val name = e.optString("title").takeIf { it.isNotBlank() } ?: "Episode $num"
                out.add(HikariEpisode(number = num, id = eid, name = name, image = media.posterUrl))
            }
        } catch (t: Throwable) { }
        return out
    }
}
