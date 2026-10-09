package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import org.json.JSONObject

/**
 * StreamedPk (streamed.pk) — live sports: matches, streams, embeds.
 * Ported from KSHITIJ8473/raghav StreamedPk to native Hikari.
 *
 *  GET /api/matches/all            live + upcoming + channels
 *  GET /api/stream/{source}/{id}   per-source streams (embedUrl fallback)
 * Embeds resolve in a WebView with m3u8/master.txt capture, referer +
 * origin set to the embed host. Upcoming events are listed but not playable.
 */
class StreamedPkProvider : HikariProvider {

    override val id = "streamedpk"
    override val name = "Streamed.pk"
    override val mainUrl = "https://streamed.pk"
    override val description = "Live sports from Streamed.pk — football, NBA, NHL, cricket, PPV."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val apiHeaders = mapOf(
        "User-Agent" to "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36",
        "Accept" to "application/json, text/plain, */*",
        "Referer" to "$mainUrl/",
        "Origin" to mainUrl,
    )
    private val capture = Regex("""https?://[^"' \s]+\.m3u8[^"' \s]*|https?://[^"' \s]*master\.txt[^"' \s]*""")

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("live", "Live Now", HikariMediaType.SERIES),
        HikariCatalog("upcoming", "Upcoming", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        if (page > 1) return emptyList()
        val body = HikariNet.getString("$mainUrl/api/matches/all", apiHeaders) ?: return emptyList()
        return try {
            val o = JSONObject(body)
            val arr = if (catalog.id == "upcoming") {
                o.optJSONArray("upcoming") ?: o.optJSONArray("upcomingMatches")
            } else {
                o.optJSONArray("live") ?: o.optJSONArray("matches")
            } ?: return emptyList()
            parseMatches(arr, catalog.id == "upcoming")
        } catch (t: Throwable) { emptyList() }
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank() || page > 1) return emptyList()
        val body = HikariNet.getString("$mainUrl/api/matches/all", apiHeaders) ?: return emptyList()
        return try {
            val o = JSONObject(body)
            val all = ArrayList<HikariMedia>()
            for (key in listOf("live", "matches", "upcoming", "upcomingMatches")) {
                o.optJSONArray(key)?.let { all.addAll(parseMatches(it, key.contains("upcoming"))) }
            }
            val q = query.trim().lowercase()
            all.filter { it.title.lowercase().contains(q) }.distinctBy { it.id }
        } catch (t: Throwable) { emptyList() }
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        return listOf(HikariEpisode(number = 1, id = media.id, name = "Watch Live", image = media.posterUrl))
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val ref = episode?.id ?: media.id
        val payload = ref.substringAfter("streamedpk:", ref)
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        fun add(name: String, url: String, headers: Map<String, String> = emptyMap()) {
            if (!url.startsWith("http") || !seen.add(url)) return
            out.add(HikariStream(name = name, url = url, headers = headers, isM3u8 = url.contains("m3u8") || url.contains("master.txt")))
        }
        try {
            val o = JSONObject(payload)
            val sources = o.optJSONArray("sources")
            if (sources != null) {
                for (i in 0 until sources.length()) {
                    val s = sources.optJSONObject(i) ?: continue
                    val src = s.optString("source")
                    val sid = s.optString("id")
                    if (src.isBlank() || sid.isBlank()) continue
                    try {
                        val st = HikariNet.getString("$mainUrl/api/stream/$src/$sid", apiHeaders) ?: continue
                        val so = JSONObject(st)
                        val arr = so.optJSONArray("streams") ?: so.optJSONArray("data")
                        if (arr != null) {
                            for (j in 0 until arr.length()) {
                                val e = arr.optJSONObject(j) ?: continue
                                val u = e.optString("url").ifBlank { e.optString("file") }
                                if (u.isNotBlank()) add("StreamedPk ${src.uppercase()}", u, apiHeaders)
                                val emb = e.optString("embedUrl")
                                if (emb.startsWith("http")) {
                                    for (r in HikariNet.resolveWithWebView(emb, capture, timeoutMs = 45_000)) add("StreamedPk ${src.uppercase()} embed", r.url, r.headers)
                                }
                            }
                        } else {
                            val u = so.optString("url").ifBlank { so.optString("file") }
                            if (u.isNotBlank()) add("StreamedPk ${src.uppercase()}", u, apiHeaders)
                        }
                    } catch (t: Throwable) { }
                }
            }
            o.optString("embedUrl").takeIf { it.startsWith("http") }?.let { emb ->
                for (r in HikariNet.resolveWithWebView(emb, capture, timeoutMs = 45_000)) add("StreamedPk embed", r.url, r.headers)
            }
        } catch (t: Throwable) {
            if (payload.startsWith("http")) add("StreamedPk", payload, apiHeaders)
        }
        return out
    }

    private fun parseMatches(arr: org.json.JSONArray, upcoming: Boolean): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        for (i in 0 until arr.length()) {
            val o = arr.optJSONObject(i) ?: continue
            var title = o.optString("title").ifBlank { o.optString("name") }.ifBlank { continue }
            if (upcoming) title = "$title (Upcoming)"
            else if (!title.startsWith("[LIVE]")) title = "[LIVE] $title"
            val poster = o.optString("poster").ifBlank { o.optString("image") }.takeIf { it.startsWith("http") }
            out.add(HikariMedia(id = "streamedpk:${o}", title = title, type = HikariMediaType.SERIES, posterUrl = poster))
        }
        return out
    }
}
