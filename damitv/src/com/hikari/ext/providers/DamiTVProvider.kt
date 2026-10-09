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
 * DamiTV (damitv.st) — live sports: matches, channels, PPV streams.
 * Ported from KSHITIJ8473/raghav DamiTVProvider to native Hikari.
 *
 * JSON (GET with browser headers):
 *  /papi/matches/all                 live + upcoming + channels + popular
 *  /papi/extract-url/{id}            fresh signed HLS right before playback
 *  /papi/stream/{source}/{id}        per-source stream list (embedUrl fallback)
 *  /papi/tv/dlhd/{id}/playlist.m3u8  DLHD direct playlist
 * Everything is live TV; episodes are single "Watch Live" entries and
 * playback URLs are resolved in getStreams (never stored).
 */
class DamiTVProvider : HikariProvider {

    override val id = "damitv"
    override val name = "DamiTV"
    override val mainUrl = "https://damitv.st"
    override val description = "Live sports from DamiTV — football, basketball, cricket, PPV, 24/7 channels."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val apiHeaders = mapOf(
        "User-Agent" to "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36",
        "Accept" to "application/json, text/plain, */*",
        "Referer" to "$mainUrl/",
        "Origin" to mainUrl,
    )
    private val capture = Regex("""https?://[^"' \s]+\.m3u8[^"' \s]*|https?://[^"' \s]+\.mpd[^"' \s]*""")

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("live", "Live Now", HikariMediaType.SERIES),
        HikariCatalog("upcoming", "Upcoming", HikariMediaType.SERIES),
        HikariCatalog("channels", "Channels", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        if (page > 1) return emptyList()
        val body = HikariNet.getString("$mainUrl/papi/matches/all", apiHeaders) ?: return emptyList()
        return try {
            val o = JSONObject(body)
            val arr = when (catalog.id) {
                "upcoming" -> o.optJSONArray("upcoming") ?: o.optJSONArray("upcomingMatches")
                "channels" -> o.optJSONArray("channels")
                else -> o.optJSONArray("live") ?: o.optJSONArray("liveMatches")
            } ?: o.optJSONArray("matches") ?: return emptyList()
            parseMatches(arr)
        } catch (t: Throwable) { emptyList() }
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank() || page > 1) return emptyList()
        val body = HikariNet.getString("$mainUrl/papi/matches/all", apiHeaders) ?: return emptyList()
        return try {
            val o = JSONObject(body)
            val all = ArrayList<HikariMedia>()
            for (key in listOf("live", "liveMatches", "upcoming", "upcomingMatches", "matches", "channels")) {
                val arr = o.optJSONArray(key) ?: continue
                all.addAll(parseMatches(arr))
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
        val payload = ref.substringAfter("damitv:", ref)
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        fun add(name: String, url: String, headers: Map<String, String> = emptyMap()) {
            if (!url.startsWith("http") || !seen.add(url)) return
            out.add(HikariStream(name = name, url = url, headers = headers, isM3u8 = url.contains("m3u8"), isMpd = url.contains("mpd")))
        }
        try {
            val o = JSONObject(payload)
            // 1) fresh signed HLS.
            o.optString("id").takeIf { it.isNotBlank() }?.let { id ->
                try {
                    val ex = HikariNet.getString("$mainUrl/papi/extract-url/$id", apiHeaders)
                    if (ex != null) {
                        val eo = JSONObject(ex)
                        val u = eo.optString("url").ifBlank { eo.optString("stream") }
                        if (u.isNotBlank()) add("DamiTV Live", u, apiHeaders)
                    }
                } catch (t: Throwable) { }
            }
            // 2) per-source streams.
            val sources = o.optJSONArray("sources")
            if (sources != null) {
                for (i in 0 until sources.length()) {
                    val s = sources.optJSONObject(i) ?: continue
                    val src = s.optString("source")
                    val sid = s.optString("id")
                    if (src.isBlank() || sid.isBlank()) continue
                    try {
                        val st = HikariNet.getString("$mainUrl/papi/stream/$src/$sid", apiHeaders) ?: continue
                        val so = JSONObject(st)
                        val arr = so.optJSONArray("streams") ?: so.optJSONArray("data")
                        if (arr != null) {
                            for (j in 0 until arr.length()) {
                                val e = arr.optJSONObject(j) ?: continue
                                val u = e.optString("url").ifBlank { e.optString("file") }
                                if (u.isNotBlank()) add("DamiTV ${src.uppercase()}", u, apiHeaders)
                                val emb = e.optString("embedUrl")
                                if (emb.startsWith("http")) {
                                    for (r in HikariNet.resolveWithWebView(emb, capture, timeoutMs = 45_000)) add("DamiTV ${src.uppercase()} embed", r.url, r.headers)
                                }
                            }
                        } else {
                            val u = so.optString("url").ifBlank { so.optString("file") }
                            if (u.isNotBlank()) add("DamiTV ${src.uppercase()}", u, apiHeaders)
                        }
                    } catch (t: Throwable) { }
                }
            }
            // 3) direct embed fallback.
            o.optString("embedUrl").takeIf { it.startsWith("http") }?.let { emb ->
                for (r in HikariNet.resolveWithWebView(emb, capture, timeoutMs = 45_000)) add("DamiTV embed", r.url, r.headers)
            }
        } catch (t: Throwable) {
            // payload is a plain URL already.
            if (payload.startsWith("http")) add("DamiTV", payload, apiHeaders)
        }
        return out
    }

    private fun parseMatches(arr: org.json.JSONArray): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        for (i in 0 until arr.length()) {
            val o = arr.optJSONObject(i) ?: continue
            val title = o.optString("title").ifBlank { o.optString("name") }.ifBlank { continue }
            val poster = o.optString("poster").ifBlank { o.optString("image") }.takeIf { it.startsWith("http") }
            val id = "damitv:${o}"
            out.add(HikariMedia(id = id, title = title, type = HikariMediaType.SERIES, posterUrl = poster))
        }
        return out
    }
}
