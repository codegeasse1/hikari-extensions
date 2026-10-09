package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import java.net.URLEncoder

/**
 * ReplayZone (replay-exc.pages.dev) — football match replays.
 * Ported from KSHITIJ8473/raghav ReplayZone to native Hikari.
 *
 * Source of truth is a plain-text index fetched from the worker:
 * https://replay.adityapangshe.workers.dev/replays.txt
 * Format: match title lines followed by "Label\tType\tURL" embed lines
 * (type hls = direct m3u8, else iframe). soccerfull.net/hls/ URLs are
 * proxied through the worker (/hls?u=); /play/ pages are scraped for the
 * m3u8Url var. Everything is live-style single-episode entries.
 */
class ReplayZoneProvider : HikariProvider {

    override val id = "replayzone"
    override val name = "ReplayZone"
    override val mainUrl = "https://replay-exc.pages.dev"
    override val description = "Football match replays — full matches, goals, highlights."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val dataUrl = "https://replay.adityapangshe.workers.dev/replays.txt"
    private val workerUrl = "https://replay.adityapangshe.workers.dev"
    private val headers = mapOf(
        "User-Agent" to "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36",
        "Accept" to "*/*",
        "Referer" to "$mainUrl/",
    )
    private val capture = Regex("""https?://[^"' \s]+\.m3u8[^"' \s]*|https?://[^"' \s]+\.mp4[^"' \s]*""")

    private data class Embed(val label: String, val type: String, val url: String)
    private data class Replay(val title: String, val thumb: String?, val embeds: List<Embed>)

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("recent", "Recent Replays", HikariMediaType.SERIES),
        HikariCatalog("all", "All Replays", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        if (page > 1) return emptyList()
        val all = fetchReplays()
        val list = if (catalog.id == "recent") all.take(30) else all
        return list.map { it.toMedia() }
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank() || page > 1) return emptyList()
        val q = query.trim().lowercase()
        return fetchReplays().filter { it.title.lowercase().contains(q) }.map { it.toMedia() }
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        return listOf(HikariEpisode(number = 1, id = media.id, name = "Watch Replay", image = media.posterUrl))
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val ref = episode?.id ?: media.id
        val idx = ref.substringAfter("replayzone:", ref).toIntOrNull() ?: return emptyList()
        val replay = fetchReplays().getOrNull(idx) ?: return emptyList()
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        for (embed in replay.embeds) {
            try {
                if (embed.type == "hls") {
                    val u = if (embed.url.contains("soccerfull.net/hls/")) {
                        "$workerUrl/hls?u=${URLEncoder.encode(embed.url, "UTF-8")}"
                    } else embed.url
                    if (seen.add(u)) out.add(HikariStream(name = "ReplayZone — ${embed.label}", url = u, headers = headers, isM3u8 = true))
                } else if (embed.url.contains("soccerfull.net/play/")) {
                    val html = HikariNet.getString(embed.url, headers) ?: continue
                    val m = Regex("""m3u8Url\s*=\s*["']([^"']+)["']""").find(html) ?: continue
                    val path = m.groupValues[1]
                    val full = if (path.startsWith("http")) path else "https://soccerfull.net$path"
                    val proxied = "$workerUrl/hls?u=${URLEncoder.encode(full, "UTF-8")}"
                    if (seen.add(proxied)) out.add(HikariStream(name = "ReplayZone — ${embed.label}", url = proxied, headers = headers, isM3u8 = true))
                } else {
                    for (r in HikariNet.resolveWithWebView(embed.url, capture, timeoutMs = 45_000)) {
                        if (!seen.add(r.url)) continue
                        out.add(HikariStream(name = "ReplayZone — ${embed.label}", url = r.url, headers = r.headers, isM3u8 = r.url.contains("m3u8")))
                    }
                }
            } catch (t: Throwable) { }
        }
        return out
    }

    private fun Replay.toMedia(): HikariMedia {
        // Index by position; embeds resolved lazily in getStreams.
        val idx = fetchCache.indexOf(this).takeIf { it >= 0 } ?: 0
        return HikariMedia(id = "replayzone:$idx", title = title, type = HikariMediaType.SERIES, posterUrl = thumb, overview = "${embeds.size} sources")
    }

    private var fetchCache: List<Replay> = emptyList()

    private suspend fun fetchReplays(): List<Replay> {
        if (fetchCache.isNotEmpty()) return fetchCache
        val body = HikariNet.getString(dataUrl, headers) ?: return emptyList()
        val out = ArrayList<Replay>()
        var title = ""
        var thumb = ""
        var embeds = ArrayList<Embed>()
        fun flush() {
            if (title.isNotBlank() && embeds.isNotEmpty()) out.add(Replay(title, thumb.takeIf { it.startsWith("http") }, embeds.toList()))
            title = ""
            thumb = ""
            embeds = ArrayList()
        }
        for (raw in body.lines()) {
            val line = raw.trim()
            if (line.isEmpty() || line.startsWith("#")) continue
            if (line.contains("\t")) {
                val parts = line.split("\t")
                if (parts.size >= 3 && title.isNotBlank()) {
                    embeds.add(Embed(parts[0].trim(), parts[1].trim().lowercase(), parts[2].trim()))
                } else if (parts.size >= 3) {
                    // Orphan embed line: start a titled entry from its label.
                    embeds.add(Embed(parts[0].trim(), parts[1].trim().lowercase(), parts[2].trim()))
                    title = parts[0].trim()
                }
            } else {
                if (title.isNotBlank() && embeds.isNotEmpty()) flush()
                if (title.isBlank()) title = line else {
                    // Second consecutive title line: previous had no embeds, replace.
                    if (embeds.isEmpty()) title = line
                }
                if (line.startsWith("http") && line.matches(Regex(""".+\.(jpg|jpeg|png|webp)(\?.*)?"""))) {
                    thumb = line
                }
            }
        }
        flush()
        fetchCache = out
        return out
    }
}
