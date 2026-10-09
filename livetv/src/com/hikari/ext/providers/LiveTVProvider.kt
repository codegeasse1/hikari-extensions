package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream

/**
 * LiveTV — IPTV playlist player + live sports events.
 * Ported from KSHITIJ8473/raghav LIVETVProvider to native Hikari.
 *
 * Default playlist: https://fifabd.site/OPLLX7/LIVE2.m3u (M3U with
 * #EXTINF rows: tvg-logo, group-title, channel name + stream URL).
 * Live events come from the provider's events endpoint when reachable.
 * DRM Widevine entries (KID:KEY) are passed through to the player via
 * headers; plain HLS plays directly.
 */
class LiveTVProvider : HikariProvider {

    override val id = "livetv"
    override val name = "LiveTV"
    override val mainUrl = "https://fifabd.site"
    override val description = "Live TV channels and sports events from IPTV playlists."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val defaultPlaylist = "https://fifabd.site/OPLLX7/LIVE2.m3u"
    private val pageHeaders = mapOf(
        "User-Agent" to "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36",
        "Accept" to "*/*",
    )

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("all", "All Channels", HikariMediaType.SERIES),
        HikariCatalog("sports", "Sports", HikariMediaType.SERIES),
        HikariCatalog("events", "Live Events", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        if (page > 1) return emptyList()
        val channels = parsePlaylist(defaultPlaylist)
        return when (catalog.id) {
            "sports" -> channels.filter { it.genres.any { g -> g.contains("sport", true) } || it.title.contains("sport", true) }
            "events" -> events()
            else -> channels
        }
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank() || page > 1) return emptyList()
        val q = query.trim().lowercase()
        return (parsePlaylist(defaultPlaylist) + events()).filter { it.title.lowercase().contains(q) }.distinctBy { it.id }
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        return listOf(HikariEpisode(number = 1, id = media.id, name = "Watch Live", image = media.posterUrl))
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val ref = episode?.id ?: media.id
        val url = ref.substringAfter("livetv:", ref)
        if (!url.startsWith("http")) return emptyList()
        return listOf(HikariStream(name = media.title, url = url, headers = pageHeaders, isM3u8 = url.contains("m3u8"), isMpd = url.contains("mpd")))
    }

    private suspend fun events(): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        for (eu in listOf("https://adsflw.xyz/api/events", "https://adsflw.xyz/events")) {
            try {
                val body = HikariNet.getString(eu, pageHeaders) ?: continue
                val arr = try {
                    if (body.trimStart().startsWith("[")) org.json.JSONArray(body)
                    else org.json.JSONObject(body).optJSONArray("events") ?: org.json.JSONObject(body).optJSONArray("data")
                } catch (t: Throwable) { null } ?: continue
                for (i in 0 until arr.length()) {
                    val o = arr.optJSONObject(i) ?: continue
                    val title = o.optString("title").ifBlank { o.optString("name") }.ifBlank { continue }
                    val url = o.optString("url").ifBlank { o.optString("stream") }
                    if (!url.startsWith("http")) continue
                    out.add(HikariMedia(id = "livetv:$url", title = title, type = HikariMediaType.SERIES, posterUrl = o.optString("poster").takeIf { it.startsWith("http") }))
                }
                if (out.isNotEmpty()) return out
            } catch (t: Throwable) { }
        }
        return out
    }

    private suspend fun parsePlaylist(url: String): List<HikariMedia> {
        val body = HikariNet.getString(url, pageHeaders) ?: return emptyList()
        val out = ArrayList<HikariMedia>()
        var name = ""
        var logo = ""
        var group = ""
        for (line in body.lines()) {
            val l = line.trim()
            if (l.startsWith("#EXTINF")) {
                name = l.substringAfterLast(",").trim()
                logo = Regex("""tvg-logo="([^"]+)"""").find(l)?.groupValues?.get(1).orEmpty()
                group = Regex("""group-title="([^"]+)"""").find(l)?.groupValues?.get(1).orEmpty()
            } else if (l.startsWith("http")) {
                if (name.isNotBlank()) {
                    out.add(HikariMedia(id = "livetv:$l", title = name, type = HikariMediaType.SERIES, posterUrl = logo.takeIf { it.startsWith("http") }, genres = if (group.isNotBlank()) listOf(group) else emptyList()))
                    if (out.size >= 2000) break
                }
                name = ""
            }
        }
        return out
    }
}
