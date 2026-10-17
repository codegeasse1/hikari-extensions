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
 * JioTV (via the free api.freeforall.dev GDL proxy) — 1000+ live TV channels,
 * verified live Oct 2026:
 *  - Catalog/search: /api/jiotv/channels?per_page=60[&category=][&q=][&page=]
 *    -> {categories:[{name,count}], channels:[{id,name,slug,logo,category}]}.
 *  - Playback: per-channel stream endpoints are resolved best-effort (channel
 *    detail variants, then the JioTV web route). MPD (DASH) sources are emitted
 *    with isMpd=true. NOTE: Hikari streams carry no DRM fields, so channels
 *    whose manifests are Widevine-encrypted will not play; clear HLS/MPD
 *    channels play directly.
 */
class JioTVProvider : HikariProvider {

    override val id = "jiotv"
    override val name = "JioTV"
    override val mainUrl = "https://api.freeforall.dev"
    override val description = "1000+ live TV channels — news, entertainment, movies, kids, music."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES)

    private val UA =
        "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36"

    companion object {
        const val API = "https://api.freeforall.dev"
    }

    private fun h(): Map<String, String> = mapOf(
        "User-Agent" to UA,
        "Accept" to "application/json",
        "Referer" to "https://play.freeforall.dev/",
    )

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("all", "All Channels", HikariMediaType.SERIES),
        HikariCatalog("news", "News", HikariMediaType.SERIES),
        HikariCatalog("entertainment", "Entertainment", HikariMediaType.SERIES),
        HikariCatalog("movies", "Movies", HikariMediaType.MOVIE),
        HikariCatalog("kids", "Kids", HikariMediaType.SERIES),
        HikariCatalog("music", "Music", HikariMediaType.SERIES),
        HikariCatalog("documentary", "Documentary", HikariMediaType.SERIES),
        HikariCatalog("international", "International", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val p = page.coerceAtLeast(1)
        val cat = when (catalog.id) {
            "news" -> "News"
            "entertainment" -> "Entertainment"
            "movies" -> "Movies"
            "kids" -> "Kids"
            "music" -> "Music"
            "documentary" -> "Documentary"
            "international" -> "International"
            else -> ""
        }
        val url = buildString {
            append("$API/api/jiotv/channels?per_page=60&page=$p")
            if (cat.isNotBlank()) append("&category=${URLEncoder.encode(cat, "UTF-8")}")
        }
        val root = HikariNet.getJson(url, h()) ?: return emptyList()
        return parseChannels(root.optJSONArray("channels"))
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        val p = page.coerceAtLeast(1)
        val root = HikariNet.getJson(
            "$API/api/jiotv/channels?per_page=60&page=$p&q=${URLEncoder.encode(q, "UTF-8")}", h(),
        ) ?: return emptyList()
        return parseChannels(root.optJSONArray("channels"))
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        // Live channel: single live entry so the player opens directly.
        return listOf(HikariEpisode(number = 1, id = media.id, name = "${media.title} · Live", image = media.posterUrl))
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val ref = episode?.id ?: media.id
        val id = ref.substringAfter("jiotv:", "").substringBefore(":")
        val slug = ref.substringAfterLast(":", "")
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        // 1) Channel-detail endpoint variants on the free API.
        val candidates = listOf(
            "$API/api/channels/$slug",
            "$API/api/jiotv/channel/$id",
            "$API/api/jiotv/play/$id",
            "$API/api/jiotv/stream/$id",
            "$API/api/jiotv/watch/$id",
        )
        for (u in candidates) {
            val txt = HikariNet.getString(u, h()) ?: continue
            if (!txt.trimStart().startsWith("{") && !txt.trimStart().startsWith("[")) continue
            var added = false
            for (m in Regex("\"(mpd|m3u8|hls|dash|stream_url|playback_url|url)\"\\s*:\\s*\"([^\"]+)\"").findAll(txt)) {
                val su = m.groupValues[2].replace("\\/", "/")
                if (!su.startsWith("http") || !seen.add(su)) continue
                out.add(
                    HikariStream(
                        name = "JioTV",
                        url = su,
                        headers = mapOf("User-Agent" to UA, "Referer" to "https://play.freeforall.dev/"),
                        isM3u8 = su.contains(".m3u8"),
                        isMpd = su.contains(".mpd"),
                    )
                )
                added = true
            }
            if (added) return out
        }
        // 2) JioTV web route (some channels expose a clear manifest).
        if (slug.isNotBlank()) {
            val page = HikariNet.getString("https://jiotv.com/$slug", h())
            if (page != null) {
                for (m in Regex("https?://[^\\s\"'<>\\\\]+\\.(?:m3u8|mpd)[^\\s\"'<>\\\\]*").findAll(page)) {
                    val su = m.value
                    if (su.startsWith("http") && seen.add(su)) {
                        out.add(
                            HikariStream(
                                name = "JioTV Web",
                                url = su,
                                headers = mapOf("User-Agent" to UA, "Referer" to "https://jiotv.com/"),
                                isM3u8 = su.contains(".m3u8"),
                                isMpd = su.contains(".mpd"),
                            )
                        )
                    }
                }
            }
        }
        return out
    }

    // ------------------------------------------------------------------
    // Parsing helpers.
    // ------------------------------------------------------------------

    private fun parseChannels(arr: org.json.JSONArray?): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        if (arr == null) return out
        for (i in 0 until arr.length()) {
            val o = arr.optJSONObject(i) ?: continue
            val id = o.optInt("id", 0)
            val name = o.optString("name").trim()
            if (id == 0 || name.isBlank()) continue
            val slug = o.optString("slug").trim()
            val logo = o.optString("logo").takeIf { it.startsWith("http") }
            out.add(
                HikariMedia(
                    id = "jiotv:$id:$slug",
                    title = name,
                    type = HikariMediaType.SERIES,
                    posterUrl = logo,
                    genres = listOfNotNull(o.optString("category").takeIf { it.isNotBlank() }),
                )
            )
        }
        return out
    }
}
