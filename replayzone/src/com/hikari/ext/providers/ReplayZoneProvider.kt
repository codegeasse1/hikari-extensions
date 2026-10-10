package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import org.json.JSONArray
import org.json.JSONObject
import java.net.URLEncoder

/**
 * ReplayZone — full-match sports replays from a worker-hosted data file.
 *
 * Faithful native port of KSHITIJ8473/raghav ReplayZoneProvider, verified
 * live Oct 2026 (replays.txt, ~1.7MB):
 *  - Lines: `# Title`, `~ Category\tSub\tThumb\tDate`,
 *    `Label\t(hls|iframe)\tURL`, blank-separated. Sorted newest-first.
 *  - 6 catalog rows (Recently Added, All Replays, Football, Baseball, Rugby,
 *    Motorsport), 30 items each, paged.
 *  - Playback: hls direct (soccerfull /hls/ proxied through the worker),
 *    soccerfull /play/ pages scraped for `m3u8Url = "..."` then proxied,
 *    direct m3u8/mp4, ok.ru + dailymotion + byse + unknown embeds via WebView
 *    capture (the source's loadExtractor equivalent).
 */
class ReplayZoneProvider : HikariProvider {

    override val id = "replayzone"
    override val name = "ReplayZone"
    override val mainUrl = "https://replay-exc.pages.dev"
    override val description = "Full-match sports replays — football, baseball, rugby, motorsport."
    override val version = 2
    override val tvTypes = setOf(HikariMediaType.SERIES)

    private val dataUrl = "https://replay.adityapangshe.workers.dev/replays.txt"
    private val workerUrl = "https://replay.adityapangshe.workers.dev"

    private data class Embed(val label: String, val type: String, val url: String)

    private data class Replay(
        val title: String,
        val category: String,
        val sub: String,
        val thumb: String,
        val date: String,
        val embeds: List<Embed>,
    )

    @Volatile
    private var cached: List<Replay>? = null

    private suspend fun fetchReplays(): List<Replay> {
        cached?.let { return it }
        return try {
            val text = HikariNet.getString(dataUrl) ?: return emptyList()
            val replays = mutableListOf<Replay>()
            var title: String? = null
            var category = ""
            var sub = ""
            var thumb = ""
            var date = ""
            var embeds = mutableListOf<Embed>()
            fun flush() {
                val t = title
                if (t != null && embeds.isNotEmpty()) {
                    replays.add(Replay(t, category, sub, thumb, date, embeds.toList()))
                }
            }
            for (line in text.split("\n")) {
                val trimmed = line.trim()
                when {
                    trimmed.startsWith("# ") -> {
                        flush()
                        title = trimmed.substring(2).trim()
                        category = ""
                        sub = ""
                        thumb = ""
                        date = ""
                        embeds = mutableListOf()
                    }
                    trimmed.startsWith("~ ") && title != null -> {
                        val parts = trimmed.substring(2).split("\t")
                        category = parts.getOrNull(0)?.trim().orEmpty()
                        sub = parts.getOrNull(1)?.trim().orEmpty()
                        thumb = parts.getOrNull(2)?.trim().orEmpty()
                        date = parts.getOrNull(3)?.trim().orEmpty()
                    }
                    trimmed.isNotEmpty() && title != null -> {
                        val parts = trimmed.split("\t")
                        if (parts.size >= 3) {
                            val url = parts[2].trim()
                            if (url.isNotEmpty()) {
                                embeds.add(
                                    Embed(
                                        parts[0].trim(),
                                        if (parts[1].trim() == "hls") "hls" else "iframe",
                                        url,
                                    ),
                                )
                            }
                        }
                    }
                }
            }
            flush()
            val sorted = replays.sortedByDescending { it.date }
            cached = sorted
            sorted
        } catch (_: Exception) {
            emptyList()
        }
    }

    private fun encodeId(r: Replay): String {
        val o = JSONObject().put("title", r.title).put("poster", r.thumb)
        val arr = JSONArray()
        for (e in r.embeds) {
            arr.put(JSONObject().put("label", e.label).put("type", e.type).put("url", e.url))
        }
        o.put("embeds", arr)
        var b64 = android.util.Base64.encodeToString(
            o.toString().toByteArray(Charsets.UTF_8),
            android.util.Base64.URL_SAFE or android.util.Base64.NO_PADDING or android.util.Base64.NO_WRAP,
        )
        return "rz$b64"
    }

    private fun decodeId(id: String): Pair<String, List<Embed>>? {
        return try {
            if (!id.startsWith("rz")) return null
            var b64 = id.removePrefix("rz").replace('-', '+').replace('_', '/')
            while (b64.length % 4 != 0) b64 += "="
            val o = JSONObject(
                String(android.util.Base64.decode(b64, android.util.Base64.DEFAULT), Charsets.UTF_8),
            )
            val embeds = ArrayList<Embed>()
            val arr = o.optJSONArray("embeds")
            if (arr != null) {
                for (i in 0 until arr.length()) {
                    val e = arr.optJSONObject(i) ?: continue
                    embeds.add(Embed(e.optString("label"), e.optString("type"), e.optString("url")))
                }
            }
            o.optString("title") to embeds
        } catch (_: Exception) {
            null
        }
    }

    private fun toMedia(r: Replay): HikariMedia? {
        if (r.title.isBlank()) return null
        val overview = buildString {
            if (r.sub.isNotBlank()) append(r.sub)
            if (r.date.isNotBlank()) {
                if (isNotEmpty()) append(" · ")
                append(r.date)
            }
            if (r.category.isNotBlank()) {
                if (isNotEmpty()) append(" · ")
                append(r.category)
            }
        }.ifBlank { null }
        return HikariMedia(
            id = encodeId(r), title = r.title, type = HikariMediaType.SERIES,
            posterUrl = r.thumb.ifBlank { null }, overview = overview,
        )
    }

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("recent", "Recently Added", HikariMediaType.SERIES),
        HikariCatalog("all", "All Replays", HikariMediaType.SERIES),
        HikariCatalog("football", "Football", HikariMediaType.SERIES),
        HikariCatalog("baseball", "Baseball", HikariMediaType.SERIES),
        HikariCatalog("rugby", "Rugby", HikariMediaType.SERIES),
        HikariCatalog("motorsport", "Motorsport", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val replays = try {
            fetchReplays()
        } catch (_: Exception) {
            return emptyList()
        }
        val pool = when (catalog.id) {
            "recent" -> replays.take(30)
            "all" -> replays
            "football" -> replays.filter { it.category.equals("Football", true) }
            "baseball" -> replays.filter { it.category.equals("Baseball", true) }
            "rugby" -> replays.filter { it.category.equals("Rugby", true) }
            "motorsport" -> replays.filter { it.category.equals("Motorsport", true) }
            else -> return emptyList()
        }
        val p = page.coerceAtLeast(1)
        return pool.drop((p - 1) * 30).take(30).mapNotNull { toMedia(it) }
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank()) return emptyList()
        val replays = try {
            fetchReplays()
        } catch (_: Exception) {
            return emptyList()
        }
        val hits = replays.filter { it.title.contains(query.trim(), true) }
        val p = page.coerceAtLeast(1)
        return hits.drop((p - 1) * 30).take(30).mapNotNull { toMedia(it) }
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val (title, embeds) = decodeId(media.id) ?: return null
        if (embeds.isEmpty()) return null
        return listOf(
            HikariEpisode(
                number = 1, id = media.id,
                name = "$title (${embeds.size} sources)", image = media.posterUrl,
            ),
        )
    }

    private val capture = Regex("""https?://[^"' \s]+\.m3u8[^"' \s]*|https?://[^"' \s]+\.mp4[^"' \s]*""")

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val (_, embeds) = decodeId(episode?.id ?: media.id) ?: return emptyList()
        if (embeds.isEmpty()) return emptyList()
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        fun emit(url: String, label: String, isM3u8: Boolean) {
            if (url.isBlank() || !seen.add(url)) return
            out.add(HikariStream(name = "[ReplayZone] $label", url = url, isM3u8 = isM3u8))
        }
        coroutineScope {
            embeds.map { embed ->
                async(Dispatchers.IO) {
                    val found = ArrayList<HikariStream>()
                    fun add(url: String, isM3u8: Boolean) {
                        if (url.isNotBlank()) {
                            found.add(
                                HikariStream(
                                    name = "[ReplayZone] ${embed.label}", url = url, isM3u8 = isM3u8,
                                ),
                            )
                        }
                    }
                    try {
                        when {
                            embed.type == "hls" -> {
                                val u = if (embed.url.contains("soccerfull.net/hls/")) {
                                    "$workerUrl/hls?u=${URLEncoder.encode(embed.url, "UTF-8")}"
                                } else embed.url
                                add(u, true)
                            }
                            embed.url.contains("soccerfull.net/play/") -> {
                                try {
                                    val html = HikariNet.getString(embed.url) ?: ""
                                    val path = Regex("""m3u8Url\s*=\s*["']([^"']+)["']""")
                                        .find(html)?.groupValues?.get(1)
                                    if (path != null) {
                                        val full = if (path.startsWith("http")) path
                                        else "https://soccerfull.net$path"
                                        add("$workerUrl/hls?u=${URLEncoder.encode(full, "UTF-8")}", true)
                                    }
                                } catch (_: Exception) {
                                }
                            }
                            embed.url.contains(".m3u8") -> add(embed.url, true)
                            embed.url.contains(".mp4") -> add(embed.url, false)
                            else -> {
                                // ok.ru / dailymotion / byse / unknown: capture playback
                                var target = embed.url
                                if (target.contains("ok.ru/videoembed/")) {
                                    target = target.replace("videoembed/", "video/")
                                }
                                if (target.contains("dailymotion.com")) {
                                    Regex("""video=([a-zA-Z0-9]+)""").find(target)
                                        ?.groupValues?.get(1)?.let { vid ->
                                            target = "https://www.dailymotion.com/video/$vid"
                                        }
                                }
                                try {
                                    for (r in HikariNet.resolveWithWebView(target, capture, timeoutMs = 60_000)) {
                                        found.add(
                                            HikariStream(
                                                name = "[ReplayZone] ${embed.label}", url = r.url,
                                                headers = r.headers,
                                                isM3u8 = r.url.contains("m3u8"),
                                            ),
                                        )
                                    }
                                } catch (_: Exception) {
                                }
                            }
                        }
                    } catch (_: Exception) {
                    }
                    found
                }
            }.forEach { d ->
                try {
                    for (s in d.await()) if (seen.add(s.url)) out.add(s)
                } catch (_: Exception) {
                }
            }
        }
        return out
    }
}
