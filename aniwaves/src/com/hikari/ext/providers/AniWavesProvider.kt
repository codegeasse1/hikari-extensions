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
 * AniWaves (aniwaves.ru) — anime via HTML grids + ajax episode/server/sources.
 * Ported from KSHITIJ8473/raghav AniWaves to native Hikari.
 *
 * Flow: cards (.ani.items .item) -> detail -> GET /ajax/episode/list/{animeId}
 * -> GET /ajax/server/list?servers={dataIds} (li[data-link-id][data-sv-id]
 * under .type[data-type=sub|dub]) -> GET /ajax/sources?id={linkId} -> embed
 * URL (megaplay or filemoon-like) resolved via WebView capture.
 */
class AniWavesProvider : HikariProvider {

    override val id = "aniwaves"
    override val name = "AniWaves"
    override val mainUrl = "https://aniwaves.ru"
    override val description = "Anime from aniwaves.ru — latest, popular, movies, sub & dub."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val pageHeaders = mapOf(
        "User-Agent" to "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36",
        "Accept" to "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "X-Requested-With" to "XMLHttpRequest",
        "Referer" to "$mainUrl/",
    )
    private val capture = Regex("""https?://[^"' \s]+\.m3u8[^"' \s]*|https?://[^"' \s]*master\.txt[^"' \s]*|https?://[^"' \s]+\.mp4[^"' \s]*""")
    private val anchorRe = Regex("""<a[^>]+href="([^"]+)"[^>]*>([\s\S]*?)</a>""", RegexOption.IGNORE_CASE)
    private val imgRe = Regex("""(?:data-src|data-original|src)="([^"]+)"""", RegexOption.IGNORE_CASE)

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("latest", "Latest Episodes", HikariMediaType.SERIES),
        HikariCatalog("popular", "Most Popular", HikariMediaType.SERIES),
        HikariCatalog("movies", "Movies", HikariMediaType.MOVIE),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val p = page.coerceAtLeast(1)
        val url = when (catalog.id) {
            "popular" -> "$mainUrl/most-popular?page=$p"
            "movies" -> "$mainUrl/movies?page=$p"
            else -> if (p <= 1) mainUrl else "$mainUrl/page/$p"
        }
        val html = HikariNet.getStringSmart(url, pageHeaders) ?: return emptyList()
        return parseCards(html)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        val p = page.coerceAtLeast(1)
        val html = HikariNet.getStringSmart("$mainUrl/search?q=${URLEncoder.encode(q, "UTF-8")}&page=$p", pageHeaders)
            ?: HikariNet.getStringSmart("$mainUrl/?s=${URLEncoder.encode(q, "UTF-8")}", pageHeaders)
            ?: return emptyList()
        return parseCards(html)
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val html = HikariNet.getStringSmart(media.id, pageHeaders) ?: return media
        val title = Regex("""<meta[^>]+property="og:title"[^>]+content="([^"]+)"""").find(html)?.groupValues?.get(1)?.let { unesc(it) }
            ?: media.title
        val poster = Regex("""<meta[^>]+property="og:image"[^>]+content="([^"]+)"""").find(html)?.groupValues?.get(1)
            ?.takeIf { it.startsWith("http") } ?: media.posterUrl
        val desc = Regex("""<meta[^>]+(?:name|property)="description"[^>]+content="([^"]+)"""").find(html)?.groupValues?.get(1)?.let { unesc(it) }
        return media.copy(title = title, posterUrl = poster, backdropUrl = poster, overview = desc ?: media.overview)
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val html = HikariNet.getStringSmart(media.id, pageHeaders) ?: return null
        val animeId = Regex("""episode/list/(\d+)|data-id="(\d+)"|/anime/(\d+)""").find(html)?.groupValues?.firstOrNull { it.isNotBlank() && it.all { c -> c.isDigit() } }
            ?: Regex("""ajax/episode/list/(\d+)""").find(html)?.groupValues?.get(1)
            ?: return fallbackEpisodes(html, media)
        val ajax = HikariNet.getString("$mainUrl/ajax/episode/list/$animeId", pageHeaders) ?: return fallbackEpisodes(html, media)
        return try {
            val resultHtml = JSONObject(ajax).optString("result", "")
            if (resultHtml.isBlank()) return fallbackEpisodes(html, media)
            val out = ArrayList<HikariEpisode>()
            for (m in Regex("""<a[^>]+href="([^"]+)"[^>]*data-ids="([^"]*)"[^>]*>([\s\S]*?)</a>""").findAll(resultHtml)) {
                val href = abs(m.groupValues[1])
                val dataIds = m.groupValues[2]
                val label = unesc(strip(m.groupValues[3])).ifBlank { "Episode" }
                val num = Regex("""(\d+)""").find(label)?.groupValues?.get(1)?.toIntOrNull() ?: (out.size / 2 + 1)
                val isDub = label.contains("dub", true)
                out.add(HikariEpisode(number = num, id = "$href|${if (isDub) "dub" else "sub"}|$animeId|$num|$dataIds", name = label))
            }
            if (out.isEmpty()) {
                for (m in anchorRe.findAll(resultHtml)) {
                    val href = m.groupValues[1]
                    if (!href.contains("/watch") && !href.contains("/episode")) continue
                    val label = unesc(strip(m.groupValues[2])).ifBlank { continue }
                    val num = Regex("""(\d+)""").find(label)?.groupValues?.get(1)?.toIntOrNull() ?: continue
                    out.add(HikariEpisode(number = num, id = "${abs(href)}|sub|$animeId|$num|", name = label))
                }
            }
            out.ifEmpty { null }
        } catch (t: Throwable) { fallbackEpisodes(html, media) }
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val ref = episode?.id ?: return emptyList()
        val parts = ref.split("|")
        val watchUrl = parts.getOrNull(0)?.takeIf { it.startsWith("http") } ?: return emptyList()
        val targetType = parts.getOrNull(1)?.takeIf { it == "dub" } ?: "sub"
        val dataIds = parts.getOrNull(4).orEmpty()
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        // Server list -> sources -> embed.
        if (dataIds.isNotBlank()) {
            try {
                val srvBody = HikariNet.getString("$mainUrl/ajax/server/list?servers=${URLEncoder.encode(dataIds, "UTF-8")}", pageHeaders)
                val srvHtml = if (srvBody != null) JSONObject(srvBody).optString("result", "") else ""
                val linkIds = ArrayList<String>()
                val section = Regex("""data-type=["']?$targetType["']?([\s\S]*?)(?:data-type=|$)""", RegexOption.IGNORE_CASE).find(srvHtml)?.groupValues?.get(1) ?: srvHtml
                for (m in Regex("""data-link-id="([^"]+)"""").findAll(section)) linkIds.add(m.groupValues[1])
                for (linkId in linkIds.take(6)) {
                    try {
                        val srcBody = HikariNet.getString("$mainUrl/ajax/sources?id=${URLEncoder.encode(linkId, "UTF-8")}&asi=0&autoPlay=0", pageHeaders) ?: continue
                        val embed = JSONObject(srcBody).let { o ->
                            val r = o.optJSONObject("result")
                            r?.optString("url").ifBlank { o.optString("url") }
                        }
                        if (embed.isNullOrBlank() || !embed.startsWith("http")) continue
                        val captured = HikariNet.resolveWithWebView(embed, capture, timeoutMs = 45_000)
                        for (r in captured) {
                            if (!seen.add(r.url)) continue
                            out.add(HikariStream(name = "AniWaves ${targetType.uppercase()}", url = r.url, headers = r.headers, isM3u8 = r.url.contains("m3u8") || r.url.contains("master.txt")))
                        }
                    } catch (t: Throwable) { }
                    if (out.isNotEmpty()) return out
                }
            } catch (t: Throwable) { }
        }
        // Fallback: watch page embeds.
        val html = HikariNet.getStringSmart(watchUrl, pageHeaders) ?: return out
        for (m in Regex("""<iframe[^>]+src="([^"]+)"""").findAll(html)) {
            val src = m.groupValues[1]
            if (!src.startsWith("http")) continue
            val captured = HikariNet.resolveWithWebView(src, capture, timeoutMs = 45_000)
            for (r in captured) {
                if (!seen.add(r.url)) continue
                out.add(HikariStream(name = "AniWaves ${targetType.uppercase()}", url = r.url, headers = r.headers, isM3u8 = r.url.contains("m3u8") || r.url.contains("master.txt")))
            }
            if (out.isNotEmpty()) return out
        }
        return out
    }

    private fun parseCards(html: String): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        val seen = HashSet<String>()
        for (m in anchorRe.findAll(html)) {
            val href = m.groupValues[1]
            if (!href.contains("/anime") && !href.contains("/movie") && !href.contains("/watch")) continue
            if (href.contains("/genre") || href.contains("/page")) continue
            val inner = m.groupValues[2]
            if (!inner.contains("<img")) continue
            val img = imgRe.find(inner)?.groupValues?.get(1) ?: ""
            var title = Regex("""alt="([^"]+)"""").find(inner)?.groupValues?.get(1)
                ?: Regex("""title="([^"]+)"""").find(m.value)?.groupValues?.get(1)
                ?: unesc(strip(inner)).lineSequence().firstOrNull { it.trim().length > 2 }
            title = title?.let { unesc(it) }?.takeIf { it.isNotBlank() } ?: continue
            val url = abs(href)
            if (!seen.add(url)) continue
            out.add(HikariMedia(id = url, title = title, type = if (href.contains("/movie")) HikariMediaType.MOVIE else HikariMediaType.SERIES, posterUrl = img.takeIf { it.startsWith("http") }))
            if (out.size >= 60) break
        }
        return out
    }

    private fun fallbackEpisodes(html: String, media: HikariMedia): List<HikariEpisode>? {
        val out = ArrayList<HikariEpisode>()
        for (m in anchorRe.findAll(html)) {
            val href = m.groupValues[1]
            if (!href.contains("/watch") && !href.contains("/episode")) continue
            val label = unesc(strip(m.groupValues[2]))
            if (label.isBlank()) continue
            val num = Regex("""(\d+)""").find(label)?.groupValues?.get(1)?.toIntOrNull() ?: continue
            out.add(HikariEpisode(number = num, id = "${abs(href)}|sub|||", name = label, image = media.posterUrl))
        }
        return out.sortedBy { it.number }.ifEmpty { null }
    }

    private fun abs(href: String): String {
        if (href.startsWith("http")) return href
        return if (href.startsWith("/")) "$mainUrl$href" else "$mainUrl/$href"
    }
    private fun unesc(s: String): String = s.replace("&amp;", "&").replace("&#39;", "'").replace("&#039;", "'").replace("&quot;", "\"").replace("&lt;", "<").replace("&gt;", ">").replace("&nbsp;", " ").trim()
    private fun strip(s: String): String = s.replace(Regex("<[^>]+>"), " ").replace(Regex("\\s+"), " ").trim()
}
