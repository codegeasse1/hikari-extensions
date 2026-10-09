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
 * NetNaija (netnaija.film) — Nollywood / Hollywood movies & series.
 * Ported from KSHITIJ8473/raghav NetNaija to native Hikari.
 *
 * The source also drives an authenticated BFF (h5-api.aoneroom.com) with a
 * JWT bootstrap; the native port uses the public HTML (card grids,
 * /search-result?keyword=, download/episode buttons) so it works without
 * tokens, resolving file hosts via WebView capture.
 */
class NetNaijaProvider : HikariProvider {

    override val id = "netnaija"
    override val name = "NetNaija"
    override val mainUrl = "https://netnaija.film"
    override val description = "Nollywood & Hollywood movies and series from NetNaija."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val pageHeaders = mapOf(
        "User-Agent" to "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36",
        "Accept" to "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer" to "$mainUrl/",
    )
    private val capture = Regex("""https?://[^"' \s]+\.m3u8[^"' \s]*|https?://[^"' \s]+\.mp4[^"' \s]*|https?://[^"' \s]+\.mkv[^"' \s]*""")
    private val anchorRe = Regex("""<a[^>]+href="([^"]+)"[^>]*>([\s\S]*?)</a>""", RegexOption.IGNORE_CASE)
    private val imgRe = Regex("""(?:data-src|data-original|src)="([^"]+)"""", RegexOption.IGNORE_CASE)

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("latest", "Latest Movies", HikariMediaType.MOVIE),
        HikariCatalog("series", "Latest Series", HikariMediaType.SERIES),
        HikariCatalog("trending", "Trending", HikariMediaType.MOVIE),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val p = page.coerceAtLeast(1)
        val base = when (catalog.id) {
            "series" -> "$mainUrl/series"
            "trending" -> "$mainUrl/trending"
            else -> "$mainUrl/movies"
        }
        val html = HikariNet.getStringSmart(if (p <= 1) base else "$base/page/$p", pageHeaders)
            ?: HikariNet.getStringSmart(if (p <= 1) mainUrl else "$mainUrl/page/$p", pageHeaders)
            ?: return emptyList()
        return parseCards(html)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank()) return emptyList()
        val q = URLEncoder.encode(query.trim(), "UTF-8")
        val html = HikariNet.getStringSmart("$mainUrl/search-result?keyword=$q", pageHeaders)
            ?: HikariNet.getStringSmart("$mainUrl/?s=$q", pageHeaders)
            ?: return emptyList()
        return parseCards(html)
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val html = HikariNet.getStringSmart(media.id, pageHeaders) ?: return media
        val title = Regex("""<meta[^>]+property="og:title"[^>]+content="([^"]+)"""").find(html)?.groupValues?.get(1)?.let { unesc(it) } ?: media.title
        val poster = Regex("""<meta[^>]+property="og:image"[^>]+content="([^"]+)"""").find(html)?.groupValues?.get(1)?.takeIf { it.startsWith("http") } ?: media.posterUrl
        val desc = Regex("""<meta[^>]+(?:name|property)="description"[^>]+content="([^"]+)"""").find(html)?.groupValues?.get(1)?.let { unesc(it) }
        return media.copy(title = title, posterUrl = poster, backdropUrl = poster, overview = desc ?: media.overview)
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val html = HikariNet.getStringSmart(media.id, pageHeaders) ?: return null
        val out = ArrayList<HikariEpisode>()
        // Download/episode buttons: "Episode 3", "S01E02", download links.
        for (m in Regex("""<a[^>]+href="([^"]+)"[^>]*>([^<]*?(?:episode|S\d+\s*E\d+|download)[^<]*)</a>""", RegexOption.IGNORE_CASE).findAll(html)) {
            val href = abs(m.groupValues[1])
            val label = unesc(m.groupValues[2])
            if (label.isBlank()) continue
            val se = Regex("""S(\d+)\s*E(\d+)""", RegexOption.IGNORE_CASE).find(label)
            val num = se?.groupValues?.get(2)?.toIntOrNull()
                ?: Regex("""episode\s*(\d+)""", RegexOption.IGNORE_CASE).find(label)?.groupValues?.get(1)?.toIntOrNull()
                ?: (out.size + 1)
            val season = se?.groupValues?.get(1)?.toIntOrNull() ?: 1
            out.add(HikariEpisode(number = num, season = season, id = href, name = label, image = media.posterUrl))
        }
        if (out.isNotEmpty()) return out.sortedWith(compareBy({ it.season }, { it.number }))
        // Single movie: the page itself is the playable item.
        if (html.contains("download", true) || Regex("""<iframe[^>]+src="http"""").containsMatchIn(html)) {
            return listOf(HikariEpisode(number = 1, id = media.id, name = media.title, image = media.posterUrl))
        }
        return null
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        var pageUrl = episode?.id?.takeIf { it.startsWith("http") } ?: media.id
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        // Download intermediate pages often link the real file host.
        repeat(2) {
            val html = HikariNet.getStringSmart(pageUrl, pageHeaders) ?: return@repeat
            for (m in capture.findAll(html)) {
                if (m.value.contains(".css") || m.value.contains(".js")) continue
                if (!seen.add(m.value)) continue
                out.add(HikariStream(name = "NetNaija", url = m.value, isM3u8 = m.value.contains("m3u8")))
            }
            if (out.isNotEmpty()) return out
            for (m in Regex("""<iframe[^>]+src="([^"]+)"""").findAll(html)) {
                val src = m.groupValues[1]
                if (!src.startsWith("http")) continue
                for (r in HikariNet.resolveWithWebView(src, capture, timeoutMs = 45_000)) {
                    if (!seen.add(r.url)) continue
                    out.add(HikariStream(name = "NetNaija", url = r.url, headers = r.headers, isM3u8 = r.url.contains("m3u8")))
                }
                if (out.isNotEmpty()) return out
            }
            // Follow the first download button one level deeper.
            val next = Regex("""<a[^>]+href="([^"]+)"[^>]*>([^<]*download[^<]*)</a>""", RegexOption.IGNORE_CASE).find(html)?.groupValues?.get(1)
            if (next != null && abs(next) != pageUrl) pageUrl = abs(next) else return@repeat
        }
        for (r in HikariNet.resolveWithWebView(pageUrl, capture, timeoutMs = 45_000)) {
            if (!seen.add(r.url)) continue
            out.add(HikariStream(name = "NetNaija", url = r.url, headers = r.headers, isM3u8 = r.url.contains("m3u8")))
        }
        return out
    }

    private fun parseCards(html: String): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        val seen = HashSet<String>()
        for (m in anchorRe.findAll(html)) {
            val href = m.groupValues[1]
            if (href.startsWith("#") || href.contains("?s=") || href.contains("search-result")) continue
            val inner = m.groupValues[2]
            if (!inner.contains("<img")) continue
            val img = imgRe.find(inner)?.groupValues?.get(1) ?: ""
            val title = Regex("""alt="([^"]+)"""").find(inner)?.groupValues?.get(1)?.let { unesc(it) }?.takeIf { it.length > 2 } ?: continue
            val url = abs(href)
            if (!seen.add(url)) continue
            out.add(HikariMedia(id = url, title = title, type = HikariMediaType.MOVIE, posterUrl = img.takeIf { it.startsWith("http") }))
            if (out.size >= 60) break
        }
        return out
    }

    private fun abs(href: String): String {
        if (href.startsWith("http")) return href
        return if (href.startsWith("/")) "$mainUrl$href" else "$mainUrl/$href"
    }
    private fun unesc(s: String): String = s.replace("&amp;", "&").replace("&#39;", "'").replace("&#039;", "'").replace("&quot;", "\"").replace("&lt;", "<").replace("&gt;", ">").replace("&nbsp;", " ").trim()
}
