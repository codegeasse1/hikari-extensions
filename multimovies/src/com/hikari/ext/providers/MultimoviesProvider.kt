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
 * Multimovies (multimovies.garden) — movies & series card grid + watch servers.
 * Ported from KSHITIJ8473/raghav Multimovies to native Hikari.
 *
 * Cards are generic grids; search is /search?q=; watch pages carry an
 * initialServers JSON config plus season/episode-range navigation
 * (?season=&ep_range=). Server embeds (filemoon/modiplay/iqsmart-style)
 * resolve via WebView m3u8 capture.
 */
class MultimoviesProvider : HikariProvider {

    override val id = "multimovies"
    override val name = "Multimovies"
    override val mainUrl = "https://multimovies.garden"
    override val description = "Movies & series from Multimovies — latest, trending, multi-server HLS."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val pageHeaders = mapOf(
        "User-Agent" to "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36",
        "Accept" to "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer" to "$mainUrl/",
    )
    private val capture = Regex("""https?://[^"' \s]+\.m3u8[^"' \s]*|https?://[^"' \s]+\.mp4[^"' \s]*""")
    private val anchorRe = Regex("""<a[^>]+href="([^"]+)"[^>]*>([\s\S]*?)</a>""", RegexOption.IGNORE_CASE)
    private val imgRe = Regex("""(?:data-src|data-original|src)="([^"]+)"""", RegexOption.IGNORE_CASE)

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("latest", "Latest Added", HikariMediaType.MOVIE),
        HikariCatalog("trending", "Trending", HikariMediaType.MOVIE),
        HikariCatalog("series", "TV Series", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val p = page.coerceAtLeast(1)
        val base = when (catalog.id) {
            "series" -> "$mainUrl/series"
            "trending" -> "$mainUrl/trending"
            else -> "$mainUrl/latest"
        }
        val url = if (p <= 1) base else "$base/page/$p"
        val html = HikariNet.getStringSmart(url, pageHeaders)
            ?: HikariNet.getStringSmart(if (p <= 1) mainUrl else "$mainUrl/page/$p", pageHeaders)
            ?: return emptyList()
        return parseCards(html)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank()) return emptyList()
        val q = URLEncoder.encode(query.trim(), "UTF-8")
        val html = HikariNet.getStringSmart("$mainUrl/search?q=$q", pageHeaders)
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
        // Explicit episode links.
        for (m in Regex("""<a[^>]+href="([^"]+)"[^>]*>([^<]*?(?:episode|ep)\s*\d+[^<]*)</a>""", RegexOption.IGNORE_CASE).findAll(html)) {
            val href = abs(m.groupValues[1])
            val label = unesc(m.groupValues[2])
            val num = Regex("""(\d+)""").find(label)?.groupValues?.get(1)?.toIntOrNull() ?: continue
            val season = Regex("""season\s*(\d+)""", RegexOption.IGNORE_CASE).find(html.substring(0, m.range.first.coerceAtMost(html.length)))?.groupValues?.get(1)?.toIntOrNull() ?: 1
            out.add(HikariEpisode(number = num, season = season, id = href, name = label, image = media.posterUrl))
        }
        if (out.isNotEmpty()) return out.sortedWith(compareBy({ it.season }, { it.number }))
        // Season navigation (?season=N) with optional ep_range pages.
        val seasons = Regex("""[?&]season=(\d+)""").findAll(html).map { it.groupValues[1].toIntOrNull() ?: 1 }.toSet().takeIf { it.isNotEmpty() }
        if (seasons != null) {
            for (sn in seasons.take(20)) {
                val sHtml = HikariNet.getStringSmart("${media.id}${if (media.id.contains("?")) "&" else "?"}season=$sn", pageHeaders) ?: continue
                val ranges = Regex("""ep_range=(\d+)""").findAll(sHtml).map { it.groupValues[1] }.toSet()
                val pages = if (ranges.isEmpty()) listOf(sHtml) else {
                    val list = ArrayList<String>()
                    list.add(sHtml)
                    for (r in ranges.take(10)) {
                        HikariNet.getStringSmart("${media.id}${if (media.id.contains("?")) "&" else "?"}season=$sn&ep_range=$r", pageHeaders)?.let { list.add(it) }
                    }
                    list
                }
                for (ph in pages) {
                    for (m in Regex("""<a[^>]+href="([^"]+)"[^>]*>([^<]*?(?:episode|ep)\s*\d+[^<]*)</a>""", RegexOption.IGNORE_CASE).findAll(ph)) {
                        val num = Regex("""(\d+)""").find(m.groupValues[2])?.groupValues?.get(1)?.toIntOrNull() ?: continue
                        out.add(HikariEpisode(number = num, season = sn, id = abs(m.groupValues[1]), name = unesc(m.groupValues[2]), image = media.posterUrl))
                    }
                }
            }
            if (out.isNotEmpty()) return out.sortedWith(compareBy({ it.season }, { it.number }))
        }
        // Movie / single-video fallback.
        if (html.contains("initialServers") || Regex("""<iframe[^>]+src="http"""").containsMatchIn(html)) {
            return listOf(HikariEpisode(number = 1, id = media.id, name = media.title, image = media.posterUrl))
        }
        return null
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val pageUrl = episode?.id?.takeIf { it.startsWith("http") } ?: media.id
        val html = HikariNet.getStringSmart(pageUrl, pageHeaders) ?: return emptyList()
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        // Watch config server URLs.
        for (m in Regex(""""url"\s*:\s*"(https?://[^"]+)"""").findAll(html)) {
            val u = m.groupValues[1]
            if (!u.startsWith("http") || !seen.add("cfg:$u")) continue
            for (r in HikariNet.resolveWithWebView(u, capture, timeoutMs = 45_000)) {
                if (!seen.add(r.url)) continue
                out.add(HikariStream(name = "Multimovies", url = r.url, headers = r.headers, isM3u8 = r.url.contains("m3u8")))
            }
            if (out.isNotEmpty()) return out
        }
        for (m in capture.findAll(html)) {
            if (!seen.add(m.value)) continue
            out.add(HikariStream(name = "Multimovies", url = m.value, isM3u8 = m.value.contains("m3u8")))
        }
        if (out.isNotEmpty()) return out
        for (m in Regex("""<iframe[^>]+src="([^"]+)"""").findAll(html)) {
            val src = m.groupValues[1]
            if (!src.startsWith("http")) continue
            for (r in HikariNet.resolveWithWebView(src, capture, timeoutMs = 60_000)) {
                if (!seen.add(r.url)) continue
                out.add(HikariStream(name = "Multimovies", url = r.url, headers = r.headers, isM3u8 = r.url.contains("m3u8")))
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
            if (href.contains("/search") || href.contains("?s=") || href.startsWith("#")) continue
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
