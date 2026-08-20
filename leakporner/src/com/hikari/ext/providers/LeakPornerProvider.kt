package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.net.URLEncoder

/**
 * LeakPorner (leakporner.org) — leaked/OnlyFans adult videos running the
 * "retrotube" WP theme.
 *
 *  - Home/category/search pages are grids of `article.loop-video` cards.
 *  - Each video page lists 4-6 player servers as `span.change-video`
 *    buttons carrying `data-embed` URLs (luluvids.top, bysezoxexe.com,
 *    playmogo.com, morencius.com, hgcloud.to, abyssplayer.com…). Those embeds
 *    are JS-driven players whose streams are only produced by a real browser,
 *    so every server is resolved with the WebView m3u8/mp4 capture helper.
 */
class LeakPornerProvider : HikariProvider {

    override val id = "leakporner"
    override val name = "LeakPorner"
    override val mainUrl = "https://leakporner.org"
    override val description = "Leaked/OF adult videos from leakporner.org — latest uploads, search and multi-server WebView-resolved playback."
    override val version = 1
    override val tvTypes: Set<HikariMediaType> = setOf(HikariMediaType.MOVIE)

    companion object {
        private const val BASE = "https://leakporner.org"
        private const val CACHE_TTL_MS = 600_000L

        private val htmlCache = HashMap<String, Pair<Long, String>>()
        private val cacheMutex = Mutex()

        private val pageHeaders = mapOf(
            "User-Agent" to HikariNet.browserHeaders["User-Agent"].orEmpty(),
            "Accept" to "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language" to "en-US,en;q=0.9",
        )

        private val streamCapture = Regex("""https?://[^"'\s]+?\.(?:m3u8|mp4)(?:[?#][^"'\s]*)?""")
    }

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("latest", "Latest Videos", HikariMediaType.MOVIE),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        if (catalog.id != "latest") return emptyList()
        val url = if (page <= 1) "$BASE/" else "$BASE/page/$page/"
        return parseCards(getCached(url) ?: return emptyList())
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        val encoded = URLEncoder.encode(q, "UTF-8").replace("+", "%20")
        val url = if (page <= 1) "$BASE/?s=$encoded" else "$BASE/page/$page/?s=$encoded"
        return parseCards(getCached(url) ?: return emptyList())
    }

    // ------------------------------------------------------------------
    //  Meta + streams
    // ------------------------------------------------------------------

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val pageUrl = media.id.takeIf { it.startsWith("http") } ?: return media
        val html = getCached(pageUrl) ?: return media
        val title = Regex("""<h1[^>]*>([\s\S]*?)</h1>""").find(html)?.groupValues?.get(1)
            ?.let { unescape(stripTags(it)) }
            ?: metaProperty(html, "og:title")?.let { unescape(it) }
            ?: media.title
        val poster = metaProperty(html, "og:image")?.takeIf { it.startsWith("http") } ?: media.posterUrl
        val overview = metaProperty(html, "og:description")?.let { unescape(it) }
            ?.takeIf { it.isNotBlank() && it != title }
        return media.copy(
            title = title,
            posterUrl = poster,
            type = HikariMediaType.MOVIE,
            overview = overview ?: media.overview,
            backdropUrl = poster,
        )
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val pageUrl = media.id.takeIf { it.startsWith("http") } ?: return emptyList()
        val html = getCached(pageUrl) ?: return emptyList()
        val embeds = Regex("""data-embed="([^"]+)""")
            .findAll(html).map { it.groupValues[1] }
            .filter { it.startsWith("http") }
            .toList()
            .distinct()
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        for (embed in embeds) {
            if (out.size >= 4) break
            val hits = try {
                HikariNet.resolveWithWebView(embed, streamCapture, timeoutMs = 45_000)
            } catch (t: Throwable) {
                continue
            }
            for (h in hits) {
                val u = h.url
                if (u.isBlank() || !seen.add(u)) continue
                out.add(
                    HikariStream(
                        name = "Server ${out.size + 1}",
                        url = u,
                        headers = h.headers,
                        isM3u8 = u.contains(".m3u8", ignoreCase = true),
                    )
                )
            }
        }
        return out
    }

    // ------------------------------------------------------------------
    //  Parsers + helpers
    // ------------------------------------------------------------------

    /** Parses `article.loop-video` cards. */
    private fun parseCards(html: String): List<HikariMedia> {
        val out = LinkedHashMap<String, HikariMedia>()
        for (chunk in html.split(Regex("""<article\s+data-video-uid""")).drop(1)) {
            val m = Regex("""<a href="(https://leakporner\.[a-z]+/[^"]+)" title="([^"]+)"""")
                .find(chunk) ?: continue
            val href = m.groupValues[1]
            val title = unescape(m.groupValues[2])
            val img = Regex("""<img[^>]*data-src="([^"]+)""").find(chunk)?.groupValues?.get(1)
            out[href] = HikariMedia(
                id = href,
                title = title,
                type = HikariMediaType.MOVIE,
                posterUrl = img?.takeIf { it.startsWith("http") },
            )
        }
        return out.values.toList()
    }

    private suspend fun getCached(url: String): String? {
        val now = System.currentTimeMillis()
        cacheMutex.withLock {
            htmlCache[url]?.let { (t, html) ->
                if (now - t < CACHE_TTL_MS) return html
            }
        }
        val html = HikariNet.getStringSmart(url, pageHeaders) ?: return null
        cacheMutex.withLock {
            if (htmlCache.size > 60) htmlCache.clear()
            htmlCache[url] = now to html
        }
        return html
    }

    private fun metaProperty(html: String, property: String): String? =
        Regex("""<meta\s+property="$property"\s+content="([^"]*)""").find(html)?.groupValues?.get(1)

    private fun stripTags(s: String): String = s
        .replace(Regex("""<[^>]+>"""), " ")
        .let { unescape(it) }
        .replace(Regex("""\s+"""), " ")
        .trim()

    private fun unescape(s: String): String = s
        .replace("&quot;", "\"")
        .replace("&#039;", "'")
        .replace("&#39;", "'")
        .replace("&#8211;", "-")
        .replace("&#8230;", "\u2026")
        .replace("&#038;", "&")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&nbsp;", " ")
        .replace(Regex("""&#(\d+);""")) { m ->
            m.groupValues[1].toIntOrNull()?.let { runCatching { it.toChar().toString() }.getOrNull() } ?: m.value
        }
        .trim()
}
