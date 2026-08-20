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
 * PornHoarder (pornhoarder.st) — mirror/aggregator of adult videos hosted on
 * streamtape, doodstream, lulustream, filemoon, mixdrop and other file hosts.
 *
 *  - Latest/Popular catalogs and search go through POST `/ajax_search.php`
 *    (`search`, `sort`, `page` fields) which returns plain card HTML.
 *  - Trending is server-rendered at `/trending-videos/?page=N`.
 *  - The video page lists every host as a `/pornvideo/<slug>/<hostHash>/`
 *    link. The site's main player (`pornhoarder.net/player.php`) is gated
 *    behind a human image captcha, but each host page has a direct download
 *    stub — `pornhoarder.net/download.php?video=<hostHash>` embeds
 *    `var durl = "<base64>"`, whose decoded value is the host's own watch
 *    page (e.g. `https://streamtape.to/v/<id>/`). Those pages are JS-driven
 *    players, so every host is resolved with the WebView mp4 capture helper
 *    (headers + cookies included), exactly like the main player flow would
 *    after a human passes the captcha.
 */
class PornhoarderProvider : HikariProvider {

    override val id = "pornhoarder"
    override val name = "PornHoarder"
    override val mainUrl = "https://pornhoarder.st"
    override val description = "Pornhoarder.tv mirrors — latest, trending and search with WebView-resolved playback from streamtape/doodstream/etc."
    override val version = 1
    override val tvTypes: Set<HikariMediaType> = setOf(HikariMediaType.MOVIE)

    companion object {
        private const val BASE = "https://pornhoarder.st"
        private const val DL = "https://pornhoarder.net"
        private const val CACHE_TTL_MS = 600_000L

        private val htmlCache = HashMap<String, Pair<Long, String>>()
        private val cacheMutex = Mutex()

        private val pageHeaders = mapOf(
            "User-Agent" to HikariNet.browserHeaders["User-Agent"].orEmpty(),
            "Accept" to "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language" to "en-US,en;q=0.9",
        )

        private val streamCapture = Regex("""https?://[^"'\\s]+?\\.(?:m3u8|mp4)(?:[?#][^"'\\s]*)?""")
    }

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("latest", "Latest Videos", HikariMediaType.MOVIE),
        HikariCatalog("trending", "Trending Videos", HikariMediaType.MOVIE),
        HikariCatalog("popular", "Popular Videos", HikariMediaType.MOVIE),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> = when (catalog.id) {
        "latest" -> ajaxSearch("", 0, page)
        "popular" -> ajaxSearch("", 2, page)
        "trending" -> parseCards(getCached("$BASE/trending-videos/?page=$page") ?: return emptyList())
        else -> emptyList()
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        return ajaxSearch(q, 0, page)
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
        val overview = Regex("""name="description"\s+content="([^"]*)"""")
            .find(html)?.groupValues?.get(1)
            ?.let { unescape(it) }
            ?.takeIf { it.isNotBlank() && !it.startsWith("Watch ") }
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

        // Every host appears both as the main player.php hash (iframe) and as
        // a `.video-detail-servers` link. Collect them all, dedupe.
        val hashes = LinkedHashSet<String>()
        Regex("""player\.php\?video=([A-Za-z0-9+/=]+)""").findAll(html).forEach {
            hashes.add(it.groupValues[1])
        }
        Regex("""<a href='/pornvideo/[^']+/([A-Za-z0-9+/=]+)' title='Watch this video on [^']+'>""")
            .findAll(html).forEach {
                hashes.add(it.groupValues[1])
            }
        if (hashes.isEmpty()) return emptyList()

        val hostUrls = ArrayList<String>()
        for (hash in hashes) {
            if (hostUrls.size >= 3) break
            // download.php does a literal comparison of the raw query value,
            // so pass the base64 hash unencoded (the site's own links do too).
            val dl = getCached("$DL/download.php?video=$hash") ?: continue
            val durl = Regex("""var durl = "([^"]+)"""").find(dl)?.groupValues?.get(1) ?: continue
            val hostUrl = HikariNet.base64Decode(durl)
                ?.let { String(it, Charsets.UTF_8) }
                ?.takeIf { it.startsWith("http") }
                ?: continue
            hostUrls.add(hostUrl)
        }

        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        for (hostUrl in hostUrls) {
            if (out.size >= 4) break
            val hits = try {
                HikariNet.resolveWithWebView(hostUrl, streamCapture, timeoutMs = 45_000)
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

    private suspend fun ajaxSearch(query: String, sort: Int, page: Int): List<HikariMedia> {
        val body = "search=${URLEncoder.encode(query, "UTF-8")}&sort=$sort&page=$page"
        val raw = HikariNet.postString(
            "$BASE/ajax_search.php",
            body,
            pageHeaders,
            "application/x-www-form-urlencoded; charset=utf-8",
        ) ?: return emptyList()
        return parseCards(raw)
    }

    /** Parses `div.video` cards (shared by ajax_search results and trending). */
    private fun parseCards(html: String): List<HikariMedia> {
        val out = LinkedHashMap<String, HikariMedia>()
        for (chunk in html.split(Regex("""<div class="video">""")).drop(1)) {
            val m = Regex("""href="/pornvideo/([^/"]+)/([A-Za-z0-9+/=]+)"""")
                .find(chunk) ?: continue
            val id = "$BASE/pornvideo/${m.groupValues[1]}/${m.groupValues[2]}/"
            val title = Regex("""<div class="video-content">\s*<h1[^>]*>([\s\S]*?)</h1>""")
                .find(chunk)?.groupValues?.get(1)
                ?.let { unescape(stripTags(it)) }
                ?.takeIf { it.isNotBlank() }
                ?: m.groupValues[1].replace('-', ' ')
            val img = Regex("""class="video-image primary b-lazy" data-src="([^"]+)"""")
                .find(chunk)?.groupValues?.get(1)
            out[id] = HikariMedia(
                id = id,
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
            if (htmlCache.size > 80) htmlCache.clear()
            htmlCache[url] = now to html
        }
        return html
    }

    private fun metaProperty(html: String, property: String): String? =
        Regex("""<meta\s+property="$property"\s+content="([^"]*)"""").find(html)?.groupValues?.get(1)

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
