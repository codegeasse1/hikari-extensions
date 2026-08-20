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
 * JavOpen (javopen.co) — JAV streaming site running the "videotube" WP theme.
 *
 *  - Home and category/search pages are grids of `div.item` cards linking to
 *    `/video/<slug>/`.
 *  - The video page hides its default player iframe base64-encoded in
 *    `#raw-payload` (a turbovidhls.com embed whose HLS manifest is in the
 *    embed page's static HTML), and lists alternate servers as
 *    `switchEmbed(url, 'embedN')` buttons (upload18.com "Server VIP", whose
 *    PLAYER_CONFIG JSON also carries the m3u8 inline).
 *  - So everything is scrapable with plain HTTP — no WebView needed — with a
 *    generic WebView capture as a last resort for unknown embed hosts.
 */
class JavOpenProvider : HikariProvider {

    override val id = "javopen"
    override val name = "JavOpen"
    override val mainUrl = "https://javopen.co"
    override val description = "JAV from javopen.co — newest uploads, censored/uncensored categories and search, with direct HLS from the site's turbovidhls/upload18 players."
    override val version = 1
    override val tvTypes: Set<HikariMediaType> = setOf(HikariMediaType.MOVIE)

    companion object {
        private const val BASE = "https://javopen.co"
        private const val CACHE_TTL_MS = 600_000L

        private val htmlCache = HashMap<String, Pair<Long, String>>()
        private val cacheMutex = Mutex()

        private val categories = listOf(
            "jav-censored" to "Censored",
            "jav-uncensored" to "Uncensored",
            "hot-jav-movies" to "Hot JAV Movies",
            "jav-chinese-subtitles" to "Chinese Subtitles",
            "jav-reupload" to "Reupload",
            "mgstage" to "MGStage",
            "amatuer-porn" to "Amateur",
            "asia-uncensored-hd" to "Asia Uncensored HD",
            "usa-euro" to "USA & Euro",
        )

        private val pageHeaders = mapOf(
            "User-Agent" to HikariNet.browserHeaders["User-Agent"].orEmpty(),
            "Accept" to "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language" to "en-US,en;q=0.9",
        )

        private val streamCapture = Regex("""https?://[^"'\s]+?\.(?:m3u8|mp4)(?:[?#][^"'\s]*)?""")
    }

    // ------------------------------------------------------------------
    //  Catalogs
    // ------------------------------------------------------------------

    override fun catalogs(): List<HikariCatalog> = buildList {
        add(HikariCatalog("latest", "Latest Videos", HikariMediaType.MOVIE))
        for ((slug, label) in categories) {
            add(HikariCatalog("cat-$slug", label, HikariMediaType.MOVIE, rawType = slug))
        }
    }

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> = when {
        catalog.id == "latest" -> if (page > 1) emptyList() else parseCards(getCached("$BASE/") ?: return emptyList())
        catalog.id.startsWith("cat-") -> {
            val slug = catalog.rawType
            val url = if (page <= 1) "$BASE/$slug/" else "$BASE/$slug/page/$page/"
            parseCards(getCached(url) ?: return emptyList())
        }
        else -> emptyList()
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
            ?.replace(Regex("""\s*-\s*Jav Online.*$"""), "")
            ?.replace(Regex("""\s*-\s*JAV.*$"""), "")
            ?.takeIf { it.isNotBlank() }
        val year = Regex("""itemprop="datePublished"[^>]*content="(\d{4})""").find(html)?.groupValues?.get(1)
            ?.toIntOrNull()
        return media.copy(
            title = title,
            posterUrl = poster,
            type = HikariMediaType.MOVIE,
            overview = overview ?: media.overview,
            year = year ?: media.year,
            backdropUrl = poster,
        )
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val pageUrl = media.id.takeIf { it.startsWith("http") } ?: return emptyList()
        val html = getCached(pageUrl) ?: return emptyList()

        val embeds = LinkedHashSet<String>()
        // Default player iframe, base64 in #raw-payload.
        Regex("""<textarea id="raw-payload"[^>]*>([\s\S]*?)</textarea>""").find(html)?.groupValues?.get(1)?.let { b64 ->
            val decoded = HikariNet.base64Decode(b64.trim())?.toString(Charsets.UTF_8)
            Regex("""src="([^"]+)""").find(decoded.orEmpty())?.groupValues?.get(1)?.let { embeds.add(it) }
        }
        // Alternate servers.
        Regex("""switchEmbed\('([^']+)'""").findAll(html).forEach { m -> embeds.add(m.groupValues[1]) }

        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        for (embed in embeds) {
            if (out.size >= 4) break
            val streams = resolveEmbed(embed)
            for (s in streams) {
                if (!seen.add(s.url)) continue
                out.add(s)
            }
        }
        return out
    }

    private suspend fun resolveEmbed(embed: String): List<HikariStream> {
        return when {
            embed.contains("turbovidhls", ignoreCase = true) ||
                embed.contains("turboviplay", ignoreCase = true) -> {
                val page = HikariNet.getStringSmart(embed, pageHeaders) ?: return emptyList()
                val m3u8 = Regex("""data-hash="(https://[^"]+\.m3u8)""").find(page)?.groupValues?.get(1)
                    ?: Regex("""urlPlay = '([^']+)'""").find(page)?.groupValues?.get(1)
                    ?: return emptyList()
                listOf(
                    HikariStream(
                        name = "Turboviplay",
                        url = m3u8,
                        headers = mapOf(
                            "User-Agent" to HikariNet.browserHeaders["User-Agent"].orEmpty(),
                            "Referer" to "https://turbovidhls.com/",
                        ),
                        isM3u8 = m3u8.contains(".m3u8", ignoreCase = true),
                    )
                )
            }
            embed.contains("upload18", ignoreCase = true) -> {
                val page = HikariNet.getStringSmart(embed, pageHeaders) ?: return emptyList()
                val m3u8 = Regex(""""m3u8":"(https?:\\/\\/[^"]+)"""").find(page)?.groupValues?.get(1)
                    ?.replace("\\/", "/")
                    ?.replace("\\u0026", "&")
                    ?.takeIf { it.startsWith("http") }
                    ?: return emptyList()
                listOf(
                    HikariStream(
                        name = "Upload18",
                        url = m3u8,
                        headers = mapOf(
                            "User-Agent" to HikariNet.browserHeaders["User-Agent"].orEmpty(),
                            "Referer" to "https://upload18.com/",
                        ),
                        isM3u8 = m3u8.contains(".m3u8", ignoreCase = true),
                    )
                )
            }
            else -> try {
                HikariNet.resolveWithWebView(embed, streamCapture, timeoutMs = 30_000)
                    .map { h ->
                        HikariStream(
                            name = "Stream",
                            url = h.url,
                            headers = h.headers,
                            isM3u8 = h.url.contains(".m3u8", ignoreCase = true),
                        )
                    }
            } catch (t: Throwable) {
                emptyList()
            }
        }
    }

    // ------------------------------------------------------------------
    //  Parsers + helpers
    // ------------------------------------------------------------------

    /** Parses `div.item` cards (home, categories, search). */
    private fun parseCards(html: String): List<HikariMedia> {
        val out = LinkedHashMap<String, HikariMedia>()
        for (chunk in html.split(Regex("""item responsive-height post""")).drop(1)) {
            val m = Regex("""<a title="([^"]+)" href="(https://javopen\.co/video/[^"]+)"""")
                .find(chunk) ?: continue
            val title = unescape(m.groupValues[1])
            val href = m.groupValues[2]
            val img = Regex("""<img[^>]*src="([^"]+)"[^>]*class="img-responsive wp-post-image""")
                .find(chunk)?.groupValues?.get(1)
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
