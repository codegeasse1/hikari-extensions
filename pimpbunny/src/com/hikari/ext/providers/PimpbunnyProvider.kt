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
 * PimpBunny (pimpbunny.com) — leaked/OnlyFans-style video site with
 * server-rendered pages (the JS "age gate" is only a client overlay; the
 * content is in the raw HTML).
 *
 *  - Catalog pages: `/videos/?sort_by=post_date|video_viewed|rating`,
 *    paginated as `/videos/<page>/?sort_by=...`.
 *  - Search: `/search/<query>/` for page 1; later pages via
 *    `?mode=async&function=get_block&block_id=...&from_videos=<page>...`
 *    (the site's own AJAX pagination).
 *  - Cards are `div.ui-card-video` blocks: link + `data-original` thumbnail;
 *    the title is either a `.ui-card-title` div or the img `alt` attribute
 *    (AJAX-fragments omit the div).
 *  - The video page embeds direct signed MP4 URLs
 *    (`/get_file/.../<id>_<res>p.mp4/?v-acctoken=<token>`) for every quality
 *    in a JS config — no WebView needed, they stream directly.
 */
class PimpbunnyProvider : HikariProvider {

    override val id = "pimpbunny"
    override val name = "PimpBunny"
    override val mainUrl = "https://pimpbunny.com"
    override val description = "PimpBunny — latest/popular/top-rated catalogs and search, with direct multi-quality MP4 playback."
    override val version = 1
    override val tvTypes: Set<HikariMediaType> = setOf(HikariMediaType.MOVIE)

    companion object {
        private const val BASE = "https://pimpbunny.com"
        private const val CACHE_TTL_MS = 600_000L

        private val htmlCache = HashMap<String, Pair<Long, String>>()
        private val cacheMutex = Mutex()

        private val pageHeaders = mapOf(
            "User-Agent" to HikariNet.browserHeaders["User-Agent"].orEmpty(),
            "Accept" to "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language" to "en-US,en;q=0.9",
        )

        private val searchBlockParams = "mode=async&function=get_block&block_id=list_videos_videos_list_search_result&from_videos=%d&ipp=30&page_type=&items_per_page=30&videos_per_page=30"
    }

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("latest", "Latest Videos", HikariMediaType.MOVIE),
        HikariCatalog("popular", "Most Viewed", HikariMediaType.MOVIE),
        HikariCatalog("rating", "Top Rated", HikariMediaType.MOVIE),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val sort = when (catalog.id) {
            "latest" -> "post_date"
            "popular" -> "video_viewed"
            "rating" -> "rating"
            else -> return emptyList()
        }
        val url = if (page <= 1) "$BASE/videos/?sort_by=$sort" else "$BASE/videos/$page/?sort_by=$sort"
        return parseCards(getCached(url) ?: return emptyList())
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        val encoded = URLEncoder.encode(q, "UTF-8").replace("+", "%20")
        val url = if (page <= 1) {
            "$BASE/search/$encoded/"
        } else {
            "$BASE/search/$encoded/?${searchBlockParams.format(page)}"
        }
        return parseCards(getCached(url) ?: return emptyList())
    }

    // ------------------------------------------------------------------
    //  Meta + streams
    // ------------------------------------------------------------------

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val pageUrl = media.id.takeIf { it.startsWith("http") } ?: return media
        val html = getCached(pageUrl) ?: return media
        val title = Regex("""video_title:\s*'([^']+)'""").find(html)?.groupValues?.get(1)
            ?.let { unescape(it) }
            ?: Regex("""<title>([\s\S]*?)</title>""").find(html)?.groupValues?.get(1)
                ?.let { unescape(it).replace(Regex("""\s*\|\s*PimpBunny.*$"""), "").trim() }
                ?.takeIf { it.isNotBlank() }
            ?: media.title
        val poster = metaProperty(html, "og:image")?.takeIf { it.startsWith("http") } ?: media.posterUrl
        val overview = Regex("""name="description" content="([^"]*)"""").find(html)?.groupValues?.get(1)
            ?.let { unescape(it) }
            ?.takeIf { it.isNotBlank() }
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

        // All signed quality variants live in the page config:
        //   https://pimpbunny.com/get_file/<hash>/<dir>/<id>/<id>[_NNNp].mp4/?v-acctoken=<token>
        val urls = LinkedHashMap<String, String>()
        Regex("""https://pimpbunny\.com/get_file/[^\s"'\\,]+""").findAll(html).forEach {
            val u = it.value
            if (u.contains("v-acctoken=")) urls[u] = u
        }
        if (urls.isEmpty()) return emptyList()

        val sorted = urls.keys.sortedWith(compareByDescending { resolutionOf(it) })
        return sorted.map { url ->
            val res = resolutionOf(url)
            HikariStream(
                name = if (res > 0) "${res}p" else "Default",
                url = url,
                headers = emptyMap(),
                isM3u8 = false,
            )
        }
    }

    private fun resolutionOf(url: String): Int =
        Regex("""_(\d+)p\.mp4""").find(url)?.groupValues?.get(1)?.toIntOrNull() ?: 0

    // ------------------------------------------------------------------
    //  Parsers + helpers
    // ------------------------------------------------------------------

    /** Parses `ui-card-video` blocks (works on full pages and AJAX fragments). */
    private fun parseCards(html: String): List<HikariMedia> {
        val out = LinkedHashMap<String, HikariMedia>()
        for (chunk in html.split(Regex("""ui-card-video__""")).drop(1)) {
            val href = Regex("""href="(https://pimpbunny\.com/videos/[^"]+)/"""").find(chunk)?.groupValues?.get(1) ?: continue
            val title = Regex("""ui-card-title__[^"]*"[^>]*>([^<]+)""").find(chunk)?.groupValues?.get(1)
                ?.let { unescape(it).trim() }
                ?.takeIf { it.isNotBlank() }
                ?: Regex("""alt="([^"]+)"""") .find(chunk)?.groupValues?.get(1)?.let { unescape(it).trim() }
                ?: href.substringAfterLast('/').replace('-', ' ')
            val img = Regex("""data-original="([^"]+)"""").find(chunk)?.groupValues?.get(1)
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
            if (htmlCache.size > 80) htmlCache.clear()
            htmlCache[url] = now to html
        }
        return html
    }

    private fun metaProperty(html: String, property: String): String? =
        Regex("""<meta\s+property="$property"\s+content="([^"]*)"""").find(html)?.groupValues?.get(1)

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
