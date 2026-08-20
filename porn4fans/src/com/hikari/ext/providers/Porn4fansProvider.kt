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
 * Porn4Fans (de.porn4fans.com) — leaked/OnlyFans adult videos.
 *
 *  - Listings (home, `/onlyfans-videos/`, categories `/categories/<slug>/`,
 *    models, search `/search/<q>/`) are grids of `<div class="item">` cards
 *    carrying the video URL, title and poster.
 *  - Each video page embeds the player config as an inline `var tXXXX = { … }`
 *    object. The relevant keys are `video_url` (480p) and `video_alt_url`
 *    (720p) — signed `…/get_file/…/<id>_<res>.mp4/?v-acctoken=…` MP4 URLs.
 *    1080p (`video_alt_url2`) redirects to a login wall, so it is skipped.
 *
 *  The signed tokens are minted server-side for the page request, so we always
 *  fetch the video page fresh and reuse the exact URLs the site's own player
 *  uses (same browser headers + Referer). As a safety net the watch page is
 *  also run through the WebView capture helper, which handles any cookie/
 *  captcha-gated cases natively.
 */
class Porn4fansProvider : HikariProvider {

    override val id = "porn4fans"
    override val name = "Porn4Fans"
    override val mainUrl = "https://de.porn4fans.com"
    override val description = "Leaked/OF adult videos from de.porn4fans.com — latest, popular, categories and search, with signed MP4 playback."
    override val version = 1
    override val tvTypes: Set<HikariMediaType> = setOf(HikariMediaType.MOVIE)

    companion object {
        private const val BASE = "https://de.porn4fans.com"
        private const val CACHE_TTL_MS = 600_000L

        private val htmlCache = HashMap<String, Pair<Long, String>>()
        private val cacheMutex = Mutex()

        private val pageHeaders = mapOf(
            "User-Agent" to HikariNet.browserHeaders["User-Agent"].orEmpty(),
            "Accept" to "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language" to "de-DE,de;q=0.9,en;q=0.8",
        )

        private val streamCapture = Regex("""https?://[^"'\s]+?\.(?:m3u8|mp4)(?:[?#][^"'\s]*)?""")

        // Popular category slugs (from the site's /categories/ index).
        private val categoryCatalogs = listOf(
            "anal" to "Anal",
            "milf" to "Milf",
            "ebony" to "Ebony",
            "bubble-butt" to "Bubble Butt",
            "lingerie" to "Lingerie",
            "squirt" to "Squirt",
            "pov" to "POV",
            "threesome" to "Threesome",
            "blowjob" to "Blowjob",
            "creampie" to "Creampie",
        )
    }

    override fun catalogs(): List<HikariCatalog> = buildList {
        add(HikariCatalog("latest", "Latest Videos", HikariMediaType.MOVIE))
        add(HikariCatalog("viewed", "Most Viewed", HikariMediaType.MOVIE))
        add(HikariCatalog("rating", "Top Rated", HikariMediaType.MOVIE))
        for ((slug, label) in categoryCatalogs) {
            add(HikariCatalog("cat-$slug", label, HikariMediaType.MOVIE))
        }
    }

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val base = when (catalog.id) {
            "latest" -> "$BASE/onlyfans-videos/"
            "viewed" -> "$BASE/onlyfans-videos/?p=video_viewed"
            "rating" -> "$BASE/onlyfans-videos/?p=rating"
            else -> {
                val slug = catalog.id.removePrefix("cat-")
                if (slug == catalog.id) return emptyList()
                "$BASE/categories/$slug/"
            }
        }
        val url = when {
            page <= 1 -> base
            base.contains("?") -> "$base&page=$page"
            else -> base.trimEnd('/') + "/$page/"
        }
        return parseCards(getCached(url) ?: return emptyList())
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        val encoded = URLEncoder.encode(q, "UTF-8").replace("+", "%20")
        val url = if (page <= 1) "$BASE/search/$encoded/" else "$BASE/search/$encoded/$page/"
        return parseCards(getCached(url) ?: return emptyList())
    }

    // ------------------------------------------------------------------
    //  Meta + streams
    // ------------------------------------------------------------------

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val pageUrl = media.id.takeIf { it.startsWith("http") } ?: return media
        val html = getCached(pageUrl) ?: return media
        val title = metaProperty(html, "og:title")?.let { unescape(it) }
            ?.takeIf { it.isNotBlank() } ?: media.title
        val poster = metaProperty(html, "og:image")?.takeIf { it.startsWith("http") } ?: media.posterUrl
        val overview = metaProperty(html, "og:description")?.let { unescape(it) }
            ?.takeIf { it.isNotBlank() && it != title }
        return media.copy(
            title = title,
            posterUrl = poster,
            overview = overview ?: media.overview,
            backdropUrl = poster,
        )
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val pageUrl = media.id.takeIf { it.startsWith("http") } ?: return emptyList()
        val html = getCached(pageUrl) ?: return emptyList()
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()

        // Signed MP4s straight from the page's own player config.
        for ((label, u) in extractConfigStreams(html)) {
            if (u.isBlank() || !seen.add(u)) continue
            out.add(
                HikariStream(
                    name = label,
                    url = u,
                    headers = mapOf(
                        "User-Agent" to HikariNet.browserHeaders["User-Agent"].orEmpty(),
                        "Referer" to pageUrl,
                        "Origin" to BASE,
                    ),
                    isM3u8 = u.contains(".m3u8", ignoreCase = true),
                )
            )
        }

        // WebView fallback — captures whatever request the site's own player
        // makes (handles cookies / JS-gated cases).
        if (out.isEmpty()) {
            val hits = try {
                HikariNet.resolveWithWebView(pageUrl, streamCapture, timeoutMs = 45_000)
            } catch (t: Throwable) {
                emptyList()
            }
            for (h in hits) {
                if (h.url.isBlank() || !seen.add(h.url)) continue
                out.add(
                    HikariStream(
                        name = "Server ${out.size + 1}",
                        url = h.url,
                        headers = h.headers,
                        isM3u8 = h.url.contains(".m3u8", ignoreCase = true),
                    )
                )
            }
        }
        return out
    }

    /** Pulls the site player's own `video_url`/`video_alt_url` signed MP4 URLs. */
    private fun extractConfigStreams(html: String): List<Pair<String, String>> {
        val texts = HashMap<String, String>()
        for (m in Regex("""([a-zA-Z0-9_]*video[a-zA-Z0-9_]*_text)\s*:\s*'([^']*)'""").findAll(html)) {
            texts[m.groupValues[1]] = m.groupValues[2]
        }
        val found = LinkedHashMap<String, String>()
        for (m in Regex("""([a-zA-Z0-9_]*video[a-zA-Z0-9_]*_url[a-zA-Z0-9_]*)\s*:\s*'([^']*)'""").findAll(html)) {
            val key = m.groupValues[1]
            var url = m.groupValues[2].trim()
            if (!url.startsWith("http")) continue
            if (url.contains("login-required") || !url.contains("/get_file/")) continue
            // video_alt_url2_text labels video_alt_url2, etc.
            val label = texts[key + "_text"]?.takeIf { it.isNotBlank() }
                ?: when {
                    key == "video_url" -> "480p"
                    key == "video_alt_url" -> "720p"
                    else -> "Server ${found.size + 1}"
                }
            found.putIfAbsent(url, label)
        }
        return found.map { it.value to it.key }
    }

    // ------------------------------------------------------------------
    //  Parsers + helpers
    // ------------------------------------------------------------------

    /** Parses the site's `<div class="item">` video grid cards. */
    private fun parseCards(html: String): List<HikariMedia> {
        val out = LinkedHashMap<String, HikariMedia>()
        for (chunk in html.split(Regex("""<div\s+class="item""")).drop(1)) {
            val m = Regex("""href="(https://de\.porn4fans\.com/video/(\d+)/[^"]*)"""").find(chunk)
                ?: continue
            val href = m.groupValues[1]
            val id = m.groupValues[2]
            if (id.isBlank()) continue
            val title = unescape(videoCardTitle(chunk))
            if (title.isBlank()) continue
            val poster = Regex("""(?:data-poster|poster)="([^"]+)"""").find(chunk)?.groupValues?.get(1)
            out[id] = HikariMedia(
                id = href,
                title = title,
                type = HikariMediaType.MOVIE,
                posterUrl = poster?.takeIf { it.startsWith("http") },
            )
        }
        return out.values.toList()
    }

    private fun videoCardTitle(chunk: String): String {
        // Primary: the `a.video-text` title link used on most grids.
        val text = Regex("""<a[^>]*class="[^"]*video-text[^"]*"[^>]*>([\s\S]*?)</a>""")
            .find(chunk)?.groupValues?.get(1)
            ?.replace(Regex("""<[^>]+>"""), "")
            ?.trim()
        if (!text.isNullOrBlank()) return text
        // Fallback: the `title="…"` attribute on the img-wrap link itself.
        val linkTitle = Regex("""(?:class="img-wrap video"[^>]*title="([^"]+)"|title="([^"]+)"[^>]*class="img-wrap video")""")
            .find(chunk)?.let { it.groupValues[1].ifBlank { it.groupValues[2] } }
        if (!linkTitle.isNullOrBlank()) return linkTitle
        // Last resort: any plausible title attribute that isn't a fav/rating label.
        for (m in Regex("""title="([^"]+)"""").findAll(chunk)) {
            val t = m.groupValues[1].trim()
            if (t.isBlank() || t.startsWith("Zu Favoriten") || t.startsWith("Später ansehen") || t.startsWith("Like")) continue
            return t
        }
        return ""
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
