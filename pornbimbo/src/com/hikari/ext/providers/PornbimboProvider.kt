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
 * PornBimbo (pornbimbo.com) — kinky/femdom/CEI/JOI tube, KVS CMS.
 *
 *  - Listing pages (`/latest-updates/`, `/most-popular/`, `/categories/<slug>/`,
 *    `/search/<query>/`) are plain HTML grids of `class="item"` cards whose
 *    links are `/video/<id>/<slug>` and posters live under
 *    `…/videos_screenshots/<prefix>/<id>/<size>/1.jpg` (sizes like 180x135;
 *    swapping in 640x480 gives the big poster),
 *  - a video page embeds a `flashvars` object with signed direct MP4 URLs:
 *    `video_url` (the 720p copy) and `event_reporting2` (the base copy), both
 *    served by `/get_file/<hash>/…/71990_720p.mp4/?v-acctoken=…` — the token
 *    is time- and IP-bound but self-contained, so the parsed URL plays as-is
 *    from the same device that fetched the page,
 *  - `/get_file` is hotlink-gated: KVS wants the site as Referer OR the
 *    `279e7`/`279e7b` cookies the page's inline JS sets (the `noref` anti-
 *    hotlink allowance). The stream carries both, so direct playback works.
 */
class PornbimboProvider : HikariProvider {

    override val id = "pornbimbo"
    override val name = "PornBimbo"
    override val mainUrl = "https://pornbimbo.com"
    override val description = "Kinky fetish tube — JOI/CEI, femdom, mommy taboo, sissy and more. Direct MP4 streams."
    override val tvTypes = setOf(HikariMediaType.MOVIE)
    override val version = 1

    companion object {
        private const val BASE = "https://pornbimbo.com"
        private const val CACHE_TTL_MS = 600_000L
        private val htmlCache = HashMap<String, Pair<Long, String>>()
        private val cacheMutex = Mutex()
    }

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("latest", "Latest Updates", HikariMediaType.MOVIE),
        HikariCatalog("popular", "Most Popular", HikariMediaType.MOVIE),
        HikariCatalog("blowjob", "Blowjob", HikariMediaType.MOVIE),
        HikariCatalog("solo", "Solo", HikariMediaType.MOVIE),
        HikariCatalog("sissy-porn", "Sissy Porn", HikariMediaType.MOVIE),
        HikariCatalog("cuckold", "Cuckold", HikariMediaType.MOVIE),
        HikariCatalog("deepthroat", "Deepthroat", HikariMediaType.MOVIE),
        HikariCatalog("lesbians", "Lesbians", HikariMediaType.MOVIE),
        HikariCatalog("shemales-transsexuals", "Shemales", HikariMediaType.MOVIE),
        HikariCatalog("bimbo-girls", "Bimbo Girls", HikariMediaType.MOVIE),
        HikariCatalog("webcams", "Webcams", HikariMediaType.MOVIE),
        HikariCatalog("orgasm-control", "Orgasm Control", HikariMediaType.MOVIE),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val path = when (catalog.id) {
            "latest" -> "/latest-updates"
            "popular" -> "/most-popular"
            else -> "/categories/${catalog.id}"
        }
        val url = if (page <= 1) "$BASE$path/" else "$BASE$path/$page/"
        return parseCards(getCached(url) ?: return emptyList())
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        val encoded = URLEncoder.encode(q, "UTF-8").replace("+", "%20")
        // The site's own form submits /search/?q=… (KVS canonicalizes to a
        // /search/<slug>/ path, but the ?q= form handles spaces/re-encodes
        // cleanly and the app only ever requests search page 1 anyway).
        return parseCards(getCached("$BASE/search/?q=$encoded") ?: return emptyList())
    }

    // ------------------------------------------------------------------
    //  Meta + streams (from the video page)
    // ------------------------------------------------------------------

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val page = getCached(videoUrl(media.id)) ?: return media
        val fv = flashvarsBlock(page)
        val title = fv?.let { flashvarString(it, "video_title") }
            ?.takeIf { it.isNotBlank() }
            ?: media.title
        val genres = buildList {
            fv?.let { flashvarString(it, "video_categories") }
                ?.split(",")
                ?.map { it.trim() }
                ?.filter { it.isNotBlank() }?.let { addAll(it) }
            fv?.let { flashvarString(it, "video_tags") }
                ?.split(",")
                ?.map { it.trim() }
                ?.filter { it.isNotBlank() }?.let { addAll(it) }
        }.distinct()
        val overview = metaProperty(page, "og:description")?.let { unescapeEntities(it) }
            ?.takeIf { it.isNotBlank() && !it.contains("KVS") }
        val poster = metaProperty(page, "og:image")?.takeIf { it.startsWith("http") }
            ?: media.posterUrl
        return media.copy(
            title = title,
            posterUrl = poster,
            overview = overview ?: media.overview,
            genres = genres.ifEmpty { media.genres },
        )
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val id = media.id.trim()
        if (!id.all { it.isDigit() }) return emptyList()
        val page = getCached(videoUrl(id)) ?: return emptyList()
        val fv = flashvarsBlock(page) ?: return emptyList()
        val headers = streamHeaders(page)
        val out = ArrayList<HikariStream>()
        flashvarString(fv, "video_url")
            ?.takeIf { it.startsWith("http") && it.contains("/get_file/") }
            ?.let { url ->
                out.add(
                    HikariStream(
                        name = "720p",
                        url = url,
                        headers = headers,
                        isM3u8 = url.contains(".m3u8", ignoreCase = true),
                    )
                )
            }
        flashvarString(fv, "event_reporting2")
            ?.takeIf { it.startsWith("http") && it.contains("/get_file/") }
            ?.let { url ->
                out.add(
                    HikariStream(
                        name = if (out.isEmpty()) "Video" else "SD",
                        url = url,
                        headers = headers,
                        isM3u8 = url.contains(".m3u8", ignoreCase = true),
                    )
                )
            }
        return out
    }

    // ------------------------------------------------------------------
    //  HTML helpers
    // ------------------------------------------------------------------

    private fun videoUrl(id: String): String = "$BASE/video/$id/"

    /** Fetches an HTML page once per CACHE_TTL_MS. */
    private suspend fun getCached(url: String): String? {
        val now = System.currentTimeMillis()
        cacheMutex.withLock {
            htmlCache[url]?.let { (t, html) ->
                if (now - t < CACHE_TTL_MS) return html
            }
        }
        val html = HikariNet.getString(url) ?: return null
        cacheMutex.withLock {
            if (htmlCache.size > 60) htmlCache.clear()
            htmlCache[url] = now to html
        }
        return html
    }

    /** Extracts the video-card grid from any KVS listing page. */
    private fun parseCards(html: String): List<HikariMedia> {
        val out = LinkedHashMap<String, HikariMedia>()
        // Cards: <a href="https://pornbimbo.com/video/<id>/<slug>" title="…">
        // … <img … data-original="…/180x135/1.jpg" …> … <strong class="title">…</strong>
        val cardRe = Regex(
            """<a href="(?:https://pornbimbo\.com)?/video/(\d+)/[^"]*"[^>]*title="([^"]*)"([\s\S]*?)</a>"""
        )
        val posterRe = Regex("""data-original="([^"]+)""")
        val strongRe = Regex("""<strong class="title">([\s\S]*?)</strong>""")
        for (card in cardRe.findAll(html)) {
            val id = card.groupValues[1]
            val block = card.value
            var title = unescapeEntities(card.groupValues[2]).trim()
            if (title.isBlank()) {
                title = strongRe.find(block)?.groupValues?.get(1)
                    ?.let { unescapeEntities(it).trim() } ?: ""
            }
            if (title.isBlank()) continue
            val poster = posterRe.find(block)?.groupValues?.get(1)
                ?.replace("180x135", "640x480")
                ?.takeIf { it.startsWith("http") }
            out[id] = HikariMedia(
                id = id,
                title = title,
                type = HikariMediaType.MOVIE,
                posterUrl = poster,
            )
        }
        return out.values.toList()
    }

    /** The `var flashvars = { … };` block of a video page. */
    private fun flashvarsBlock(html: String): String? {
        val start = html.indexOf("var flashvars = {")
        if (start < 0) return null
        val end = html.indexOf("};", start)
        if (end < 0) return null
        return html.substring(start, end + 2)
    }

    /** Reads a quoted string value out of the flashvars block. */
    private fun flashvarString(flashvars: String, key: String): String? =
        Regex("""\b$key:\s*'([^']*)'""").find(flashvars)?.groupValues?.get(1)

    /**
     * Headers the direct /get_file MP4 needs: a site Referer (KVS hotlink
     * gate) plus the noreferrer-allowance cookies the page's inline JS sets
     * (`279e7` is a static base64 blob; `279e7b` is a fresh unix timestamp).
     */
    private fun streamHeaders(page: String): Map<String, String> {
        val headers = mutableMapOf("Referer" to "$BASE/")
        Regex("""document\.cookie='279e7=([^;]+)""").find(page)?.let { m ->
            val now = System.currentTimeMillis() / 1000
            headers["Cookie"] = "279e7=${m.groupValues[1]}; 279e7b=$now"
        }
        return headers
    }

    private fun metaProperty(html: String, prop: String): String? =
        Regex("""<meta\s+property="$prop"\s+content="([^"]*)""")
            .find(html)?.groupValues?.get(1)

    private fun unescapeEntities(s: String): String = s
        .replace("&quot;", "\"")
        .replace("&#039;", "'")
        .replace("&#39;", "'")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&nbsp;", " ")
        .replace(Regex("""&#(\d+);""")) { m ->
            m.groupValues[1].toIntOrNull()?.let { runCatching { it.toChar().toString() }.getOrNull() } ?: m.value
        }
        .replace(Regex("""\s+"""), " ")
        .trim()
}
