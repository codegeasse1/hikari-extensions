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
 * Shared base for the three KVS/"Trex" tube sites (WhoresHub, PornTrex, WowXxx).
 *
 *  - Listing pages (`/latest-updates/`, `/most-popular/`, `/top-rated/`,
 *    `/categories/<slug>/`, `/search/?q=<query>`) are plain HTML grids of
 *    cards, one `<a href="…/<id>/<slug>/">` poster link + its `<img data-src>`.
 *  - An embed page (`/embed/<id>`) carries a `flashvars` object with a direct
 *    MP4 URL in `video_url` (plus the site's canonical preview image).
 *
 * All three sites serve the same KVS HTML, differing only in hostname, URL
 * shapes and thumbnail CDN — the differences are the abstract members here.
 */
abstract class KvsTubeProvider : HikariProvider {

    abstract val base: String
    /** path segment between the host and the id/slug on listing pages ("videos" or "video"). */
    abstract val videoPath: String
    /** optional (slug → pretty name) rows for the catalog grid. */
    abstract val categoryRows: List<Pair<String, String>>
    /** extra headers to attach to the parsed stream URL (CDN Referer gates etc.). */
    abstract val streamHeaders: Map<String, String>
    /** extracts the numeric video id from a poster URL. */
    abstract val idFromPosterRe: Regex

    override val tvTypes: Set<HikariMediaType> = setOf(HikariMediaType.MOVIE)

    companion object {
        private const val CACHE_TTL_MS = 600_000L
        private val htmlCache = HashMap<String, Pair<Long, String>>()
        private val cacheMutex = Mutex()
    }

    override fun catalogs(): List<HikariCatalog> = buildList {
        add(HikariCatalog("latest", "Latest Updates", HikariMediaType.MOVIE))
        add(HikariCatalog("popular", "Most Popular", HikariMediaType.MOVIE))
        add(HikariCatalog("top-rated", "Top Rated", HikariMediaType.MOVIE))
        for ((slug, name) in categoryRows) {
            add(HikariCatalog("cat-$slug", name, HikariMediaType.MOVIE))
        }
    }

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val path = when {
            catalog.id == "latest" -> "/latest-updates"
            catalog.id == "popular" -> "/most-popular"
            catalog.id == "top-rated" -> "/top-rated"
            catalog.id.startsWith("cat-") -> "/categories/${catalog.id.removePrefix("cat-")}"
            else -> return emptyList()
        }
        val url = if (page <= 1) "$base$path/" else "$base$path/$page/"
        return parseCards(getCached(url) ?: return emptyList())
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        // The sites' forms submit /search/?q=… (KVS canonicalizes internally);
        // this form handles spaces/re-encoding cleanly.
        val encoded = URLEncoder.encode(q, "UTF-8").replace("+", "%20")
        return parseCards(getCached("$base/search/?q=$encoded") ?: return emptyList())
    }

    // ------------------------------------------------------------------
    //  Meta + streams (from the embed page)
    // ------------------------------------------------------------------

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val page = getCached("$base/embed/${media.id}") ?: return media
        val fv = flashvarsBlock(page) ?: return media
        val title = flashvarString(fv, "video_title")
            ?.takeIf { it.isNotBlank() }
            ?: media.title
        val genres = buildList {
            flashvarString(fv, "video_categories")
                ?.split(",")
                ?.map { it.trim() }
                ?.filter { it.isNotBlank() }?.let { addAll(it) }
            flashvarString(fv, "video_tags")
                ?.split(",")
                ?.map { it.trim() }
                ?.filter { it.isNotBlank() }?.let { addAll(it) }
        }.distinct()
        val poster = flashvarString(fv, "preview_url")
            ?.takeIf { it.isNotBlank() }
            ?.let { absolute(it) }
            ?: media.posterUrl
        return media.copy(
            title = title,
            posterUrl = poster,
            genres = genres.ifEmpty { media.genres },
        )
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val id = media.id.trim()
        if (!id.all { it.isDigit() }) return emptyList()
        val page = getCached("$base/embed/$id") ?: return emptyList()
        val fv = flashvarsBlock(page) ?: return emptyList()
        val url = flashvarString(fv, "video_url")
            ?.takeIf { it.startsWith("http") && it.contains("/get_file/") } ?: return emptyList()
        val label = flashvarString(fv, "video_url_text")
            ?.takeIf { it.isNotBlank() }
            ?: "Video"
        return listOf(
            HikariStream(
                name = label,
                url = url,
                headers = streamHeaders,
                isM3u8 = url.contains(".m3u8", ignoreCase = true),
            )
        )
    }

    // ------------------------------------------------------------------
    //  HTML helpers
    // ------------------------------------------------------------------

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

    /**
     * Parses a KVS listing grid. Splits the page at each video anchor so the
     * poster + title + id pairing stays inside one card (a single lazy regex
     * across the whole page can pair an anchor with the NEXT card's poster).
     */
    protected fun parseCards(html: String): List<HikariMedia> {
        val splitRe = Regex("(?=<a [^>]*href=\"${Regex.escape(base)}/$videoPath/)")
        val anchorRe = Regex("""<a [^>]*href="$base/$videoPath/([^"]+?)/?"[^>]*?(?:title="([^"]*)")?""")
        val posterRe = Regex("""data-src="((?:https?:)?//[^"]*?\.jpg(?:[?][^"]*)?)""")
        val out = LinkedHashMap<String, HikariMedia>()
        for (chunk in html.split(splitRe)) {
            val anchor = anchorRe.find(chunk) ?: continue
            val poster = posterRe.find(chunk)?.groupValues?.get(1) ?: continue
            val id = idFromPosterRe.find(poster)?.groupValues?.get(1) ?: continue
            var title = unescapeEntities(anchor.groupValues[2]).trim()
            if (title.isBlank()) {
                title = Regex("""alt="([^"]*)""").find(chunk)?.groupValues?.get(1)
                    ?.let { unescapeEntities(it).trim() } ?: ""
            }
            if (title.isBlank()) continue
            out[id] = HikariMedia(
                id = id,
                title = title,
                type = HikariMediaType.MOVIE,
                posterUrl = absolute(poster),
            )
        }
        return out.values.toList()
    }

    /** Fixes protocol-relative CDN URLs (KVS posters/previews are `//cdn/…`). */
    private fun absolute(url: String): String =
        if (url.startsWith("//")) "https:$url" else url

    /** The `var flashvars = { … };` block of an embed/video page. */
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

/** WhoresHub (whoreshub.com) — KVS tube, posts under /videos/<id>/<slug>/. */
class WhoresHubProvider : KvsTubeProvider() {
    override val id = "whoreshub"
    override val name = "WhoresHub"
    override val mainUrl = "https://www.whoreshub.com"
    override val description = "Large HD tube of straight/gonzo videos. Direct MP4 streams."
    override val version = 1

    override val base = "https://www.whoreshub.com"
    override val videoPath = "videos"
    override val streamHeaders: Map<String, String> = emptyMap()
    override val idFromPosterRe = Regex("""/videos_screenshots/\d+/(\d+)/""")
    override val categoryRows: List<Pair<String, String>> = listOf(
        "blowjob" to "Blowjob",
        "milf" to "MILF",
        "anal" to "Anal",
        "asian" to "Asian",
        "babe" to "Babe",
        "big-tits" to "Big Tits",
        "latina" to "Latina",
        "pov" to "POV",
        "hardcore" to "Hardcore",
        "teen" to "Teen",
    )
}

/** PornTrex (porntrex.com) — KVS tube, posts under /video/<id>/<slug>. */
class PornTrexProvider : KvsTubeProvider() {
    override val id = "porntrex"
    override val name = "PornTrex"
    override val mainUrl = "https://www.porntrex.com"
    override val description = "Big free HD/4K tube with everything from amateur to studio content. Direct MP4 streams."
    override val version = 1

    override val base = "https://www.porntrex.com"
    override val videoPath = "video"
    override val streamHeaders: Map<String, String> = emptyMap()
    override val idFromPosterRe = Regex("""/videos_screenshots/\d+/(\d+)/""")
    override val categoryRows: List<Pair<String, String>> = listOf(
        "amateur" to "Amateur",
        "milf" to "MILF",
        "blowjob" to "Blowjob",
        "asian" to "Asian",
        "lesbian" to "Lesbian",
        "homemade" to "Homemade",
        "webcam" to "Webcam",
        "hentai" to "Hentai",
        "hd" to "HD",
        "4k-porn" to "4K",
    )
}

/** WowXxx (wowxxx.to, ex WOW.XXX) — KVS tube, premium-site rehosts. */
class WowXxxProvider : KvsTubeProvider() {
    override val id = "wowxxx"
    override val name = "WowXxx"
    override val mainUrl = "https://www.wowxxx.to"
    override val description = "Premium-site porn rehosts (Brazzers, MYLF, KINK, TUSHY…). Direct MP4 streams."
    override val version = 1

    override val base = "https://www.wowxxx.to"
    override val videoPath = "videos"
    override val streamHeaders: Map<String, String> = mapOf("Referer" to "https://www.wowxxx.to/")
    override val idFromPosterRe = Regex("""/(\d+)/medium@2x/1\.jpg""")
    override val categoryRows: List<Pair<String, String>> = listOf(
        "milf" to "MILF",
        "blowjob" to "Blowjob",
        "anal" to "Anal",
        "pov" to "POV",
        "lesbians" to "Lesbians",
        "big-tits" to "Big Tits",
        "russian" to "Russian",
        "creampie" to "Creampie",
        "doggystyle" to "Doggystyle",
        "babe" to "Babe",
    )
}
