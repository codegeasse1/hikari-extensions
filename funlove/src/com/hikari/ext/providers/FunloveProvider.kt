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
import org.json.JSONArray
import org.json.JSONObject
import java.net.URLEncoder

/**
 * FunLove (funlove.info) — exclusive FC2PPV / JAV / China AV streaming site.
 * The UI is a React SPA on funlove.info backed by a JSON API on funlove.pro.
 *
 *  - Catalogs/search hit the open endpoints: `/api/product` (home sections),
 *    `/api/product/movies/<page>` and `/api/product/popular/<page>` (both
 *    accept `?keyword=` for search) returning `data.videos[]`.
 *  - Detail (`movie/<slug>`) and playback (`iframe-video/<slug>`) endpoints
 *    reject datacenter/proxy IPs with "Unauthorized access" but serve real
 *    browsers, so those calls are made with browser headers from the device's
 *    own IP. The player URL is pulled out of the iframe-video JSON with a
 *    defensive key walk; a WebView capture of the site's own
 *    `/video-iframe/<slug>` page is the fallback.
 */
class FunloveProvider : HikariProvider {

    override val id = "funlove"
    override val name = "FunLove"
    override val mainUrl = "https://funlove.info"
    override val description = "Exclusive FC2PPV / JAV from funlove.info — new, popular and top-viewed rows, movies archive and search."
    override val version = 2
    override val tvTypes: Set<HikariMediaType> = setOf(HikariMediaType.MOVIE)

    companion object {
        private const val API = "https://funlove.pro/api/product"
        private const val SITE = "https://funlove.info"
        private const val CACHE_TTL_MS = 600_000L

        private val jsonCache = HashMap<String, Pair<Long, String>>()
        private val cacheMutex = Mutex()

        private val apiHeaders = mapOf(
            "User-Agent" to HikariNet.browserHeaders["User-Agent"].orEmpty(),
            "Accept" to "application/json, text/plain, */*",
            "Referer" to "$SITE/",
            "Origin" to SITE,
        )

        private val streamCapture = Regex("""https?://[^"'\s]+?\.(?:m3u8|mp4)(?:[?#][^"'\s]*)?""")

        // Field names the iframe-video player JSON has been seen using.
        private val urlKeys = listOf(
            "url", "src", "file", "video", "videoUrl", "video_url", "m3u8", "source",
            "link", "play", "playUrl", "player", "playerUrl", "stream", "embed", "iframe",
        )
    }

    // ------------------------------------------------------------------
    //  Catalogs
    // ------------------------------------------------------------------

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("latest", "New Movies", HikariMediaType.MOVIE),
        HikariCatalog("popular", "Popular", HikariMediaType.MOVIE),
        HikariCatalog("movies", "Movies Archive", HikariMediaType.MOVIE),
        HikariCatalog("topview", "Top Viewed", HikariMediaType.MOVIE),
        HikariCatalog("toplike", "Top Liked", HikariMediaType.MOVIE),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        return when (catalog.id) {
            "latest" -> homeSection("newMovie", page)
            "topview" -> homeSection("topView", page)
            "toplike" -> homeSection("topLike", page)
            "popular" -> parseVideos(getJsonCached("$API/popular/$page") ?: return emptyList())
            "movies" -> parseVideos(getJsonCached("$API/movies/$page") ?: return emptyList())
            else -> emptyList()
        }
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        val encoded = URLEncoder.encode(q, "UTF-8").replace("+", "%20")
        val url = if (page <= 1) "$API/movies/?keyword=$encoded" else "$API/movies/$page/?keyword=$encoded"
        return parseVideos(getJsonCached(url, maxAgeMs = 30_000L) ?: return emptyList())
    }

    private suspend fun homeSection(key: String, page: Int): List<HikariMedia> {
        if (page > 1) return emptyList()
        val json = getJsonCached(API) ?: return emptyList()
        val d = json.optJSONObject("data") ?: return emptyList()
        val arr = d.optJSONArray(key) ?: return emptyList()
        val out = ArrayList<HikariMedia>()
        for (i in 0 until arr.length()) {
            arr.optJSONObject(i)?.let { toMedia(it) }?.let { out.add(it) }
        }
        return out
    }

    // ------------------------------------------------------------------
    //  Meta + streams
    // ------------------------------------------------------------------

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        // The movie endpoint may be IP-gated; when it answers, enrich the card.
        val detail = getJsonCached("$API/movie/${media.id}", maxAgeMs = 60_000L)
        if (detail == null) return media
        val d = detail.optJSONObject("data") ?: detail
        val title = d.optString("name").takeIf { it.isNotBlank() && it != "null" } ?: media.title
        val poster = d.optString("image").takeIf { it.startsWith("http") } ?: media.posterUrl
        val overview = firstString(d, "description", "desc", "content", "synopsis", "plot")
            ?.takeIf { it.isNotBlank() && it != "null" }
        val genres = arrayOfNames(d, "genre", "genres", "category", "categories")
        return media.copy(
            title = title,
            posterUrl = poster,
            overview = overview ?: media.overview,
            genres = if (genres.isNotEmpty()) genres else media.genres,
            backdropUrl = poster,
        )
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val slug = media.id.takeIf { it.isNotBlank() } ?: return emptyList()
        val watchPage = "$SITE/video-iframe/$slug"
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()

        // 1) The site's own player API (browser headers, device IP).
        val body = HikariNet.getString("$API/iframe-video/$slug", apiHeaders)
        if (body != null) {
            for (u in extractUrls(body)) {
                if (u.isBlank() || !seen.add(u)) continue
                out.add(
                    HikariStream(
                        name = "Stream",
                        url = u,
                        headers = mapOf(
                            "User-Agent" to HikariNet.browserHeaders["User-Agent"].orEmpty(),
                            "Referer" to watchPage,
                            "Origin" to SITE,
                        ),
                        isM3u8 = looksLikeHls(u),
                    )
                )
            }
        }

        // 2) Also run the site's own watch page in a WebView and capture the
        //    video request it makes — an alternative server if the API URL's
        //    CDN is picky about seeking.
        try {
            val hits = HikariNet.resolveWithWebView(watchPage, streamCapture, timeoutMs = 45_000)
            for (h in hits) {
                if (h.url.isBlank() || !seen.add(h.url)) continue
                out.add(
                    HikariStream(
                        name = "Server",
                        url = h.url,
                        headers = h.headers,
                        isM3u8 = looksLikeHls(h.url),
                    )
                )
            }
        } catch (t: Throwable) {
            // ignore — the API streams (if any) are still usable
        }
        return out
    }

    /** HLS detection beyond the obvious extension (some CDNs serve HLS without
     *  a `.m3u8` in the URL path, which the progressive player would choke on). */
    private fun looksLikeHls(u: String): Boolean =
        u.contains(".m3u8", ignoreCase = true) || u.contains("m3u8", ignoreCase = true)

    // ------------------------------------------------------------------
    //  Parsing helpers
    // ------------------------------------------------------------------

    private fun parseVideos(json: JSONObject): List<HikariMedia> {
        val d = json.optJSONObject("data") ?: return emptyList()
        val arr = d.optJSONArray("videos") ?: return emptyList()
        val out = ArrayList<HikariMedia>()
        for (i in 0 until arr.length()) {
            arr.optJSONObject(i)?.let { toMedia(it) }?.let { out.add(it) }
        }
        return out
    }

    private fun toMedia(o: JSONObject): HikariMedia? {
        val slug = o.optString("slug").takeIf { it.isNotBlank() } ?: return null
        val name = o.optString("name").takeIf { it.isNotBlank() } ?: return null
        val poster = o.optString("image").takeIf { it.startsWith("http") }
        val year = o.optString("published").takeIf { it.length >= 4 }?.substring(0, 4)?.toIntOrNull()
        return HikariMedia(
            id = slug,
            title = name,
            type = HikariMediaType.MOVIE,
            posterUrl = poster,
            year = year,
        )
    }

    /**
     * Pulls a playable URL out of the iframe-video response body, whatever its
     * shape: a bare URL string, `data` as string/array, or an object whose
     * nested string fields carry the URL (possibly as an `<iframe>` tag).
     */
    private fun extractUrls(body: String): List<String> {
        val found = LinkedHashSet<String>()
        val trimmed = body.trim()
        if (trimmed.startsWith("http") || trimmed.startsWith("//")) {
            found.add(trimmed)
        }
        val json = runCatching { JSONObject(body) }.getOrNull()
        if (json != null) {
            collectJson(json, found)
        } else {
            val arr = runCatching { JSONArray(body) }.getOrNull()
            if (arr != null) {
                for (i in 0 until arr.length()) {
                    arr.opt(i)?.let { v -> collectAny(v, found) }
                }
            }
        }
        // <iframe src="..."> inside any string value.
        val iframe = Regex("""<iframe[^>]*src=["'](https?://[^"']+)""").find(body)?.groupValues?.get(1)
        if (iframe != null) found.add(iframe)
        return found.toList()
    }

    private fun collectJson(json: JSONObject, out: MutableSet<String>) {
        val data = json.opt("data")
        if (data != null) {
            collectAny(data, out)
            return
        }
        for (k in urlKeys) {
            if (!json.has(k)) continue
            collectAny(json.opt(k), out)
        }
    }

    private fun collectAny(v: Any?, out: MutableSet<String>) {
        when (v) {
            is String -> collectString(v, out)
            is JSONArray -> for (i in 0 until v.length()) collectAny(v.opt(i), out)
            is JSONObject -> {
                for (k in urlKeys) {
                    if (v.has(k)) collectAny(v.opt(k), out)
                }
                collectJson(v, out)
            }
        }
    }

    private fun collectString(s: String, out: MutableSet<String>) {
        val t = s.trim()
        if (t.startsWith("http") && t.length < 500 && !t.contains("<")) {
            out.add(t)
            return
        }
        val inner = Regex("""src=["'](https?://[^"']+)""").find(t)?.groupValues?.get(1)
        if (inner != null) out.add(inner)
    }

    private fun firstString(o: JSONObject, vararg keys: String): String? {
        for (k in keys) {
            val v = o.opt(k)
            if (v is String && v.isNotBlank()) return v
            if (v is JSONArray && v.length() > 0) {
                v.opt(0)?.let { if (it is String && it.isNotBlank()) return it }
            }
        }
        return null
    }

    private fun arrayOfNames(o: JSONObject, vararg keys: String): List<String> {
        val out = LinkedHashSet<String>()
        for (k in keys) {
            val v = o.opt(k)
            when (v) {
                is JSONArray -> for (i in 0 until v.length()) {
                    v.optString(i).takeIf { it.isNotBlank() }?.let { out.add(it) }
                }
                is JSONObject -> {
                    for (key in listOf("name", "title", "alias")) {
                        v.optString(key).takeIf { it.isNotBlank() }?.let { out.add(it) }
                    }
                }
                is String -> if (v.isNotBlank()) out.add(v)
            }
        }
        return out.toList()
    }

    // ------------------------------------------------------------------
    //  Cache
    // ------------------------------------------------------------------

    private suspend fun getJsonCached(url: String, maxAgeMs: Long = CACHE_TTL_MS): JSONObject? {
        val now = System.currentTimeMillis()
        cacheMutex.withLock {
            jsonCache[url]?.let { (t, body) ->
                if (now - t < maxAgeMs) return runCatching { JSONObject(body) }.getOrNull()
            }
        }
        val body = HikariNet.getString(url, apiHeaders) ?: return null
        val parsed = runCatching { JSONObject(body) }.getOrNull()
        if (parsed != null) {
            cacheMutex.withLock {
                if (jsonCache.size > 60) jsonCache.clear()
                jsonCache[url] = now to body
            }
        }
        return parsed
    }
}
