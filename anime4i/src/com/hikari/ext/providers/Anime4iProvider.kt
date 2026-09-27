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
import org.json.JSONObject
import java.net.URLEncoder

/**
 * Anime4i (anime4i.com) — Chinese anime / donghua site running the "Agnostic"
 * WordPress theme, content served from the site's Dailymotion player.
 *
 *  - Home/lists/search/genre pages are grids of `article.bs` cards carrying
 *    the post id (`rel`/`data-id`), poster (`img.ts-post-image[data-src]`)
 *    and a clean series title in the `.tt` text node.
 *  - Series pages (`/anime/<slug>`) server-render the full episode list in
 *    `.epcheck .eplister`; episode/watch pages render the same list as an
 *    `.as-episode-module` number grid and embed the player as base64 in the
 *    `data-default-embed` attribute and in a `<select class="mirror">` whose
 *    options are base64 iframes, one per server (Dailymotion, Okru, …).
 *  - Streams per server: decode the embed → dailymotion access id →
 *    `player/metadata/video/<id>` → `qualities.auto[].url` (a signed HLS
 *    manifest); or an ok.ru `videoembed/<id>` page whose embedded JSON carries
 *    a signed `hlsManifestUrl`. Both signatures are bound to the IP that asked
 *    for them (the device itself), so playback is direct (no WebView needed).
 */
class Anime4iProvider : HikariProvider {

    override val id = "anime4i"
    override val name = "Anime4i"
    override val mainUrl = "https://anime4i.com"
    override val description = "Chinese anime / donghua from anime4i.com — latest episodes, popular, genre browsing, full episode lists, direct HLS from the Dailymotion and Okru servers."
    override val version = 2
    override val tvTypes: Set<HikariMediaType> = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    companion object {
        private const val BASE = "https://anime4i.com"
        private val LATEST_URL = "$BASE/anime/?status=&type=&order=update"
        private const val CACHE_TTL_MS = 600_000L

        private val htmlCache = HashMap<String, Pair<Long, String>>()
        private val cacheMutex = Mutex()

        private val genres = listOf(
            "action", "adventure", "comedy", "drama", "ecchi", "fantasy",
            "harem", "historical", "horror", "isekai", "martial-arts",
            "mystery", "psychological", "romance", "sci-fi", "shounen",
            "slice-of-life", "supernatural", "suspense", "xianxia", "xuanhuan",
        )

        private val pageHeaders = mapOf(
            "User-Agent" to HikariNet.browserHeaders["User-Agent"].orEmpty(),
            "Accept" to "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language" to "en-US,en;q=0.9",
        )

        private val dmHeaders = mapOf(
            "User-Agent" to HikariNet.browserHeaders["User-Agent"].orEmpty(),
            "Accept" to "application/json, text/plain, */*",
            "Referer" to "https://www.dailymotion.com/",
            "Origin" to "https://www.dailymotion.com",
        )

        private val okruHeaders = mapOf(
            "User-Agent" to HikariNet.browserHeaders["User-Agent"].orEmpty(),
            "Accept" to "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer" to "https://ok.ru/",
            "Origin" to "https://ok.ru",
        )

        // The site's own player URL for a video — grabs the access id.
        private val embedSrcRe = Regex("""(?:video|id)=([A-Za-z0-9]+)""")
        private val streamCapture = Regex("""https?://[^"'\s]+?\.(?:m3u8|mp4)(?:[?#][^"'\s]*)?""")
    }

    // ------------------------------------------------------------------
    //  Catalogs
    // ------------------------------------------------------------------

    override fun catalogs(): List<HikariCatalog> = buildList {
        add(HikariCatalog("latest", "Latest Episodes", HikariMediaType.SERIES))
        add(HikariCatalog("popular", "Popular Today", HikariMediaType.SERIES))
        for (g in genres) {
            val label = g.split("-").joinToString(" ") { w -> w.replaceFirstChar { it.uppercase() } }
            add(HikariCatalog("genre-$g", label, HikariMediaType.SERIES, rawType = g))
        }
    }

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> = when {
        catalog.id == "latest" -> parseListing(LATEST_URL, page)
        catalog.id == "popular" -> if (page > 1) emptyList() else parsePopular()
        catalog.id.startsWith("genre-") -> parseListing("$BASE/genres/${catalog.rawType}", page)
        else -> emptyList()
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        val encoded = URLEncoder.encode(q, "UTF-8").replace("+", "%20")
        val url = if (page <= 1) "$BASE/?s=$encoded" else "$BASE/page/$page/?s=$encoded"
        val results = parseListing(url, 1)
        if (results.isNotEmpty() || page > 1) return results
        // Genre chips in the app call search() — route single-word queries to a genre archive.
        val slug = q.lowercase().replace(Regex("[^a-z0-9-]+"), "-").trim('-')
        if (slug.length < 3) return emptyList()
        return parseListing("$BASE/genres/$slug", 1)
    }

    // ------------------------------------------------------------------
    //  Meta + episodes + streams
    // ------------------------------------------------------------------

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val pageUrl = media.id.takeIf { it.startsWith("http") } ?: return media
        val html = getCached(pageUrl) ?: return media
        val title = Regex("""<h2 itemprop="partOfSeries">([\s\S]*?)</h2>""").find(html)?.groupValues?.get(1)
            ?.let { unescape(it) }
            ?: Regex("""<h1 class="entry-title"[^>]*>([\s\S]*?)</h1>""").find(html)?.groupValues?.get(1)
                ?.let { unescape(it) }
            ?: media.title
        val poster = metaProperty(html, "og:image")?.takeIf { it.startsWith("http") } ?: media.posterUrl
        val overview = Regex("""<div class="entry-content"[^>]*>([\s\S]*?)</div>""").find(html)?.groupValues?.get(1)
            ?.let { stripTags(it) }
            ?: Regex("""<div class="desc mindes">([\s\S]*?)</div>""").find(html)?.groupValues?.get(1)
                ?.let { stripTags(it) }
        val genres = Regex("""<div class="genxed">([\s\S]*?)</div>""").find(html)?.groupValues?.get(1)
            ?.let { g ->
                Regex(""">([^<]+)</a>""").findAll(g).mapNotNull { m ->
                    unescape(m.groupValues[1]).trim().takeIf { it.isNotBlank() }
                }.toList()
            }
            ?: emptyList()
        val year = Regex("""<b>Released on:</b>[\s\S]*?datetime="(\d{4})""").find(html)?.groupValues?.get(1)
            ?.toIntOrNull() ?: media.year
        return media.copy(
            title = title,
            posterUrl = poster,
            type = HikariMediaType.SERIES,
            overview = overview ?: media.overview,
            genres = if (genres.isNotEmpty()) genres else media.genres,
            year = year,
            backdropUrl = poster,
        )
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val pageUrl = media.id.takeIf { it.startsWith("http") } ?: return null
        var html = getCached(pageUrl) ?: return null
        // Episode/watch pages render the list as a module; hop to the series
        // page so the full `.eplister` (ep titles + dates) is parsed.
        if (!html.contains("epcheck") || html.contains("as-episode-module")) {
            val seriesUrl = Regex("""href="(https://anime4i\.com/anime/[a-z0-9-]+)""").find(html)?.groupValues?.get(1)
            if (seriesUrl != null && seriesUrl != pageUrl) {
                html = getCached(seriesUrl) ?: html
            }
        }
        val out = ArrayList<HikariEpisode>()
        if (html.contains("eplister")) {
            for (chunk in html.split(Regex("""<li data-index="""")).drop(1)) {
                val href = Regex("""<a href="(https://anime4i\.com/[^"]+)"[^>]*>""").find(chunk)?.groupValues?.get(1) ?: continue
                val num = Regex("""<div class="epl-num">([^<]+)</div>""").find(chunk)?.groupValues?.get(1)?.toIntOrNull() ?: continue
                val name = Regex("""<div class="epl-title">([\s\S]*?)</div>""").find(chunk)?.groupValues?.get(1)?.let { unescape(it) }
                out.add(
                    HikariEpisode(
                        number = num,
                        id = href,
                        name = name ?: "Episode $num",
                        image = media.posterUrl,
                    )
                )
            }
        }
        if (out.isEmpty() && html.contains("episode-number")) {
            // Fallback: the ep-page module grid.
            for (chunk in html.split(Regex("""class="episode-number""")).drop(1)) {
                val href = Regex("""href="(https://anime4i\.com/[^"]+)""").find(chunk)?.groupValues?.get(1) ?: continue
                val num = Regex("""data-num="(\d+)""").find(chunk)?.groupValues?.get(1)?.toIntOrNull() ?: continue
                val name = Regex("""title="([^"]+)""").find(chunk)?.groupValues?.get(1)?.let { unescape(it) }
                out.add(
                    HikariEpisode(
                        number = num,
                        id = href,
                        name = name ?: "Episode $num",
                        image = media.posterUrl,
                    )
                )
            }
        }
        return out
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val pageUrl = episode?.id?.takeIf { it.startsWith("http") }
            ?: media.id.takeIf { it.startsWith("http") }
            ?: return emptyList()
        val html = getCached(pageUrl) ?: return emptyList()

        // The watch page's server selector: a `<select class="mirror">` whose
        // `<option>` values are base64-encoded iframes, one per server
        // (Dailymotion, Okru, …). `data-default-embed` is just the first one.
        val embeds = ArrayList<Pair<String, String>>() // (server name, iframe src)
        val mirror = Regex("""<select[^>]*class="[^"]*mirror[^"]*"[^>]*>([\s\S]*?)</select>""")
            .find(html)?.groupValues?.get(1)
        if (mirror != null) {
            for (opt in Regex("""<option[^>]*value="([^"]+)"[^>]*>\s*([^<]*)""").findAll(mirror)) {
                val name = opt.groupValues[2].trim().ifBlank { "Server" }
                val dec = HikariNet.base64Decode(opt.groupValues[1])?.toString(Charsets.UTF_8) ?: continue
                val src = Regex("""\bsrc="([^"]+)""").find(dec)?.groupValues?.get(1) ?: continue
                if (src.startsWith("http")) embeds.add(name to src)
            }
        }
        if (embeds.isEmpty()) {
            val b64 = Regex("""data-default-embed="([^"]+)""").find(html)?.groupValues?.get(1) ?: return emptyList()
            val embed = HikariNet.base64Decode(b64)?.toString(Charsets.UTF_8) ?: return emptyList()
            val src = Regex("""\bsrc="([^"]+)""").find(embed)?.groupValues?.get(1) ?: return emptyList()
            if (src.startsWith("http")) embeds.add("Server" to src)
        }

        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        for ((name, embedSrc) in embeds) {
            if (embedSrc.contains("dailymotion", ignoreCase = true)) {
                for (s in resolveDailymotion(embedSrc)) {
                    if (s.url.isBlank() || !seen.add(s.url)) continue
                    out.add(s)
                }
            } else if (embedSrc.contains("ok.ru", ignoreCase = true)) {
                val u = resolveOkru(embedSrc)
                if (u != null && seen.add(u)) {
                    out.add(
                        HikariStream(
                            name = "$name · HLS",
                            url = u,
                            headers = okruHeaders,
                            isM3u8 = true,
                        )
                    )
                }
            } else {
                // Unknown embed host — capture the stream via WebView.
                val hits = try {
                    HikariNet.resolveWithWebView(embedSrc, streamCapture, timeoutMs = 30_000)
                } catch (t: Throwable) {
                    continue
                }
                for (h in hits) {
                    if (h.url.isBlank() || !seen.add(h.url)) continue
                    out.add(
                        HikariStream(
                            name = "$name · Stream",
                            url = h.url,
                            headers = h.headers,
                            isM3u8 = h.url.contains(".m3u8", ignoreCase = true),
                        )
                    )
                }
            }
        }
        return out
    }

    /** Dailymotion embed → access id → metadata API → signed HLS manifest. */
    private suspend fun resolveDailymotion(embedSrc: String): List<HikariStream> {
        val accessId = embedSrcRe.find(embedSrc)?.groupValues?.get(1) ?: return emptyList()
        val metaJson = HikariNet.getString(
            "https://www.dailymotion.com/player/metadata/video/$accessId", dmHeaders
        ) ?: return emptyList()
        val meta = runCatching { JSONObject(metaJson) }.getOrNull() ?: return emptyList()
        val qualities = meta.optJSONObject("qualities") ?: return emptyList()
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        val keys = qualities.keys()
        while (keys.hasNext()) {
            val key = keys.next()
            val arr = qualities.optJSONArray(key) ?: continue
            for (i in 0 until arr.length()) {
                val q = arr.optJSONObject(i) ?: continue
                val u = q.optString("url").trim()
                if (u.isBlank() || !seen.add(u)) continue
                val isM3u8 = u.contains(".m3u8", ignoreCase = true) || q.optString("type").contains("mpegURL", true)
                out.add(
                    HikariStream(
                        name = "Dailymotion · ${if (key == "auto") "Auto" else key}",
                        url = u,
                        headers = mapOf(
                            "User-Agent" to HikariNet.browserHeaders["User-Agent"].orEmpty(),
                            "Referer" to "https://www.dailymotion.com/",
                        ),
                        isM3u8 = isM3u8,
                    )
                )
            }
        }
        return out
    }

    /**
     * Okru embed → `videoembed/<id>` page → signed `hlsManifestUrl` in the
     * page's (double-escaped) JSON config.
     */
    private suspend fun resolveOkru(embedSrc: String): String? {
        val id = Regex("""videoembed/(\d+)""").find(embedSrc)?.groupValues?.get(1) ?: return null
        val page = HikariNet.getStringSmart("https://ok.ru/videoembed/$id", okruHeaders) ?: return null
        val key = page.indexOf("hlsManifestUrl")
        if (key < 0) return null
        val urlStart = page.indexOf("https://", key)
        if (urlStart < 0) return null
        val tail = page.substring(urlStart)
        val cut = listOf(tail.indexOf("\\&quot;"), tail.indexOf("&quot;"))
            .filter { it >= 0 }.minOrNull() ?: return null
        val u = tail.substring(0, cut).trim()
        if (!u.contains(".m3u8")) return null
        return unescapeJson(u)
    }

    /** Un-escapes ok.ru's double-escaped JSON strings (`\\u0026` → `&`, `\/` → `/`). */
    private fun unescapeJson(s: String): String = s
        .replace("\\\\", "\\")
        .replace(Regex("""\\u([0-9a-fA-F]{4})""")) { m ->
            m.groupValues[1].toInt(16).toChar().toString()
        }
        .replace("\\/", "/")

    // ------------------------------------------------------------------
    //  Listing parsers
    // ------------------------------------------------------------------

    private suspend fun parseListing(url: String, page: Int): List<HikariMedia> {
        val pageUrl = if (page <= 1) url else insertPage(url, page)
        val html = getCached(pageUrl) ?: return emptyList()
        return parseCards(html)
    }

    private suspend fun parsePopular(): List<HikariMedia> {
        val html = getCached("$BASE/") ?: return emptyList()
        val section = Regex("""class="popconslide"[\s\S]*?(?=class="listupd normal"|class="kln")""")
            .find(html)?.value ?: return emptyList()
        return parseCards(section)
    }

    /** Parses `article.bs` cards from a `.listupd` grid. */
    private fun parseCards(html: String): List<HikariMedia> {
        val out = LinkedHashMap<String, HikariMedia>()
        for (chunk in html.split(Regex("""<article class="bs""")).drop(1)) {
            val href = Regex("""<a href="(https://anime4i\.com/[^"]+)""").find(chunk)?.groupValues?.get(1) ?: continue
            val postId = Regex("""\b(?:rel|data-id)="(\d+)""").find(chunk)?.groupValues?.get(1)
                ?: href.substringAfterLast("/").trimEnd('/')
            var title = Regex("""<div class="tt">([\s\S]*?)<h2""").find(chunk)?.groupValues?.get(1)
                ?.let { unescape(stripTags(it)) }
            if (title.isNullOrBlank()) {
                title = Regex("""<h2 itemprop="headline">([\s\S]*?)</h2>""").find(chunk)?.groupValues?.get(1)
                    ?.let { unescape(it).replace(Regex("""\s+Episode\s+\d+.*$"""), "") }
            }
            if (title.isNullOrBlank()) continue
            val imgTag = Regex("""<img[^>]*class="[^"]*ts-post-image[^"]*"[^>]*>""").find(chunk)?.value
                ?: Regex("""<img[^>]*data-src="[^"]*thumbnails[^"]*"[^>]*>""").find(chunk)?.value
            val poster = imgTag?.let {
                Regex("""data-src="([^"]+)""").find(it)?.groupValues?.get(1)
                    ?: Regex("""src="([^"]+)""").find(it)?.groupValues?.get(1)
            }?.substringBefore("?")
            out[postId] = HikariMedia(
                id = href,
                title = title,
                type = HikariMediaType.SERIES,
                posterUrl = poster?.takeIf { it.startsWith("http") },
            )
        }
        return out.values.toList()
    }

    private fun insertPage(url: String, page: Int): String {
        val q = url.indexOf('?')
        val base = if (q >= 0) url.substring(0, q) else url
        val query = if (q >= 0) url.substring(q) else ""
        return "${base.trimEnd('/')}/page/$page/$query"
    }

    // ------------------------------------------------------------------
    //  HTML helpers
    // ------------------------------------------------------------------

    private suspend fun getCached(url: String): String? {
        val now = System.currentTimeMillis()
        cacheMutex.withLock {
            htmlCache[url]?.let { (t, html) ->
                if (now - t < CACHE_TTL_MS) return html
            }
        }
        // getStringSmart: plain HTTP first, WebView render fallback if a WAF
        // challenge blocks the okhttp client.
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
