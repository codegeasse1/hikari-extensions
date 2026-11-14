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
 *    link (plus the main `player.php` iframe). Each host hash resolves
 *    through `pornhoarder.net/download.php?video=<hostHash>`, whose
 *    `var durl = "<base64>"` decodes to the host's own page (e.g.
 *    `https://luluvdo.com/d/<id>/`). Those host pages usually hide the
 *    stream URL inside a Dean Edwards p,a,c,k packed script, so we fetch
 *    each host page over HTTP and unpack it; JS-only hosts (captcha-gated
 *    doodstream, etc.) fall back to the WebView capture helper.
 *
 *  Dailymotion-hosted mirrors are deliberately skipped — Dailymotion is
 *  blocked/geo-banned in several countries, so those servers would just
 *  appear as broken "Server N" entries.
 */
class PornhoarderProvider : HikariProvider {

    override val id = "pornhoarder"
    override val name = "PornHoarder"
    override val mainUrl = "https://pornhoarder.st"
    override val description = "Pornhoarder.tv mirrors — latest, trending and search with HTTP-resolved playback from lulustream/streamtape/etc."
    override val version = 3
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

        private val streamCapture = Regex("""https?://[^"'\s]+?\.(?:m3u8|mp4)(?:[?#][^"'\s]*)?""")

        private val absStreamRegex = Regex("""https?://[^"'\s>]+?\.(?:m3u8|mp4)(?:[?#][^"'\s>]*)?""")
        private val relM3u8Regex = Regex("""(?:"|')(/[^"'\s]+?\.m3u8[^"'\s]*)(?:"|')""")
        private const val PACKED_OPEN = "function(p,a,c,k,e,d)"
        private const val PACKED_MID = "return p}("
        private const val BASE62 = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

        // player.php gates the real player behind a "Press to play" form
        // (`#form-button-click`); submitting it is what reveals the host iframe
        // whose video request we capture. Native form.submit() needs no jQuery.
        private const val CLICK_PLAY_SCRIPT =
            "(function(){try{var f=document.getElementById('form-button-click');" +
                "if(f){f.submit();return;}var b=document.getElementById('play-button');if(b){b.click();}}catch(e){}})();"
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

        // The video page's own player hash, plus every host listed as a
        // `/pornvideo/<slug>/<hash>/` link. Each of those server pages carries
        // its OWN player.php hash (different from the link hash), so resolve
        // the pages to get the real player URLs.
        val mainHash = Regex("""player\.php\?video=([A-Za-z0-9+/=]+)""").find(html)?.groupValues?.get(1)
        val servers = Regex("""<a href='/pornvideo/([^']+?)/([A-Za-z0-9+/=]+)' title='Watch this video on ([^']+)'>""")
            .findAll(html).map { Triple(it.groupValues[1], it.groupValues[2], it.groupValues[3].trim()) }
            .toList()

        val playerUrls = LinkedHashMap<String, String>() // playerHash -> server label
        if (mainHash != null) playerUrls[mainHash] = ""
        for ((slug, urlHash, label) in servers) {
            if (playerUrls.size >= 5) break
            val serverPage = getCached("$BASE/pornvideo/$slug/$urlHash/") ?: continue
            val h = Regex("""player\.php\?video=([A-Za-z0-9+/=]+)""").find(serverPage)?.groupValues?.get(1)
            if (h != null) playerUrls.putIfAbsent(h, label)
        }
        if (playerUrls.isEmpty()) return emptyList()

        // download.php resolves a hash to its actual host page (lulustream /
        // vidhide-family pages unpack straight to the m3u8 over plain HTTP).
        val hostUrls = ArrayList<String>()
        if (mainHash != null) {
            val dl = getCached("$DL/download.php?video=$mainHash")
            val durl = dl?.let { Regex("""var durl = "([^"]+)"""").find(it)?.groupValues?.get(1) }
            durl?.let { HikariNet.base64Decode(it) }
                ?.let { String(it, Charsets.UTF_8) }
                ?.takeIf { it.startsWith("http") && !isDailymotion(it) }
                ?.let { hostUrls.add(it) }
        }

        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        val started = System.currentTimeMillis()
        fun add(u: String, name: String, referer: String) {
            if (u.isBlank() || isDailymotion(u) || !seen.add(u)) return
            out.add(
                HikariStream(
                    name = name,
                    url = u,
                    headers = mapOf("Referer" to referer),
                    isM3u8 = u.contains(".m3u8", ignoreCase = true),
                )
            )
        }

        // 1) Cheap HTTP unpack of the host page (lulustream/vidhide-family).
        for (hostUrl in hostUrls) {
            if (out.size >= 4) break
            val hostHtml = HikariNet.getStringSmart(hostUrl, pageHeaders + mapOf("Referer" to pageUrl))
            if (hostHtml != null) {
                for (u in extractStreamUrls(hostHtml, hostUrl)) {
                    add(u, "Server ${out.size + 1}", hostUrl)
                }
                extractStreamTape(hostHtml)?.let { add(it, "Server ${out.size + 1}", hostUrl) }
            }
            if (out.isNotEmpty()) break
        }

        // 2) The site's own player pages in a WebView, clicking the play gate
        //    (`#form-button-click`). The host iframe that page loads fires the
        //    real m3u8/mp4 request (lulustream, streamtape, mixdrop…) — exactly
        //    what a browser would play, cookies and all.
        if (out.isEmpty()) {
            for ((hash, label) in playerUrls) {
                if (out.size >= 4) break
                if (System.currentTimeMillis() - started > 120_000) break
                val name = if (label.isBlank()) "Server ${out.size + 1}" else "Server · $label"
                val hits = try {
                    HikariNet.resolveWithWebView(
                        "$DL/player.php?video=$hash",
                        streamCapture,
                        timeoutMs = 35_000,
                        script = CLICK_PLAY_SCRIPT,
                    )
                } catch (t: Throwable) {
                    continue
                }
                for (h in hits) {
                    add(h.url, name, h.headers["Referer"] ?: "$DL/")
                }
                if (out.isNotEmpty()) break
            }
        }
        return out
    }

    // ------------------------------------------------------------------
    //  Stream extraction helpers
    // ------------------------------------------------------------------

    private fun isDailymotion(u: String): Boolean {
        val host = u.substringAfter("://").substringBefore("/").lowercase()
        return host.contains("dailymotion") || host.contains("dmcdn")
    }

    /** Unpacks every Dean Edwards p,a,c,k packed script in [html] and returns the decoded JS. */
    private fun unpackPackedScripts(html: String): String {
        val out = StringBuilder()
        var idx = 0
        while (true) {
            val start = html.indexOf(PACKED_OPEN, idx)
            if (start < 0) break
            val open = html.indexOf(PACKED_MID, start)
            if (open < 0) break
            val q = open + PACKED_MID.length
            if (q >= html.length || html[q] != '\'') {
                idx = open + PACKED_MID.length
                continue
            }
            val enc = StringBuilder()
            var i = q + 1
            while (i < html.length && html[i] != '\'') {
                val ch = html[i]
                if (ch == '\\' && i + 1 < html.length) {
                    enc.append(html[i + 1])
                    i += 2
                } else {
                    enc.append(ch)
                    i++
                }
            }
            if (i >= html.length) break
            val rest = html.substring(i + 1)
            val m = Regex(""",(\d+),(\d+),'([\s\S]*?)'\.split\('\|'\)""").find(rest) ?: break
            val base = m.groupValues[1].toIntOrNull() ?: break
            val count = m.groupValues[2].toIntOrNull() ?: break
            val words = m.groupValues[3].split("|")
            var decoded = enc.toString()
            for (ci in count - 1 downTo 0) {
                val token = if (base <= 36) ci.toString(base) else toBase62(ci, base)
                decoded = decoded.replace(Regex("\\b" + token + "\\b"), words.getOrElse(ci) { "" })
            }
            out.append('\n').append(decoded)
            val after = rest.indexOf(".split('|')") + ".split('|')".length
            idx = i + 1 + after
        }
        return out.toString()
    }

    private fun toBase62(n: Int, base: Int): String {
        var v = n
        if (v == 0) return "0"
        val sb = StringBuilder()
        while (v > 0) {
            sb.append(BASE62[v % base])
            v /= base
        }
        return sb.reverse().toString()
    }

    private fun isUsableStreamUrl(u: String): Boolean {
        val l = u.lowercase()
        if (l.contains(".jpg") || l.contains(".jpeg") || l.contains(".png") ||
            l.contains(".webp") || l.contains(".gif") || l.contains(".svg") || l.contains(".txt")
        ) return false
        return l.contains(".m3u8") || l.contains(".mp4")
    }

    private fun resolveRelative(rel: String, base: String): String? {
        val schemeEnd = base.indexOf("://")
        if (schemeEnd < 0) return null
        val pathStart = base.indexOf('/', schemeEnd + 3)
        val origin = if (pathStart < 0) base else base.substring(0, pathStart)
        return origin + rel
    }

    /** Extracts stream URLs from an embed/host page: raw HTML first, then decoded packed JS. */
    private fun extractStreamUrls(html: String, baseUrl: String): List<String> {
        val text = (html + "\n" + unpackPackedScripts(html)).replace("\\/", "/")
        val abs = LinkedHashSet<String>()
        val rel = LinkedHashSet<String>()
        for (m in absStreamRegex.findAll(text)) {
            val u = m.value.trim().trimEnd('"', '\'')
            if (isUsableStreamUrl(u)) abs.add(u)
        }
        for (m in relM3u8Regex.findAll(text)) {
            val r = resolveRelative(m.groupValues[1], baseUrl) ?: continue
            if (isUsableStreamUrl(r)) rel.add(r)
        }
        val all = LinkedHashSet<String>()
        for (u in abs + rel) if (u.contains(".m3u8", ignoreCase = true)) all.add(u)
        for (u in abs + rel) if (!u.contains(".m3u8", ignoreCase = true)) all.add(u)
        return all.toList()
    }

    /**
     * StreamTape embeds hide the file in `<div id="ideoolink" style="display:none;">
     * /streamtape.com/get_video?id=…&expires=…&ip=…&token=…</div>`. That URL
     * 302-redirects to the real `*.tapecontent.net/…mp4?stream=1` file, which
     * the player follows transparently, so it is exposed as the stream.
     */
    private fun extractStreamTape(embedHtml: String): String? {
        for (id in listOf("ideoolink", "robotlink", "botlink")) {
            val raw = Regex("""<div[^>]*id=["']$id["'][^>]*>([^<]*)</div>""")
                .find(embedHtml)?.groupValues?.get(1)
                ?: continue
            val v = raw.trim()
            if (v.isBlank()) continue
            val u = when {
                v.startsWith("http") -> v
                v.startsWith("/streamtape.com/") -> "https://streamtape.com" + v.removePrefix("/streamtape.com")
                v.startsWith("/") -> "https://streamtape.com$v"
                else -> continue
            }
            if (u.contains("/get_video?")) return u
        }
        return null
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
