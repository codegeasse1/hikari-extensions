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
 *    playmogo.com, morencius.com, hgcloud.to, abyssplayer.com…).
 *
 *  Most of those embeds hide their stream URL inside a Dean Edwards p,a,c,k
 *  packed script in the embed's own HTML (luluvids/vidhide-style jwplayer
 *  pages). We therefore fetch each embed over HTTP first, unpack the packed
 *  scripts and pull the m3u8/mp4 out of the decoded JS — fast and reliable.
 *  JS-only players (React SPAs, captcha-gated hosts) fall back to the
 *  WebView m3u8/mp4 capture helper.
 */
class LeakPornerProvider : HikariProvider {

    override val id = "leakporner"
    override val name = "LeakPorner"
    override val mainUrl = "https://leakporner.org"
    override val description = "Leaked/OF adult videos from leakporner.org — latest uploads, search and multi-server HTTP-resolved playback."
    override val version = 3
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

        private val absStreamRegex = Regex("""https?://[^"'\s>]+?\.(?:m3u8|mp4)(?:[?#][^"'\s>]*)?""")
        private val relM3u8Regex = Regex("""(?:"|')(/[^"'\s]+?\.m3u8[^"'\s]*)(?:"|')""")
        private const val PACKED_OPEN = "function(p,a,c,k,e,d)"
        private const val PACKED_MID = "return p}("
        private const val BASE62 = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
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
        val embeds = Regex("""data-embed="([^"]+)"""")
            .findAll(html).map { it.groupValues[1] }
            .filter { it.startsWith("http") }
            .toList()
            .distinct()

        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        val started = System.currentTimeMillis()
        fun add(u: String, referer: String) {
            if (u.isBlank() || !seen.add(u)) return
            out.add(
                HikariStream(
                    name = "Server ${out.size + 1}",
                    url = u,
                    headers = mapOf("Referer" to referer),
                    isM3u8 = u.contains(".m3u8", ignoreCase = true),
                )
            )
        }

        for (embed in embeds) {
            if (out.size >= 5) break
            if (System.currentTimeMillis() - started > 90_000) break

            // 1) HTTP resolution — the embed's own HTML, its Dean Edwards
            //    packed script (luluvids/vidhide-family m3u8s), or its
            //    streamtape `ideoolink` div usually contain the stream.
            val embedHtml = HikariNet.getStringSmart(
                embed,
                pageHeaders + mapOf("Referer" to pageUrl, "X-Requested-With" to "XMLHttpRequest"),
            )
            if (embedHtml != null) {
                for (u in extractStreamUrls(embedHtml, embed)) add(u, embed)
                extractStreamTape(embedHtml)?.let { add(it, embed) }
            }
            if (out.size >= 5) break

            // 2) WebView fallback for JS-only players (React SPAs, iamcdn…).
            //    DoodStream/PlayMogo embeds are Turnstile-captcha gated even
            //    in a real browser, so skip the wasted WebView for those.
            if (isDoodCaptchaHost(embed)) continue
            if (System.currentTimeMillis() - started > 90_000) break
            val hits = try {
                HikariNet.resolveWithWebView(embed, streamCapture, timeoutMs = 20_000)
            } catch (t: Throwable) {
                continue
            }
            for (h in hits) add(h.url, embed)
        }
        return out
    }

    // ------------------------------------------------------------------
    //  Stream extraction helpers
    // ------------------------------------------------------------------

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

    /** True for DoodStream-family hosts whose embeds are Turnstile-captcha gated. */
    private fun isDoodCaptchaHost(embed: String): Boolean {
        val host = embed.substringAfter("://").substringBefore("/").lowercase()
        return host.contains("playmogo") || host.contains("dood")
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

    /** Parses `article.loop-video` cards. */
    private fun parseCards(html: String): List<HikariMedia> {
        val out = LinkedHashMap<String, HikariMedia>()
        for (chunk in html.split(Regex("""<article\s+data-video-uid""")).drop(1)) {
            val m = Regex("""<a href="(https://leakporner\.[a-z]+/[^"]+)" title="([^"]+)"""")
                .find(chunk) ?: continue
            val href = m.groupValues[1]
            val title = unescape(m.groupValues[2])
            val img = Regex("""<img[^>]*data-src="([^"]+)"""").find(chunk)?.groupValues?.get(1)
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
