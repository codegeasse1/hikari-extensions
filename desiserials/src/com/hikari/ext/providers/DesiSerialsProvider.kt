package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import org.json.JSONObject
import java.net.URLEncoder
import java.util.concurrent.Semaphore

/**
 * DesiSerials — Indian TV serial episodes (Star Plus, Colors, Zee, Sony...).
 *
 * Site model (verified live Oct 2026): episode posts at
 * /{slug}/{16-hex-id}/ with poster + 5 server rows (JW Player, Video.js,
 * Plyr, Shaka, HLS), each a go.searchping.com getlink.php URL (?v=&part2=
 * &part3=&type=). Catalogs are channel/show category pages plus the home
 * feed, all paginated /page/N/ with .film-tile cards. Search is
 * /search.php?s=. Media items are episodes (single-episode SERIES), so
 * every catalog entry plays directly.
 */
class DesiSerialsProvider : HikariProvider {

    override val id = "desiserials"
    override val name = "Desi Serials"
    override val mainUrl = "https://www.desiserials.su"
    override val description = "Indian TV serial episodes — Star Plus, Colors TV, Zee TV, Sony, SAB and more."
    override val version = 2
    override val tvTypes = setOf(HikariMediaType.SERIES)

    private val ua =
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"

    private fun pageHeaders(referer: String? = null): Map<String, String> {
        val h = mutableMapOf(
            "User-Agent" to ua,
            "Accept" to "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language" to "en-US,en;q=0.9",
            "Referer" to "$mainUrl/",
            "Origin" to mainUrl,
        )
        if (referer != null) h["Referer"] = referer
        return h
    }

    // ---------------- catalogs ----------------

    private data class Cat(val id: String, val name: String, val path: String)

    private val cats = listOf(
        Cat("latest", "Latest Episodes", "/"),
        Cat("star-plus", "Star Plus", "/category/star-plus/"),
        Cat("colors-tv", "Colors TV", "/category/colors-tv/"),
        Cat("zee-tv", "Zee TV", "/category/zee-tv/"),
        Cat("sony-tv", "Sony TV", "/category/sony-tv/"),
        Cat("sab-tv", "SAB TV", "/category/sab-tv/"),
        Cat("dangal-tv", "Dangal TV", "/category/dangal-tv/"),
        Cat("star-jalsha", "Star Jalsha", "/category/star-jalsha/"),
        Cat("jiohotstar", "JioHotstar", "/category/jiohotstar/"),
    )

    override fun catalogs(): List<HikariCatalog> =
        cats.map { HikariCatalog(it.id, it.name, HikariMediaType.SERIES) }

    private val episodeHref = Regex("""href="(/[a-z0-9][a-z0-9\-]*/[a-f0-9]{16}/)"""")
    private val tileBlock = Regex("""<article[^>]*class="film-tile"[^>]*>([\s\S]*?)</article>""")
    private val tileImg = Regex("""<img[^>]+src="([^"]+)"""")
    private val tileTitle = Regex("""<h3[^>]*>\s*<a[^>]*>([^<]{3,200})</a>""")
    private val tileAria = Regex("""aria-label="Watch ([^"]{3,200})"""")

    private fun absUrl(u: String): String = when {
        u.startsWith("http") -> u
        u.startsWith("/") -> mainUrl + u
        else -> "$mainUrl/$u"
    }

    private fun cleanTitle(s: String): String = s
        .replace("&amp;", "&").replace("&quot;", "\"").replace("&#039;", "'")
        .replace("&rsquo;", "'").replace("&nbsp;", " ").replace("\\s+".toRegex(), " ")
        .trim()

    private fun encodeMedia(path: String, title: String, poster: String?): String {
        val t = cleanTitle(title)
        return "desi|" + path + "|" + t.replace("|", " ") + "|" + (poster ?: "")
    }

    private fun decodeMedia(id: String): Triple<String, String, String?>? {
        return try {
            if (!id.startsWith("desi|")) return null
            val parts = id.split("|", limit = 4)
            if (parts.size < 3) return null
            Triple(parts[1], parts[2], parts.getOrNull(3)?.ifBlank { null })
        } catch (_: Exception) {
            null
        }
    }

    private fun parseTiles(html: String): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        val seen = HashSet<String>()
        for (m in tileBlock.findAll(html)) {
            val block = m.groupValues[1]
            val href = episodeHref.find(block)?.groupValues?.get(1) ?: continue
            if (!seen.add(href)) continue
            val img = tileImg.find(block)?.groupValues?.get(1)
            var title = tileTitle.find(block)?.groupValues?.get(1)?.let { cleanTitle(it) }
            if (title.isNullOrBlank()) {
                title = tileAria.find(block)?.groupValues?.get(1)?.let { cleanTitle(it) }
            }
            if (title.isNullOrBlank()) {
                title = href.trim('/').split('/').firstOrNull()
                    ?.split('-')?.dropLast(4)?.joinToString(" ")
                    ?.split(' ')?.joinToString(" ") {
                        it.replaceFirstChar { c -> c.uppercase() }
                    }?.ifBlank { "Episode" } ?: "Episode"
            }
            val poster = img?.let { absUrl(it) }
            out.add(
                HikariMedia(
                    id = encodeMedia(href, title, poster),
                    title = title, type = HikariMediaType.SERIES,
                    posterUrl = poster,
                ),
            )
        }
        // fallback: bare episode anchors (category pages without tiles)
        if (out.isEmpty()) {
            for (m in episodeHref.findAll(html)) {
                val href = m.groupValues[1]
                if (!seen.add(href)) continue
                val title = href.trim('/').split('/').firstOrNull()
                    ?.split('-')?.dropLast(4)?.joinToString(" ")
                    ?.split(' ')?.joinToString(" ") {
                        it.replaceFirstChar { c -> c.uppercase() }
                    }?.ifBlank { "Episode" } ?: "Episode"
                out.add(
                    HikariMedia(
                        id = encodeMedia(href, title, null),
                        title = title, type = HikariMediaType.SERIES,
                    ),
                )
                if (out.size >= 60) break
            }
        }
        return out
    }

    private suspend fun fetchHtml(url: String): String? {
        return try {
            HikariNet.getStringSmart(url, pageHeaders())
        } catch (_: Exception) {
            null
        } ?: try {
            HikariNet.getString(url, pageHeaders())
        } catch (_: Exception) {
            null
        }
    }

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val cat = cats.firstOrNull { it.id == catalog.id } ?: return emptyList()
        val p = page.coerceAtLeast(1)
        val url = if (p <= 1) "$mainUrl${cat.path}" else "$mainUrl${cat.path}page/$p/"
        val html = fetchHtml(url) ?: return emptyList()
        return parseTiles(html)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        if (page > 1) return emptyList()
        val url = "$mainUrl/search.php?s=" + URLEncoder.encode(q, "UTF-8")
        val html = fetchHtml(url) ?: return emptyList()
        return parseTiles(html)
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val (path, title, poster) = decodeMedia(media.id) ?: return media
        return try {
            val html = fetchHtml(absUrl(path)) ?: return media
            val desc = Regex("""<meta[^>]+property="og:description"[^>]+content="([^"]{10,600})"""")
                .find(html)?.groupValues?.get(1)?.let { cleanTitle(it) }
            val img = Regex("""<meta[^>]+property="og:image"[^>]+content="([^"]+)"""")
                .find(html)?.groupValues?.get(1)
            media.copy(
                overview = desc,
                posterUrl = img?.let { absUrl(it) } ?: poster ?: media.posterUrl,
                title = if (title.isNotBlank()) title else media.title,
            )
        } catch (_: Exception) {
            media
        }
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val (path, title, poster) = decodeMedia(media.id) ?: return null
        return listOf(HikariEpisode(number = 1, id = media.id, name = title, image = poster))
    }

    // ---------------- streams ----------------

    private val m3u8Url = Regex("""https?://[^\s"'<>]+\.m3u8(?:[?#][^\s"'<>]*)?""")
    private val mp4Url = Regex("""https?://[^\s"'<>]+\.mp4(?:[?#][^\s"'<>]*)?""")
    private val frameSrc = Regex("""<iframe[^>]+src="([^"]+)"""")
    private val capture = Regex("""(?i)\.m3u8(?:\?|$)|[^a-z]master\.txt|\.mp4(?:\?|$)""")

    private val hookScript = """
        (function() {
            if (window.__interceptor_installed) return "already_installed";
            Object.defineProperty(window, '__interceptor_installed', {
                value: true, writable: true, configurable: true, enumerable: false
            });
            function triggerInterception(url) {
                if (!url) return;
                var urlStr = (url && typeof url.toString === 'function') ? url.toString() : url;
                if (urlStr.indexOf('m3u8') !== -1 || urlStr.indexOf('master.txt') !== -1 || urlStr.indexOf('.mp4') !== -1) {
                    window.location.href = urlStr;
                }
            }
            try {
                var d = Object.getOwnPropertyDescriptor(HTMLMediaElement.prototype, 'src');
                if (d && d.set) {
                    var origSet = d.set;
                    Object.defineProperty(HTMLMediaElement.prototype, 'src', {
                        set: function(val) { triggerInterception(val); return origSet.apply(this, arguments); },
                        configurable: true, enumerable: true
                    });
                }
            } catch(e) {}
            try {
                var d2 = Object.getOwnPropertyDescriptor(HTMLSourceElement.prototype, 'src');
                if (d2 && d2.set) {
                    var origSet2 = d2.set;
                    Object.defineProperty(HTMLSourceElement.prototype, 'src', {
                        set: function(val) { triggerInterception(val); return origSet2.apply(this, arguments); },
                        configurable: true, enumerable: true
                    });
                }
            } catch(e) {}
        })();
    """.trimIndent()

    private data class Server(val name: String, val url: String)

    private fun parseServers(html: String): List<Server> {
        val out = ArrayList<Server>()
        val seen = HashSet<String>()
        // split into .stream-row blocks so an info row (no link) can never
        // steal the next player row's link
        val blocks = html.split("<div class=\"stream-row\">")
        for (block in blocks) {
            val href = Regex("""href="(https://go\.searchping\.com/desiserials/getlink\.php\?[^"]+)"""")
                .find(block)?.groupValues?.get(1) ?: continue
            var name = Regex("""stream-title">([^<]{2,120})</div>""")
                .find(block)?.groupValues?.get(1)?.let { cleanTitle(it) } ?: "Server"
            if (name.contains("Browser Support", true) || name.contains("New Tab", true)) continue
            val url = href.replace("&amp;", "&")
            if (!seen.add(url)) continue
            name = name.substringBefore("–").substringBefore("-").trim().ifBlank { "Server" }
            out.add(Server(name, url))
        }
        return out
    }

    private fun playHeaders(episodeUrl: String): Map<String, String> = mapOf(
        "User-Agent" to ua,
        "Referer" to episodeUrl,
        "Origin" to mainUrl,
        "Accept" to "*/*",
    )

    private fun navScript(serverUrl: String): String {
        // The resolver gate (go.searchping.com) only answers navigations that
        // carry this episode page as Referer — a cold WebView load gets
        // "Access denied". So the WebView opens the EPISODE page first and
        // navigates from there: the request then carries the right Referer
        // plus the site's cookies, the gate redirects into the file-host
        // player, and the capture sees the playlist it requests.
        val target = JSONObject.quote(serverUrl)
        return """
            (function() {
                function hook() {
                    function trigger(u) {
                        if (!u) return;
                        var s = (u && u.toString) ? u.toString() : u;
                        if (s.indexOf('m3u8') !== -1 || s.indexOf('master.txt') !== -1 || s.indexOf('.mp4') !== -1) {
                            window.location.href = s;
                        }
                    }
                    try {
                        var d = Object.getOwnPropertyDescriptor(HTMLMediaElement.prototype, 'src');
                        if (d && d.set) {
                            var o = d.set;
                            Object.defineProperty(HTMLMediaElement.prototype, 'src', {
                                set: function(v) { trigger(v); return o.apply(this, arguments); },
                                configurable: true, enumerable: true
                            });
                        }
                    } catch(e) {}
                    try {
                        var d2 = Object.getOwnPropertyDescriptor(HTMLSourceElement.prototype, 'src');
                        if (d2 && d2.set) {
                            var o2 = d2.set;
                            Object.defineProperty(HTMLSourceElement.prototype, 'src', {
                                set: function(v) { trigger(v); return o2.apply(this, arguments); },
                                configurable: true, enumerable: true
                            });
                        }
                    } catch(e) {}
                }
                try { hook(); } catch(e) {}
                try { window.addEventListener('load', function() { try { hook(); } catch(e) {} }); } catch(e) {}
                setTimeout(function() { try { window.location.href = $target; } catch(e) {} }, 1200);
            })();
        """.trimIndent()
    }

    private suspend fun resolveServer(
        server: Server,
        episodeUrl: String,
        out: MutableList<HikariStream>,
        seen: MutableSet<String>,
    ) {
        val tag = "[${server.name}]"
        // Tier 1: hardened HTTP with episode Referer — getlink may hand back a
        // player page carrying a direct file, or an inner embed URL.
        try {
            val body = HikariNet.getString(server.url, pageHeaders(episodeUrl))
            if (!body.isNullOrBlank()) {
                val direct = m3u8Url.find(body)?.value ?: mp4Url.find(body)?.value
                if (direct != null) {
                    val h = playHeaders(episodeUrl)
                    synchronized(seen) {
                        if (seen.add(direct)) {
                            out.add(
                                HikariStream(
                                    name = "$tag Direct", url = direct, headers = h,
                                    isM3u8 = direct.contains("m3u8"),
                                ),
                            )
                        }
                    }
                    return
                }
                val frame = frameSrc.find(body)?.groupValues?.get(1)
                if (!frame.isNullOrBlank()) {
                    val embed = if (frame.startsWith("//")) "https:$frame" else frame
                    resolveEmbed(embed, server.name, episodeUrl, out, seen)
                    return
                }
            }
        } catch (_: Exception) {
        }
        // Tier 2: episode-context WebView — opens the episode page, then
        // navigates to the server link from inside it so the gate sees the
        // episode Referer + cookies and redirects into the real player.
        try {
            val results = HikariNet.resolveWithWebView(
                episodeUrl, capture, timeoutMs = 60_000, script = navScript(server.url),
            )
            var got = false
            for (r in results) {
                val h = if (r.headers.isNotEmpty()) r.headers else playHeaders(episodeUrl)
                synchronized(seen) {
                    if (seen.add(r.url)) {
                        out.add(
                            HikariStream(
                                name = "$tag ${server.name}", url = r.url, headers = h,
                                isM3u8 = r.url.contains("m3u8", true) || r.url.contains("master.txt", true),
                            ),
                        )
                        got = true
                    }
                }
            }
            if (got) return
        } catch (_: Exception) {
        }
        // Tier 3: external fallback — opens the server link in the browser so
        // the episode is never left with zero playable entries.
        synchronized(seen) {
            if (seen.add("ext:" + server.url)) {
                out.add(HikariStream(name = "$tag External", url = server.url, externalUrl = true))
            }
        }
    }

    private suspend fun resolveEmbed(
        embed: String,
        serverName: String,
        episodeUrl: String,
        out: MutableList<HikariStream>,
        seen: MutableSet<String>,
    ) {
        try {
            val results = HikariNet.resolveWithWebView(
                embed, capture, timeoutMs = 45_000, script = hookScript,
            )
            for (r in results) {
                val h = if (r.headers.isNotEmpty()) r.headers else playHeaders(episodeUrl)
                synchronized(seen) {
                    if (seen.add(r.url)) {
                        out.add(
                            HikariStream(
                                name = "[$serverName] Embed", url = r.url, headers = h,
                                isM3u8 = r.url.contains("m3u8", true) || r.url.contains("master.txt", true),
                            ),
                        )
                    }
                }
            }
        } catch (_: Exception) {
        }
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val (path) = decodeMedia(episode?.id ?: media.id) ?: return emptyList()
        val episodeUrl = absUrl(path)
        val html = fetchHtml(episodeUrl) ?: return emptyList()
        val servers = parseServers(html)
        if (servers.isEmpty()) return emptyList()
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        val sem = Semaphore(3)
        coroutineScope {
            servers.map { server ->
                async(Dispatchers.IO) {
                    sem.acquire()
                    try {
                        resolveServer(server, episodeUrl, out, seen)
                    } finally {
                        sem.release()
                    }
                }
            }.forEach {
                try {
                    it.await()
                } catch (_: Exception) {
                }
            }
        }
        return out
    }
}
