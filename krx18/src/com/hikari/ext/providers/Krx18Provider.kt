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
 * Krx18 (krx18.com) — DooPlay-theme erotic/JAV movie site.
 *
 *  - Archive pages (`/movies/`, `/movies/?orderby=…`, `/movies/page/N/`) and
 *    WordPress search (`/?s=…`) are grids of `<article class="item">` cards
 *    carrying the numeric post id, poster and title.
 *  - The DooPlay REST API serves A–Z browsing (`/wp-json/dooplay/glossary/`)
 *    and, per server, the player embed URL
 *    (`/wp-json/dooplayer/v2/<post>/<type>/<nume>` → `{embed_url}`).
 *  - Each embed is a JS-driven JWPlayer page (play.playkrx18.site,
 *    mov18plus.cloud) whose real source URL is produced by browser JS, so it is
 *    resolved with the WebView m3u8/mp4 capture helper.
 */
class Krx18Provider : HikariProvider {

    override val id = "krx18"
    override val name = "Krx18"
    override val mainUrl = "https://krx18.com"
    override val description = "Erotic/JAV movies, DooPlay-powered, with multi-server JWPlayer embeds."
    override val tvTypes: Set<HikariMediaType> = setOf(HikariMediaType.MOVIE)
    override val version = 1

    companion object {
        private const val BASE = "https://krx18.com"
        private const val CACHE_TTL_MS = 600_000L
        private val htmlCache = HashMap<String, Pair<Long, String>>()
        private val cacheMutex = Mutex()
        private val streamCapture = Regex("""https?://[^"'\s]+?\.(?:m3u8|mp4)(?:[?#][^"'\s]*)?""")
    }

    override fun catalogs(): List<HikariCatalog> = buildList {
        add(HikariCatalog("latest", "Latest Updates", HikariMediaType.MOVIE))
        add(HikariCatalog("popular", "Most Popular", HikariMediaType.MOVIE))
        add(HikariCatalog("top-rated", "Top Rated", HikariMediaType.MOVIE))
        for (c in 'a'..'z') {
            add(HikariCatalog("letter-$c", "A–Z · ${c.uppercase()}", HikariMediaType.MOVIE))
        }
    }

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        return when {
            catalog.id == "latest" -> parseArchive("$BASE/movies/", page)
            catalog.id == "popular" -> parseArchive("$BASE/movies/?orderby=views", page)
            catalog.id == "top-rated" -> parseArchive("$BASE/movies/?orderby=rating", page)
            catalog.id.startsWith("letter-") -> parseGlossary(catalog.id.removePrefix("letter-"))
            else -> emptyList()
        }
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        val encoded = URLEncoder.encode(q, "UTF-8").replace("+", "%20")
        return parseArchive("$BASE/?s=$encoded", page)
    }

    // ------------------------------------------------------------------
    //  Meta + streams
    // ------------------------------------------------------------------

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val pageUrl = media.rawType.takeIf { it.isNotBlank() } ?: return media
        val html = getCached(pageUrl) ?: return media
        val title = metaProperty(html, "og:title")?.let { unescapeEntities(it) }
            ?.takeIf { it.isNotBlank() } ?: media.title
        val overview = metaProperty(html, "og:description")?.let { unescapeEntities(it) }
            ?.takeIf { it.isNotBlank() }
        val poster = metaProperty(html, "og:image")
            ?.takeIf { it.isNotBlank() } ?: media.posterUrl
        val year = Regex("""<div class="data">[\s\S]*?<h3[^>]*>[\s\S]*?</h3>[\s\S]*?<span>(\d{4})</span>""")
            .find(html)?.groupValues?.get(1)?.toIntOrNull() ?: media.year
        val genres = Regex("""nav class="genres"[\s\S]*?</nav>""")
            .find(html)?.value
            ?.let { g ->
                Regex("""href="[^"]*"[\s\S]*?>([^<]+)</a>""")
                    .findAll(g).mapNotNull { m ->
                        unescapeEntities(m.groupValues[1]).trim().takeIf { it.isNotBlank() }
                    }.toList()
            }
            ?: emptyList()
        return media.copy(
            title = title,
            posterUrl = poster,
            overview = overview ?: media.overview,
            year = year,
            genres = genres,
        )
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val pageUrl = media.rawType.takeIf { it.isNotBlank() } ?: return emptyList()
        val html = getCached(pageUrl) ?: return emptyList()
        val postId = Regex("""data-postid='(\d+)'""").find(html)?.groupValues?.get(1) ?: return emptyList()
        // Server options: <li class='dooplay_player_option' data-type='movie' data-post='87592' data-nume='1'>…<span class='server'>playkrx18.site</span>
        val options = Regex(
            """<li[^>]*class=['"][^'"]*dooplay_player_option[^'"]*['"][^>]*data-type=['"]([^'"]+)['"][^>]*data-post=['"](\d+)['"][^>]*data-nume=['"](\d+)['"][\s\S]*?<span class=['"]server['"]>([^<]*)</span>"""
        ).findAll(html).map { m ->
            Triple(m.groupValues[2], m.groupValues[1], m.groupValues[3]) to m.groupValues[4].trim()
        }.toList()
        if (options.isEmpty()) return emptyList()

        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        for ((opt, label) in options) {
            val (post, type, nume) = opt
            if (type != "movie") continue
            val playerJson = getCached("$BASE/wp-json/dooplayer/v2/$post/$type/$nume") ?: continue
            val embed = runCatching { JSONObject(playerJson).optString("embed_url") }.getOrNull()
                ?.takeIf { it.startsWith("http") } ?: continue
            val targets = if (embed.contains("<iframe")) {
                Regex("""src="([^"]+)""").find(embed)?.groupValues?.get(1) ?: continue
            } else embed
            val hits = try {
                HikariNet.resolveWithWebView(targets, streamCapture, timeoutMs = 45_000)
            } catch (t: Throwable) {
                continue
            }
            for (h in hits) {
                val u = h.url
                if (u.isBlank() || !seen.add(u)) continue
                out.add(
                    HikariStream(
                        name = if (label.isBlank()) "Stream" else "Server · $label",
                        url = u,
                        headers = h.headers,
                        isM3u8 = u.contains(".m3u8", ignoreCase = true),
                    )
                )
            }
            if (out.isNotEmpty()) break // first working server is enough
        }
        return out
    }

    // ------------------------------------------------------------------
    //  Listing parsers
    // ------------------------------------------------------------------

    private suspend fun parseArchive(url: String, page: Int): List<HikariMedia> {
        val pageUrl = if (page <= 1) url else insertPage(url, page)
        val html = getCached(pageUrl) ?: return emptyList()
        val out = LinkedHashMap<String, HikariMedia>()
        for (chunk in html.split(Regex("""<article class="item""")).drop(1)) {
            val link = Regex("""<a href="(https://krx18\.com/movies/[^"]+)""").find(chunk)?.groupValues?.get(1) ?: continue
            val img = Regex("""<img\s+src="([^"]+)""").find(chunk)?.groupValues?.get(1) ?: continue
            var title: String? = Regex("""<h3 class="title">([\s\S]*?)</h3>""").find(chunk)?.groupValues?.get(1)
                ?.let { unescapeEntities(it).trim() }
            if (title.isNullOrBlank()) {
                title = Regex("""<h3><a href="[^"]*">([^<]+)</a></h3>""").find(chunk)?.groupValues?.get(1)
                    ?.let { unescapeEntities(it).trim() }
            }
            if (title.isNullOrBlank()) {
                title = Regex("""<img\s+src="[^"]+" alt="([^"]*)""").find(chunk)?.groupValues?.get(1) ?: ""
            }
            if (title.isNullOrBlank()) continue
            val id = Regex("""id="post-(\d+)""").find(chunk)?.groupValues?.get(1)
                ?: link.substringAfterLast("/").trimEnd('/')
            if (id.isBlank()) continue
            out[id] = HikariMedia(
                id = id,
                title = title,
                type = HikariMediaType.MOVIE,
                posterUrl = httpsify(img),
                rawType = link,
            )
        }
        return out.values.toList()
    }

    /** DooPlay A–Z glossary: `{ "<postid>": {title,url,img,year} }`. Needs the
     *  page nonce, fetched fresh from the cached home page (WP nonces rotate). */
    private suspend fun parseGlossary(letter: String): List<HikariMedia> {
        val nonce = Regex("""var dtGonza = \{[\s\S]*?"nonce":"([^"]+)""")
            .find(getCached("$BASE/") ?: return emptyList())?.groupValues?.get(1) ?: return emptyList()
        val json = getCached("$BASE/wp-json/dooplay/glossary/?nonce=$nonce&term=$letter&type=movies")
            ?: return emptyList()
        val obj = runCatching { JSONObject(json) }.getOrNull() ?: return emptyList()
        val out = ArrayList<HikariMedia>()
        val keys = obj.keys()
        while (keys.hasNext()) {
            val id = keys.next()
            val v = obj.optJSONObject(id) ?: continue
            val title = v.optString("title").ifBlank { continue }
            out.add(
                HikariMedia(
                    id = id,
                    title = unescapeEntities(title),
                    type = HikariMediaType.MOVIE,
                    posterUrl = httpsify(v.optString("img")).takeIf { it.startsWith("http") },
                    year = v.optString("year").toIntOrNull(),
                    rawType = v.optString("url"),
                )
            )
        }
        return out.sortedBy { it.title }
    }

    private fun insertPage(url: String, page: Int): String {
        val q = url.indexOf('?')
        val base = if (q >= 0) url.substring(0, q) else url
        val query = if (q >= 0) url.substring(q) else ""
        return "${base.trimEnd('/')}/page/$page/$query"
    }

    private fun httpsify(url: String): String =
        if (url.startsWith("http://")) "https://" + url.substring(7) else url

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
        val html = HikariNet.getString(url) ?: return null
        cacheMutex.withLock {
            if (htmlCache.size > 60) htmlCache.clear()
            htmlCache[url] = now to html
        }
        return html
    }

    private fun metaProperty(html: String, property: String): String? =
        Regex("""<meta\s+property="$property"\s+content="([^"]*)""")
            .find(html)?.groupValues?.get(1)

    private fun unescapeEntities(s: String): String = s
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
        .replace(Regex("""\s+"""), " ")
        .trim()
}
