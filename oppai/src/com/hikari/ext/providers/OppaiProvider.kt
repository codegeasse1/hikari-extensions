package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import com.hikari.ext.HikariSubtitle
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.net.URLEncoder

/**
 * Oppai Stream (oppai.stream) — hentai anime in HD/4K, direct from Blu-ray rips.
 *
 * Everything is plain, unauthenticated server-rendered HTML:
 *  - browse/search: `https://oppai.stream/actions/search.php?text=&order=<o>&page=<p>&limit=30&genres=<g>…`
 *    returns the video-card grid as HTML fragments (orders: recent, uploaded,
 *    views, rating, az, random; genres: blowjob, bigboobs, …),
 *  - a watch page (`/watch?e=<slug>`) embeds a `<source>` with the direct
 *    `https://myspacecat.pictures/<folder>/720/E<NN>.mp4`, a signed subtitle
 *    VTT, and the JS builds DASH manifests at
 *    `https://s2.myspacecat.pictures/<folder>/<res>/E<NN>_dash/E<NN>_dash.mpd`
 *    for 720 / 1080 / 4k.
 *
 * getStreams returns the direct MP4 plus the DASH variants that actually
 * exist (verified with a status probe, exactly like the site's own checkDash).
 */
class OppaiProvider : HikariProvider {

    override val id = "oppai"
    override val name = "Oppai Stream"
    override val mainUrl = "https://oppai.stream"
    override val description = "Hentai anime in HD & 4K — direct Blu-ray rips, many genres."
    override val tvTypes = setOf(HikariMediaType.MOVIE)

    companion object {
        private const val BASE = "https://oppai.stream"
        private const val SEARCH_API = "https://oppai.stream/actions/search.php"
        private const val CDN = "https://myspacecat.pictures"
        private const val CDN_DASH = "https://s2.myspacecat.pictures"
        private const val PAGE_SIZE = 30
        private const val CACHE_TTL_MS = 600_000L
        private val htmlCache = HashMap<String, Pair<Long, String>>()
        private val cacheMutex = Mutex()
    }

    private data class CatalogSpec(val order: String?, val genre: String?)

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("recent", "New Releases", HikariMediaType.MOVIE),
        HikariCatalog("uploaded", "Recently Uploaded", HikariMediaType.MOVIE),
        HikariCatalog("views", "Most Viewed", HikariMediaType.MOVIE),
        HikariCatalog("rating", "Highest Rated", HikariMediaType.MOVIE),
        HikariCatalog("az", "A-Z", HikariMediaType.MOVIE),
        HikariCatalog("random", "Random", HikariMediaType.MOVIE),
        HikariCatalog("blowjob", "Blowjob", HikariMediaType.MOVIE),
        HikariCatalog("bigboobs", "Big Boobs", HikariMediaType.MOVIE),
        HikariCatalog("creampie", "Creampie", HikariMediaType.MOVIE),
        HikariCatalog("ntr", "NTR", HikariMediaType.MOVIE),
        HikariCatalog("milf", "Milf & Mature", HikariMediaType.MOVIE),
        HikariCatalog("ahegao", "Ahegao", HikariMediaType.MOVIE),
        HikariCatalog("schoolgirl", "Schoolgirl", HikariMediaType.MOVIE),
        HikariCatalog("uncensored", "Uncensored", HikariMediaType.MOVIE),
        HikariCatalog("anal", "Anal", HikariMediaType.MOVIE),
        HikariCatalog("cosplay", "Cosplay", HikariMediaType.MOVIE),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val spec = catalogSpec(catalog.id) ?: return emptyList()
        val html = getCached(searchUrl(spec, page)) ?: return emptyList()
        return parseCards(html)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        val url = "$SEARCH_API?text=${enc(q)}&order=recent&page=$page&limit=$PAGE_SIZE" +
            "&genres=&blacklist=&studio=&ibt=0&swa=1"
        val html = getCached(url) ?: return emptyList()
        return parseCards(html)
    }

    // ------------------------------------------------------------------
    //  Meta + streams
    // ------------------------------------------------------------------

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val page = getCached("$BASE/watch?e=${enc(media.id)}") ?: return media
        val ogTitle = metaProperty(page, "og:title")?.let { unescape(it) } ?: media.title
        // "Watch Furachi EP 2 in HD on Oppai.Stream" → "Furachi 2"
        val title = Regex("""Watch (.+?) EP? (\d+) in HD on Oppai\.Stream""")
            .find(ogTitle)
            ?.let { "${it.groupValues[1]} ${it.groupValues[2]}" }
            ?: media.title
        val desc = metaProperty(page, "og:description")?.let { unescape(it) }
        val backdrop = metaProperty(page, "og:image")?.takeIf { it.startsWith("$CDN/") }
        return media.copy(
            title = title,
            overview = desc?.takeIf { it.isNotBlank() && !it.startsWith("Oppai") },
            backdropUrl = backdrop ?: media.backdropUrl,
        )
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val page = getCached("$BASE/watch?e=${enc(media.id)}") ?: return emptyList()

        // Direct MP4 from the <source> tag.
        val src = Regex("""<source src="(https://myspacecat\.pictures/[^"]+\.mp4)""")
            .find(page)?.groupValues?.get(1) ?: return emptyList()
        val parts = Regex("""https://myspacecat\.pictures/([^/]+)/(\d+)/([^/]+)\.mp4""")
            .find(src) ?: return emptyList()
        val folderEnc = encPath(parts.groupValues[1])
        val res = parts.groupValues[2]
        val base = parts.groupValues[3]
        val ua = HikariNet.browserHeaders.getValue("User-Agent")
        val referer = "$BASE/"

        val streams = mutableListOf<HikariStream>()
        streams += HikariStream(
            name = "MP4 $res",
            url = "$CDN/$folderEnc/$res/$base.mp4",
            headers = mapOf("Referer" to referer, "User-Agent" to ua),
        )

        // DASH variants (720/1080/4k) that actually exist.
        for (r in listOf("720", "1080", "4k")) {
            val mpd = "$CDN_DASH/$folderEnc/$r/${base}_dash/${base}_dash.mpd"
            val ok = HikariNet.fetch(mpd)?.let { it.status in 200..299 } == true
            if (ok) {
                streams += HikariStream(
                    name = "DASH $r",
                    url = mpd,
                    headers = mapOf("Referer" to referer, "User-Agent" to ua),
                    isMpd = true,
                )
            }
        }

        // English subtitle VTT.
        val sub = Regex("""<track id='sub-en' label='en' src='([^']+)'""")
            .find(page)?.groupValues?.get(1)
        if (sub != null) {
            val subEnc = sub.replace(parts.groupValues[1], folderEnc)
            streams.firstOrNull()?.let { s ->
                streams[0] = s.copy(subtitles = listOf(HikariSubtitle("en", subEnc)))
            }
        }
        return streams
    }

    // ------------------------------------------------------------------
    //  HTML helpers
    // ------------------------------------------------------------------

    private fun catalogSpec(id: String): CatalogSpec? = when (id) {
        "recent" -> CatalogSpec("recent", null)
        "uploaded" -> CatalogSpec("uploaded", null)
        "views" -> CatalogSpec("views", null)
        "rating" -> CatalogSpec("rating", null)
        "az" -> CatalogSpec("az", null)
        "random" -> CatalogSpec("random", null)
        else -> CatalogSpec("views", id)
    }

    private fun searchUrl(spec: CatalogSpec, page: Int): String =
        "$SEARCH_API?text=&order=${spec.order ?: "recent"}&page=$page&limit=$PAGE_SIZE" +
            "&genres=${spec.genre ?: ""}&blacklist=&studio=&ibt=0&swa=1"

    /** Fetches an HTML page once per CACHE_TTL_MS (home builds many rows). */
    private suspend fun getCached(url: String): String? {
        val now = System.currentTimeMillis()
        cacheMutex.withLock {
            htmlCache[url]?.let { (t, html) -> if (now - t < CACHE_TTL_MS) return html }
        }
        val html = HikariNet.getString(url) ?: return null
        cacheMutex.withLock {
            if (htmlCache.size > 40) htmlCache.clear()
            htmlCache[url] = now to html
        }
        return html
    }

    /** Extracts the video-card grid from a browse/search response. */
    private fun parseCards(html: String): List<HikariMedia> {
        val re = Regex(
            """<div class='in-grid episode-shown'[^>]*id='[^']*'[^>]*folder='([^']*)'[^>]*ep='([^']*)'[^>]*name='([^']*)'""" +
                """[\s\S]*?href='https://oppai\.stream/watch\?e=([^'&]+)&for=[^']*'""" +
                """[\s\S]*?<img class='cover-img-in' src='([^']*)'"""
        )
        val out = LinkedHashMap<String, HikariMedia>()
        for (m in re.findAll(html)) {
            val slug = m.groupValues[4]
            if (slug.isBlank()) continue
            val name = unescape(m.groupValues[3].replace('+', ' '))
            val ep = m.groupValues[2]
            val title = if (ep.isBlank()) name else "$name $ep"
            val thumb = m.groupValues[5]
            out[slug] = HikariMedia(
                id = slug,
                title = title,
                type = HikariMediaType.MOVIE,
                posterUrl = thumb,
            )
        }
        return out.values.toList()
    }

    private fun metaProperty(html: String, prop: String): String? =
        Regex("""<meta\s+property="[^"]*$prop[^"]*"\s+content="([^"]*)"""")
            .find(html)?.groupValues?.get(1)
            ?: Regex("""<meta\s+property='[^']*$prop[^']*'\s+content='([^']*)'""")
                .find(html)?.groupValues?.get(1)

    private fun enc(s: String): String = URLEncoder.encode(s, "UTF-8").replace("+", "%20")

    private fun encPath(p: String): String =
        p.split("/").joinToString("/") { enc(it) }

    private fun unescape(s: String): String = s
        .replace("&quot;", "\"")
        .replace("&#039;", "'")
        .replace("&#39;", "'")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&nbsp;", " ")
        .replace(Regex("""\s+"""), " ")
        .trim()
}
