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
 * Hanime1 (hanime1.me) — Taiwanese hentai/MMD site with a huge community feed.
 *
 * The homepage is plain HTML built from ~12 titled rows (new releases, recent
 * uploads, genre feeds 裏番/泡麵番/Motion/3DCG/2.5D/2D/AI/MMD/Cosplay, and the
 * popular feed). Every row maps to a `search?sort=`/`search?genre=` URL.
 *
 * Cloudflare caveats (verified):
 *  - the homepage itself is reachable without a challenge, but the search,
 *    genre and API endpoints sit behind a CF managed challenge, so search and
 *    row paging are best-effort (they work from a real phone UA/network, and
 *    silently return nothing when CF blocks),
 *  - the watch page is also CF-challenged, so streams are captured by loading
 *    the page in a real WebView (the player's own HLS request comes back with
 *    URL + headers) instead of parsing it server-side.
 */
class Hanime1Provider : HikariProvider {

    override val id = "hanime1"
    override val name = "Hanime1"
    override val mainUrl = "https://hanime1.me"
    override val description = "Hanime1 feed — 12 home rows of hentai & MMD in the built-in player."
    override val tvTypes = setOf(HikariMediaType.MOVIE)

    private data class RowSpec(val id: String, val label: String)
    private data class Row(val spec: RowSpec?, val title: String, val href: String, val start: Int)

    companion object {
        private const val BASE = "https://hanime1.me"
        private const val CACHE_TTL_MS = 600_000L
        private val htmlCache = HashMap<String, Pair<Long, String>>()
        private val cacheMutex = Mutex()

        private val ROW_SPECS = listOf(
            RowSpec("new-releases", "最新上市"),
            RowSpec("recently-uploaded", "最新上傳"),
            RowSpec("rihan", "裏番"),
            RowSpec("paomian", "泡麵番"),
            RowSpec("motion", "Motion Anime"),
            RowSpec("3dcg", "3DCG"),
            RowSpec("2-5d", "2.5D動畫"),
            RowSpec("2d", "2D動畫"),
            RowSpec("ai", "AI生成"),
            RowSpec("mmd", "MMD"),
            RowSpec("cosplay", "Cosplay"),
            RowSpec("watching", "他們在看"),
        )
    }

    override fun catalogs(): List<HikariCatalog> = ROW_SPECS.map {
        HikariCatalog(it.id, it.label, HikariMediaType.MOVIE)
    }

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val rows = rows()
        val row = rows.find { it.spec?.id == catalog.id } ?: return emptyList()
        if (page <= 1) return cardsIn(rows, row)
        // Row paging goes through the (CF-protected) search endpoint — best effort.
        val url = row.href + "&page=$page"
        return parseCards(HikariNet.getStringSmart(url) ?: return emptyList())
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        val enc = URLEncoder.encode(q, "UTF-8").replace("+", "%20")
        val url = "$BASE/search?query=$enc&sort=最新上傳&page=$page"
        val html = HikariNet.getStringSmart(url) ?: return emptyList()
        return parseCards(html)
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        // Watch pages are CF-challenged; try anyway and fall back to the card data.
        val page = HikariNet.getStringSmart("$BASE/watch?v=${media.id}") ?: return media
        val ogTitle = metaProperty(page, "og:title")?.let { unescape(it) }
        val ogImage = metaProperty(page, "og:image")
        return media.copy(
            title = ogTitle?.takeIf { it.isNotBlank() && !it.startsWith("Hanime1") } ?: media.title,
            backdropUrl = ogImage?.takeIf { it.startsWith("http") } ?: media.backdropUrl,
        )
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val watchUrl = "$BASE/watch?v=${media.id}"

        // The watch page serves every MP4 quality in plain <source> tags
        // (vdownload.hembed.com signed URLs) — no WebView/JS needed. Parsing
        // them directly is far more reliable than hoping the page's player
        // autoplays inside a capture WebView.
        val page = HikariNet.getStringSmart(watchUrl) ?: return emptyList()
        val re = Regex(
            """<source\s+src="(https://vdownload\.hembed\.com/[^"]+\.mp4[^"]*)"\s+type="video/mp4"\s+size="(\d+)""""
        )
        val sources = re.findAll(page).mapNotNull { m ->
            val size = m.groupValues[2].toIntOrNull() ?: return@mapNotNull null
            size to m.groupValues[1]
        }.toList()
        if (sources.isNotEmpty()) {
            val headers = HikariNet.browserHeaders + mapOf("Referer" to "$BASE/")
            return sources.sortedByDescending { it.first }.map { (size, url) ->
                HikariStream(
                    name = "MP4 ${size}p",
                    url = url,
                    headers = headers,
                )
            }
        }

        // Fallback: the player page approach (older video pages may differ).
        val captured = HikariNet.resolveWithWebView(
            watchUrl,
            capture = Regex("""https://[^\s"'\\<>]+\.m3u8(\?[^\s"'\\<>]*)?"""),
            additional = listOf(
                Regex("""https://[^\s"'\\<>]+\.mp4(\?[^\s"'\\<>]*)?"""),
                Regex("""https://[^\s"'\\<>]+/manifest\.mpd(\?[^\s"'\\<>]*)?"""),
            )
        )
        captured.firstOrNull()?.let { hit ->
            val isHls = hit.url.contains(".m3u8")
            val isMpd = hit.url.contains(".mpd")
            return listOf(
                HikariStream(
                    name = if (isHls) "HLS" else if (isMpd) "DASH" else "MP4",
                    url = hit.url,
                    headers = hit.headers + mapOf("Referer" to "$BASE/"),
                    isM3u8 = isHls,
                    isMpd = isMpd,
                )
            )
        }
        return emptyList()
    }

    // ------------------------------------------------------------------
    //  Home row parsing
    // ------------------------------------------------------------------

    private suspend fun rows(): List<Row> {
        val html = getCached("$BASE/") ?: return emptyList()
        val re = Regex("""<a class="horizontal-row-title"[^>]*href="(https://hanime1\.me/search\?[^"]+)"[^>]*>\s*<h3>([^<]+)""")
        val out = mutableListOf<Row>()
        for (m in re.findAll(html)) {
            val href = m.groupValues[1].replace("&amp;", "&")
            val title = unescape(m.groupValues[2]).trim()
            val spec = ROW_SPECS.find { it.label == title }
            out += Row(spec, title, href, m.range.last)
        }
        return out
    }

    /** Cards between this row's heading and the next one's. */
    private suspend fun cardsIn(rows: List<Row>, row: Row): List<HikariMedia> {
        val html = getCached("$BASE/") ?: return emptyList()
        val idx = rows.indexOf(row)
        val end = if (idx + 1 < rows.size) rows[idx + 1].start else html.length
        return parseCards(html.substring(row.start, end.coerceAtLeast(row.start)))
    }

    private fun parseCards(section: String): List<HikariMedia> {
        val re = Regex(
            """<a href="https://hanime1\.me/watch\?v=(\d+)" class="video-link">""" +
                """[\s\S]*?<img class="main-thumb" src="([^"]+)"[\s\S]*?<div class="title">([\s\S]*?)</div>"""
        )
        val out = LinkedHashMap<String, HikariMedia>()
        for (m in re.findAll(section)) {
            val id = m.groupValues[1]
            val title = unescape(m.groupValues[3])
            if (title.isBlank()) continue
            out[id] = HikariMedia(
                id = id,
                title = title,
                type = HikariMediaType.MOVIE,
                posterUrl = m.groupValues[2].takeIf { it.startsWith("http") },
            )
        }
        return out.values.toList()
    }

    private suspend fun getCached(url: String): String? {
        val now = System.currentTimeMillis()
        cacheMutex.withLock {
            htmlCache[url]?.let { (t, html) -> if (now - t < CACHE_TTL_MS) return html }
        }
        val html = HikariNet.getStringSmart(url) ?: return null
        cacheMutex.withLock {
            if (htmlCache.size > 30) htmlCache.clear()
            htmlCache[url] = now to html
        }
        return html
    }

    private fun metaProperty(html: String, prop: String): String? =
        Regex("""<meta\s+property="[^"]*$prop[^"]*"\s+content="([^"]*)"""")
            .find(html)?.groupValues?.get(1)
            ?: Regex("""<meta\s+property='[^']*$prop[^']*'\s+content='([^']*)'""")
                .find(html)?.groupValues?.get(1)

    private fun unescape(s: String): String = s
        .replace("&amp;", "&")
        .replace("&quot;", "\"")
        .replace("&#039;", "'")
        .replace("&#39;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&nbsp;", " ")
        .replace(Regex("""\s+"""), " ")
        .trim()
}
