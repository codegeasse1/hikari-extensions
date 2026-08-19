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
 * HStream (hstream.moe) — English-subbed hentai in HD/FHD/4K (Livewire app).
 *
 *  - `/search?order=<order>&page=N` server-renders the full video grid
 *    (orders: recently-uploaded, recently-released, view-count, az, za,
 *    oldest-uploads, oldest-releases) — those are the catalogs,
 *  - `/search?s=<query>` filters server-side (Livewire) — search works,
 *  - the watch page is SSR too, but the stream URL is only built client-side
 *    by the player (`<cdn>/<stream_url>/720/manifest.mpd`, or `x264.720p.mp4`
 *    on legacy servers), so getStreams loads the page in a real WebView and
 *    captures the DASH/HLS/MP4 request the player makes.
 */
class HstreamProvider : HikariProvider {

    override val id = "hstream"
    override val name = "HStream"
    override val mainUrl = "https://hstream.moe"
    override val description = "English subbed hentai in HD, FHD & 4K."
    override val tvTypes = setOf(HikariMediaType.MOVIE)

    companion object {
        private const val BASE = "https://hstream.moe"
        private const val CACHE_TTL_MS = 600_000L
        private val htmlCache = HashMap<String, Pair<Long, String>>()
        private val cacheMutex = Mutex()
    }

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("recently-uploaded", "Recently Uploaded", HikariMediaType.MOVIE),
        HikariCatalog("recently-released", "Recently Released", HikariMediaType.MOVIE),
        HikariCatalog("view-count", "Most Watched", HikariMediaType.MOVIE),
        HikariCatalog("az", "A-Z", HikariMediaType.MOVIE),
        HikariCatalog("za", "Z-A", HikariMediaType.MOVIE),
        HikariCatalog("oldest-uploads", "Oldest Uploads", HikariMediaType.MOVIE),
        HikariCatalog("oldest-releases", "Oldest Releases", HikariMediaType.MOVIE),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val html = getCached("$BASE/search?order=${catalog.id}&page=$page") ?: return emptyList()
        return parseCards(html)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        val enc = URLEncoder.encode(q, "UTF-8").replace("+", "%20")
        val html = getCached("$BASE/search?s=$enc&order=recently-uploaded&page=$page") ?: return emptyList()
        return parseCards(html)
    }

    // ------------------------------------------------------------------
    //  Meta + streams
    // ------------------------------------------------------------------

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val page = getCached("$BASE/hentai/${media.id}") ?: return media
        val ogTitle = metaProperty(page, "og:title")?.let { unescape(it) }
        val title = ogTitle?.let { t ->
            val idx = t.indexOf(" - ")
            if (idx > 0) t.substring(0, idx) else t
        } ?: media.title
        val backdrop = metaProperty(page, "og:image")?.let {
            if (it.startsWith("/")) "$BASE$it" else it
        }
        val genres = Regex(""""genre":\[([^\]]*)\]""").find(page)
            ?.groupValues?.get(1)
            ?.split(",")
            ?.mapNotNull { unescape(it).trim().trim('"').ifBlank { null } }
            ?: emptyList()
        val views = Regex(""""userInteractionCount":(\d+)""").find(page)?.groupValues?.get(1)
        val uploadDate = Regex("\"uploadDate\":\"(\\d{4})\"").find(page)?.groupValues?.get(1)
        val overview = buildString {
            if (genres.isNotEmpty()) append(genres.joinToString(", "))
            val bits = mutableListOf<String>()
            views?.let { bits += "$it views" }
            uploadDate?.let { bits += it }
            if (bits.isNotEmpty()) {
                if (isNotEmpty()) append(" · ")
                append(bits.joinToString(" · "))
            }
        }.trim()
        return media.copy(
            title = title,
            backdropUrl = backdrop ?: media.backdropUrl,
            genres = genres.ifEmpty { media.genres },
            overview = overview.ifBlank { media.overview },
        )
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val watchUrl = "$BASE/hentai/${media.id}"
        val captured = HikariNet.resolveWithWebView(
            watchUrl,
            capture = Regex("""https://[^\s"'\\<>]+/manifest\.mpd(\?[^\s"'\\<>]*)?"""),
            additional = listOf(
                Regex("""https://[^\s"'\\<>]+\.m3u8(\?[^\s"'\\<>]*)?"""),
                Regex("""https://[^\s"'\\<>]+\.mp4(\?[^\s"'\\<>]*)?"""),
            )
        )
        val ordered = captured.sortedBy {
            when {
                it.url.contains(".mpd") -> 0
                it.url.contains(".m3u8") -> 1
                else -> 2
            }
        }
        ordered.firstOrNull()?.let { hit ->
            val isMpd = hit.url.contains(".mpd")
            val isHls = hit.url.contains(".m3u8")
            return listOf(
                HikariStream(
                    name = if (isMpd) "DASH" else if (isHls) "HLS" else "MP4",
                    url = hit.url,
                    headers = hit.headers + mapOf("Referer" to "$BASE/"),
                    isMpd = isMpd,
                    isM3u8 = isHls,
                )
            )
        }
        return emptyList()
    }

    // ------------------------------------------------------------------
    //  HTML helpers
    // ------------------------------------------------------------------

    private fun parseCards(html: String): List<HikariMedia> {
        val re = Regex(
            """href="https://hstream\.moe/hentai/([^"]+)"[\s\S]*?<img\s+src="(/images/hentai/[^"]+)"[\s\S]*?<h3[^>]*>([\s\S]*?)</h3>"""
        )
        val out = LinkedHashMap<String, HikariMedia>()
        for (m in re.findAll(html)) {
            val slug = m.groupValues[1]
            val title = unescape(m.groupValues[3]).replace(Regex("""\s+"""), " ").trim()
            if (slug.isBlank() || title.isBlank()) continue
            out[slug] = HikariMedia(
                id = slug,
                title = title,
                type = HikariMediaType.MOVIE,
                posterUrl = "$BASE" + m.groupValues[2],
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
            if (htmlCache.size > 40) htmlCache.clear()
            htmlCache[url] = now to html
        }
        return html
    }

    private fun metaProperty(html: String, prop: String): String? =
        Regex("""<meta\s+property="[^"]*$prop[^"]*"\s+content="([^"]*)"""")
            .find(html)?.groupValues?.get(1)

    private fun unescape(s: String): String = s
        .replace("&amp;", "&")
        .replace("&quot;", "\"")
        .replace("&#039;", "'")
        .replace("&#39;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&nbsp;", " ")
        .trim()
}
