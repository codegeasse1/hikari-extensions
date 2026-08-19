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

/**
 * HanimeTV (hanime.tv) — curated 720p/1080p hentai, Astro SSR front-end.
 *
 *  - the homepage server-renders four video rows (Recent Uploads, New
 *    Releases, Trending, Random) with full card grids — those are the
 *    catalogs,
 *  - `/browse/` and search are client-side (the guest API behind them is
 *    Cloudflare-blocked), so those aren't scrapable server-side — search is
 *    intentionally not wired up,
 *  - the video page's player (HTVPlayer) is client-side too, so streams are
 *    captured by loading the page in a real WebView and catching the HLS /
 *    DASH / MP4 request the player makes.
 */
class HanimetvProvider : HikariProvider {

    override val id = "hanimetv"
    override val name = "HanimeTV"
    override val mainUrl = "https://hanime.tv"
    override val description = "Curated 720p/1080p hentai — new releases, trending and random."
    override val tvTypes = setOf(HikariMediaType.MOVIE)

    private data class RowSpec(val id: String, val label: String)
    private data class Card(val media: HikariMedia, val start: Int)

    companion object {
        private const val BASE = "https://hanime.tv"
        private const val CACHE_TTL_MS = 600_000L
        private val htmlCache = HashMap<String, Pair<Long, String>>()
        private val cacheMutex = Mutex()

        private val ROW_SPECS = listOf(
            RowSpec("recent-uploads", "Recent Uploads"),
            RowSpec("new-releases", "New Releases"),
            RowSpec("trending", "Trending"),
            RowSpec("random", "Random"),
        )
    }

    override fun catalogs(): List<HikariCatalog> = ROW_SPECS.map {
        HikariCatalog(it.id, it.label, HikariMediaType.MOVIE)
    }

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        if (page > 1) return emptyList()
        val spec = ROW_SPECS.find { it.id == catalog.id } ?: return emptyList()
        val cards = cards() ?: return emptyList()
        val titles = titles()
        return cards.filter { c ->
            val title = titles.lastOrNull { it.first <= c.start }?.second ?: ""
            title == spec.label
        }.map { it.media }
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> = emptyList()

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val page = getCached("$BASE/videos/hentai/${media.id}") ?: return media
        val backdrop = metaProperty(page, "og:image")?.takeIf { it.startsWith("https://hanime-cdn.com/") }
        val duration = Regex("\"duration\":\"PT(\\d+)M(\\d+)S\"").find(page)
        val runtime = duration?.let { m ->
            val mins = m.groupValues[1].toIntOrNull() ?: 0
            val secs = m.groupValues[2].toIntOrNull() ?: 0
            "Runtime: ${"%d:%02d".format(mins, secs)}"
        }
        val overview = listOfNotNull(runtime).joinToString(" · ")
        return media.copy(
            backdropUrl = backdrop ?: media.backdropUrl,
            overview = overview.ifBlank { media.overview },
        )
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val watchUrl = "$BASE/videos/hentai/${media.id}"
        val captured = HikariNet.resolveWithWebView(
            watchUrl,
            capture = Regex("""https://[^\s"'\\<>]+\.m3u8(\?[^\s"'\\<>]*)?"""),
            additional = listOf(
                Regex("""https://[^\s"'\\<>]+/manifest\.mpd(\?[^\s"'\\<>]*)?"""),
                Regex("""https://[^\s"'\\<>]+\.mp4(\?[^\s"'\\<>]*)?"""),
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
    //  Home parsing
    // ------------------------------------------------------------------

    /** Row heading titles with their byte offsets (document order). */
    private suspend fun titles(): List<Pair<Int, String>> {
        val html = getCached("$BASE/") ?: return emptyList()
        val re = Regex("""(?:[^A-Za-z0-9_])title&quot;:\[0,&quot;([^&]+)&quot;\]""")
        return re.findAll(html).mapNotNull { m ->
            val title = unescape(m.groupValues[1]).trim()
            if (title.isBlank()) null else m.range.first to title
        }.toList()
    }

    private suspend fun cards(): List<Card>? {
        val html = getCached("$BASE/") ?: return null
        val re = Regex("""<a href="/videos/hentai/([^"]+)" title="[^"]*" class="relative block overflow-hidden[^"]*"[^>]*>""")
        val out = mutableListOf<Card>()
        for (m in re.findAll(html)) {
            val slug = m.groupValues[1]
            if (slug.isBlank()) continue
            val start = m.range.last
            val endIdx = html.indexOf("</a>", start)
            val block = html.substring(start, if (endIdx < 0) html.length else endIdx)
            val poster = Regex("""src="(https://hanime-cdn\.com/[^"]+)""")
                .find(block)?.groupValues?.get(1)
            val title = Regex("""<h3[^>]*>([\s\S]*?)</h3>""")
                .find(block)?.groupValues?.get(1)?.let { unescape(it) } ?: slug
            out += Card(
                HikariMedia(
                    id = slug,
                    title = title,
                    type = HikariMediaType.MOVIE,
                    posterUrl = poster,
                ),
                start,
            )
        }
        return out
    }

    private suspend fun getCached(url: String): String? {
        val now = System.currentTimeMillis()
        cacheMutex.withLock {
            htmlCache[url]?.let { (t, html) -> if (now - t < CACHE_TTL_MS) return html }
        }
        val html = HikariNet.getString(url) ?: return null
        cacheMutex.withLock {
            if (htmlCache.size > 30) htmlCache.clear()
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
        .replace(Regex("""\s+"""), " ")
        .trim()
}
