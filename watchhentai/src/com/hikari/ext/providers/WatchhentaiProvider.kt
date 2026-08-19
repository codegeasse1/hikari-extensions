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
import java.net.URLDecoder

/**
 * WatchHentai (watchhentai.net) — Dooplay (WordPress) hentai site.
 *
 *  - listings: `/series/page/N/` (series), `/videos/page/N/` (episodes),
 *    `/genre/<slug>/page/N/` and `/trending/` — all plain server-rendered
 *    HTML, no Cloudflare.  (The old `/tvshows/` listing slug now serves 404.)
 *  - a series page (`/series/<slug>/`) lists its episodes as
 *    `<li class='mark-N'>` items whose links point to `/videos/<ep-slug>/`.
 *  - a video page (`/videos/<slug>/`) embeds a jwplayer iframe whose
 *    `source` query param is the direct `https://hstorage.xyz/files/J/<folder>/…mp4`;
 *    the jwplayer page additionally serves `_1080p.mp4` / `_720p.mp4` variants,
 *    so streams are built from the base MP4 URL (no WebView needed).
 */
class WatchhentaiProvider : HikariProvider {

    override val id = "watchhentai"
    override val name = "WatchHentai"
    override val mainUrl = "https://watchhentai.net"
    override val description = "1,400+ series & episodes — 720p/1080p direct MP4 streams."
    override val tvTypes = setOf(HikariMediaType.MOVIE, HikariMediaType.SERIES)

    companion object {
        private const val BASE = "https://watchhentai.net"
        private const val CACHE_TTL_MS = 600_000L
        private val htmlCache = HashMap<String, Pair<Long, String>>()
        private val cacheMutex = Mutex()
    }

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("series", "Latest Series", HikariMediaType.SERIES),
        HikariCatalog("episodes", "Latest Episodes", HikariMediaType.MOVIE),
        HikariCatalog("trending", "Trending", HikariMediaType.MOVIE),
        HikariCatalog("uncensored", "Uncensored", HikariMediaType.SERIES),
        HikariCatalog("censored", "Censored", HikariMediaType.SERIES),
        HikariCatalog("ahegao", "Ahegao", HikariMediaType.SERIES),
        HikariCatalog("anal", "Anal", HikariMediaType.SERIES),
        HikariCatalog("big-boobs", "Big Boobs", HikariMediaType.SERIES),
        HikariCatalog("blowjob", "Blowjob", HikariMediaType.SERIES),
        HikariCatalog("bondage", "Bondage", HikariMediaType.SERIES),
        HikariCatalog("creampie", "Creampie", HikariMediaType.SERIES),
        HikariCatalog("milf", "Milf", HikariMediaType.SERIES),
        HikariCatalog("ntr", "NTR", HikariMediaType.SERIES),
        HikariCatalog("schoolgirl", "Schoolgirl", HikariMediaType.SERIES),
        HikariCatalog("dubbed", "Dubbed", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val url = when (catalog.id) {
            "series" -> pageUrl("$BASE/series/", page)
            // /videos/ is the episodes listing (the /episodes/ slug has been
            // intermittently serving 404 while /videos/ is the same page).
            "episodes" -> pageUrl("$BASE/videos/", page)
            "trending" -> if (page <= 1) "$BASE/trending/" else null
            else -> pageUrl("$BASE/genre/${catalog.id}/", page)
        } ?: return emptyList()
        val html = getCached(url) ?: return emptyList()
        return if (catalog.id == "episodes") parseEpisodeCards(html)
        else parseSeriesCards(html)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        val enc = java.net.URLEncoder.encode(q, "UTF-8").replace("+", "%20")
        val html = getCached("$BASE/?s=$enc&post_type=episodes") ?: return emptyList()
        return parseEpisodeCards(html) + parseSeriesCards(html)
    }

    // ------------------------------------------------------------------
    //  Episodes
    // ------------------------------------------------------------------

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        if (media.type != HikariMediaType.SERIES) return null
        val html = getCached("$BASE/series/${media.id}/") ?: return null
        val title = media.title.lowercase().trim()
        val out = mutableListOf<HikariEpisode>()
        for (e in parseEpisodes(html)) {
            // Series-page <li> items carry no serie name, so accept them all;
            // listing-card items only when their serie matches (or is blank).
            val serie = e.serie.lowercase().trim()
            if (title.isBlank() || e.serie.isBlank() || serie == title) {
                out += HikariEpisode(
                    number = e.number,
                    id = e.slug,
                    name = "Episode ${e.number}",
                    image = e.poster,
                )
            }
        }
        return out.ifEmpty { null }
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        var videoSlug = episode?.id ?: media.id
        if (media.type == HikariMediaType.SERIES) {
            // No episode chosen — grab the first one from the series page.
            val seriesPage = getCached("$BASE/series/${media.id}/") ?: return emptyList()
            videoSlug = parseEpisodes(seriesPage).firstOrNull()?.slug ?: return emptyList()
        }
        val page = getCached("$BASE/videos/$videoSlug/") ?: return emptyList()
        val iframe = Regex("""data-litespeed-src='([^']*jwplayer[^']*)'""")
            .find(page)?.groupValues?.get(1) ?: return emptyList()
        val src = Regex("""source=([^&"']+)""").find(iframe)?.groupValues?.get(1) ?: return emptyList()
        val baseUrl = runCatching { URLDecoder.decode(src, "UTF-8") }.getOrDefault(src)
        if (!baseUrl.startsWith("http")) return emptyList()
        val stem = baseUrl.removeSuffix(".mp4")
        val headers = mapOf(
            "Referer" to "$BASE/",
            "User-Agent" to HikariNet.browserHeaders.getValue("User-Agent"),
        )
        return listOf(
            HikariStream(name = "1080p", url = "$stem" + "_1080p.mp4", headers = headers),
            HikariStream(name = "720p", url = "$stem" + "_720p.mp4", headers = headers),
            HikariStream(name = "Default", url = baseUrl, headers = headers),
        )
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val url = if (media.type == HikariMediaType.SERIES) "$BASE/series/${media.id}/"
        else "$BASE/videos/${media.id}/"
        val page = getCached(url) ?: return media
        val backdrop = metaProperty(page, "og:image")?.takeIf { it.startsWith("$BASE/") }
        return media.copy(backdropUrl = backdrop ?: media.backdropUrl)
    }

    // ------------------------------------------------------------------
    //  Parsing
    // ------------------------------------------------------------------

    // Handles every card variant: genre/trending cards
    // (`<article id="post-N" class="item tvshows">` with a poster + title attr),
    // and the homepage slider (`<article class="item" id="post-N">` with a
    // backdrop + `<h3 class="title">`).  Skips `sidebar.php` thumbs so
    // sidebar duplicates can't shadow the real grid entries.
    private fun parseSeriesCards(html: String): List<HikariMedia> {
        val re = Regex(
            """data-src=["']((?![^"']*sidebar\.php)[^"']+)["'][\s\S]{0,3000}?<a href="https://watchhentai\.net/series/([^"]+)/"[^>]*?title="([^"]*)"[\s\S]{0,600}?(?:<h3[^>]*>([\s\S]*?)</h3>)?"""
        )
        val out = LinkedHashMap<String, HikariMedia>()
        for (m in re.findAll(html)) {
            val slug = m.groupValues[2]
            if (slug.isBlank()) continue
            val h3 = m.groupValues[4]
            val title = unescape(
                if (h3.isNotBlank() && !h3.contains("<")) h3 else m.groupValues[3]
            )
            if (title.isBlank()) continue
            out[slug] = HikariMedia(
                id = slug,
                title = title,
                type = HikariMediaType.SERIES,
                posterUrl = m.groupValues[1].takeIf { it.startsWith("http") },
            )
        }
        return out.values.toList()
    }

    private fun parseEpisodeCards(html: String): List<HikariMedia> =
        parseEpisodes(html).map {
            HikariMedia(
                id = it.slug,
                title = "${it.serie} - Episode ${it.number}",
                type = HikariMediaType.MOVIE,
                posterUrl = it.poster,
            )
        }

    private data class Ep(
        val slug: String,
        val serie: String,
        val number: Int,
        val poster: String?,
    )

    private fun parseEpisodes(html: String): List<Ep> {
        val out = LinkedHashMap<String, Ep>()

        // Pattern A — episode listing cards on `/videos/`:
        // <article class="item se episodes">…data-src="IMG"…<a href="…/videos/SLUG/">…<span class="serie">S</span>…<h3>Episode N</h3>
        val reA = Regex(
            """<article class="item se episodes"[^>]*>[\s\S]*?data-src="([^"]+)"[\s\S]*?<a href="https://watchhentai\.net/videos/([^"]+)/"[\s\S]*?<span class="serie">([\s\S]*?)</span>[\s\S]*?<h3>Episode\s*(\d+)</h3>"""
        )
        for (m in reA.findAll(html)) {
            // group 1 = the poster's data-src (image URL), group 2 = the video
            // page slug — they were swapped before, so every episode's id was
            // the IMAGE URL and the video page fetch 404'd ("no playable
            // source").
            val slug = m.groupValues[2]
            if (out.containsKey(slug)) continue
            out[slug] = Ep(
                slug = slug,
                serie = unescape(m.groupValues[3]),
                number = m.groupValues[4].toIntOrNull() ?: 1,
                poster = m.groupValues[1].takeIf { it.startsWith("http") },
            )
        }

        // Pattern B — episode list on a series page:
        // <li class='mark-N'><div class='imagen'><img … data-src='POSTER' …></div><div class='episodiotitle'><a href='https://watchhentai.net/videos/SLUG/' title='Episode N (PREVIEW) Watch Hentai'>
        val reB = Regex(
            """<li class='mark-\d+'[^>]*>[\s\S]*?data-src='([^']+)'[\s\S]*?<a href='https://watchhentai\.net/videos/([^']+)/'[^>]*title='Episode\s*(\d+)"""
        )
        for (m in reB.findAll(html)) {
            val slug = m.groupValues[2]
            if (out.containsKey(slug)) continue
            out[slug] = Ep(
                slug = slug,
                serie = "",
                number = m.groupValues[3].toIntOrNull() ?: 1,
                poster = m.groupValues[1].takeIf { it.startsWith("http") },
            )
        }

        return out.values.toList()
    }

    private fun pageUrl(base: String, page: Int): String =
        if (page <= 1) base else "${base}page/$page/"

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
            ?: Regex("""<meta\s+property='[^']*$prop[^']*'\s+content='([^']*)'""")
                .find(html)?.groupValues?.get(1)

    private fun unescape(s: String): String = s
        .replace("&#8211;", "-")
        .replace("&#8217;", "'")
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
