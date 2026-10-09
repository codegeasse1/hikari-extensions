package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import java.net.URLEncoder

/**
 * TheMoviesFlix (themoviesflixhq.com) — movies & series download posts.
 * Ported from KSHITIJ8473/raghav TheMoviesFlix to native Hikari.
 *
 * Cards: article.latestpost grids (quality tags 4K/1080p/720p in titles).
 * Detail pages aggregate drive/redirect groups per season; every quality
 * group lists the same episodes, so the union of drive pages is the episode
 * count. Watch/download links resolve via WebView capture.
 */
class TheMoviesFlixProvider : HikariProvider {

    override val id = "themoviesflix"
    override val name = "TheMoviesFlix"
    override val mainUrl = "https://themoviesflixhq.com"
    override val description = "Movies & series from TheMoviesFlix — 480p to 4K direct links."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val pageHeaders = mapOf(
        "User-Agent" to "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36",
        "Accept" to "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer" to "$mainUrl/",
    )
    private val capture = Regex("""https?://[^"' \s]+\.m3u8[^"' \s]*|https?://[^"' \s]+\.mp4[^"' \s]*|https?://[^"' \s]+\.mkv[^"' \s]*""")
    private val anchorRe = Regex("""<a[^>]+href="([^"]+)"[^>]*>([\s\S]*?)</a>""", RegexOption.IGNORE_CASE)
    private val imgRe = Regex("""(?:data-src|data-original|src)="([^"]+)"""", RegexOption.IGNORE_CASE)

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("latest", "Latest Added", HikariMediaType.MOVIE),
        HikariCatalog("movies", "Movies", HikariMediaType.MOVIE),
        HikariCatalog("series", "Web Series", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val p = page.coerceAtLeast(1)
        val base = when (catalog.id) {
            "movies" -> "$mainUrl/movies"
            "series" -> "$mainUrl/web-series"
            else -> mainUrl
        }
        val html = HikariNet.getStringSmart(if (p <= 1) base else "$base/page/$p", pageHeaders)
            ?: HikariNet.getStringSmart(if (p <= 1) mainUrl else "$mainUrl/page/$p", pageHeaders)
            ?: return emptyList()
        return parseCards(html)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank()) return emptyList()
        val html = HikariNet.getStringSmart("$mainUrl/?s=${URLEncoder.encode(query.trim(), "UTF-8")}", pageHeaders)
            ?: return emptyList()
        return parseCards(html)
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val html = HikariNet.getStringSmart(media.id, pageHeaders) ?: return media
        val title = Regex("""<meta[^>]+property="og:title"[^>]+content="([^"]+)"""").find(html)?.groupValues?.get(1)?.let { unesc(it) } ?: media.title
        val poster = Regex("""<meta[^>]+property="og:image"[^>]+content="([^"]+)"""").find(html)?.groupValues?.get(1)?.takeIf { it.startsWith("http") } ?: media.posterUrl
        return media.copy(title = title, posterUrl = poster, backdropUrl = poster)
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val html = HikariNet.getStringSmart(media.id, pageHeaders) ?: return null
        val isSeries = html.contains("Episode", true) || html.contains("Season", true) || Regex("""S\d+\s*E\d+""").containsMatchIn(html)
        if (!isSeries) {
            return listOf(HikariEpisode(number = 1, id = media.id, name = media.title, image = media.posterUrl))
        }
        // Collect per-season drive/group links, union episode numbers.
        val seasons = Regex("""Season\s*(\d+)""", RegexOption.IGNORE_CASE).findAll(html).map { it.groupValues[1].toIntOrNull() ?: 1 }.toSet().ifEmpty { setOf(1) }
        val out = ArrayList<HikariEpisode>()
        for (sn in seasons.take(20)) {
            val nums = LinkedHashSet<Int>()
            for (m in Regex("""[Ee]pisode\s*(\d{1,3})""").findAll(html)) nums.add(m.groupValues[1].toIntOrNull() ?: continue)
            for (m in Regex("""S0*${sn}\s*E(\d{1,3})""", RegexOption.IGNORE_CASE).findAll(html)) nums.add(m.groupValues[1].toIntOrNull() ?: continue)
            for (m in anchorRe.findAll(html)) {
                val href = m.groupValues[1]
                if (!href.startsWith("http")) continue
                if (href.contains("nexdrive") || href.contains("vegadrive") || href.contains("vcloud") || href.contains("filebee") || href.contains("gdrive") || href.contains("drive")) {
                    val n = Regex("""(\d{1,3})""").find(unesc(strip(m.groupValues[2])))?.groupValues?.get(1)?.toIntOrNull()
                    if (n != null && n < 500) nums.add(n)
                }
            }
            if (nums.isEmpty()) {
                out.add(HikariEpisode(number = 1, season = sn, id = "${media.id}|$sn|1", name = "Season $sn Episode 1", image = media.posterUrl))
            } else {
                for (n in nums.sorted()) out.add(HikariEpisode(number = n, season = sn, id = "${media.id}|$sn|$n", name = "S${sn} E$n", image = media.posterUrl))
            }
        }
        return out.ifEmpty { null }
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val ep = episode
        val detailUrl = media.id
        val html = HikariNet.getStringSmart(detailUrl, pageHeaders) ?: return emptyList()
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        // Drive/redirect links on the detail page.
        val driveLinks = anchorRe.findAll(html).map { it.groupValues[1] }.filter {
            it.startsWith("http") && (it.contains("nexdrive") || it.contains("vegadrive") || it.contains("vcloud") || it.contains("filebee") || it.contains("drive") || it.contains("watch") || it.contains("download"))
        }.distinct().take(8).toList()
        for (d in driveLinks) {
            try {
                val dh = HikariNet.getStringSmart(d, pageHeaders) ?: continue
                for (m in capture.findAll(dh)) {
                    if (!seen.add(m.value)) continue
                    out.add(HikariStream(name = labelFor(m.value, ep), url = m.value, isM3u8 = m.value.contains("m3u8")))
                }
                if (out.isNotEmpty()) return out
                for (m in Regex("""<iframe[^>]+src="([^"]+)"""").findAll(dh)) {
                    val src = m.groupValues[1]
                    if (!src.startsWith("http")) continue
                    for (r in HikariNet.resolveWithWebView(src, capture, timeoutMs = 45_000)) {
                        if (!seen.add(r.url)) continue
                        out.add(HikariStream(name = labelFor(r.url, ep), url = r.url, headers = r.headers, isM3u8 = r.url.contains("m3u8")))
                    }
                    if (out.isNotEmpty()) return out
                }
            } catch (t: Throwable) { }
        }
        for (r in HikariNet.resolveWithWebView(detailUrl, capture, timeoutMs = 45_000)) {
            if (!seen.add(r.url)) continue
            out.add(HikariStream(name = labelFor(r.url, ep), url = r.url, headers = r.headers, isM3u8 = r.url.contains("m3u8")))
        }
        return out
    }

    private fun parseCards(html: String): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        val seen = HashSet<String>()
        // Prefer the site's own post cards.
        val cardHtml = Regex("""<article[^>]*latestpost[\s\S]*?</article>""", RegexOption.IGNORE_CASE).findAll(html).joinToString("\n") { it.value }.ifBlank { html }
        for (m in anchorRe.findAll(cardHtml)) {
            val href = m.groupValues[1]
            if (href.contains("?s=") || href.startsWith("#")) continue
            val inner = m.groupValues[2]
            if (!inner.contains("<img")) continue
            val img = imgRe.find(inner)?.groupValues?.get(1) ?: ""
            val title = Regex("""alt="([^"]+)"""").find(inner)?.groupValues?.get(1)?.let { unesc(it) }?.takeIf { it.length > 3 } ?: continue
            val url = abs(href)
            if (!seen.add(url)) continue
            val isSeries = title.contains("series", true) || title.contains("season", true) || title.contains("episode", true)
            out.add(HikariMedia(id = url, title = title, type = if (isSeries) HikariMediaType.SERIES else HikariMediaType.MOVIE, posterUrl = img.takeIf { it.startsWith("http") }))
            if (out.size >= 60) break
        }
        return out
    }

    private fun labelFor(u: String, ep: HikariEpisode?): String {
        val q = listOf("2160p", "1080p", "720p", "480p").firstOrNull { u.contains(it, true) }?.let { " $it" }.orEmpty()
        return if (ep != null && ep.number > 1) "TMF S${ep.season} E${ep.number}$q" else "TMF$q"
    }

    private fun abs(href: String): String {
        if (href.startsWith("http")) return href
        return if (href.startsWith("/")) "$mainUrl$href" else "$mainUrl/$href"
    }
    private fun unesc(s: String): String = s.replace("&amp;", "&").replace("&#39;", "'").replace("&#039;", "'").replace("&quot;", "\"").replace("&lt;", "<").replace("&gt;", ">").replace("&nbsp;", " ").trim()
    private fun strip(s: String): String = s.replace(Regex("<[^>]+>"), " ").replace(Regex("\\s+"), " ").trim()
}
