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
 * AnimeWorldIndia (watchanimeworld.top) — Hindi-dubbed anime WordPress site.
 * Ported from KSHITIJ8473/raghav AnimeWorldIndia to native Hikari.
 *
 * Home/search are card grids; series pages list /episode/ links directly plus
 * season switching via /wp-admin/admin-ajax.php?action=action_select_season.
 * Episode pages embed zephyrix/zephyrflick players (/video/{id} ->
 * /player/index.php?data={id}&do=getVideo); everything resolves via WebView
 * m3u8 capture with direct-file regex as first pass.
 */
class AnimeWorldIndiaProvider : HikariProvider {

    override val id = "animeworldindia"
    override val name = "Anime World India"
    override val mainUrl = "https://watchanimeworld.top"
    override val description = "Hindi-dubbed anime from Anime World India — series, seasons, movies."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val pageHeaders = mapOf(
        "User-Agent" to "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36",
        "Accept" to "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer" to "$mainUrl/",
    )
    private val capture = Regex("""https?://[^"' \s]+\.m3u8[^"' \s]*|https?://[^"' \s]+\.mp4[^"' \s]*""")
    private val anchorRe = Regex("""<a[^>]+href="([^"]+)"[^>]*>([\s\S]*?)</a>""", RegexOption.IGNORE_CASE)
    private val imgRe = Regex("""(?:data-src|data-original|src)="([^"]+)"""", RegexOption.IGNORE_CASE)

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("latest", "Latest Added", HikariMediaType.SERIES),
        HikariCatalog("popular", "Popular", HikariMediaType.SERIES),
        HikariCatalog("movies", "Movies", HikariMediaType.MOVIE),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val p = page.coerceAtLeast(1)
        val url = when (catalog.id) {
            "movies" -> if (p <= 1) "$mainUrl/movies" else "$mainUrl/movies/page/$p"
            "popular" -> if (p <= 1) "$mainUrl/popular" else "$mainUrl/popular/page/$p"
            else -> if (p <= 1) mainUrl else "$mainUrl/page/$p"
        }
        val html = HikariNet.getStringSmart(url, pageHeaders) ?: return emptyList()
        return parseCards(html)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank()) return emptyList()
        val p = page.coerceAtLeast(1)
        val q = URLEncoder.encode(query.trim(), "UTF-8")
        val url = if (p <= 1) "$mainUrl/?s=$q" else "$mainUrl/page/$p/?s=$q"
        val html = HikariNet.getStringSmart(url, pageHeaders) ?: return emptyList()
        return parseCards(html)
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val html = HikariNet.getStringSmart(media.id, pageHeaders) ?: return media
        val title = Regex("""<meta[^>]+property="og:title"[^>]+content="([^"]+)"""").find(html)?.groupValues?.get(1)?.let { unesc(it) } ?: media.title
        val poster = Regex("""<meta[^>]+property="og:image"[^>]+content="([^"]+)"""").find(html)?.groupValues?.get(1)?.takeIf { it.startsWith("http") } ?: media.posterUrl
        val desc = Regex("""<meta[^>]+(?:name|property)="description"[^>]+content="([^"]+)"""").find(html)?.groupValues?.get(1)?.let { unesc(it) }
        return media.copy(title = title, posterUrl = poster, backdropUrl = poster, overview = desc ?: media.overview)
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val html = HikariNet.getStringSmart(media.id, pageHeaders) ?: return null
        val links = LinkedHashSet<String>()
        for (m in Regex("""href="([^"]*?/episode/[^"]*)"""").findAll(html)) {
            val u = abs(m.groupValues[1])
            links.add(u)
        }
        // Season switcher via admin-ajax.
        val postId = Regex("""data-post=["']?(\d+)""").find(html)?.groupValues?.get(1)
            ?: Regex("""post\s*=\s*(\d+)""").find(html)?.groupValues?.get(1)
        if (postId != null) {
            val seasons = Regex("""data-season=["']?(\d+)""").findAll(html).map { it.groupValues[1] }.toSet()
            for (s in seasons.take(12)) {
                try {
                    val ajax = HikariNet.getString("$mainUrl/wp-admin/admin-ajax.php?action=action_select_season&season=$s&post=$postId", pageHeaders) ?: continue
                    for (m in Regex("""href="([^"]*?/episode/[^"]*)"""").findAll(ajax)) links.add(abs(m.groupValues[1]))
                } catch (t: Throwable) { }
            }
        }
        if (links.isEmpty()) return null
        val out = ArrayList<HikariEpisode>()
        for (u in links) {
            val epNum = Regex("""/episode/[^/]*?(\d{1,4})""").find(u)?.groupValues?.get(1)?.toIntOrNull() ?: (out.size + 1)
            out.add(HikariEpisode(number = epNum, id = u, name = cleanName(u), image = media.posterUrl))
        }
        return out.sortedBy { it.number }
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val pageUrl = episode?.id?.takeIf { it.startsWith("http") } ?: media.id
        val html = HikariNet.getStringSmart(pageUrl, pageHeaders) ?: return emptyList()
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        for (m in capture.findAll(html)) {
            if (!seen.add(m.value)) continue
            out.add(HikariStream(name = "AnimeWorldIndia", url = m.value, isM3u8 = m.value.contains("m3u8")))
        }
        if (out.isNotEmpty()) return out
        val iframes = Regex("""<iframe[^>]+src="([^"]+)"""").findAll(html).map { it.groupValues[1] }.filter { it.startsWith("http") }.toList()
        for (src in iframes) {
            val captured = HikariNet.resolveWithWebView(src, capture, timeoutMs = 60_000)
            for (r in captured) {
                if (!seen.add(r.url)) continue
                out.add(HikariStream(name = "AnimeWorldIndia", url = r.url, headers = r.headers, isM3u8 = r.url.contains("m3u8")))
            }
            if (out.isNotEmpty()) return out
        }
        // Last resort: run the episode page itself in the WebView.
        for (r in HikariNet.resolveWithWebView(pageUrl, capture, timeoutMs = 60_000)) {
            if (!seen.add(r.url)) continue
            out.add(HikariStream(name = "AnimeWorldIndia", url = r.url, headers = r.headers, isM3u8 = r.url.contains("m3u8")))
        }
        return out
    }

    private fun parseCards(html: String): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        val seen = HashSet<String>()
        for (m in anchorRe.findAll(html)) {
            val href = m.groupValues[1]
            if (href.contains("/episode/") || href.contains("wp-admin") || href.contains("#")) continue
            if (!href.contains("/") || href == mainUrl || href == "$mainUrl/") continue
            val inner = m.groupValues[2]
            if (!inner.contains("<img")) continue
            val img = imgRe.find(inner)?.groupValues?.get(1) ?: ""
            val title = Regex("""alt="([^"]+)"""").find(inner)?.groupValues?.get(1)?.let { unesc(it) }
                ?.takeIf { it.length > 2 } ?: continue
            val url = abs(href)
            if (!seen.add(url)) continue
            out.add(HikariMedia(id = url, title = title, type = HikariMediaType.SERIES, posterUrl = img.takeIf { it.startsWith("http") }))
            if (out.size >= 60) break
        }
        return out
    }

    private fun cleanName(url: String): String {
        val raw = url.substringAfter("/episode/").trimEnd('/').replace("-", " ")
        return raw.split(" ").filter { it.isNotBlank() && !it.equals("ep", true) && !it.equals("episode", true) }
            .joinToString(" ").replaceFirstChar { it.uppercase() }.ifBlank { "Episode" }
    }

    private fun abs(href: String): String {
        if (href.startsWith("http")) return href
        return if (href.startsWith("/")) "$mainUrl$href" else "$mainUrl/$href"
    }
    private fun unesc(s: String): String = s.replace("&amp;", "&").replace("&#39;", "'").replace("&#039;", "'").replace("&quot;", "\"").replace("&lt;", "<").replace("&gt;", ">").replace("&nbsp;", " ").trim()
}
