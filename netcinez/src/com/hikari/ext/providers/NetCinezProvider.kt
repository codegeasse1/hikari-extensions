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
 * NetCinez (netcinebc.lat mirror of netcinez.si) — Brazilian Portuguese films
 * and series, wpmovies4 theme, verified live Oct 2026:
 *  - Menu rows: /tvshows/ (latest series) + /tvshows/category/<acao|animacao|
 *    aventura|comedia|crime|...> and /category/<ultimos-filmes|acao|animacao|...>
 *    for films; search is standard WP ?s=.
 *  - Cards: .items .movie blocks (.imagen img + h2 title + <a href> detail link).
 *  - Detail pages: .player-menu li tabs + .player-content iframes (multi-server),
 *    plus series season/episode blocks.
 */
class NetCinezProvider : HikariProvider {

    override val id = "netcinez"
    override val name = "NetCinez"
    override val mainUrl = "https://netcinebc.lat"
    override val description = "Filmes e series online gratis — ultimos lancamentos, acao, animacao, comedia."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val UA =
        "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36"

    companion object {
        val BASES = listOf("https://netcinebc.lat", "https://netcinez.si")
        var base = BASES[0]
    }

    private fun h(referer: String = "$base/"): Map<String, String> =
        mapOf("User-Agent" to UA, "Referer" to referer, "Accept-Language" to "pt-BR,pt;q=0.9")

    private suspend fun get(path: String): String? {
        for (b in BASES) {
            val t = HikariNet.getStringSmart(if (path.startsWith("http")) path else b + path, h("$b/"))
            if (t != null && t.length > 5000) {
                base = b
                return t
            }
        }
        return null
    }

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("films-latest", "Ultimos Filmes", HikariMediaType.MOVIE),
        HikariCatalog("films-action", "Filmes Acao", HikariMediaType.MOVIE),
        HikariCatalog("films-anim", "Filmes Animacao", HikariMediaType.MOVIE),
        HikariCatalog("films-comedy", "Filmes Comedia", HikariMediaType.MOVIE),
        HikariCatalog("tv-latest", "Series Atualizadas", HikariMediaType.SERIES),
        HikariCatalog("tv-action", "Series Acao", HikariMediaType.SERIES),
        HikariCatalog("tv-anim", "Series Animacao", HikariMediaType.SERIES),
        HikariCatalog("tv-comedy", "Series Comedia", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val p = page.coerceAtLeast(1)
        val section = when (catalog.id) {
            "films-latest" -> "/category/ultimos-filmes/"
            "films-action" -> "/category/acao/"
            "films-anim" -> "/category/animacao/"
            "films-comedy" -> "/category/comedia/"
            "tv-latest" -> "/tvshows/"
            "tv-action" -> "/tvshows/category/acao/"
            "tv-anim" -> "/tvshows/category/animacao/"
            else -> "/tvshows/category/comedia/"
        }
        val path = if (p > 1) section.trimEnd('/') + "/page/$p/" else section
        val html = get(path) ?: return emptyList()
        return parseMovieCards(html)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank() || page > 1) return emptyList()
        val html = get("/?s=${URLEncoder.encode(q, "UTF-8")}") ?: return emptyList()
        return parseMovieCards(html)
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val html = HikariNet.getStringSmart(media.id, h()) ?: return media
        val ogTitle = Regex("<meta[^>]+property=\"og:title\"[^>]+content=\"([^\"]+)\"")
            .find(html)?.groupValues?.get(1)?.let { unescape(it) }
        val ogImage = Regex("<meta[^>]+property=\"og:image\"[^>]+content=\"([^\"]+)\"")
            .find(html)?.groupValues?.get(1)
        val ogDesc = Regex("<meta[^>]+(?:property=\"og:description\"|name=\"description\")[^>]+content=\"([^\"]+)\"")
            .find(html)?.groupValues?.get(1)?.let { unescape(it) }
        return media.copy(
            title = ogTitle?.takeIf { it.isNotBlank() } ?: media.title,
            posterUrl = ogImage?.takeIf { it.startsWith("http") } ?: media.posterUrl,
            backdropUrl = ogImage?.takeIf { it.startsWith("http") } ?: media.backdropUrl,
            overview = ogDesc?.takeIf { it.isNotBlank() },
        )
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val html = HikariNet.getStringSmart(media.id, h()) ?: return null
        val eps = parseSeasonEpisodes(html, media)
        if (eps.isNotEmpty()) return eps
        return listOf(HikariEpisode(number = 1, id = media.id + "#full", name = media.title, image = media.posterUrl))
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val ref = (episode?.id ?: media.id).substringBefore("#")
        val html = HikariNet.getStringSmart(ref, h()) ?: return emptyList()
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        // 1) Player tab iframes (.player-content iframe + generic iframes).
        val frames = Regex("<iframe[^>]+src=[\"']([^\"']+)[\"']").findAll(html)
            .map { it.groupValues[1] }.filter { it.startsWith("http") }.distinct().toList()
        for (f in frames) {
            resolveEmbed(f, ref, out, seen)
        }
        // 2) Bare m3u8/mp4 links in page.
        if (out.isEmpty()) {
            for (m in Regex("https?://[^\\s\"'<>\\\\]+\\.(?:m3u8|mp4)[^\\s\"'<>\\\\]*").findAll(html)) {
                val u = m.value
                if (u.startsWith("http") && seen.add(u)) {
                    out.add(HikariStream(name = "NetCinez", url = u, headers = mapOf("User-Agent" to UA, "Referer" to ref), isM3u8 = u.contains(".m3u8")))
                }
            }
        }
        if (out.isEmpty()) {
            try {
                val hits = HikariNet.resolveWithWebView(
                    ref,
                    Regex("https?://[^\"'\\s]+?\\.(?:m3u8|mp4)(?:[?#][^\"'\\s]*)?"),
                    timeoutMs = 45_000,
                )
                for (hit in hits) {
                    if (hit.url.isBlank() || !seen.add(hit.url)) continue
                    out.add(HikariStream(name = "NetCinez", url = hit.url, headers = hit.headers, isM3u8 = hit.url.contains(".m3u8", true)))
                }
            } catch (t: Throwable) { }
        }
        return out
    }

    // ------------------------------------------------------------------
    // Parsing helpers.
    // ------------------------------------------------------------------

    fun parseMovieCards(html: String): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        val seen = HashSet<String>()
        // wpmovies4 .movie blocks: <div class="movie">...<a href="DETAIL">...<img ...><h2>TITLE</h2>
        val blockRe = Regex("<div[^>]*class=\"[^\"]*\\bmovie\\b[^\"]*\"[^>]*>(.*?)</div>\\s*</div>", RegexOption.DOT_MATCHES_ALL)
        var matched = false
        for (b in blockRe.findAll(html)) {
            matched = true
            val block = b.groupValues[1]
            val href = Regex("<a[^>]+href=\"([^\"]+)\"").find(block)?.groupValues?.get(1) ?: continue
            if (!href.startsWith("http") || !seen.add(href)) continue
            val title = Regex("<h2[^>]*>(.*?)</h2>", RegexOption.DOT_MATCHES_ALL).find(block)?.groupValues?.get(1)
                ?.let { stripTags(unescape(it)) }?.trim()?.takeIf { it.isNotBlank() } ?: continue
            val img = Regex("<img[^>]+(?:data-src|src)=\"([^\"]+)\"").find(block)?.groupValues?.get(1)
                ?.takeIf { it.startsWith("http") }
            val isTv = href.contains("/tvshows", true) || href.contains("/serie", true) || block.contains("temporada", true)
            out.add(HikariMedia(id = href, title = title, type = if (isTv) HikariMediaType.SERIES else HikariMediaType.MOVIE, posterUrl = img))
        }
        if (matched) return out
        // Fallback: any site-internal detail links with image + title nearby.
        val linkRe = Regex("<a[^>]+href=\"(https?://netcine[a-z]*\\.[a-z]+/[^\"']+/[^\"']/)\"[^>]*>")
        for (m in linkRe.findAll(html)) {
            val url = m.groupValues[1]
            if (!seen.add(url)) continue
            if (url.contains("/category/", true) || url.contains("/tvshows/category/", true)) continue
            val window = html.substring(m.range.last, (m.range.last + 1200).coerceAtMost(html.length))
            val title = Regex("<h2[^>]*>(.*?)</h2>", RegexOption.DOT_MATCHES_ALL).find(window)?.groupValues?.get(1)
                ?.let { stripTags(unescape(it)) }?.trim()
                ?: Regex("title=\"([^\"]+)\"").find(m.value)?.groupValues?.get(1)?.let { unescape(it) }?.trim()
                ?: continue
            if (title.isBlank() || title.length > 120) continue
            val img = Regex("<img[^>]+(?:data-src|src)=\"([^\"]+)\"").find(window)?.groupValues?.get(1)
                ?.takeIf { it.startsWith("http") }
            out.add(HikariMedia(id = url, title = title, type = HikariMediaType.SERIES, posterUrl = img))
        }
        return out
    }

    fun parseSeasonEpisodes(html: String, media: HikariMedia): List<HikariEpisode> {
        val out = ArrayList<HikariEpisode>()
        // Series episode links: /episodio/, ?ep=, #ep, temporada blocks.
        val epRe = Regex("<a[^>]+href=\"([^\"]+)\"[^>]*>([^<]{1,80})</a>")
        var idx = 0
        for (m in epRe.findAll(html)) {
            val url = m.groupValues[1]
            val label = stripTags(unescape(m.groupValues[2])).trim()
            if (!url.startsWith("http")) continue
            val looksEp = url.contains("episodio", true) || url.contains("temporada", true) ||
                Regex("[Tt]emp\\.?\\s*\\d|E[p\\u00e9]?\\s*\\d|S\\d+\\s*E\\d+").containsMatchIn(label) ||
                Regex("cap[íi]tulo\\s+\\d+", RegexOption.IGNORE_CASE).containsMatchIn(label)
            if (!looksEp) continue
            idx++
            val nums = Regex("(\\d+)").findAll(label).mapNotNull { it.groupValues[1].toIntOrNull() }.toList()
            out.add(HikariEpisode(number = nums.lastOrNull() ?: idx, id = url, name = label.ifBlank { "Episode $idx" }, image = media.posterUrl))
            if (out.size > 400) break
        }
        return out
    }

    private suspend fun resolveEmbed(embedUrl: String, referer: String, out: MutableList<HikariStream>, seen: HashSet<String>) {
        val page = HikariNet.getString(embedUrl, mapOf("User-Agent" to UA, "Referer" to referer))
        if (page != null) {
            for (m in Regex("https?://[^\\s\"'<>\\\\]+\\.(?:m3u8|mp4)[^\\s\"'<>\\\\]*").findAll(page)) {
                val u = m.value
                if (u.startsWith("http") && seen.add(u)) {
                    out.add(HikariStream(name = "NetCinez", url = u, headers = mapOf("User-Agent" to UA, "Referer" to embedUrl), isM3u8 = u.contains(".m3u8")))
                }
            }
            if (out.isNotEmpty()) return
        }
        try {
            val hits = HikariNet.resolveWithWebView(
                embedUrl,
                Regex("https?://[^\"'\\s]+?\\.(?:m3u8|mp4)(?:[?#][^\"'\\s]*)?"),
                timeoutMs = 45_000,
            )
            for (hit in hits) {
                if (hit.url.isBlank() || !seen.add(hit.url)) continue
                out.add(HikariStream(name = "NetCinez", url = hit.url, headers = hit.headers, isM3u8 = hit.url.contains(".m3u8", true)))
            }
        } catch (t: Throwable) { }
    }

    private fun stripTags(s: String): String =
        s.replace(Regex("<[^>]+>"), " ").replace(Regex("\\s+"), " ").trim()

    private fun unescape(s: String): String =
        s.replace("&#8217;", "'").replace("&#038;", "&").replace("&amp;", "&")
            .replace("&#8230;", "...").replace("&quot;", "\"").replace("&#8211;", "-")
}
