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
 * AnimeAV1 (animeav1.com) — Spanish-subbed anime catalog, SvelteKit SPA,
 * verified live Oct 2026:
 *  - Home SSR HTML embeds SvelteKit data JSON with featured:[{id,slug,title,
 *    category:{name},genres:[{name}]}]; covers at
 *    https://cdn.animeav1.com/covers/<id>.jpg, backdrops .../backdrops/<id>.jpg,
 *    episode thumbs .../thumbnails/<id>.jpg, screenshots .../screenshots/.
 *  - Cards link to /media/<slug> (series detail). Catalog: /catalogo (+ ?search=
 *    &genre=&type=&status=&year=&order= filters); search form GETs /catalogo.
 *  - Player per dex strings: https://player.zilla-networks.com/m3u8/<id>.
 *    Media pages embed episode ids + zilla URLs; streams fall back to iframe
 *    scrape + WebView m3u8/mp4 capture.
 */
class AnimeAv1Provider : HikariProvider {

    override val id = "animeav1"
    override val name = "AnimeAv1"
    override val mainUrl = "https://animeav1.com"
    override val description = "Anime online gratis en HD y sub espanol — estrenos, series, peliculas y OVAs."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val UA =
        "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36"

    companion object {
        var base = "https://animeav1.com"
        const val ZILLA = "https://player.zilla-networks.com/m3u8/"
    }

    private fun h(referer: String = "$base/"): Map<String, String> =
        mapOf("User-Agent" to UA, "Referer" to referer, "Accept-Language" to "es-ES,es;q=0.9")

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("featured", "Destacados", HikariMediaType.SERIES),
        HikariCatalog("latest", "Ultimos Episodios", HikariMediaType.SERIES),
        HikariCatalog("catalog", "Catalogo", HikariMediaType.SERIES),
        HikariCatalog("movies", "Peliculas", HikariMediaType.MOVIE),
        HikariCatalog("ovas", "OVAs", HikariMediaType.SERIES),
        HikariCatalog("top", "Populares", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val p = page.coerceAtLeast(1)
        if (catalog.id == "featured" || catalog.id == "latest") {
            val html = HikariNet.getStringSmart("$base/", h()) ?: return emptyList()
            if (catalog.id == "featured") {
                val f = parseFeatured(html)
                if (f.isNotEmpty()) return f
            }
            return parseMediaCards(html)
        }
        val path = when (catalog.id) {
            "movies" -> "/catalogo?type=pelicula&page=$p"
            "ovas" -> "/catalogo?type=ova&page=$p"
            "top" -> "/catalogo?order=popular&page=$p"
            else -> "/catalogo?page=$p"
        }
        val html = HikariNet.getStringSmart("$base$path", h()) ?: return emptyList()
        val cards = parseMediaCards(html)
        if (cards.isNotEmpty()) return cards
        return parseFeatured(html)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        val p = page.coerceAtLeast(1)
        val html = HikariNet.getStringSmart("$base/catalogo?search=${URLEncoder.encode(q, "UTF-8")}&page=$p", h())
            ?: return emptyList()
        val cards = parseMediaCards(html)
        if (cards.isNotEmpty()) return cards
        return parseFeatured(html)
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val html = HikariNet.getStringSmart(media.id, h()) ?: return media
        val title = Regex("<title>(.*?)</title>", RegexOption.DOT_MATCHES_ALL).find(html)?.groupValues?.get(1)
            ?.let { stripTags(it) }?.substringBefore(" - ")?.substringBefore(" | ")?.trim()
        val desc = Regex("<meta[^>]+name=\"description\"[^>]+content=\"([^\"]+)\"").find(html)?.groupValues?.get(1)
            ?.let { unescape(it) }
        val coverId = Regex("cdn\\.animeav1\\.com/covers/(\\d+)\\.jpg").find(html)?.groupValues?.get(1)
        val cover = if (coverId != null) "https://cdn.animeav1.com/covers/$coverId.jpg" else media.posterUrl
        val backdrop = Regex("cdn\\.animeav1\\.com/backdrops/(\\d+)\\.jpg").find(html)?.groupValues?.get(1)
            ?.let { "https://cdn.animeav1.com/backdrops/$it.jpg" } ?: media.backdropUrl
        return media.copy(
            title = title?.takeIf { it.isNotBlank() } ?: media.title,
            posterUrl = cover,
            backdropUrl = backdrop,
            overview = desc?.takeIf { it.isNotBlank() },
        )
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val html = HikariNet.getStringSmart(media.id, h()) ?: return null
        val eps = parseEpisodes(html, media)
        if (eps.isNotEmpty()) return eps
        return listOf(HikariEpisode(number = 1, id = media.id + "#ep1", name = media.title, image = media.posterUrl))
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val ref = (episode?.id ?: media.id).substringBefore("#")
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        // Episode pages (/ver/<slug>/<n> or media page with zilla ids).
        val html = HikariNet.getStringSmart(ref, h())
        if (html != null) {
            // 1) Direct zilla player URLs embedded in page.
            for (m in Regex("https?://player\\.zilla-networks\\.com/m3u8/([A-Za-z0-9_-]+)").findAll(html)) {
                val u = m.value
                if (seen.add(u)) {
                    out.add(HikariStream(name = "AnimeAV1 Zilla", url = u, headers = mapOf("User-Agent" to UA, "Referer" to "$base/", "Origin" to base), isM3u8 = true))
                }
            }
            // 2) Episode watch links -> resolve each (first only to stay fast).
            if (out.isEmpty()) {
                val ver = Regex("href=\"(/ver/[^\"]+)\"").findAll(html).map { it.groupValues[1] }.distinct().take(3).toList()
                for (v in ver) {
                    val pg = HikariNet.getStringSmart("$base$v", h(ref))
                    if (pg != null) {
                        for (m in Regex("https?://player\\.zilla-networks\\.com/m3u8/([A-Za-z0-9_-]+)").findAll(pg)) {
                            if (seen.add(m.value)) {
                                out.add(HikariStream(name = "AnimeAV1 Zilla", url = m.value, headers = mapOf("User-Agent" to UA, "Referer" to "$base/", "Origin" to base), isM3u8 = true))
                            }
                        }
                        for (m in Regex("https?://[^\\s\"'<>\\\\]+\\.(?:m3u8|mp4)[^\\s\"'<>\\\\]*").findAll(pg)) {
                            if (m.value.startsWith("http") && seen.add(m.value)) {
                                out.add(HikariStream(name = "AnimeAV1", url = m.value, headers = mapOf("User-Agent" to UA, "Referer" to "$base$v"), isM3u8 = m.value.contains(".m3u8")))
                            }
                        }
                    }
                    if (out.isNotEmpty()) break
                }
            }
            // 3) Generic iframes.
            if (out.isEmpty()) {
                for (f in Regex("<iframe[^>]+src=[\"']([^\"']+)[\"']").findAll(html).map { it.groupValues[1] }.filter { it.startsWith("http") }.distinct()) {
                    resolveEmbed(f, ref, out, seen)
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
                    out.add(HikariStream(name = "AnimeAV1", url = hit.url, headers = hit.headers, isM3u8 = hit.url.contains(".m3u8", true)))
                }
            } catch (t: Throwable) { }
        }
        return out
    }

    // ------------------------------------------------------------------
    // Parsing helpers.
    // ------------------------------------------------------------------

    fun parseFeatured(html: String): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        val seen = HashSet<String>()
        // SvelteKit data payload: slug:"<slug>",...,title:"<title>",id:<n>
        val feat = Regex("\\{category:\\{[^}]*\\}[^\\{]*?id:(\\d+),slug:\"([^\"]+)\"[^\\}]*?title:\"([^\"]+)\"")
        for (m in feat.findAll(html)) {
            val id = m.groupValues[1]
            val slug = m.groupValues[2]
            val title = unescape(m.groupValues[3]).trim()
            if (title.isBlank() || !seen.add(slug)) continue
            out.add(
                HikariMedia(
                    id = "$base/media/$slug",
                    title = title,
                    type = HikariMediaType.SERIES,
                    posterUrl = "https://cdn.animeav1.com/covers/$id.jpg",
                    backdropUrl = "https://cdn.animeav1.com/backdrops/$id.jpg",
                )
            )
            if (out.size > 60) break
        }
        return out
    }

    fun parseMediaCards(html: String): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        val seen = HashSet<String>()
        val linkRe = Regex("<a[^>]+href=\"(/media/[^\"]+)\"[^>]*>")
        for (m in linkRe.findAll(html)) {
            val path = m.groupValues[1]
            val url = "$base$path"
            if (!seen.add(url)) continue
            val window = html.substring(m.range.last, (m.range.last + 2000).coerceAtMost(html.length))
            val title = Regex("alt=\"(?:Portada|P[óo]ster) de ([^\"]+)\"").find(window)?.groupValues?.get(1)
                ?.let { unescape(it) }?.trim()
                ?: Regex("<h3[^>]*>(.*?)</h3>", RegexOption.DOT_MATCHES_ALL).find(window)?.groupValues?.get(1)
                    ?.let { stripTags(unescape(it)) }?.trim()?.takeIf { it.isNotBlank() }
                ?: path.substringAfter("/media/").replace("-", " ").split(" ").map { w -> w.replaceFirstChar { c -> c.uppercase() } }.joinToString(" ")
            val img = Regex("<img[^>]+src=\"(https://cdn\\.animeav1\\.com/[^\"]+)\"").find(window)?.groupValues?.get(1)
            val kind = Regex("(TV Anime|Pel[íi]cula|OVA|Especial)", RegexOption.IGNORE_CASE).find(window)?.groupValues?.get(1)?.lowercase() ?: ""
            val isMovie = kind.contains("pel")
            out.add(
                HikariMedia(
                    id = url,
                    title = title,
                    type = if (isMovie) HikariMediaType.MOVIE else HikariMediaType.SERIES,
                    posterUrl = img,
                )
            )
            if (out.size > 200) break
        }
        return out
    }

    fun parseEpisodes(html: String, media: HikariMedia): List<HikariEpisode> {
        val out = ArrayList<HikariEpisode>()
        val seen = HashSet<String>()
        // Episode watch links /ver/<slug>/<n> with optional thumb + title nearby.
        val verRe = Regex("<a[^>]+href=\"(/ver/[^\"]+)\"[^>]*>")
        for (m in verRe.findAll(html)) {
            val path = m.groupValues[1]
            val url = "$base$path"
            if (!seen.add(url)) continue
            val window = html.substring(m.range.last, (m.range.last + 1500).coerceAtMost(html.length))
            val epNum = Regex("/ver/[^/\"']+/(\\d+)").find(path)?.groupValues?.get(1)?.toIntOrNull()
                ?: Regex("[Ee]pisodio\\s+(\\d+)").find(window)?.groupValues?.get(1)?.toIntOrNull()
                ?: (out.size + 1)
            val img = Regex("<img[^>]+src=\"(https://cdn\\.animeav1\\.com/[^\"]+)\"").find(window)?.groupValues?.get(1)
            out.add(HikariEpisode(number = epNum, id = url, name = "Episode $epNum", image = img ?: media.posterUrl))
            if (out.size > 500) break
        }
        // Oldest-first ordering.
        return out.sortedBy { it.number }
    }

    private suspend fun resolveEmbed(embedUrl: String, referer: String, out: MutableList<HikariStream>, seen: HashSet<String>) {
        val page = HikariNet.getString(embedUrl, mapOf("User-Agent" to UA, "Referer" to referer))
        if (page != null) {
            for (m in Regex("https?://[^\\s\"'<>\\\\]+\\.(?:m3u8|mp4)[^\\s\"'<>\\\\]*").findAll(page)) {
                if (m.value.startsWith("http") && seen.add(m.value)) {
                    out.add(HikariStream(name = "AnimeAV1", url = m.value, headers = mapOf("User-Agent" to UA, "Referer" to embedUrl), isM3u8 = m.value.contains(".m3u8")))
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
                out.add(HikariStream(name = "AnimeAV1", url = hit.url, headers = hit.headers, isM3u8 = hit.url.contains(".m3u8", true)))
            }
        } catch (t: Throwable) { }
    }

    private fun stripTags(s: String): String =
        s.replace(Regex("<[^>]+>"), " ").replace(Regex("\\s+"), " ").trim()

    private fun unescape(s: String): String =
        s.replace("\\\"", "\"").replace("\\/", "/").replace("\\n", " ")
            .replace("&#039;", "'").replace("&amp;", "&").replace("&quot;", "\"")
}
