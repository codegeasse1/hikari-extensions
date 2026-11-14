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
 * LatAnime (latanime.org) — Spanish-latino anime directory, verified live Oct 2026:
 *  - Home hero carousel links /anime/<slug>/ with .hero-cover-preview images
 *    (data-src https://latanime.org/assets/img/serie/imagen/<slug>-<ts>.jpg/.webp).
 *  - Nav rows: /animes (Directorio), /emision (En emision), /calendario.
 *  - Detail pages list episode watch links (/ver/, /capitulo, /episodio);
 *    players are iframes + direct m3u8/mp4 with WebView fallback.
 */
class LatAnimeProvider : HikariProvider {

    override val id = "latanime"
    override val name = "LatAnime"
    override val mainUrl = "https://latanime.org"
    override val description = "Anime en latino — directorio, emision, calendario y estrenos."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val UA =
        "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36"

    companion object {
        var base = "https://latanime.org"
    }

    private fun h(referer: String = "$base/"): Map<String, String> =
        mapOf("User-Agent" to UA, "Referer" to referer, "Accept-Language" to "es-ES,es;q=0.9")

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("home", "Destacados", HikariMediaType.SERIES),
        HikariCatalog("directory", "Directorio", HikariMediaType.SERIES),
        HikariCatalog("airing", "En Emision", HikariMediaType.SERIES),
        HikariCatalog("calendar", "Calendario", HikariMediaType.SERIES),
        HikariCatalog("movies", "Peliculas", HikariMediaType.MOVIE),
        HikariCatalog("latest", "Ultimos Agregados", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val p = page.coerceAtLeast(1)
        val path = when (catalog.id) {
            "directory" -> "/animes?page=$p"
            "airing" -> "/emision?page=$p"
            "calendar" -> "/calendario?page=$p"
            "movies" -> "/animes?tipo=pelicula&page=$p"
            "latest" -> "/animes?orden=agregados&page=$p"
            else -> if (p > 1) "/animes?page=$p" else "/"
        }
        val html = HikariNet.getStringSmart("$base$path", h()) ?: return emptyList()
        val cards = parseAnimeCards(html)
        if (cards.isNotEmpty()) return cards
        return parseGenericCards(html)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        val p = page.coerceAtLeast(1)
        val tries = listOf(
            "$base/animes?buscar=${URLEncoder.encode(q, "UTF-8")}&page=$p",
            "$base/animes?search=${URLEncoder.encode(q, "UTF-8")}&page=$p",
            "$base/?s=${URLEncoder.encode(q, "UTF-8")}",
        )
        for (u in tries) {
            val html = HikariNet.getStringSmart(u, h()) ?: continue
            val cards = parseAnimeCards(html)
            if (cards.isNotEmpty()) return cards
            val gen = parseGenericCards(html)
            if (gen.isNotEmpty()) return gen
        }
        return emptyList()
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val html = HikariNet.getStringSmart(media.id, h()) ?: return media
        val ogTitle = Regex("<meta[^>]+property=\"og:title\"[^>]+content=\"([^\"]+)\"").find(html)?.groupValues?.get(1)?.let { unescape(it) }
        val ogImage = Regex("<meta[^>]+property=\"og:image\"[^>]+content=\"([^\"]+)\"").find(html)?.groupValues?.get(1)
        val ogDesc = Regex("<meta[^>]+(?:property=\"og:description\"|name=\"description\")[^>]+content=\"([^\"]+)\"").find(html)?.groupValues?.get(1)?.let { unescape(it) }
        return media.copy(
            title = ogTitle?.takeIf { it.isNotBlank() } ?: media.title,
            posterUrl = ogImage?.takeIf { it.startsWith("http") } ?: media.posterUrl,
            backdropUrl = ogImage?.takeIf { it.startsWith("http") } ?: media.backdropUrl,
            overview = ogDesc?.takeIf { it.isNotBlank() },
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
        val html = HikariNet.getStringSmart(ref, h()) ?: return emptyList()
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        val frames = Regex("<iframe[^>]+src=[\"']([^\"']+)[\"']").findAll(html)
            .map { it.groupValues[1] }.filter { it.startsWith("http") }.distinct().toList()
        for (f in frames) {
            resolveEmbed(f, ref, out, seen)
        }
        if (out.isEmpty()) {
            for (m in Regex("https?://[^\\s\"'<>\\\\]+\\.(?:m3u8|mp4)[^\\s\"'<>\\\\]*").findAll(html)) {
                if (m.value.startsWith("http") && seen.add(m.value)) {
                    out.add(HikariStream(name = "LatAnime", url = m.value, headers = mapOf("User-Agent" to UA, "Referer" to ref), isM3u8 = m.value.contains(".m3u8")))
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
                    out.add(HikariStream(name = "LatAnime", url = hit.url, headers = hit.headers, isM3u8 = hit.url.contains(".m3u8", true)))
                }
            } catch (t: Throwable) { }
        }
        return out
    }

    // ------------------------------------------------------------------
    // Parsing helpers.
    // ------------------------------------------------------------------

    fun parseAnimeCards(html: String): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        val seen = HashSet<String>()
        val linkRe = Regex("<a[^>]+href=\"(https://latanime\\.org/anime/[^\"]+)\"[^>]*>")
        for (m in linkRe.findAll(html)) {
            val url = m.groupValues[1].substringBefore("\"").trim()
            if (!seen.add(url)) continue
            val window = html.substring(m.range.last, (m.range.last + 1500).coerceAtMost(html.length))
            val title = Regex("alt=\"([^\"]+)\"").find(window)?.groupValues?.get(1)?.let { unescape(it) }?.trim()
                ?.takeIf { it.isNotBlank() && !it.equals("anime", true) }
                ?: Regex("<h[2-4][^>]*>(.*?)</h[2-4]>", RegexOption.DOT_MATCHES_ALL).find(window)?.groupValues?.get(1)
                    ?.let { stripTags(unescape(it)) }?.trim()?.takeIf { it.isNotBlank() }
                ?: url.substringAfter("/anime/").trim('/').replace("-", " ").split(" ").map { w -> w.replaceFirstChar { c -> c.uppercase() } }.joinToString(" ")
            val img = Regex("<img[^>]+(?:data-src|src)=\"([^\"]+)\"").find(m.value + window)?.groupValues?.get(1)
                ?.takeIf { it.contains("/assets/img/serie/") || it.startsWith("http") }?.let { if (it.startsWith("//")) "https:$it" else it }
                ?.takeIf { it.startsWith("http") }
            out.add(HikariMedia(id = url, title = title, type = HikariMediaType.SERIES, posterUrl = img))
            if (out.size > 200) break
        }
        return out
    }

    fun parseGenericCards(html: String): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        val seen = HashSet<String>()
        val linkRe = Regex("<a[^>]+href=\"(https://latanime\\.org/(?:anime|pelicula)[^\"]*)\"[^>]*>(.*?)</a>", RegexOption.DOT_MATCHES_ALL)
        for (m in linkRe.findAll(html)) {
            val url = m.groupValues[1]
            if (!seen.add(url)) continue
            val inner = m.groupValues[2]
            val title = stripTags(unescape(inner)).trim().takeIf { it.isNotBlank() && it.length < 120 }
                ?: url.substringAfterLast("/").replace("-", " ")
            val img = Regex("<img[^>]+(?:data-src|src)=\"([^\"]+)\"").find(inner + html.substring(m.range.last, (m.range.last + 800).coerceAtMost(html.length)))?.groupValues?.get(1)
                ?.takeIf { it.startsWith("http") || it.startsWith("//") }?.let { if (it.startsWith("//")) "https:$it" else it }
            out.add(HikariMedia(id = url, title = title, type = HikariMediaType.SERIES, posterUrl = img))
            if (out.size > 200) break
        }
        return out
    }

    fun parseEpisodes(html: String, media: HikariMedia): List<HikariEpisode> {
        val out = ArrayList<HikariEpisode>()
        val seen = HashSet<String>()
        val epRe = Regex("<a[^>]+href=\"([^\"]+)\"[^>]*>([^<]{1,60})</a>")
        var idx = 0
        for (m in epRe.findAll(html)) {
            val url = m.groupValues[1]
            if (!url.startsWith("http") || !url.contains("latanime.org")) continue
            val label = stripTags(unescape(m.groupValues[2])).trim()
            val looksEp = url.contains("/ver/", true) || url.contains("capitulo", true) || url.contains("episodio", true) ||
                Regex("cap[íi]tulo|episodio|episode\\s+\\d+", RegexOption.IGNORE_CASE).containsMatchIn(label)
            if (!looksEp) continue
            if (!seen.add(url)) continue
            idx++
            val n = Regex("(\\d+)").findAll(label).mapNotNull { it.groupValues[1].toIntOrNull() }.lastOrNull() ?: idx
            out.add(HikariEpisode(number = n, id = url, name = label.ifBlank { "Episode $n" }, image = media.posterUrl))
            if (out.size > 500) break
        }
        return out.sortedBy { it.number }
    }

    private suspend fun resolveEmbed(embedUrl: String, referer: String, out: MutableList<HikariStream>, seen: HashSet<String>) {
        val page = HikariNet.getString(embedUrl, mapOf("User-Agent" to UA, "Referer" to referer))
        if (page != null) {
            for (m in Regex("https?://[^\\s\"'<>\\\\]+\\.(?:m3u8|mp4)[^\\s\"'<>\\\\]*").findAll(page)) {
                if (m.value.startsWith("http") && seen.add(m.value)) {
                    out.add(HikariStream(name = "LatAnime", url = m.value, headers = mapOf("User-Agent" to UA, "Referer" to embedUrl), isM3u8 = m.value.contains(".m3u8")))
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
                out.add(HikariStream(name = "LatAnime", url = hit.url, headers = hit.headers, isM3u8 = hit.url.contains(".m3u8", true)))
            }
        } catch (t: Throwable) { }
    }

    private fun stripTags(s: String): String =
        s.replace(Regex("<[^>]+>"), " ").replace(Regex("\\s+"), " ").trim()

    private fun unescape(s: String): String =
        s.replace("&#039;", "'").replace("&#8217;", "'").replace("&amp;", "&")
            .replace("&quot;", "\"").replace("&#8230;", "...")
}
