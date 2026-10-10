package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import java.net.URLEncoder
import java.net.URLDecoder

/**
 * CineFreak (cinefreak.ch) — Hollywood / Bollywood / Hindi-dubbed movies and
 * web series, verified live Oct 2026:
 *  - Rows: home grid + category archives (/web-series/, /hindi-dubbed-movies/,
 *    /hindi-movies/, /english-movies/, /k-drama/, /animation/). Cards carry
 *    h2 > a titles, TMDB posters and cine-play-btn links.
 *  - Search: yagaverse Typesense index (search.yagaverse.net/api/search?q=)
 *    returning {hits:[{document:{id,slug,title,thumb,cats,quality}}]}.
 *  - Detail posts carry dlbtn-watch links (cinecloud /x/<id>) whose pages embed
 *    player.yagaverse.net/embed2/?id=<r2 direct file>&host=direct — the id param
 *    URL-decodes to the playable file. dlbtn-download (/f/<id>) pages are
 *    JS/login-gated and skipped; HubCloud/NeoDrive links resolved when present.
 */
class CineFreakProvider : HikariProvider {

    override val id = "cinefreak"
    override val name = "CineFreak"
    override val mainUrl = "https://cinefreak.ch"
    override val description = "Hollywood, Bollywood & Hindi-dubbed movies and web series — watch online."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val UA =
        "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36"

    companion object {
        var base = "https://cinefreak.ch"
        const val YAGA_SEARCH = "https://search.yagaverse.net/api/search?q="
    }

    private fun h(referer: String = "$base/"): Map<String, String> =
        mapOf("User-Agent" to UA, "Referer" to referer)

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("latest", "Latest", HikariMediaType.MOVIE),
        HikariCatalog("series", "Web Series", HikariMediaType.SERIES),
        HikariCatalog("hindi", "Hindi Dubbed Movies", HikariMediaType.MOVIE),
        HikariCatalog("bollywood", "Hindi Movies", HikariMediaType.MOVIE),
        HikariCatalog("english", "English Movies", HikariMediaType.MOVIE),
        HikariCatalog("kdrama", "K-Drama", HikariMediaType.SERIES),
        HikariCatalog("animation", "Animation", HikariMediaType.MOVIE),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val p = page.coerceAtLeast(1)
        val path = when (catalog.id) {
            "series" -> "web-series"
            "hindi" -> "hindi-dubbed-movies"
            "bollywood" -> "hindi-movies"
            "english" -> "english-movies"
            "kdrama" -> "k-drama"
            "animation" -> "animation"
            else -> ""
        }
        val url = if (path.isBlank()) {
            if (p > 1) "$base/page/$p/" else "$base/"
        } else {
            if (p > 1) "$base/$path/page/$p/" else "$base/$path/"
        }
        val html = HikariNet.getString(url, h()) ?: return emptyList()
        return parseCards(html, catalog.id == "series")
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank() || page > 1) return emptyList()
        val root = HikariNet.getJson(YAGA_SEARCH + URLEncoder.encode(q, "UTF-8"), h()) ?: return emptyList()
        val hits = root.optJSONArray("hits") ?: return emptyList()
        val out = ArrayList<HikariMedia>()
        for (i in 0 until hits.length()) {
            val d = hits.optJSONObject(i)?.optJSONObject("document") ?: continue
            val slug = d.optString("slug").trim()
            val title = d.optString("title").trim()
            if (slug.isBlank() || title.isBlank()) continue
            val thumb = d.optString("thumb").takeIf { it.startsWith("http") }
            val cats = d.optJSONArray("cats")?.let { a -> (0 until a.length()).map { a.optString(it) } } ?: emptyList()
            val isSeries = cats.any { it.contains("series", true) || it.contains("drama", true) || it.contains("show", true) }
            out.add(
                HikariMedia(
                    id = "$base/$slug/",
                    title = title.substringBefore(" | ").trim(),
                    type = if (isSeries) HikariMediaType.SERIES else HikariMediaType.MOVIE,
                    posterUrl = thumb,
                )
            )
        }
        return out
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val html = HikariNet.getString(media.id, h()) ?: return media
        val ogTitle = Regex("<meta[^>]+property=\"og:title\"[^>]+content=\"([^\"]+)\"")
            .find(html)?.groupValues?.get(1)?.let { unescape(it) }
        val ogImage = Regex("<meta[^>]+property=\"og:image\"[^>]+content=\"([^\"]+)\"")
            .find(html)?.groupValues?.get(1)
        val h1 = Regex("<h1 class=\"page-title\">(.*?)</h1>", RegexOption.DOT_MATCHES_ALL)
            .find(html)?.groupValues?.get(1)?.let { stripTags(unescape(it)) }
        return media.copy(
            title = (ogTitle ?: h1)?.substringBefore(" | ")?.takeIf { it.isNotBlank() } ?: media.title,
            posterUrl = ogImage?.takeIf { it.startsWith("http") } ?: media.posterUrl,
            backdropUrl = ogImage?.takeIf { it.startsWith("http") } ?: media.backdropUrl,
        )
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val html = HikariNet.getString(media.id, h()) ?: return null
        val watches = Regex("<a\\s+href=\"([^\"]*?/x/[a-z0-9]+)\"[^>]*class=\"[^\"]*dlbtn-watch[^\"]*\"[^>]*>(.*?)</a>", RegexOption.DOT_MATCHES_ALL)
            .findAll(html).toList()
        if (watches.isEmpty()) {
            // Single-movie post: the whole page is one playable entry.
            return listOf(HikariEpisode(number = 1, id = media.id, name = media.title, image = media.posterUrl))
        }
        return watches.mapIndexed { idx, m ->
            val label = stripTags(m.groupValues[2]).trim().ifBlank { "Episode ${idx + 1}" }
            val n = Regex("(\\d{1,4})").findAll(label).mapNotNull { it.groupValues[1].toIntOrNull() }.lastOrNull() ?: (idx + 1)
            HikariEpisode(number = n, id = m.groupValues[1], name = label, image = media.posterUrl)
        }
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val ref = episode?.id ?: media.id
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        if (ref.contains("/x/")) {
            resolveCineWatch(ref, media.title, out, seen)
        } else {
            val html = HikariNet.getString(ref, h()) ?: return emptyList()
            // Watch buttons first (direct playable flow).
            for (m in Regex("<a\\s+href=\"([^\"]*?/x/[a-z0-9]+)\"").findAll(html)) {
                resolveCineWatch(m.groupValues[1], media.title, out, seen)
            }
            // HubCloud / NeoDrive / PixelDrain mirrors when present.
            for (m in Regex("https?://[^\\s\"'<>]*hubcloud[^\\s\"'<>]*").findAll(html)) {
                val u = m.value
                if (u.startsWith("http") && seen.add(u)) {
                    out.add(HikariStream(name = "CineFreak Hub", url = u, headers = h(ref)))
                }
            }
            // Trailer only as a last resort is skipped (ytId trailers are noise).
        }
        return out
    }

    // ------------------------------------------------------------------
    // Parsing helpers.
    // ------------------------------------------------------------------

    fun parseCards(html: String, seriesRow: Boolean): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        val seen = HashSet<String>()
        val headRe = Regex("<a\\s+href=\"(https://cinefreak\\.ch/[^\"]+/)\">([^<]{3,200})</a></h2>")
        for (m in headRe.findAll(html)) {
            val url = m.groupValues[1]
            if (!seen.add(url) || url.contains("/category/")) continue
            val title = unescape(m.groupValues[2]).trim()
            if (title.isBlank()) continue
            val before = html.substring(maxOf(0, m.range.first - 1500), m.range.first)
            val img = Regex("<img[^>]+src=\"(https?://[^\"]+)\"").findAll(before).lastOrNull()?.groupValues?.get(1)
            val isSeries = seriesRow || url.contains("web-series", true) || title.contains("season", true) || title.contains("series", true)
            out.add(
                HikariMedia(
                    id = url,
                    title = title.replace(Regex("\\s*\\|\\s*Hindi.*$", RegexOption.IGNORE_CASE), "").trim(),
                    type = if (isSeries) HikariMediaType.SERIES else HikariMediaType.MOVIE,
                    posterUrl = img,
                )
            )
        }
        return out
    }

    private suspend fun resolveCineWatch(watchUrl: String, label: String, out: MutableList<HikariStream>, seen: HashSet<String>) {
        val page = HikariNet.getString(watchUrl, h()) ?: return
        // yagaverse embed carries the direct R2 file in its id param.
        for (m in Regex("<iframe[^>]+(?:src|data-cf-src)=\"([^\"]+)\"").findAll(page)) {
            val src = unescape(m.groupValues[1])
            if (!src.contains("yagaverse.net")) continue
            val idParam = Regex("[?&]id=([^&]+)").find(src)?.groupValues?.get(1) ?: continue
            val direct = try {
                URLDecoder.decode(URLDecoder.decode(idParam, "UTF-8"), "UTF-8")
            } catch (t: Throwable) {
                try { URLDecoder.decode(idParam, "UTF-8") } catch (t2: Throwable) { "" }
            }
            if (direct.startsWith("http") && seen.add(direct)) {
                out.add(
                    HikariStream(
                        name = "CineFreak $label".take(64),
                        url = direct,
                        headers = mapOf("User-Agent" to UA, "Referer" to "https://player.yagaverse.net/"),
                        isM3u8 = direct.contains(".m3u8"),
                    )
                )
            }
        }
        // Generic media URLs on the watch page.
        for (m in Regex("https?://[^\\s\"'<>\\\\]+\\.(?:m3u8|mp4|mkv)[^\\s\"'<>\\\\]*").findAll(page)) {
            val u = m.value
            if (u.startsWith("http") && !u.contains("tailwindcss") && seen.add(u)) {
                out.add(HikariStream(name = "CineFreak $label".take(64), url = u, headers = h(watchUrl), isM3u8 = u.contains(".m3u8")))
            }
        }
    }

    private fun stripTags(s: String): String =
        s.replace(Regex("<[^>]+>"), " ").replace(Regex("\\s+"), " ").trim()

    private fun unescape(s: String): String =
        s.replace("&#8217;", "'").replace("&#038;", "&").replace("&amp;", "&")
            .replace("&#8230;", "...").replace("&quot;", "\"").replace("&#8211;", "-")
}
