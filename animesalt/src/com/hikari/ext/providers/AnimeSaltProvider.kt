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
 * AnimeSalt (animesalt.cx) — anime & cartoons in Hindi, Tamil, Telugu & English,
 * torofilm WP theme, verified live Oct 2026:
 *  - Language rows: /category/language/<hindi|tamil|telugu|bengali|malayalam|
 *    kannada|english|japanese|korean> (lang-btn data-lang attributes on home).
 *  - Search: standard WP ?s=. Cards: torofilm article/post cards with
 *    <a href> detail + <img> poster + heading title.
 *  - Playback per dex strings: /player/index.php?data=<token> + &do=getVideo,
 *    /wp-admin/admin-ajax.php, enc-dec.app/api/dec-abyss, megaplay.buzz,
 *    abyssplayer/playhydrax/awstream embeds, pixdrive/hydrax files.
 */
class AnimeSaltProvider : HikariProvider {

    override val id = "animesalt"
    override val name = "AnimeSalt"
    override val mainUrl = "https://animesalt.cx"
    override val description = "Anime & cartoons in Hindi, Tamil, Telugu & English — subbed & dubbed in HD."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val UA =
        "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36"

    companion object {
        var base = "https://animesalt.cx"
    }

    private fun h(referer: String = "$base/"): Map<String, String> =
        mapOf("User-Agent" to UA, "Referer" to referer)

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("latest", "Latest Updates", HikariMediaType.SERIES),
        HikariCatalog("hindi", "Hindi", HikariMediaType.SERIES),
        HikariCatalog("tamil", "Tamil", HikariMediaType.SERIES),
        HikariCatalog("telugu", "Telugu", HikariMediaType.SERIES),
        HikariCatalog("english", "English", HikariMediaType.SERIES),
        HikariCatalog("movies", "Movies", HikariMediaType.MOVIE),
        HikariCatalog("series", "Series", HikariMediaType.SERIES),
        HikariCatalog("japanese", "Japanese", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val p = page.coerceAtLeast(1)
        val section = when (catalog.id) {
            "hindi" -> "/category/language/hindi/"
            "tamil" -> "/category/language/tamil/"
            "telugu" -> "/category/language/telugu/"
            "english" -> "/category/language/english/"
            "japanese" -> "/category/language/japanese/"
            "movies" -> "/category/movies/"
            "series" -> "/category/series/"
            else -> "/"
        }
        val path = if (p > 1 && section != "/") section.trimEnd('/') + "/page/$p/"
        else if (p > 1) "/page/$p/"
        else section
        val html = HikariNet.getStringSmart("$base$path", h()) ?: return emptyList()
        return parseCards(html)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank() || page > 1) return emptyList()
        val html = HikariNet.getStringSmart("$base/?s=${URLEncoder.encode(q, "UTF-8")}", h()) ?: return emptyList()
        return parseCards(html)
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
        val hasPlayer = html.contains("/player/index.php?data=") || html.contains("admin-ajax.php")
        if (hasPlayer) {
            return listOf(HikariEpisode(number = 1, id = media.id + "#ep1", name = media.title, image = media.posterUrl))
        }
        return listOf(HikariEpisode(number = 1, id = media.id + "#full", name = media.title, image = media.posterUrl))
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val ref = (episode?.id ?: media.id).substringBefore("#")
        val html = HikariNet.getStringSmart(ref, h()) ?: return emptyList()
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        // 1) Salt player tokens: /player/index.php?data=<token>.
        val playerUrls = Regex("(/player/index\\.php\\?data=[^\"'<>\\s&]+(?:&[^\"'<>\\s]*)?)").findAll(html)
            .map { it.groupValues[1] }.distinct().toList()
        for (pu in playerUrls) {
            val abs = if (pu.startsWith("http")) pu else base + pu
            resolveSaltPlayer(abs, ref, out, seen)
        }
        // 2) Direct embeds (abyss/hydrax/megaplay/awstream/pixdrive/...).
        if (out.isEmpty()) {
            val frames = Regex("<iframe[^>]+src=[\"']([^\"']+)[\"']").findAll(html)
                .map { it.groupValues[1] }.filter { it.startsWith("http") }.distinct().toList()
            for (f in frames) {
                resolveEmbed(f, ref, out, seen)
            }
        }
        // 3) Bare m3u8/mp4.
        if (out.isEmpty()) {
            for (m in Regex("https?://[^\\s\"'<>\\\\]+\\.(?:m3u8|mp4)[^\\s\"'<>\\\\]*").findAll(html)) {
                if (m.value.startsWith("http") && seen.add(m.value)) {
                    out.add(HikariStream(name = "AnimeSalt", url = m.value, headers = mapOf("User-Agent" to UA, "Referer" to ref), isM3u8 = m.value.contains(".m3u8")))
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
                    out.add(HikariStream(name = "AnimeSalt", url = hit.url, headers = hit.headers, isM3u8 = hit.url.contains(".m3u8", true)))
                }
            } catch (t: Throwable) { }
        }
        return out
    }

    // ------------------------------------------------------------------
    // Parsing helpers.
    // ------------------------------------------------------------------

    fun parseCards(html: String): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        val seen = HashSet<String>()
        // Generic WP card: <article ...><a href="DETAIL">...<img src>...<h2/h3>TITLE
        val artRe = Regex("<article[^>]*>(.*?)</article>", RegexOption.DOT_MATCHES_ALL)
        var anyArticle = false
        for (a in artRe.findAll(html)) {
            anyArticle = true
            val block = a.groupValues[1]
            val href = Regex("<a[^>]+href=\"([^\"]+)\"").find(block)?.groupValues?.get(1) ?: continue
            if (!href.startsWith("http") || !href.contains("animesalt.cx")) continue
            if (!seen.add(href)) continue
            val title = Regex("<h[2-4][^>]*>(.*?)</h[2-4]>", RegexOption.DOT_MATCHES_ALL).find(block)?.groupValues?.get(1)
                ?.let { stripTags(unescape(it)) }?.trim()?.takeIf { it.isNotBlank() } ?: continue
            if (title.length > 140) continue
            val img = Regex("<img[^>]+(?:data-src|src)=\"([^\"]+)\"").find(block)?.groupValues?.get(1)
                ?.takeIf { it.startsWith("http") }
            val isMovie = title.contains("movie", true) || href.contains("movie", true)
            out.add(HikariMedia(id = href, title = title, type = if (isMovie) HikariMediaType.MOVIE else HikariMediaType.SERIES, posterUrl = img))
            if (out.size > 200) break
        }
        if (anyArticle) return out
        // Fallback: heading-linked post URLs.
        val linkRe = Regex("<a[^>]+href=\"(https://animesalt\\.cx/[^\"']+/[^\"']/)\"[^>]*>(.*?)</a>", RegexOption.DOT_MATCHES_ALL)
        for (m in linkRe.findAll(html)) {
            val url = m.groupValues[1]
            if (!seen.add(url)) continue
            if (url.contains("/category/", true) || url.contains("wp-content", true)) continue
            val title = stripTags(unescape(m.groupValues[2])).trim()
            if (title.isBlank() || title.length > 140) continue
            out.add(HikariMedia(id = url, title = title, type = HikariMediaType.SERIES))
            if (out.size > 200) break
        }
        return out
    }

    fun parseEpisodes(html: String, media: HikariMedia): List<HikariEpisode> {
        val out = ArrayList<HikariEpisode>()
        val seen = HashSet<String>()
        val epRe = Regex("<a[^>]+href=\"([^\"]+)\"[^>]*>([^<]{1,80})</a>")
        var idx = 0
        for (m in epRe.findAll(html)) {
            val url = m.groupValues[1]
            if (!url.startsWith("http") || !url.contains("animesalt.cx")) continue
            val label = stripTags(unescape(m.groupValues[2])).trim()
            val looksEp = url.contains("/episode", true) || url.contains("ep-", true) ||
                Regex("episode\\s+\\d+|^E\\d+$|^\\d+\\s*$", RegexOption.IGNORE_CASE).containsMatchIn(label)
            if (!looksEp) continue
            if (!seen.add(url)) continue
            idx++
            val n = Regex("(\\d+)").findAll(label).mapNotNull { it.groupValues[1].toIntOrNull() }.lastOrNull() ?: idx
            out.add(HikariEpisode(number = n, id = url, name = label.ifBlank { "Episode $n" }, image = media.posterUrl))
            if (out.size > 500) break
        }
        return out.sortedBy { it.number }
    }

    private suspend fun resolveSaltPlayer(playerUrl: String, referer: String, out: MutableList<HikariStream>, seen: HashSet<String>) {
        // Player page embeds getVideo params / direct sources; try JSON-ish
        // source extraction first, then generic embed resolution.
        val page = HikariNet.getString(playerUrl, mapOf("User-Agent" to UA, "Referer" to referer))
        if (page != null) {
            // getVideo endpoint with data token.
            val data = Regex("[?&]data=([^\"'&\\s]+)").find(playerUrl)?.groupValues?.get(1)
            if (data != null) {
                for (doParam in listOf("getVideo", "getVideoSource", "getVideoImage")) {
                    val api = playerUrl.substringBefore("?") + "?data=$data&do=$doParam"
                    val txt = HikariNet.getString(api, mapOf("User-Agent" to UA, "Referer" to playerUrl, "X-Requested-With" to "XMLHttpRequest"))
                    if (txt != null) {
                        for (m in Regex("https?://[^\\s\"'<>\\\\]+\\.(?:m3u8|mp4)[^\\s\"'<>\\\\]*").findAll(txt)) {
                            if (m.value.startsWith("http") && seen.add(m.value)) {
                                out.add(HikariStream(name = "AnimeSalt", url = m.value, headers = mapOf("User-Agent" to UA, "Referer" to playerUrl), isM3u8 = m.value.contains(".m3u8")))
                            }
                        }
                        if (out.isNotEmpty()) return
                    }
                }
            }
            for (m in Regex("https?://[^\\s\"'<>\\\\]+\\.(?:m3u8|mp4)[^\\s\"'<>\\\\]*").findAll(page)) {
                if (m.value.startsWith("http") && seen.add(m.value)) {
                    out.add(HikariStream(name = "AnimeSalt", url = m.value, headers = mapOf("User-Agent" to UA, "Referer" to playerUrl), isM3u8 = m.value.contains(".m3u8")))
                }
            }
            if (out.isNotEmpty()) return
            // Nested embeds (abyss/hydrax/megaplay) inside the player page.
            val frames = Regex("<iframe[^>]+src=[\"']([^\"']+)[\"']").findAll(page)
                .map { it.groupValues[1] }.filter { it.startsWith("http") }.distinct().toList()
            for (f in frames) {
                resolveEmbed(f, playerUrl, out, seen)
                if (out.isNotEmpty()) return
            }
        }
        try {
            val hits = HikariNet.resolveWithWebView(
                playerUrl,
                Regex("https?://[^\"'\\s]+?\\.(?:m3u8|mp4)(?:[?#][^\"'\\s]*)?"),
                timeoutMs = 45_000,
            )
            for (hit in hits) {
                if (hit.url.isBlank() || !seen.add(hit.url)) continue
                out.add(HikariStream(name = "AnimeSalt", url = hit.url, headers = hit.headers, isM3u8 = hit.url.contains(".m3u8", true)))
            }
        } catch (t: Throwable) { }
    }

    private suspend fun resolveEmbed(embedUrl: String, referer: String, out: MutableList<HikariStream>, seen: HashSet<String>) {
        val page = HikariNet.getString(embedUrl, mapOf("User-Agent" to UA, "Referer" to referer))
        if (page != null) {
            for (m in Regex("https?://[^\\s\"'<>\\\\]+\\.(?:m3u8|mp4)[^\\s\"'<>\\\\]*").findAll(page)) {
                if (m.value.startsWith("http") && seen.add(m.value)) {
                    out.add(HikariStream(name = "AnimeSalt", url = m.value, headers = mapOf("User-Agent" to UA, "Referer" to embedUrl), isM3u8 = m.value.contains(".m3u8")))
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
                out.add(HikariStream(name = "AnimeSalt", url = hit.url, headers = hit.headers, isM3u8 = hit.url.contains(".m3u8", true)))
            }
        } catch (t: Throwable) { }
    }

    private fun stripTags(s: String): String =
        s.replace(Regex("<[^>]+>"), " ").replace(Regex("\\s+"), " ").trim()

    private fun unescape(s: String): String =
        s.replace("&#8217;", "'").replace("&#038;", "&").replace("&amp;", "&")
            .replace("&#8230;", "...").replace("&quot;", "\"").replace("&#8211;", "-")
}
