package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import com.hikari.ext.HikariSubtitle
import java.net.URLEncoder

/**
 * ChikiAnimation (chikianimation.com) — Chinese anime (donghua) in 2D/3D with
 * multi-language subtitles. WordPress + Tsundere theme, verified live Oct 2026:
 *  - Series archive: /anime/?status=&type=&order=update (types: series/movie/
 *    ai animes/ai short drama), cards are <a class="tip" href> + title + img.
 *  - Series pages /anime/<slug>/ carry .episodelist > ul > li > a episode links.
 *  - Episode posts carry .player-embed iframes (dailymotion geo player via
 *    src or data-litespeed-src, Galaxy GDPlayer) + "All Episodes" series link.
 *  - Search: ?s= (standard WP).
 */
class ChikiAnimationProvider : HikariProvider {

    override val id = "chikianimation"
    override val name = "ChikiAnimation"
    override val mainUrl = "https://chikianimation.com"
    override val description = "Chinese anime (donghua) with multi-language subtitles — latest updates, series, movies."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val UA =
        "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36"

    private fun h(): Map<String, String> = mapOf("User-Agent" to UA, "Referer" to "$base/")

    companion object {
        var base = "https://chikianimation.com"
    }

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("latest", "Latest Updates", HikariMediaType.SERIES),
        HikariCatalog("episodes", "Latest Episodes", HikariMediaType.SERIES),
        HikariCatalog("series", "Series", HikariMediaType.SERIES),
        HikariCatalog("movies", "Movies", HikariMediaType.MOVIE),
        HikariCatalog("ai", "AI Animes", HikariMediaType.SERIES),
        HikariCatalog("short", "AI Short Drama", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val p = page.coerceAtLeast(1)
        if (catalog.id == "episodes") {
            val html = HikariNet.getString(if (p > 1) "$base/page/$p/" else "$base/", h())
                ?: return emptyList()
            return parseTipCards(html, episodes = true)
        }
        val type = when (catalog.id) {
            "movies" -> "movie"
            "ai" -> "ai+animes"
            "short" -> "ai+short+drama"
            else -> ""
        }
        val url = if (p > 1) "$base/anime/page/$p/?status=&type=$type&order=update"
        else "$base/anime/?status=&type=$type&order=update"
        val html = HikariNet.getString(url, h()) ?: return emptyList()
        val series = parseTipCards(html, episodes = false)
        if (series.isNotEmpty()) return series
        // Fallback: any /anime/ links on the page.
        return parseAnimeLinks(html)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank() || page > 1) return emptyList()
        val html = HikariNet.getString("$base/?s=${URLEncoder.encode(q, "UTF-8")}", h())
            ?: return emptyList()
        val series = parseAnimeLinks(html)
        if (series.isNotEmpty()) return series
        return parseTipCards(html, episodes = false)
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val html = HikariNet.getString(media.id, h()) ?: return media
        val ogTitle = Regex("<meta[^>]+property=\"og:title\"[^>]+content=\"([^\"]+)\"")
            .find(html)?.groupValues?.get(1)?.let { unescape(it) }?.substringBefore(" Episode ")
        val ogImage = Regex("<meta[^>]+property=\"og:image\"[^>]+content=\"([^\"]+)\"")
            .find(html)?.groupValues?.get(1)
        val ogDesc = Regex("<meta[^>]+property=\"og:description\"[^>]+content=\"([^\"]+)\"")
            .find(html)?.groupValues?.get(1)?.let { unescape(it) }
        val isMovie = html.contains("/anime/?status=&type=movie") ||
            Regex("type[\"']?\\s*[:=]\\s*[\"']?movie", RegexOption.IGNORE_CASE).containsMatchIn(html)
        return media.copy(
            title = ogTitle?.takeIf { it.isNotBlank() } ?: media.title,
            posterUrl = ogImage?.takeIf { it.startsWith("http") } ?: media.posterUrl,
            backdropUrl = ogImage?.takeIf { it.startsWith("http") } ?: media.backdropUrl,
            overview = ogDesc?.takeIf { it.isNotBlank() && !it.startsWith("Watch [post_title]") },
            type = if (isMovie) HikariMediaType.MOVIE else media.type,
        )
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        // Episode-post media: single playable episode.
        if (!isSeriesUrl(media.id)) {
            val n = episodeNumberFromTitle(media.title)
            return listOf(HikariEpisode(number = n, id = media.id, name = media.title, image = media.posterUrl))
        }
        val html = HikariNet.getString(media.id, h()) ?: return null
        val eps = parseEpisodeList(html)
        if (eps.isEmpty()) return null
        // Site lists newest first; return oldest-first for sane ordering.
        return eps.asReversed().mapIndexed { idx, e ->
            HikariEpisode(
                number = episodeNumberFromTitle(e.second).takeIf { it > 0 } ?: (idx + 1),
                id = e.first,
                name = e.second,
                image = media.posterUrl,
            )
        }
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val pageUrl = episode?.id ?: media.id
        val html = HikariNet.getString(pageUrl, h()) ?: return emptyList()
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        // 1) Player iframes (src or lazy data-litespeed-src).
        val frames = Regex("<iframe[^>]+(?:src|data-litespeed-src)=[\"']([^\"']+)[\"']")
            .findAll(html).map { it.groupValues[1] }.filter { it.startsWith("http") }.distinct().toList()
        for (f in frames) {
            when {
                f.contains("dailymotion.com") -> resolveDailymotion(f, out, seen)
                f.contains("galaxydonghua") -> resolveGalaxy(f, out, seen)
                else -> {
                    val m3u8 = Regex("https?://[^\\s\"'<>\\\\]+\\.m3u8[^\\s\"'<>\\\\]*").find(f)?.value
                    if (m3u8 != null && seen.add(m3u8)) {
                        out.add(HikariStream(name = "ChikiAnimation", url = m3u8, headers = mapOf("Referer" to f, "User-Agent" to UA), isM3u8 = true))
                    }
                }
            }
        }
        // 2) Galaxy player links outside iframes.
        for (m in Regex("https?://galaxydonghua\\.xyz/[^\\s\"'<>]*").findAll(html)) {
            resolveGalaxy(m.value, out, seen)
        }
        // 3) WebView capture fallback (JS-driven players).
        if (out.isEmpty()) {
            try {
                val hits = HikariNet.resolveWithWebView(
                    pageUrl,
                    Regex("https?://[^\"'\\s]+?\\.(?:m3u8|mp4)(?:[?#][^\"'\\s]*)?"),
                    timeoutMs = 45_000,
                )
                for (hit in hits) {
                    if (hit.url.isBlank() || !seen.add(hit.url)) continue
                    out.add(
                        HikariStream(
                            name = "ChikiAnimation",
                            url = hit.url,
                            headers = hit.headers,
                            isM3u8 = hit.url.contains(".m3u8", ignoreCase = true),
                        )
                    )
                }
            } catch (t: Throwable) { }
        }
        return out
    }

    // ------------------------------------------------------------------
    // Parsing helpers (pure functions of HTML — unit-tested in CI logs).
    // ------------------------------------------------------------------

    fun parseTipCards(html: String, episodes: Boolean): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        val seen = HashSet<String>()
        val cardRe = Regex("<a\\s+href=\"([^\"]+)\"[^>]*class=\"tip\"[^>]*>")
        for (m in cardRe.findAll(html)) {
            val href = m.value.let { cardRe.find(it)?.groupValues?.get(1) } ?: continue
            if (!href.startsWith(base) && !href.startsWith("/")) continue
            val url = if (href.startsWith("/")) base + href else href
            val isSeries = isSeriesUrl(url)
            if (episodes == isSeries) continue
            if (!seen.add(url)) continue
            val block = html.substring(m.range.first, (m.range.first + 1500).coerceAtMost(html.length))
            val title = Regex("title=\"([^\"]+)\"").find(m.value)?.groupValues?.get(1)?.let { unescape(it) }
                ?: Regex("<h[23][^>]*>(.*?)</h[23]>", RegexOption.DOT_MATCHES_ALL).find(block)?.groupValues?.get(1)?.let { stripTags(unescape(it)) }
                ?: continue
            if (title.isBlank()) continue
            val img = Regex("<img[^>]+src=\"([^\"]+)\"").find(block)?.groupValues?.get(1)
            out.add(
                HikariMedia(
                    id = url,
                    title = cleanTitle(title),
                    type = if (url.contains("movie", true)) HikariMediaType.MOVIE else HikariMediaType.SERIES,
                    posterUrl = img?.takeIf { it.startsWith("http") },
                )
            )
        }
        return out
    }

    fun parseAnimeLinks(html: String): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        val seen = HashSet<String>()
        val linkRe = Regex("<a\\s+href=\"((?:https://chikianimation\\.com)?/anime/[^\"']+)/?\"[^>]*>")
        for (m in linkRe.findAll(html)) {
            var url = m.groupValues[1]
            if (url.startsWith("/")) url = base + url
            if (!seen.add(url)) continue
            val after = html.substring(m.range.last + 1, (m.range.last + 1200).coerceAtMost(html.length))
            val title = Regex("title=\"([^\"]+)\"").find(m.value)?.groupValues?.get(1)?.let { unescape(it) }
                ?: Regex("^[^<]*").find(after)?.value?.trim()?.let { unescape(it) }?.takeIf { it.isNotBlank() }
                ?: continue
            val img = Regex("<img[^>]+src=\"([^\"]+)\"").find(after)?.groupValues?.get(1)
            out.add(
                HikariMedia(
                    id = url,
                    title = cleanTitle(title),
                    type = HikariMediaType.SERIES,
                    posterUrl = img?.takeIf { it.startsWith("http") },
                )
            )
        }
        return out
    }

    fun parseEpisodeList(html: String): List<Pair<String, String>> {
        val out = ArrayList<Pair<String, String>>()
        val seen = HashSet<String>()
        val zone = Regex("<div class=\"episodelist\">(.*?)</ul>", RegexOption.DOT_MATCHES_ALL)
            .find(html)?.groupValues?.get(1) ?: html
        val linkRe = Regex("<a\\s+href=\"([^\"]+)\"[^>]*>\\s*(?:<div class=\"thumbnel\">.*?</div>\\s*)?<div class=\"playinfo\">\\s*<h3>(.*?)</h3>", RegexOption.DOT_MATCHES_ALL)
        for (m in linkRe.findAll(zone)) {
            val url = m.groupValues[1]
            if (!url.startsWith("http") || !seen.add(url)) continue
            val title = stripTags(unescape(m.groupValues[2])).trim()
            if (title.isBlank()) continue
            out.add(url to cleanTitle(title))
        }
        return out
    }

    private fun isSeriesUrl(url: String): Boolean = url.contains("/anime/")

    private fun episodeNumberFromTitle(title: String): Int {
        val m = Regex("[Ee]pisode\\s+(\\d+)").find(title)?.groupValues?.get(1)?.toIntOrNull()
        if (m != null) return m
        return Regex("(\\d+)\\s*(?:to|–|-|\\[|$)").findAll(title).mapNotNull { it.groupValues[1].toIntOrNull() }.lastOrNull() ?: 1
    }

    private fun cleanTitle(t: String): String =
        t.replace(Regex("\\s*in Multiple[~\\- ]*Subtitles\\s*$", RegexOption.IGNORE_CASE), "").trim()

    private fun stripTags(s: String): String =
        s.replace(Regex("<[^>]+>"), " ").replace(Regex("\\s+"), " ").trim()

    private fun unescape(s: String): String =
        s.replace("&#8217;", "'").replace("&#8211;", "-").replace("&#038;", "&")
            .replace("&amp;", "&").replace("&#8230;", "...").replace("&quot;", "\"")

    // ------------------------------------------------------------------
    // Player resolvers.
    // ------------------------------------------------------------------

    private suspend fun resolveDailymotion(playerUrl: String, out: MutableList<HikariStream>, seen: HashSet<String>) {
        // geo player: https://geo.dailymotion.com/player/<cfg>.html?video=<id>
        val vid = Regex("[?&]video=([^&#]+)").find(playerUrl)?.groupValues?.get(1) ?: return
        // Public metadata endpoint exposes progressive + HLS URLs.
        val meta = HikariNet.getString(
            "https://www.dailymotion.com/player/metadata/video/$vid",
            mapOf("User-Agent" to UA, "Referer" to "https://www.dailymotion.com/"),
        )
        if (meta != null) {
            val quals = Regex("\"(\\d+)\":\\s*\\[\\s*\\{[^}]*\"type\"\\s*:\\s*\"(?:video\\/mp4|application\\/x-mpegURL)\"[^}]*\"url\"\\s*:\\s*\"([^\"]+)\"")
                .findAll(meta)
            for (q in quals) {
                var u = q.groupValues[2].replace("\\/", "/")
                if (!u.startsWith("http")) continue
                if (seen.add(u)) {
                    out.add(
                        HikariStream(
                            name = "ChikiAnimation Dailymotion ${q.groupValues[1]}p",
                            url = u,
                            headers = mapOf("User-Agent" to UA, "Referer" to "https://www.dailymotion.com/"),
                            isM3u8 = u.contains(".m3u8"),
                        )
                    )
                }
            }
            if (out.isNotEmpty()) return
            val hls = Regex("\"auto\"[^]]*\"url\"\\s*:\\s*\"([^\"]+\\.m3u8[^\"]*)\"").find(meta)?.groupValues?.get(1)?.replace("\\/", "/")
            if (hls != null && hls.startsWith("http") && seen.add(hls)) {
                out.add(HikariStream(name = "ChikiAnimation Dailymotion", url = hls, headers = mapOf("User-Agent" to UA), isM3u8 = true))
                return
            }
        }
        // Fallback: scrape the player page itself for media URLs.
        val page = HikariNet.getString(playerUrl, mapOf("User-Agent" to UA, "Referer" to "$base/")) ?: return
        for (m in Regex("https?://[^\\s\"'<>\\\\]+\\.(?:m3u8|mp4)[^\\s\"'<>\\\\]*").findAll(page)) {
            val u = m.value.replace("\\/", "/")
            if (u.startsWith("http") && seen.add(u)) {
                out.add(HikariStream(name = "ChikiAnimation Dailymotion", url = u, headers = mapOf("User-Agent" to UA), isM3u8 = u.contains(".m3u8")))
            }
        }
    }

    private suspend fun resolveGalaxy(embedUrl: String, out: MutableList<HikariStream>, seen: HashSet<String>) {
        // GDPlayer (Google Drive player) pages expose a drive id; resolve via
        // the player page's own download/manifest links, else WebView capture.
        val page = HikariNet.getString(embedUrl, mapOf("User-Agent" to UA, "Referer" to "$base/"))
        if (page != null) {
            for (m in Regex("https?://[^\\s\"'<>\\\\]+\\.(?:m3u8|mp4)[^\\s\"'<>\\\\]*").findAll(page)) {
                val u = m.value
                if (u.startsWith("http") && !u.contains("galaxydonghua.xyz") && seen.add(u)) {
                    out.add(HikariStream(name = "ChikiAnimation Galaxy", url = u, headers = mapOf("User-Agent" to UA, "Referer" to "https://galaxydonghua.xyz/"), isM3u8 = u.contains(".m3u8")))
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
                out.add(HikariStream(name = "ChikiAnimation Galaxy", url = hit.url, headers = hit.headers, isM3u8 = hit.url.contains(".m3u8", true)))
            }
        } catch (t: Throwable) { }
    }
}
