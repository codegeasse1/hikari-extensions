package com.hikari.ext.providers

import com.hikari.ext.HikariNet
import com.hikari.ext.HikariStream
import com.hikari.ext.HikariSubtitle
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import org.json.JSONObject
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap

/**
 * JustPlay's TheMoviesFlix + Multimovies site backends (port of TmfSite /
 * MultimoviesSite from JustPlaySites.kt). Search/detail parsing is regex over
 * the HTML strings; link resolution reuses the vendored TmfNet/TmfSources and
 * Mm* helpers, with stream names namespaced per JustPlay site.
 */

// ---------------------------------------------------------------------------
// TheMoviesFlix
// ---------------------------------------------------------------------------

internal object JpTmfSite {

    private val packRegex = Regex("""(?i)\b(zip|rar|7z|batch)\b""")

    private fun searchAnchors(html: String): List<Pair<String, String>> {
        val out = mutableListOf<Pair<String, String>>()
        val articles = Regex(
            """<article[^>]*>(.*?)</article>""",
            setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
        ).findAll(html)
        for (art in articles) {
            val body = art.groupValues[1]
            if (!body.contains("featured-thumbnail")) continue
            for (a in JpHtml.anchors(body)) {
                if (!a.attrs.contains("featured-thumbnail")) continue
                var t = Regex(
                    """title="([^"]+)"""",
                    RegexOption.IGNORE_CASE,
                ).find(a.attrs)?.groupValues?.get(1)?.trim().orEmpty()
                if (t.isBlank()) {
                    t = Regex(
                        """<img[^>]*alt="([^"]+)"[^>]*>""",
                        RegexOption.IGNORE_CASE,
                    ).find(body)?.groupValues?.get(1)?.trim().orEmpty()
                }
                if (t.isNotBlank() && a.href.startsWith("http")) out.add(t to a.href)
            }
        }
        return out.distinctBy { it.second }
    }

    private data class DriveGroup(val label: String, val redirectUrl: String)

    private fun downloadGroups(html: String): List<DriveGroup> {
        val out = mutableListOf<DriveGroup>()
        val divs = Regex(
            """<div[^>]*class="[^"]*mfx-download-group[^"]*"[^>]*>(.*?)</div>""",
            setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
        ).findAll(html)
        for (d in divs) {
            val body = d.groupValues[1]
            val label = Regex(
                """<h3[^>]*>(.*?)</h3>""",
                setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
            ).find(body)?.groupValues?.get(1)?.let { JpHtml.strip(it) }.orEmpty()
            if (packRegex.containsMatchIn(label)) continue
            for (a in JpHtml.anchors(body)) {
                val text = a.text.trim()
                val href = a.href.trim()
                when {
                    !href.startsWith("http") -> {}
                    text.contains("Batch", true) || text.contains("Zip", true) -> {}
                    packRegex.containsMatchIn(text) -> {}
                    else -> out.add(DriveGroup(label, href))
                }
            }
        }
        return out.distinctBy { it.redirectUrl }
    }

    suspend fun invoke(
        res: JpPlayLink,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ) {
        try {
            val title = res.title ?: return
            val domain = TmfNet.domain()
            val searchHtml = TmfNet.fetchPage("$domain/?s=${TmfNet.urlEncode(title)}") ?: return
            val anchors = searchAnchors(searchHtml)
            if (anchors.isEmpty()) return

            val matched = anchors.filter { (t, _) -> JpNet.titleMatches(t, title) }
            val target = matched.firstOrNull { (t, _) ->
                if (res.season != null) JpNet.seasonsOf(t)?.contains(res.season) == true
                else JpNet.yearMatches(t, res.matchYear)
            } ?: matched.firstOrNull() ?: anchors.firstOrNull() ?: return

            val postHtml = TmfNet.fetchPage(target.second, domain) ?: return
            val groups = downloadGroups(postHtml)
            val driveUrls: List<String> = if (res.season == null) {
                groups.map { it.redirectUrl }
            } else {
                val seasonRe = Regex("(?i)Season\\s*${res.season}(?!\\d)")
                groups.filter { (label, _) ->
                    seasonRe.containsMatchIn(label) ||
                        JpNet.seasonsOf(label)?.contains(res.season) == true
                }.map { it.redirectUrl }
            }
            if (driveUrls.isEmpty()) return

            val tmfCb: (HikariStream) -> Unit = { s ->
                callback(
                    s.copy(
                        name = JpLabels.buildLabel("themoviesflix", s.name, ""),
                        subtitles = s.subtitles,
                    ),
                )
            }
            coroutineScope {
                driveUrls.forEach { driveUrl ->
                    async(Dispatchers.IO) {
                        val page = TmfNet.fetchDrivePage(driveUrl) ?: return@async
                        val hrefs = if (res.season != null && res.episode != null) {
                            page.episodes[res.episode] ?: emptyList()
                        } else {
                            page.links
                        }
                        if (hrefs.isEmpty()) return@async
                        // re-attach this drive page's own info tag to each link
                        val info = page.info
                        if (info.isBlank()) {
                            TmfSources.emitAll(hrefs, page.quality, "", driveUrl, tmfCb)
                        } else {
                            TmfSources.emitAll(hrefs, page.quality, info, driveUrl) { s ->
                                tmfCb(s.copy(name = "${s.name} · $info"))
                            }
                        }
                    }
                }
            }
        } catch (_: Exception) {
        }
    }
}

// ---------------------------------------------------------------------------
// Multimovies
// ---------------------------------------------------------------------------

internal object JpMmSite {
    private const val DEFAULT_DOMAIN = "https://multimovies.garden"

    private data class MmCard(
        val title: String,
        val url: String,
        val year: Int?,
        val isTv: Boolean,
    )

    @Volatile
    private var resolvedDomain: String? = null

    private suspend fun currentDomain(): String {
        resolvedDomain?.let { return it }
        val target = (JpDomains.get("justplay_multimovies") ?: JpDomains.get("multimovies"))
            ?.trimEnd('/')
        var domain = DEFAULT_DOMAIN
        if (target != null && target != DEFAULT_DOMAIN) {
            try {
                val html = JpNet.fetchWithCf(target)
                if (html != null && html.contains("/assets/js/player.js")) domain = target
            } catch (_: Exception) {
            }
        }
        resolvedDomain = domain
        return domain
    }

    private fun cardOf(fragment: String, domain: String): MmCard? {
        val raw = Regex(
            """data-save-title\s*=\s*&quot;(.*?)&quot;""",
            setOf(RegexOption.DOT_MATCHES_ALL),
        ).find(fragment)?.groupValues?.get(1)
            ?: Regex(
                """data-save-title\s*=\s*"(\{.*?\})"""",
            ).find(fragment)?.groupValues?.get(1)
            ?: return null
        val data = try {
            JSONObject(raw.replace("&quot;", "\""))
        } catch (_: Exception) {
            return null
        }
        val url = data.optString("url")
        val title = data.optString("title")
        if (url.isBlank() || title.isBlank()) return null
        val isTv = data.optString("type") == "tv" || url.contains("/series/")
        return MmCard(title, JpNet.absolute(url, domain), data.optInt("year", 0).takeIf { it > 0 }, isTv)
    }

    private suspend fun searchCards(domain: String, query: String): List<MmCard> = try {
        val html = JpNet.fetchWithCf("$domain/search?q=${JpNet.urlEncode(query)}")
            ?: return emptyList()
        val seen = HashSet<String>()
        Regex(
            """<article[^>]*class="[^"]*poster-card[^"]*"[^>]*>(.*?)</article>""",
            setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
        ).findAll(html).mapNotNull { cardOf(it.groupValues[1], domain) }
            .filter { seen.add(it.url) }.toList()
    } catch (_: Exception) {
        emptyList()
    }

    private fun findEpisodeCard(html: String, domain: String, season: Int?, episode: Int?): String? {
        val cards = Regex(
            """<[^>]*id="(ep-item-\d+-\d+)"[^>]*>(.*?)</(?:div|article)>""",
            setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
        ).findAll(html)
        for (c in cards) {
            val m = Regex("""ep-item-(\d+)-(\d+)""").find(c.groupValues[1]) ?: continue
            if (season != null && m.groupValues[1].toIntOrNull() != season) continue
            if (episode != null && m.groupValues[2].toIntOrNull() != episode) continue
            val href = Regex(
                """<a[^>]*class="[^"]*cinejoy-ep-thumb-link[^"]*"[^>]*href="([^"]+)"""",
                RegexOption.IGNORE_CASE,
            ).find(c.groupValues[2])?.groupValues?.get(1)?.trim()
                ?: Regex(
                    """<a[^>]*href="([^"]+)"[^>]*class="[^"]*cinejoy-ep-thumb-link""",
                    RegexOption.IGNORE_CASE,
                ).find(c.groupValues[2])?.groupValues?.get(1)?.trim()
                ?: continue
            if (href.isNotBlank()) return JpNet.absolute(href, domain)
        }
        return null
    }

    private suspend fun episodeHref(
        domain: String,
        seriesUrl: String,
        season: Int?,
        episode: Int?,
    ): String? {
        val b = seriesUrl.substringBefore('?')
        val first = JpNet.fetchWithCf(b) ?: return null
        val renderedSeason = try {
            JSONObject(extractBracedObject(first, "const watchConfig = ")).optInt("season", 1)
        } catch (_: Exception) {
            1
        }
        var html = first
        if (season != null && season != renderedSeason) {
            html = JpNet.fetchWithCf("$b?season=$season") ?: return null
        }
        findEpisodeCard(html, domain, season, episode)?.let { return it }
        if (episode == null) return null
        val selectBody = Regex(
            """<select[^>]*class="[^"]*episode-range-select[^"]*"[^>]*>(.*?)</select>""",
            setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
        ).find(html)?.groupValues?.get(1) ?: return null
        val options = Regex(
            """<option[^>]*value="(\d+)"[^>]*>""",
            RegexOption.IGNORE_CASE,
        ).findAll(selectBody).map { it.groupValues[1].toIntOrNull() ?: -1 }
            .filter { it >= 0 }.toList()
        val selected = Regex(
            """<option[^>]*value="(\d+)"[^>]*selected[^>]*>""",
            RegexOption.IGNORE_CASE,
        ).find(selectBody)?.groupValues?.get(1)?.toIntOrNull()
        val target = options.filter { it <= episode }.maxOrNull() ?: return null
        if (target == selected) return null
        val rangeHtml = JpNet.fetchWithCf("$b?season=${season ?: renderedSeason}&ep_range=$target")
            ?: return null
        return findEpisodeCard(rangeHtml, domain, season, episode)
    }

    private data class WatchServer(val id: String, val name: String, val url: String)

    private fun parseWatchConfig(html: String): List<WatchServer> {
        val root = try {
            JSONObject(extractBracedObject(html, "const watchConfig = "))
        } catch (_: Exception) {
            return emptyList()
        }
        val servers = root.optJSONArray("initialServers") ?: return emptyList()
        val out = mutableListOf<WatchServer>()
        for (i in 0 until servers.length()) {
            val s = servers.optJSONObject(i) ?: continue
            val url = s.optString("url")
            if (url.isBlank()) continue
            out.add(WatchServer(s.optString("id"), s.optString("name"), url))
        }
        return out
    }

    private fun extractBracedObject(html: String, marker: String): String {
        val start = html.indexOf(marker)
        if (start < 0) return ""
        val braceStart = html.indexOf('{', start)
        if (braceStart < 0) return ""
        var depth = 0
        var inString = false
        var escaped = false
        for (i in braceStart until html.length) {
            val ch = html[i]
            if (escaped) {
                escaped = false
                continue
            }
            when {
                ch == '\\' && inString -> escaped = true
                ch == '"' -> inString = !inString
                ch == '{' && !inString -> depth++
                ch == '}' && !inString -> {
                    depth--
                    if (depth == 0) return html.substring(braceStart, i + 1)
                }
            }
        }
        return ""
    }

    private data class TitleIds(val tmdbId: String?, val imdbId: String?)

    private fun extractIds(servers: List<WatchServer>): TitleIds {
        var tmdb: String? = null
        var imdb: String? = null
        for (server in servers) {
            if (tmdb == null) {
                tmdb = Regex("""(?:movie\?id=|watch/movie/|watch/tv/|embed/tmdb/tv\?id=|/tv/)(\d+)""")
                    .find(server.url)?.groupValues?.get(1)
            }
            if (imdb == null) {
                imdb = Regex("""(tt\d{6,})""").find(server.url)?.groupValues?.get(1)
            }
        }
        return TitleIds(tmdb, imdb)
    }

    private fun yearOf(html: String): String? {
        val pills = Regex(
            """<[^>]*class="[^"]*cinejoy-meta-pill[^"]*"[^>]*>(.*?)</[^>]+>""",
            setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
        ).findAll(html).map { JpHtml.strip(it.groupValues[1]) }.joinToString(" ")
        return Regex("""\b((?:19|20)\d{2})\b""").find(pills)?.groupValues?.get(1)
    }

    private suspend fun resolveServer(
        server: WatchServer,
        ids: TitleIds,
        isTv: Boolean,
        season: Int?,
        episode: Int?,
        title: String,
        year: String?,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ) {
        // namespace every stream from this site, deduplicated by URL
        val seen = Collections.newSetFromMap(ConcurrentHashMap<String, Boolean>())
        val linkCb: (HikariStream) -> Unit = { link ->
            if (seen.add(link.url)) {
                val clean = link.name.replace(Regex("\\s+"), " ").trim()
                callback(link.copy(name = "[Multimovies] - $clean"))
            }
        }
        val host = MmNet.hostOf(server.url)
        when {
            host.contains("modiplay.xyz") ->
                MmCineverse.resolve(server.url, server.name, linkCb)
            host.contains("iqsmartgames.com") ->
                MmIqsmart.resolve(server.url, server.name, linkCb)
            host.contains("filesforever.link") ->
                MmIqsmart.resolveFilesforever(server.url, server.name, linkCb)
            host.contains("vidout.pages.dev") -> {
                val tmdb = ids.tmdbId ?: return
                MmVidout.resolve(tmdb, isTv, season, episode, server.name, subtitleCallback, linkCb)
            }
            host.contains("vidsync.pro") -> {
                val url = if (ids.tmdbId != null) server.url.replace("{tmdbId}", ids.tmdbId) else server.url
                MmVidsync.resolve(url, server.name, linkCb)
            }
            host.contains("bingr.one") -> {
                val tmdb = ids.tmdbId ?: return
                MmBingr.resolve(isTv, tmdb, title, year, season, episode, server.name, subtitleCallback, linkCb)
            }
            host.contains("filmu.in") -> {
                val tmdb = ids.tmdbId ?: return
                MmFilmu.resolve(isTv, tmdb, season, episode, server.name, subtitleCallback, linkCb)
            }
            host.contains("vidbolt.xyz") -> {
                val tmdb = ids.tmdbId ?: return
                MmVidbolt.resolve(isTv, tmdb, ids.imdbId, title, year, season, episode, server.name, linkCb)
            }
            else -> {
                val html = MmNet.get(server.url, referer = "https://multimovies.garden/")
                if (html != null) {
                    val urls = Regex("""https?://[^"'\\\s\\\\]+\.m3u8[^"'\\\s\\\\]*""")
                        .findAll(html)
                        .map { MmNet.deEsc(it.value) }
                        .toSet()
                    for (u in urls) {
                        if (!JpNet.m3u8Alive(u)) continue
                        linkCb(HikariStream(name = server.name, url = u, isM3u8 = true))
                    }
                }
            }
        }
    }

    private suspend fun resolvePost(
        pageUrl: String,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ) {
        try {
            val html = JpNet.fetchWithCf(pageUrl) ?: return
            val servers = parseWatchConfig(html)
            if (servers.isEmpty()) return
            val ids = extractIds(servers)
            val isTv = pageUrl.contains("/series/")
            val season = Regex("""/season/(\d+)/episode/""").find(pageUrl)?.groupValues?.get(1)?.toIntOrNull()
            val episode = Regex("""/season/\d+/episode/(\d+)""").find(pageUrl)?.groupValues?.get(1)?.toIntOrNull()
            val title = Regex(
                """<h1[^>]*>(.*?)</h1>""",
                setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
            ).find(html)?.groupValues?.get(1)?.let { JpHtml.strip(it) }.orEmpty()
            val year = yearOf(html)
            coroutineScope {
                servers.forEach { server ->
                    async(Dispatchers.IO) {
                        try {
                            resolveServer(server, ids, isTv, season, episode, title, year, subtitleCallback, callback)
                        } catch (_: Exception) {
                        }
                    }
                }
            }
        } catch (_: Exception) {
        }
    }

    private fun pickCard(cards: List<MmCard>, title: String, year: Int?): MmCard? {
        if (cards.isEmpty()) return null
        val norm = JpNet.normalizeTitle(title)
        val exact = cards.filter { JpNet.normalizeTitle(it.title) == norm }
        val pool = exact.ifEmpty { cards }
        return pool.firstOrNull { c ->
            c.year == null || year == null || kotlin.math.abs(c.year - year) <= 1
        } ?: pool.first()
    }

    suspend fun invoke(
        res: JpPlayLink,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ) {
        try {
            val domain = currentDomain()
            val title = res.title ?: return
            val cards = searchCards(domain, title)
            if (res.season == null) {
                val matches = cards.filter { !it.isTv && JpNet.titleMatches(it.title, title) }
                val movie = pickCard(matches, title, res.year) ?: return
                resolvePost(movie.url, subtitleCallback, callback)
            } else {
                val matches = cards.filter { it.isTv && JpNet.titleMatches(it.title, title) }
                val show = pickCard(matches, title, res.matchYear) ?: return
                val epUrl = episodeHref(domain, show.url, res.season, res.episode) ?: show.url
                resolvePost(epUrl, subtitleCallback, callback)
            }
        } catch (_: Exception) {
        }
    }
}
