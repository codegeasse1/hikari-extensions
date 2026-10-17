package com.hikari.ext.providers

import com.hikari.ext.HikariNet
import com.hikari.ext.HikariStream
import com.hikari.ext.HikariSubtitle
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import org.json.JSONObject
import java.util.concurrent.Semaphore

/** Shared HTML section/anchor helpers (jsoup-free). */
internal object JpHtml {

    private val headingRe = { tags: String ->
        Regex(
            """<($tags)[^>]*>(.*?)</\1>""",
            setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
        )
    }

    fun strip(s: String): String =
        s.replace(Regex("<[^>]+>"), " ").replace(Regex("\\s+"), " ").trim()

    data class Section(val heading: String, val body: String)

    /** Split html into (heading, following-segment) pairs for the given heading tags. */
    fun sections(html: String, tags: String = "h2|h3|h4|h5"): List<Section> {
        val heads = headingRe(tags).findAll(html).toList()
        if (heads.isEmpty()) return emptyList()
        return heads.mapIndexed { i, m ->
            val end = heads.getOrNull(i + 1)?.range?.first ?: html.length
            Section(strip(m.groupValues[2]), html.substring(m.range.last + 1, end))
        }
    }

    data class Anchor(val href: String, val text: String, val attrs: String)

    fun anchors(html: String): List<Anchor> =
        Regex(
            """<a([^>]*)>(.*?)</a>""",
            setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
        ).findAll(html).mapNotNull { m ->
            val attrs = m.groupValues[1]
            val href = Regex("""href="([^"]+)"""", RegexOption.IGNORE_CASE)
                .find(attrs)?.groupValues?.get(1)?.trim().orEmpty()
            if (href.isBlank()) return@mapNotNull null
            Anchor(href, strip(m.groupValues[2]), attrs)
        }.toList()
}

/** Shared drive-page fan-out used by the download-post sites. Port of DrivePages. */
internal object JpDrivePages {

    private val junk = Regex(
        "gmpg\\.org|googleapis|googletagmanager|fonts\\.|schema\\.org|w3\\.org|" +
            "twitter\\.com|facebook\\.com|pinterest|whatsapp|telegram|t\\.me/|/tg/|catimages|tinyurl|bonuscaf|" +
            "winexch|a-ads|pixabay|i-poster|scene-source|vglist|vegamovies-apk|hdhub4u\\.download|hdhub4u\\.tv",
    )

    private fun isSelfLink(href: String, hosts: Set<String>): Boolean {
        return try {
            hosts.contains(java.net.URI(href).host?.lowercase())
        } catch (_: Exception) {
            false
        }
    }

    private fun episodeRegexes(episode: Int, season: Int?): List<Regex> {
        val s2 = (season ?: 1).toString().padStart(2, '0')
        return listOf(
            Regex("(?i)Episodes?\\s*:?\\s*0*$episode(?!\\d)"),
            Regex("(?i)\\b${s2}\\s*[xXeE]\\s*0*$episode(?!\\d)"),
            Regex("(?i)\\b${season ?: 1}\\s*[xX]\\s*$episode(?!\\d)"),
        )
    }

    suspend fun emit(
        site: String,
        driveUrl: String,
        episode: Int?,
        season: Int? = null,
        label: String = "",
        siteDomain: String? = null,
        referer: String? = null,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ) {
        try {
            val fetchUrl = driveUrl.replace("mobilejsr.rest", "nexdrive.fit")
            val html = JpNet.fetchDrivePage(fetchUrl, referer) ?: return
            val pageTitle = Regex("""<title>([^<]*)""", RegexOption.IGNORE_CASE)
                .find(html)?.groupValues?.get(1)
                ?.substringBefore(" – ")?.substringBefore(" - ")?.trim().orEmpty()
            if (pageTitle.contains("zip", true)) return
            val driveHost = JpNet.hostOf(fetchUrl)
            val hosts = listOfNotNull(
                driveHost.ifBlank { null },
                siteDomain?.let { JpNet.hostOf(it).ifBlank { null } },
            ).toSet()

            if (episode != null) {
                val regexes = episodeRegexes(episode, season)
                val heads = JpHtml.sections(html).filter { s -> regexes.any { it.containsMatchIn(s.heading) } }
                if (heads.isNotEmpty()) {
                    val picked = heads.flatMap { s ->
                        JpHtml.anchors(s.body).mapNotNull { a ->
                            if (a.href.startsWith("http") && !junk.containsMatchIn(a.href) && !isSelfLink(a.href, hosts)) {
                                a.href to s.heading
                            } else null
                        }
                    }.distinctBy { it.first }
                    coroutineScope {
                        picked.forEach { (href, headLabel) ->
                            async(Dispatchers.IO) {
                                val quality = listOf(headLabel, label).firstNotNullOfOrNull {
                                    JpNet.getIndexQuality(it).takeIf { q -> q != 0 }
                                }
                                JpNet.emitOwnLink(site, href, "Episode $episode", quality, fetchUrl, subtitleCallback, callback)
                            }
                        }
                    }
                    return
                }
                val row = JpHtml.anchors(html).firstOrNull { a ->
                    a.href.startsWith("http") && !junk.containsMatchIn(a.href) &&
                        regexes.any { it.containsMatchIn(a.text) }
                }
                if (row != null && !isSelfLink(row.href, hosts)) {
                    JpNet.emitOwnLink(site, row.href, "Episode $episode", null, fetchUrl, subtitleCallback, callback)
                    return
                }
                return
            }

            val external = JpHtml.anchors(html).mapNotNull { a ->
                val href = a.href
                if (!href.startsWith("http")) null
                else if (junk.containsMatchIn(href) || isSelfLink(href, hosts)) null
                else href
            }.distinct()

            val effectiveLabel = label.ifBlank { pageTitle }
            val quality = if (effectiveLabel.isBlank()) null else JpNet.getIndexQuality(effectiveLabel)
            coroutineScope {
                external.forEach { href ->
                    async(Dispatchers.IO) {
                        JpNet.emitOwnLink(site, href, effectiveLabel, quality, fetchUrl, subtitleCallback, callback)
                    }
                }
            }
        } catch (_: Exception) {
        }
    }
}

internal object JpVegaMovies {
    private const val DEFAULT_DOMAIN = "https://vegamovies.gallery"

    private fun headers(api: String): Map<String, String> = mapOf(
        "User-Agent" to PLAY_UA,
        "Accept" to "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language" to "en-US,en;q=0.9",
        "Cache-Control" to "no-cache",
        "cookie" to "xla=s4t",
        "Referer" to "$api/",
    )

    private suspend fun fetchResults(api: String, query: String): List<JSONObject> {
        val text = JpNet.retry {
            try {
                JpNet.get(
                    "$api/search.php?q=${JpNet.urlEncode(query)}",
                    "$api/", headers(api).filterKeys { it != "User-Agent" && it != "Referer" },
                )
            } catch (_: Exception) {
                null
            }
        } ?: return emptyList()
        return try {
            val hits = JSONObject(text).optJSONArray("hits") ?: return emptyList()
            (0 until hits.length()).mapNotNull { hits.optJSONObject(it)?.optJSONObject("document") }
        } catch (_: Exception) {
            emptyList()
        }
    }

    private fun pickDoc(docs: List<JSONObject>, res: JpPlayLink): JSONObject? {
        val title = res.title ?: return null
        docs.firstOrNull { res.imdbId != null && it.optString("imdb_id").equals(res.imdbId, ignoreCase = true) }
            ?.let { return it }
        val matched = docs.filter { JpNet.titleMatches(it.optString("post_title"), title) }
        if (res.season != null) {
            val seasonPosts = matched.filter { JpNet.seasonsOf(it.optString("post_title"))?.contains(res.season) == true }
            if (seasonPosts.isNotEmpty()) {
                return seasonPosts.firstOrNull { JpNet.yearMatches(it.optString("post_title"), res.matchYear) } ?: seasonPosts.first()
            }
            return matched.firstOrNull { JpNet.seasonsOf(it.optString("post_title")) == null }
        }
        return matched.firstOrNull { JpNet.yearMatches(it.optString("post_title"), res.matchYear) }
            ?: matched.firstOrNull()
    }

    private fun collectRows(html: String, season: Int?): List<Pair<String, String>> {
        val seasonRegex = season?.let { Regex("(?i)Season\\s*$it(?!\\d)|\\bS${it.toString().padStart(2, '0')}\\b") }
        val hostHints = listOf(
            "nexdrive", "mobilejsr", "fastdl", "vcloud", "hubcloud", "gdflix",
            "gdlink", "gdtot", "filebee", "filepress", "pixeldrain", "gofile",
            "dropgalaxy", "hubdrive", "hubcdn", "hblinks",
        )
        val targets = mutableListOf<Pair<String, String>>()
        for (sec in JpHtml.sections(html, "h3|h4|h5")) {
            if (seasonRegex != null && !seasonRegex.containsMatchIn(sec.heading)) continue
            for (a in JpHtml.anchors(sec.body)) {
                val href = a.href
                if (!href.startsWith("http")) continue
                if (a.text.contains("Batch", true) || a.text.contains("Zip", true)) continue
                if (a.attrs.contains("dwd-button") ||
                    a.text.contains("V-Cloud", true) || a.text.contains("G-Direct", true) ||
                    hostHints.any { href.contains(it, true) }
                ) {
                    targets.add(href to sec.heading)
                }
            }
        }
        return targets.distinctBy { it.first }
    }

    suspend fun invoke(
        res: JpPlayLink,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ) {
        try {
            val api = JpDomains.get("justplay_vegamovies") ?: JpDomains.get("vegamovies") ?: DEFAULT_DOMAIN
            val title = res.title ?: return
            val imdbId = res.imdbId?.takeIf { it.isNotBlank() }

            val docs = if (imdbId != null) {
                val byImdb = fetchResults(api, imdbId)
                val imdbMatch = byImdb.firstOrNull { it.optString("imdb_id").equals(imdbId, ignoreCase = true) }
                if (imdbMatch != null) listOf(imdbMatch) else fetchResults(api, title)
            } else {
                fetchResults(api, title)
            }
            if (docs.isEmpty()) return
            val target = pickDoc(docs, res) ?: return
            val permalink = target.optString("permalink").takeIf { it.isNotBlank() } ?: return
            val postUrl = if (permalink.startsWith("http")) permalink else api + permalink
            val html = JpNet.get(postUrl, api, headers(api).filterKeys { it != "User-Agent" && it != "Referer" }) ?: return

            val rows = collectRows(html, res.season)
            if (res.season == null) {
                coroutineScope {
                    val gate = Semaphore(4)
                    rows.forEach { (link, label) ->
                        async(Dispatchers.IO) {
                            gate.acquire()
                            try {
                                JpDrivePages.emit("vegamovies", link, null, null, label, api, api, subtitleCallback, callback)
                            } finally {
                                gate.release()
                            }
                        }
                    }
                }
            } else {
                rows.forEach { (link, label) ->
                    JpDrivePages.emit("vegamovies", link, res.episode, res.season, label, api, api, subtitleCallback, callback)
                }
            }
        } catch (_: Exception) {
        }
    }
}

internal object JpHdHub4u {
    private const val DEFAULT_DOMAIN = "https://new6.hdhub4u.cl"

    private suspend fun typesenseSearch(domain: String, query: String): List<Pair<String, String>> {
        return try {
            val url = "https://search.pingora.fyi/collections/post/documents/search" +
                "?q=${JpNet.urlEncode(query)}" +
                "&query_by=post_title,category&query_by_weights=4,2" +
                "&sort_by=sort_by_date:desc&limit=20&highlight_fields=none&use_cache=true&page=1"
            val text = JpNet.retry {
                try {
                    HikariNet.getString(url, mapOf("User-Agent" to PLAY_UA, "Referer" to "$domain/"))
                } catch (_: Exception) {
                    null
                }
            } ?: return emptyList()
            val hits = JSONObject(text).optJSONArray("hits") ?: return emptyList()
            (0 until hits.length()).mapNotNull { i ->
                val d = hits.optJSONObject(i)?.optJSONObject("document") ?: return@mapNotNull null
                val postTitle = d.optString("post_title")
                val permalink = d.optString("permalink")
                if (postTitle.isBlank() || permalink.isBlank()) return@mapNotNull null
                val url2 = if (permalink.startsWith("http", true)) {
                    val path = try {
                        java.net.URI(permalink).path
                    } catch (_: Exception) {
                        null
                    }
                    if (path.isNullOrBlank()) permalink else domain + path
                } else {
                    domain + permalink
                }
                postTitle to url2
            }
        } catch (_: Exception) {
            emptyList()
        }
    }

    private suspend fun emitResolved(
        url: String,
        label: String,
        referer: String?,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ) {
        val target = if (url.contains("id=")) JpNet.decryptIdLink(url, referer) ?: return else url
        if (target.isBlank()) return
        JpNet.emitOwnLink("hdhub4u", target, label, JpNet.getIndexQuality(label), referer, subtitleCallback, callback)
    }

    private suspend fun processPost(
        domain: String,
        postUrl: String,
        res: JpPlayLink,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ) {
        try {
            val html = JpNet.get(postUrl, "$domain/", JpNet.headers("$domain/")) ?: return
            if (res.season != null) {
                val epRegex = Regex("(?i)episode\\s*(\\d+)")
                for (sec in JpHtml.sections(html, "h3")) {
                    val links = JpHtml.anchors(sec.body)
                    val epLink = links.firstOrNull { it.text.contains("episode", true) } ?: continue
                    val epNum = epRegex.find(epLink.text)?.groupValues?.get(1)?.toIntOrNull() ?: continue
                    if (res.episode != null && epNum != res.episode) continue
                    val watch = links.firstOrNull { it.text.trim().equals("watch", true) }
                    if (watch != null) {
                        val watchHref = JpNet.absolute(watch.href, domain)
                        if (watchHref.startsWith("http")) {
                            JpNet.emitOwnLink("hdhub4u", watchHref, "Episode $epNum Watch", null, domain, subtitleCallback, callback)
                        }
                    }
                    val href = JpNet.absolute(epLink.href, domain)
                    if (!href.startsWith("http")) continue
                    emitResolved(href, "Episode $epNum", domain, subtitleCallback, callback)
                }
            } else {
                for (sec in JpHtml.sections(html, "h3|h4")) {
                    for (a in JpHtml.anchors(sec.body)) {
                        if (!Regex("(?i)480|720|1080|2160|4K").containsMatchIn(a.text)) continue
                        val href = JpNet.absolute(a.href, domain)
                        if (!href.startsWith("http")) continue
                        emitResolved(href, a.text.trim(), domain, subtitleCallback, callback)
                    }
                }
            }
        } catch (_: Exception) {
        }
    }

    suspend fun invoke(
        res: JpPlayLink,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ) {
        try {
            val domain = JpDomains.get("justplay_hdhub4u") ?: JpDomains.get("hdhub4u") ?: DEFAULT_DOMAIN
            val title = res.title ?: return
            val posts = typesenseSearch(domain, title)
            if (posts.isEmpty()) return

            val titleMatched = posts.filter { (postTitle, _) -> JpNet.titleMatches(postTitle, title) }
            val chosen = if (res.season != null) {
                val seasonPosts = titleMatched.filter { (postTitle, _) ->
                    JpNet.seasonsOf(postTitle)?.contains(res.season) == true
                }
                if (seasonPosts.isNotEmpty()) seasonPosts
                else titleMatched.filter { (postTitle, _) -> JpNet.seasonsOf(postTitle) == null }
            } else {
                titleMatched.filter { (postTitle, _) -> JpNet.yearMatches(postTitle, res.matchYear) }
                    .ifEmpty { titleMatched }
            }.take(2)
            coroutineScope {
                chosen.forEach { (_, postUrl) ->
                    async(Dispatchers.IO) {
                        processPost(domain, postUrl, res, subtitleCallback, callback)
                    }
                }
            }
        } catch (_: Exception) {
        }
    }
}

internal object Jp4kHub {
    private const val DEFAULT_DOMAIN = "https://4khdhub.one"

    private suspend fun emitLinks(
        hrefs: List<String>,
        label: String,
        domain: String,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ) {
        val gate = Semaphore(6)
        coroutineScope {
            hrefs.distinct().forEach { href ->
                async(Dispatchers.IO) {
                    gate.acquire()
                    try {
                        if (!href.startsWith("http")) return@async
                        val target = if (href.contains("id=")) JpNet.decryptIdLink(href, domain) ?: return@async else href
                        if (target.isNotBlank()) {
                            JpNet.emitOwnLink("4khdhub", target, label, JpNet.getIndexQuality(label), domain, subtitleCallback, callback)
                        }
                    } finally {
                        gate.release()
                    }
                }
            }
        }
    }

    private fun blockLabel(block: String): String {
        Regex("""<[^>]*class="[^"]*episode-file-title[^"]*"[^>]*>(.*?)</[^>]+>""", RegexOption.DOT_MATCHES_ALL)
            .find(block)?.groupValues?.get(1)?.let { JpHtml.strip(it) }
            ?.takeIf { it.isNotBlank() }?.let { return it }
        Regex("""<div[^>]*class="[^"]*file-title[^"]*"[^>]*>(.*?)</div>""", RegexOption.DOT_MATCHES_ALL)
            .find(block)?.groupValues?.get(1)?.let { JpHtml.strip(it) }
            ?.takeIf { it.isNotBlank() }?.let { return it }
        Regex(
            """<div[^>]*class="[^"]*(?:download-header|episode-header)[^"]*"[^>]*>(.*?)</div>""",
            RegexOption.DOT_MATCHES_ALL,
        ).find(block)?.groupValues?.get(1)?.let { JpHtml.strip(it) }
            ?.takeIf { it.isNotBlank() }?.let { return it }
        return ""
    }

    suspend fun invoke(
        res: JpPlayLink,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ) {
        try {
            val domain = JpDomains.get("justplay_4khdhub") ?: JpDomains.get("4khdhub") ?: DEFAULT_DOMAIN
            val title = res.title ?: return

            val searchHtml = JpNet.retry {
                try {
                    JpNet.fetchPage("$domain/?s=${JpNet.urlEncode(title)}")
                } catch (_: Exception) {
                    null
                }
            } ?: return
            val cards = Regex(
                """<a[^>]*class="[^"]*movie-card[^"]*"[^>]*>(.*?)</a>""",
                setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
            ).findAll(searchHtml).toList()
            fun contentOf(card: String): String =
                Regex(
                    """<div[^>]*class="[^"]*movie-card-content[^"]*"[^>]*>(.*?)</div>""",
                    setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
                ).find(card)?.groupValues?.get(1)?.let { JpHtml.strip(it) }?.lowercase() ?: ""
            val matched = cards.firstOrNull { c ->
                val content = contentOf(c.value)
                JpNet.titleMatches(content, title) && JpNet.yearMatches(content, res.year)
            } ?: cards.firstOrNull { c -> JpNet.titleMatches(contentOf(c.value), title) } ?: return

            val href = Regex("""href="([^"]+)"""", RegexOption.IGNORE_CASE)
                .find(matched.value.substringBefore(">"))?.groupValues?.get(1)?.trim()
                ?: Regex("""<a[^>]*href="([^"]+)"""", RegexOption.IGNORE_CASE).find(matched.value)?.groupValues?.get(1)?.trim()
                ?: return
            val detailUrl = if (href.startsWith("http")) href else domain + href
            val html = JpNet.fetchPage(detailUrl, domain) ?: return

            if (res.season != null) {
                val seasonText = "S" + res.season.toString().padStart(2, '0')
                val epRegex = if (res.episode != null) {
                    Regex("(?i)${seasonText}\\s*[xXeE]\\s*0*${res.episode}(?!\\d)")
                } else {
                    Regex("(?i)\\b$seasonText(?!\\d)")
                }
                val items = Regex(
                    """<div[^>]*class="[^"]*episode-download-item[^"]*"[^>]*>(.*?)</div>\s*(?=<div[^>]*class="[^"]*episode-download-item|<div[^>]*class="[^"]*footer|$)""",
                    setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
                ).findAll(html).map { it.groupValues[1] }.filter { block ->
                    epRegex.containsMatchIn(block.replace('\u00A0', ' '))
                }.toList()
                val hrefs = items.flatMap { block ->
                    Regex(
                        """<div[^>]*class="[^"]*episode-links[^"]*"[^>]*>(.*?)</div>""",
                        setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
                    ).findAll(block).flatMap { JpHtml.anchors(it.groupValues[1]) }.toList()
                        .ifEmpty { JpHtml.anchors(block) }
                        .map { it.href.trim() }
                }
                val label = items.map { blockLabel(it) }.firstOrNull { it.isNotBlank() }?.let { l ->
                    res.episode?.let { "S${res.season}E$it - $l" } ?: l
                } ?: "S${res.season}${res.episode?.let { "E$it" } ?: ""}"
                emitLinks(hrefs, label, domain, subtitleCallback, callback)
            } else {
                val items = Regex(
                    """<div[^>]*class="[^"]*download-item[^"]*"[^>]*>(.*?)</div>\s*(?=<div[^>]*class="[^"]*download-item|<div[^>]*class="[^"]*footer|$)""",
                    setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
                ).findAll(html).map { it.groupValues[1] }.toList()
                if (items.isNotEmpty()) {
                    val pageTitle = Regex("""<title>([^<]*)""", RegexOption.IGNORE_CASE)
                        .find(html)?.groupValues?.get(1).orEmpty()
                    items.forEach { block ->
                        val hrefs = JpHtml.anchors(block).map { it.href.trim() }
                        val label = blockLabel(block).ifBlank { pageTitle }
                        emitLinks(hrefs, label, domain, subtitleCallback, callback)
                    }
                } else {
                    val hrefs = JpHtml.anchors(html)
                        .filter { it.attrs.contains("download-item") || it.href.contains("id=") }
                        .map { it.href.trim() }
                    val pageTitle = Regex("""<title>([^<]*)""", RegexOption.IGNORE_CASE)
                        .find(html)?.groupValues?.get(1).orEmpty()
                    emitLinks(hrefs, pageTitle, domain, subtitleCallback, callback)
                }
            }
        } catch (_: Exception) {
        }
    }
}

internal object JpMovies4u {
    private const val DEFAULT_DOMAIN = "https://new1.movies4u.garden"
    private const val DOMAIN_REGISTRY = "https://raw.githubusercontent.com/phisher98/TVVVV/refs/heads/main/domains.json"
    private val driveHostRegex = Regex("mdrive\\.cloud|nexdrive\\.fit|mobilejsr\\.rest|mdisk")
    private val junk = Regex("winexch|a-ads|tinyurl|t\\.me/|/tg/|telegram|googleapis|googletagmanager|schema\\.org|w3\\.org")

    private suspend fun candidates(): List<String> {
        val list = mutableListOf<String>()
        JpDomains.get("justplay_movies4u")?.let { list.add(it) }
        JpDomains.get("movies4u")?.let { list.add(it) }
        JpDomains.registry(DOMAIN_REGISTRY, "movies4u")?.let { list.add(it) }
        list.add(DEFAULT_DOMAIN)
        return list.distinct()
    }

    private suspend fun searchPosts(domain: String, query: String): List<Pair<String, String>>? {
        val text = JpNet.retry {
            try {
                JpNet.get(
                    "$domain/lookup.php?q=${JpNet.urlEncode(query)}&page=1&per_page=30",
                    domain, JpNet.headers(),
                )
            } catch (_: Exception) {
                null
            }
        } ?: return null
        return try {
            val hits = JSONObject(text).optJSONArray("hits") ?: return emptyList()
            (0 until hits.length()).mapNotNull { i ->
                val hit = hits.optJSONObject(i) ?: return@mapNotNull null
                val postTitle = hit.optString("post_title")
                val permalink = hit.optString("permalink")
                if (postTitle.isBlank() || permalink.isBlank()) null
                else postTitle to JpNet.absolute(permalink, domain)
            }
        } catch (_: Exception) {
            null
        }
    }

    private suspend fun findPosts(title: String): List<Pair<String, String>> {
        for (domain in candidates()) {
            val posts = searchPosts(domain, title) ?: continue
            return posts.map { (postTitle, url) ->
                postTitle to url.replaceFirst(Regex("^[^/]*//[^/]+"), domain)
            }
        }
        return emptyList()
    }

    private suspend fun getPage(url: String): String? = try {
        JpNet.get(url, JpNet.getBaseUrl(url), JpNet.headers(JpNet.getBaseUrl(url)))
    } catch (_: Exception) {
        null
    }

    private fun qualityBlocks(html: String): List<Pair<String, String>> {
        val out = mutableListOf<Pair<String, String>>()
        for (m in Regex(
            """<div[^>]*class="[^"]*downloads-btns-div[^"]*"[^>]*>(.*?)</div>""",
            setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
        ).findAll(html)) {
            val block = m.groupValues[1]
            val idx = m.range.first
            var label = ""
            val before = html.substring(maxOf(0, idx - 2000), idx)
            val heads = Regex("""<(h2|h3|h4)[^>]*>(.*?)</\1>""", setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE))
                .findAll(before).toList()
            if (heads.isNotEmpty()) {
                label = JpHtml.strip(heads.last().groupValues[2])
            }
            for (a in JpHtml.anchors(block)) {
                val href = a.href
                if (!href.startsWith("http")) continue
                if (a.text.contains("Batch", true) || a.text.contains("Zip", true)) continue
                if (junk.containsMatchIn(href)) continue
                out.add(href to label)
            }
        }
        return out.distinctBy { it.first }
    }

    private suspend fun emitM4uLinks(
        linksUrl: String,
        label: String,
        episode: Int?,
        season: Int?,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ) {
        val html = getPage(linksUrl) ?: return
        val quality = if (label.isBlank()) null else JpNet.getIndexQuality(label)

        val rows = mutableListOf<Pair<String, String>>()
        if (episode != null) {
            val epRegex = Regex("(?i)Episodes?\\s*:\\s*0*$episode(?!\\d)")
            for (sec in JpHtml.sections(html, "h5")) {
                if (!epRegex.containsMatchIn(sec.heading)) continue
                for (a in JpHtml.anchors(sec.body)) {
                    if (a.href.startsWith("http") && !junk.containsMatchIn(a.href)) {
                        rows.add(a.href to label)
                    }
                }
            }
        }
        if (rows.isEmpty()) {
            for ((href, headLabel) in qualityBlocks(html)) {
                rows.add(href to headLabel.ifBlank { label })
            }
        }
        if (rows.isEmpty()) {
            for (m in Regex(
                """<div[^>]*class="[^"]*downloads-btns-div[^"]*"[^>]*>(.*?)</div>""",
                setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
            ).findAll(html)) {
                for (a in JpHtml.anchors(m.groupValues[1])) {
                    if (!a.href.startsWith("http")) continue
                    if (a.text.contains("Batch", true) || a.text.contains("Zip", true)) continue
                    if (junk.containsMatchIn(a.href)) continue
                    rows.add(a.href to label)
                }
            }
        }

        coroutineScope {
            rows.distinctBy { it.first }.forEach { (href, rowLabel) ->
                async(Dispatchers.IO) {
                    val rowQuality = if (rowLabel.isBlank()) quality else JpNet.getIndexQuality(rowLabel)
                    emitSource(href, rowLabel, rowQuality, linksUrl, season, subtitleCallback, callback)
                }
            }
        }
    }

    private suspend fun emitSource(
        url: String,
        label: String,
        quality: Int?,
        referer: String?,
        season: Int?,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ) {
        val host = JpNet.hostOf(url)
        when {
            host.contains("m4ulinks") ->
                emitM4uLinks(url, label, null, season, subtitleCallback, callback)
            driveHostRegex.containsMatchIn(url) ->
                JpDrivePages.emit("movies4u", url, null, season, label, null, referer, subtitleCallback, callback)
            else ->
                JpNet.emitOwnLink("movies4u", url, label, quality, referer, subtitleCallback, callback)
        }
    }

    suspend fun invoke(
        res: JpPlayLink,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ) {
        try {
            val title = res.title ?: return
            val posts = findPosts(title)
            if (posts.isEmpty()) return

            val matched = posts.filter { (postTitle, _) -> JpNet.titleMatches(postTitle, title) }
            val chosen = if (res.season != null) {
                val seasonPosts = matched.filter { (postTitle, _) ->
                    JpNet.seasonsOf(postTitle)?.contains(res.season) == true
                }
                if (seasonPosts.isNotEmpty()) seasonPosts
                else matched.filter { (postTitle, _) -> JpNet.seasonsOf(postTitle) == null }
            } else {
                matched.filter { (postTitle, _) -> JpNet.yearMatches(postTitle, res.matchYear) }
                    .ifEmpty { matched }
            }.take(2)
            if (chosen.isEmpty()) return

            chosen.forEach { (_, postUrl) ->
                try {
                    val html = getPage(postUrl) ?: return@forEach
                    if (res.season == null) {
                        val blocks = qualityBlocks(html)
                        coroutineScope {
                            blocks.forEach { (href, label) ->
                                async(Dispatchers.IO) {
                                    emitSource(href, label, JpNet.getIndexQuality(label), postUrl, null, subtitleCallback, callback)
                                }
                            }
                        }
                    } else {
                        val seasonRegex = Regex("(?i)Season\\s*${res.season}(?!\\d)")
                        val blocks = qualityBlocks(html).filter { (_, label) ->
                            seasonRegex.containsMatchIn(label)
                        }.ifEmpty {
                            qualityBlocks(html).filter { (_, label) -> JpNet.seasonsOf(label) == null }
                        }
                        blocks.forEach { (href, label) ->
                            emitM4uLinks(href, label, res.episode, res.season, subtitleCallback, callback)
                        }
                    }
                } catch (_: Exception) {
                }
            }
        } catch (_: Exception) {
        }
    }
}
