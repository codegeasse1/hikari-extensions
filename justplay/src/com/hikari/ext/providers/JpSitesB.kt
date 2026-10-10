package com.hikari.ext.providers

import com.hikari.ext.HikariStream
import com.hikari.ext.HikariSubtitle
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.json.JSONArray
import org.json.JSONObject
import java.security.MessageDigest
import java.util.concurrent.Semaphore
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

/**
 * Shared PlayLink model (port of the source's PlayLinkData) + the four
 * JustPlay site backends that were still missing: MoviesDrive, HindMoviez,
 * NetNaija and NetMirror. Regex-based (no jsoup), Hikari types throughout.
 */
internal data class JpPlayLink(
    val id: Int? = null,
    val imdbId: String? = null,
    val type: String? = null,
    val season: Int? = null,
    val episode: Int? = null,
    val title: String? = null,
    val orgTitle: String? = null,
    val year: Int? = null,
    val showYear: Int? = null,
    val isMovie: Boolean = false,
) {
    val matchYear: Int?
        get() = if (season != null) (showYear ?: year) else year
}

// ---------------------------------------------------------------------------
// MoviesDrive
// ---------------------------------------------------------------------------

internal object JpMoviesDrive {
    private const val DEFAULT_DOMAIN = "https://new1.moviesdrive.beer"
    private const val DOMAIN_REGISTRY =
        "https://raw.githubusercontent.com/SaurabhKaperwan/Utils/refs/heads/main/urls.json"

    private val driveLink = Regex("hubcloud|gdflix|gdlink", RegexOption.IGNORE_CASE)
    private val packText = Regex("""(?i)\b(zip|rar|7z|batch)\b""")

    private data class DrivePost(val title: String, val url: String, val imdbId: String)

    private suspend fun candidates(): List<String> {
        val list = mutableListOf<String>()
        JpDomains.get("justplay_moviesdrive")?.let { list.add(it) }
        JpDomains.get("moviesdrive")?.let { list.add(it) }
        JpDomains.registry(DOMAIN_REGISTRY, "moviesdrive")?.let { list.add(it) }
        list.add(DEFAULT_DOMAIN)
        return list.distinct()
    }

    private suspend fun searchPosts(domain: String, query: String): List<DrivePost> {
        val text = JpNet.retry {
            try {
                JpNet.get(
                    "$domain/search.php?q=${JpNet.urlEncode(query)}",
                    domain, JpNet.headers(),
                )
            } catch (_: Exception) {
                null
            }
        } ?: return emptyList()
        return try {
            val hits = JSONObject(text).optJSONArray("hits") ?: return emptyList()
            (0 until hits.length()).mapNotNull { i ->
                val d = hits.optJSONObject(i)?.optJSONObject("document") ?: return@mapNotNull null
                val postTitle = d.optString("post_title")
                val permalink = d.optString("permalink")
                if (postTitle.isBlank() || permalink.isBlank()) return@mapNotNull null
                DrivePost(postTitle, JpNet.absolute(permalink, domain), d.optString("imdb_id"))
            }
        } catch (_: Exception) {
            emptyList()
        }
    }

    private fun pickPost(posts: List<DrivePost>, res: JpPlayLink): DrivePost? {
        val title = res.title ?: return null
        res.imdbId?.takeIf { it.isNotBlank() }?.let { id ->
            posts.firstOrNull { it.imdbId.equals(id, ignoreCase = true) }?.let { return it }
        }
        val matched = posts.filter { JpNet.titleMatches(it.title, title) }
        if (res.season != null) {
            val seasonPosts = matched.filter { JpNet.seasonsOf(it.title)?.contains(res.season) == true }
            if (seasonPosts.isNotEmpty()) {
                return seasonPosts.firstOrNull { JpNet.yearMatches(it.title, res.matchYear) }
                    ?: seasonPosts.first()
            }
            return matched.firstOrNull { JpNet.seasonsOf(it.title) == null }
        }
        return matched.firstOrNull { JpNet.yearMatches(it.title, res.matchYear) }
            ?: matched.firstOrNull()
    }

    private suspend fun getPage(url: String): String? = try {
        JpNet.get(url, JpNet.getBaseUrl(url), JpNet.headers(JpNet.getBaseUrl(url)))
    } catch (_: Exception) {
        null
    }

    private data class H5(val text: String, val inner: String, val hasLink: Boolean, val start: Int)

    private fun h5Blocks(html: String): List<H5> =
        Regex(
            """<h5[^>]*>(.*?)</h5>""",
            setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
        ).findAll(html).map { m ->
            val inner = m.groupValues[1]
            H5(
                JpHtml.strip(inner),
                inner,
                inner.contains("<a", ignoreCase = true),
                m.range.first,
            )
        }.toList()

    private fun anchorsOf(fragment: String): List<String> =
        JpHtml.anchors(fragment).map { it.href.trim() }.filter { it.startsWith("http") }.distinct()

    private suspend fun emitDriveLinks(
        archiveUrl: String,
        label: String,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ) {
        val html = getPage(archiveUrl) ?: return
        val quality = if (label.isBlank()) null else JpNet.getIndexQuality(label)
        val hrefs = anchorsOf(html).filter { driveLink.containsMatchIn(it) }
        coroutineScope {
            hrefs.forEach { href ->
                async(Dispatchers.IO) {
                    JpNet.emitOwnLink("moviesdrive", href, label, quality, archiveUrl, subtitleCallback, callback)
                }
            }
        }
    }

    private suspend fun emitMovie(
        postUrl: String,
        html: String,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ) {
        val gate = Semaphore(4)
        val rows = Regex(
            """<h5[^>]*>\s*<a[^>]*href="([^"]+)"[^>]*>(.*?)</a>\s*</h5>""",
            setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
        ).findAll(html).mapNotNull { m ->
            val href = m.groupValues[1].trim()
            val label = JpHtml.strip(m.groupValues[2])
            if (href.isBlank() || !href.startsWith("http")) null
            else if (packText.containsMatchIn(label)) null
            else href to label
        }.toList()
        coroutineScope {
            rows.forEach { (href, label) ->
                async(Dispatchers.IO) {
                    gate.acquire()
                    try {
                        if (driveLink.containsMatchIn(href)) {
                            JpNet.emitOwnLink(
                                "moviesdrive", href, label,
                                JpNet.getIndexQuality(label), postUrl, subtitleCallback, callback,
                            )
                        } else {
                            emitDriveLinks(href, label, subtitleCallback, callback)
                        }
                    } finally {
                        gate.release()
                    }
                }
            }
        }
    }

    private suspend fun emitEpisode(
        archiveUrl: String,
        season: Int,
        episode: Int,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ) {
        val html = getPage(archiveUrl) ?: return
        val epRe = Regex(
            "(?i)\\bE(?:p(?:isode)?)?\\.?\\s*0*$episode(?!\\d)|" +
                "\\bS0*$season\\s*[xXeE]\\s*0*$episode(?!\\d)",
        )
        val boundRe = Regex("(?i)\\bep?(?:isode)?\\s*\\d+|\\bS\\d+\\s*[xXeE]\\s*\\d+")
        val blocks = h5Blocks(html)
        val headIdx = blocks.indexOfFirst { epRe.containsMatchIn(it.text) }
        if (headIdx < 0) return
        val label = blocks[headIdx].text
        val quality = if (label.isBlank()) null else JpNet.getIndexQuality(label)
        val hrefs = mutableListOf<String>()
        for (i in headIdx until blocks.size) {
            if (i > headIdx && boundRe.containsMatchIn(blocks[i].text) &&
                !epRe.containsMatchIn(blocks[i].text)
            ) break
            hrefs.addAll(anchorsOf(blocks[i].inner))
            if (hrefs.isNotEmpty() && i > headIdx) break
        }
        // also sweep anchors sitting between the heading and the next h5
        if (hrefs.isEmpty()) {
            val from = html.indexOf("</h5>", blocks[headIdx].start) + 5
            val to = blocks.getOrNull(headIdx + 1)?.start ?: html.length
            hrefs.addAll(anchorsOf(html.substring(from.coerceAtLeast(0), to.coerceAtLeast(from.coerceAtLeast(0)))))
        }
        coroutineScope {
            hrefs.distinct().forEach { href ->
                async(Dispatchers.IO) {
                    JpNet.emitOwnLink("moviesdrive", href, label, quality, archiveUrl, subtitleCallback, callback)
                }
            }
        }
    }

    suspend fun invoke(
        res: JpPlayLink,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ) {
        try {
            val title = res.title ?: return
            val imdbId = res.imdbId?.takeIf { it.isNotBlank() }
            var posts = emptyList<DrivePost>()
            for (domain in candidates()) {
                val byId = if (imdbId != null) searchPosts(domain, imdbId) else emptyList()
                val target = byId.ifEmpty { searchPosts(domain, title) }
                if (target.isNotEmpty()) {
                    posts = target.map { it.copy(url = it.url.replaceFirst(Regex("^[^/]*//[^/]+"), domain)) }
                    break
                }
            }
            if (posts.isEmpty()) return
            val chosen = pickPost(posts, res) ?: return
            val html = getPage(chosen.url) ?: return
            if (res.season == null) {
                emitMovie(chosen.url, html, subtitleCallback, callback)
            } else {
                val wanted = Regex(
                    "(?i)Season\\s*${res.season}(?!\\d)|\\bS${res.season.toString().padStart(2, '0')}\\b",
                )
                val gate = Semaphore(4)
                // season heading = h5 without a link; the archive link is the
                // first anchor after it
                val blocks = h5Blocks(html)
                coroutineScope {
                    blocks.filter { !it.hasLink && wanted.containsMatchIn(it.text) }.forEach { head ->
                        val after = html.substring(head.start)
                        val href = Regex(
                            """<a[^>]*href="(http[^"]+)"[^>]*>""",
                            RegexOption.IGNORE_CASE,
                        ).find(after)?.groupValues?.get(1)?.trim() ?: return@forEach
                        if (packText.containsMatchIn(head.text)) return@forEach
                        async(Dispatchers.IO) {
                            gate.acquire()
                            try {
                                emitEpisode(href, res.season, res.episode ?: return@async, subtitleCallback, callback)
                            } finally {
                                gate.release()
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
// HindMoviez
// ---------------------------------------------------------------------------

internal object JpHindMoviez {
    private const val DEFAULT_DOMAIN = "https://hindmovie.dev"
    private const val DOMAIN_REGISTRY =
        "https://raw.githubusercontent.com/SaurabhKaperwan/Utils/refs/heads/main/urls.json"

    private val shareSecret: ByteArray by lazy {
        try {
            android.util.Base64.decode(
                "NWU5NjA4NWM1NmUwZjU0ZWRhNjU3NzkwYWM1OGQxOWIyNzE0NzljNTA0MzY3ZmM5ZTZhNmMzM2YxZjgyNGU2Yg==",
                android.util.Base64.DEFAULT,
            )
        } catch (_: Exception) {
            ByteArray(0)
        }
    }

    private suspend fun candidates(): List<String> {
        val list = mutableListOf<String>()
        JpDomains.get("justplay_hindmoviez")?.let { list.add(it) }
        JpDomains.get("hindmoviez")?.let { list.add(it) }
        JpDomains.registry(DOMAIN_REGISTRY, "hindmoviez")?.let { list.add(it) }
        list.add(DEFAULT_DOMAIN)
        return list.distinct()
    }

    private fun base64Url(input: String): String =
        android.util.Base64.encodeToString(
            input.toByteArray(),
            android.util.Base64.URL_SAFE or android.util.Base64.NO_PADDING or android.util.Base64.NO_WRAP,
        )

    private fun b64d(s: String): String? = try {
        String(android.util.Base64.decode(s.trim(), android.util.Base64.DEFAULT), Charsets.UTF_8)
    } catch (_: Exception) {
        null
    }

    private fun signShare(rawId: String, domain: String): String {
        val t = System.currentTimeMillis() / 1000
        val encoded = base64Url(rawId)
        val mac = Mac.getInstance("HmacSHA256")
        mac.init(SecretKeySpec(shareSecret, "HmacSHA256"))
        val signature = mac.doFinal("$encoded|$t".toByteArray())
            .joinToString("") { "%02x".format(it) }
            .substring(0, 16)
        return "$domain/r.php?d=${JpNet.urlEncode(encoded)}&t=$t&s=$signature"
    }

    private fun wrappedDirectUrl(href: String): String? {
        return try {
            val outer = href.substringAfter("url=")
            val layer1 = b64d(outer) ?: return null
            val inner = layer1.substringAfter("url=").substringBefore("&")
            b64d(inner)?.trim()?.takeIf { it.startsWith("http") }
        } catch (_: Exception) {
            null
        }
    }

    private suspend fun searchPosts(domain: String, query: String): List<Pair<String, String>> {
        val html = JpNet.retry {
            try {
                JpNet.get("$domain/?s=${JpNet.urlEncode(query)}", domain, JpNet.headers())
            } catch (_: Exception) {
                null
            }
        } ?: return emptyList()
        return try {
            Regex(
                """<h2[^>]*>(.*?)</h2>""",
                setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
            ).findAll(html).mapNotNull { h ->
                val a = Regex(
                    """<a[^>]*href="([^"]+)"[^>]*>(.*?)</a>""",
                    setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
                ).find(h.groupValues[1]) ?: return@mapNotNull null
                val title = JpHtml.strip(a.groupValues[2])
                val href = a.groupValues[1].trim()
                if (title.isBlank() || !href.startsWith("http")) null else title to href
            }.toList()
        } catch (_: Exception) {
            emptyList()
        }
    }

    private fun pickPost(posts: List<Pair<String, String>>, res: JpPlayLink): Pair<String, String>? {
        val title = res.title ?: return null
        val matched = posts.filter { JpNet.titleMatches(it.first, title) }
        if (res.season != null) {
            val seasonPosts = matched.filter { JpNet.seasonsOf(it.first)?.contains(res.season) == true }
            if (seasonPosts.isNotEmpty()) {
                return seasonPosts.firstOrNull { JpNet.yearMatches(it.first, res.matchYear) }
                    ?: seasonPosts.first()
            }
            return matched.firstOrNull { JpNet.seasonsOf(it.first) == null }
        }
        return matched.firstOrNull { JpNet.yearMatches(it.first, res.matchYear) }
            ?: matched.firstOrNull()
    }

    private data class MaxBtn(val href: String, val pos: Int)

    private fun maxButtons(html: String): List<MaxBtn> =
        Regex("""<a([^>]*)>""", RegexOption.IGNORE_CASE).findAll(html).mapNotNull { m ->
            val attrs = m.groupValues[1]
            if (!attrs.contains("maxbutton", ignoreCase = true)) return@mapNotNull null
            val href = Regex("""href="([^"]+)"""", RegexOption.IGNORE_CASE)
                .find(attrs)?.groupValues?.get(1)?.trim() ?: return@mapNotNull null
            if (!href.startsWith("http")) return@mapNotNull null
            MaxBtn(href, m.range.first)
        }.toList()

    private fun headingBefore(html: String, pos: Int): String {
        val before = html.substring(0, pos.coerceIn(0, html.length)).takeLast(3000)
        val heads = Regex(
            """<(h1|h2|h3|h4)[^>]*>(.*?)</\1>""",
            setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
        ).findAll(before).toList()
        return heads.lastOrNull()?.let { JpHtml.strip(it.groupValues[2]) }.orEmpty()
    }

    private suspend fun emitLink(
        url: String,
        info: String,
        referer: String,
        callback: (HikariStream) -> Unit,
    ) {
        if (!JpNet.alive(url, false, referer = referer)) return
        callback(
            HikariStream(
                name = JpLabels.buildLabel("hindmoviez", "", info),
                url = url,
                headers = mapOf("Referer" to referer),
            ),
        )
    }

    private suspend fun emitShareLinks(
        shareHref: String,
        heading: String,
        callback: (HikariStream) -> Unit,
    ) {
        val base = shareHref.substringBefore("/?id=")
        if (base == shareHref) return
        val rawId = shareHref.substringAfter("id=")
        val html = try {
            JpNet.get(signShare(rawId, base), base, JpNet.headers()) ?: return
        } catch (_: Exception) {
            return
        }
        val name = Regex(
            """<p[^>]*>\s*Name:\s*(.*?)</p>""",
            setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
        ).find(html)?.groupValues?.get(1)?.let { JpHtml.strip(it) }.orEmpty()
        val size = Regex(
            """<p[^>]*>\s*Size:\s*(.*?)</p>""",
            setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
        ).find(html)?.groupValues?.get(1)?.let { JpHtml.strip(it) }.orEmpty()
        val info = listOf(name, size).filter { it.isNotBlank() }.joinToString(" ").ifBlank { heading }

        val btnInfo = Regex(
            """<a[^>]*class="[^"]*btn-info[^"]*"[^>]*href="([^"]+)"[^>]*>""",
            RegexOption.IGNORE_CASE,
        ).find(html)?.groupValues?.get(1)?.trim()
            ?: Regex(
                """<a[^>]*href="([^"]+)"[^>]*class="[^"]*btn-info[^"]*"[^>]*>""",
                RegexOption.IGNORE_CASE,
            ).find(html)?.groupValues?.get(1)?.trim()
            ?: return
        if (!btnInfo.startsWith("http")) return

        val urls = mutableListOf<String>()
        val page = JpNet.fetchWithCf(btnInfo)
        if (page != null) {
            for (a in JpHtml.anchors(page)) {
                if (a.attrs.contains("button") && a.href.startsWith("http")) urls.add(a.href)
            }
        }
        if (urls.isEmpty()) wrappedDirectUrl(btnInfo)?.let { urls.add(it) }
        urls.distinct().forEach { emitLink(it, info, "$base/", callback) }
    }

    private suspend fun emitMovieLinks(linkUrl: String, heading: String, callback: (HikariStream) -> Unit) {
        val html = try {
            JpNet.get(linkUrl, JpNet.getBaseUrl(linkUrl), JpNet.headers()) ?: return
        } catch (_: Exception) {
            return
        }
        val shareHref = Regex(
            """<a[^>]*class="[^"]*get-link-btn[^"]*"[^>]*href="([^"]+)"[^>]*>""",
            RegexOption.IGNORE_CASE,
        ).find(html)?.groupValues?.get(1)?.trim()
            ?: Regex(
                """<a[^>]*href="([^"]+)"[^>]*class="[^"]*get-link-btn[^"]*"[^>]*>""",
                RegexOption.IGNORE_CASE,
            ).find(html)?.groupValues?.get(1)?.trim()
            ?: return
        if (shareHref.startsWith("http")) emitShareLinks(shareHref, heading, callback)
    }

    private suspend fun emitEpisodeLinks(linkUrl: String, episode: Int?, callback: (HikariStream) -> Unit) {
        if (episode == null) return
        val html = try {
            JpNet.get(linkUrl, JpNet.getBaseUrl(linkUrl), JpNet.headers()) ?: return
        } catch (_: Exception) {
            return
        }
        val epRe = Regex("(?i)Episode\\s*:?\\s*0*$episode(?!\\d)")
        val anchor = Regex(
            """<h3[^>]*>\s*<a[^>]*href="([^"]+)"[^>]*>(.*?)</a>""",
            setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
        ).findAll(html).firstOrNull { epRe.containsMatchIn(JpHtml.strip(it.groupValues[2])) }
            ?: return
        val shareHref = anchor.groupValues[1].trim()
        if (shareHref.startsWith("http")) emitShareLinks(shareHref, "", callback)
    }

    suspend fun invoke(
        res: JpPlayLink,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ) {
        try {
            val title = res.title ?: return
            val imdbId = res.imdbId?.takeIf { it.isNotBlank() }
            var posts = emptyList<Pair<String, String>>()
            for (domain in candidates()) {
                val byId = if (imdbId != null) searchPosts(domain, imdbId) else emptyList()
                val target = byId.ifEmpty { searchPosts(domain, title) }
                if (target.isNotEmpty()) {
                    posts = target
                    break
                }
            }
            if (posts.isEmpty()) return
            val chosen = pickPost(posts, res) ?: return
            val postHtml = try {
                JpNet.get(chosen.second, JpNet.getBaseUrl(chosen.second), JpNet.headers()) ?: return
            } catch (_: Exception) {
                return
            }
            val gate = Semaphore(3)
            coroutineScope {
                if (res.season == null) {
                    maxButtons(postHtml).forEach { b ->
                        async(Dispatchers.IO) {
                            gate.acquire()
                            try {
                                emitMovieLinks(b.href, headingBefore(postHtml, b.pos), callback)
                            } finally {
                                gate.release()
                            }
                        }
                    }
                } else {
                    val buttons = maxButtons(postHtml).map { b -> b to headingBefore(postHtml, b.pos) }
                    val picked = buttons.filter { (_, heading) ->
                        JpNet.seasonsOf(heading)?.contains(res.season) == true
                    }.ifEmpty { buttons.filter { (_, heading) -> JpNet.seasonsOf(heading) == null } }
                    picked.forEach { (b, _) ->
                        async(Dispatchers.IO) {
                            gate.acquire()
                            try {
                                emitEpisodeLinks(b.href, res.episode, callback)
                            } finally {
                                gate.release()
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
// NetNaija (BFF JSON API)
// ---------------------------------------------------------------------------

internal object JpNetNaija {
    private const val API = "https://h5-api.aoneroom.com"
    private const val BFF = "$API/wefeed-h5api-bff"
    private const val DEFAULT_SITE = "https://netnaija.film"
    private const val NA_UA =
        "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36"

    @Volatile
    private var token: String? = null
    private val tokenMutex = Mutex()

    private fun xClientToken(): String {
        val ts = System.currentTimeMillis() / 1000
        val reversed = ts.toString().reversed()
        val md5 = MessageDigest.getInstance("MD5").digest(reversed.toByteArray())
            .joinToString("") { "%02x".format(it) }
        return "$ts,$md5"
    }

    private fun baseHeaders(site: String): Map<String, String> = mapOf(
        "User-Agent" to NA_UA,
        "Accept" to "application/json",
        "Origin" to site,
        "Referer" to "$site/",
        "X-Request-Lang" to "en",
        "X-Client-Info" to """{"timezone":"Asia/Kolkata"}""",
    )

    private fun readToken(headers: Map<String, List<String>>) {
        try {
            val xUser = headers.entries.firstOrNull { it.key.equals("x-user", true) }
                ?.value?.firstOrNull() ?: return
            if (xUser.isBlank()) return
            JSONObject(xUser).optString("token").takeIf { it.isNotBlank() }?.let { token = it }
        } catch (_: Exception) {
        }
    }

    private suspend fun ensureToken(site: String): String? {
        token?.let { return it }
        return tokenMutex.withLock {
            token?.let { return@withLock it }
            try {
                val res = JpNet.request(
                    "GET", "$BFF/subject/trending?page=1&perPage=1",
                    referer = "$site/",
                    extraHeaders = baseHeaders(site) + mapOf("X-Client-Token" to xClientToken()),
                )
                res?.let { readToken(it.headers) }
                token
            } catch (_: Exception) {
                null
            }
        }
    }

    private suspend fun authHeaders(site: String, extra: Map<String, String> = emptyMap()): Map<String, String> {
        val h = baseHeaders(site).toMutableMap()
        val t = ensureToken(site)
        if (!t.isNullOrBlank()) {
            h["Cookie"] = "token=$t"
            h["Authorization"] = "Bearer $t"
        } else {
            h["X-Client-Token"] = xClientToken()
        }
        h.putAll(extra)
        return h
    }

    private fun dubLabel(lanName: String?, type: Int?, original: Boolean?): String {
        if (original == true) return "Original"
        val base = (lanName ?: "").substringBefore(" dub").substringBefore(" sub").trim()
        val pretty = when (base.lowercase()) {
            "ptbr" -> "PT-BR"
            "esla" -> "ES-LA"
            else -> base.replaceFirstChar { it.uppercase() }
        }
        if (pretty.isBlank()) return "Original"
        return if (type == 1) "$pretty Hardsub" else pretty
    }

    private data class NaSubject(
        val subjectId: String,
        val subjectType: Int,
        val title: String,
        val releaseDate: String,
        val detailPath: String,
        val dubs: List<JSONObject>,
    )

    private fun parseSubjects(arr: JSONArray?): List<NaSubject> {
        if (arr == null) return emptyList()
        val out = mutableListOf<NaSubject>()
        for (i in 0 until arr.length()) {
            val o = arr.optJSONObject(i) ?: continue
            val sid = o.optString("subjectId")
            if (sid.isBlank()) continue
            val dubs = mutableListOf<JSONObject>()
            o.optJSONArray("dubs")?.let { da ->
                for (j in 0 until da.length()) da.optJSONObject(j)?.let { dubs.add(it) }
            }
            out.add(
                NaSubject(
                    sid,
                    o.optInt("subjectType", 0),
                    o.optString("title"),
                    o.optString("releaseDate"),
                    o.optString("detailPath"),
                    dubs,
                ),
            )
        }
        return out
    }

    private fun pickSubject(items: List<NaSubject>, res: JpPlayLink): NaSubject? {
        val wantTv = res.season != null
        val title = res.title ?: return null
        val typeOk: (NaSubject) -> Boolean = {
            when (it.subjectType) {
                1 -> !wantTv
                2 -> wantTv
                else -> false
            }
        }
        fun yearOk(s: NaSubject): Boolean {
            val itemYear = s.releaseDate.take(4).toIntOrNull() ?: return true
            val want = res.matchYear ?: return true
            return kotlin.math.abs(itemYear - want) <= 1
        }
        return items.firstOrNull { typeOk(it) && yearOk(it) && JpNet.normalizeTitle(it.title) == JpNet.normalizeTitle(title) }
            ?: items.firstOrNull { typeOk(it) && yearOk(it) && JpNet.titleMatches(it.title, title) }
    }

    suspend fun invoke(
        res: JpPlayLink,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ) {
        try {
            val site = JpDomains.get("justplay_netnaija") ?: JpDomains.get("netnaija") ?: DEFAULT_SITE
            val title = res.title ?: return

            val searchRes = JpNet.retry {
                try {
                    val body = JSONObject()
                        .put("keyword", title)
                        .put("page", 1)
                        .put("perPage", 30).toString()
                    JpNet.postJson("$BFF/subject/search", body, "$site/", authHeaders(site))
                } catch (_: Exception) {
                    null
                }
            } ?: return
            readToken(searchRes.headers)
            val items = try {
                parseSubjects(JSONObject(searchRes.body).optJSONObject("data")?.optJSONArray("items"))
            } catch (_: Exception) {
                emptyList()
            }
            val subject = pickSubject(items, res) ?: return
            if (subject.detailPath.isBlank()) return

            val detailRes = JpNet.request(
                "GET", "$BFF/detail?detailPath=${JpNet.urlEncode(subject.detailPath)}",
                referer = "$site/", extraHeaders = authHeaders(site),
            ) ?: return
            readToken(detailRes.headers)
            val detailData = try {
                JSONObject(detailRes.body).optJSONObject("data")
            } catch (_: Exception) {
                null
            } ?: return
            val subj = detailData.optJSONObject("subject") ?: return
            if (res.season != null) {
                val seasons = detailData.optJSONObject("resource")?.optJSONArray("seasons")
                var ok = false
                if (seasons != null) {
                    for (i in 0 until seasons.length()) {
                        if (seasons.optJSONObject(i)?.optInt("se", -1) == res.season) {
                            ok = true
                            break
                        }
                    }
                }
                if (!ok) return
            }
            val dubsJson = subj.optJSONArray("dubs")
            val dubs = mutableListOf<JSONObject>()
            if (dubsJson != null) {
                for (i in 0 until dubsJson.length()) dubsJson.optJSONObject(i)?.let { dubs.add(it) }
            }
            if (dubs.isEmpty()) {
                dubs.add(
                    JSONObject()
                        .put("subjectId", subj.optString("subjectId").ifBlank { subject.subjectId })
                        .put("detailPath", subject.detailPath)
                        .put("lanName", "Original")
                        .put("type", 0)
                        .put("original", true),
                )
            }

            coroutineScope {
                dubs.take(6).forEach { dub ->
                    async(Dispatchers.IO) {
                        val dubSubjectId = dub.optString("subjectId").ifBlank { return@async }
                        val dubDetailPath = dub.optString("detailPath").ifBlank { subject.detailPath }
                        val audioLabel = dubLabel(
                            dub.optString("lanName").ifBlank { null },
                            dub.optInt("type", 0).takeIf { dub.has("type") },
                            if (dub.has("original")) dub.optBoolean("original") else null,
                        )
                        try {
                            val playUrl = "$BFF/subject/play?subjectId=${JpNet.urlEncode(dubSubjectId)}" +
                                "&se=${res.season ?: 0}&ep=${res.episode ?: 0}" +
                                "&detailPath=${JpNet.urlEncode(dubDetailPath)}"
                            val playRes = JpNet.request(
                                "GET", playUrl, "$site/videoPlayPage/$dubDetailPath",
                                authHeaders(
                                    site,
                                    mapOf(
                                        "X-Source" to "webNetnaijaSite",
                                        "Referer" to "$site/videoPlayPage/$dubDetailPath",
                                    ),
                                ),
                            ) ?: return@async
                            readToken(playRes.headers)
                            val play = try {
                                JSONObject(playRes.body).optJSONObject("data")
                            } catch (_: Exception) {
                                null
                            } ?: return@async

                            val mp4Headers = mapOf(
                                "Referer" to "$site/",
                                "Origin" to site,
                                "User-Agent" to NA_UA,
                            )
                            play.optJSONArray("streams")?.let { arr ->
                                for (i in 0 until arr.length()) {
                                    val s = arr.optJSONObject(i) ?: continue
                                    if (s.optBoolean("vipLocked", false)) continue
                                    val url = s.optString("url")
                                    if (url.isBlank()) continue
                                    if (!JpNet.alive(url, false, mp4Headers)) continue
                                    val quality = s.optString("resolutions")
                                    val sizeMb = s.optString("size").toLongOrNull()
                                        ?.let { if (it > 0) "${it / 1048576} MB" else "" }.orEmpty()
                                    val parts = listOfNotNull(
                                        audioLabel.takeIf { it.isNotBlank() },
                                        quality.takeIf { it.isNotBlank() }?.let { "${it}p" },
                                        sizeMb.takeIf { it.isNotBlank() },
                                    )
                                    callback(
                                        HikariStream(
                                            name = "[NetNaija] - " + parts.joinToString(" · "),
                                            url = url,
                                            headers = mp4Headers,
                                        ),
                                    )
                                }
                            }
                            play.optJSONArray("dash")?.let { arr ->
                                for (i in 0 until arr.length()) {
                                    val s = arr.optJSONObject(i) ?: continue
                                    val url = s.optString("url")
                                    if (url.isBlank()) continue
                                    if (!JpNet.alive(url, false, mp4Headers)) continue
                                    val dashName = if (audioLabel.isBlank()) "[NetNaija] - DASH"
                                    else "[NetNaija] - $audioLabel · DASH"
                                    callback(
                                        HikariStream(
                                            name = dashName, url = url,
                                            headers = mp4Headers, isMpd = true,
                                        ),
                                    )
                                }
                            }
                            play.optJSONArray("hls")?.let { arr ->
                                for (i in 0 until arr.length()) {
                                    val s = arr.optJSONObject(i) ?: continue
                                    val url = s.optString("url")
                                    if (url.isBlank()) continue
                                    if (!JpNet.m3u8Alive(url, mapOf("User-Agent" to NA_UA))) continue
                                    val hlsName = if (audioLabel.isBlank()) "[NetNaija] - HLS"
                                    else "[NetNaija] - $audioLabel · HLS"
                                    callback(
                                        HikariStream(
                                            name = hlsName, url = url,
                                            headers = mapOf("User-Agent" to NA_UA),
                                            isM3u8 = true,
                                        ),
                                    )
                                }
                            }
                            val firstStreamId = play.optJSONArray("streams")?.let { arr ->
                                (0 until arr.length()).mapNotNull { arr.optJSONObject(it) }
                                    .firstOrNull { !it.optBoolean("vipLocked", false) }
                                    ?.optString("id")
                            }
                            if (!firstStreamId.isNullOrBlank()) {
                                try {
                                    val capUrl = "$BFF/subject/caption?format=MP4" +
                                        "&id=${JpNet.urlEncode(firstStreamId)}" +
                                        "&subjectId=${JpNet.urlEncode(dubSubjectId)}" +
                                        "&detailPath=${JpNet.urlEncode(dubDetailPath)}"
                                    val capBody = JpNet.get(capUrl, "$site/", authHeaders(site))
                                    if (capBody != null) {
                                        val caps = JSONObject(capBody).optJSONObject("data")
                                            ?.optJSONArray("captions")
                                        if (caps != null) {
                                            for (i in 0 until caps.length()) {
                                                val cap = caps.optJSONObject(i) ?: continue
                                                val curl = cap.optString("url")
                                                if (curl.isBlank()) continue
                                                subtitleCallback(
                                                    HikariSubtitle(
                                                        cap.optString("lanName").ifBlank {
                                                            cap.optString("lan").ifBlank { "English" }
                                                        },
                                                        curl,
                                                    ),
                                                )
                                            }
                                        }
                                    }
                                } catch (_: Exception) {
                                }
                            }
                        } catch (_: Exception) {
                        }
                    }
                }
            }
        } catch (_: Exception) {
        }
    }
}

// ---------------------------------------------------------------------------
// NetMirror
// ---------------------------------------------------------------------------

internal object JpNetMirror {
    private val DEFAULT_BASES = listOf("https://net77.cc", "https://net52.cc")

    private const val SOLVE_UA =
        "Mozilla/5.0 (Linux; Android 12; RMX2117 Build/SP1A.210812.016; wv) " +
            "AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/147.0.7727.55 " +
            "Mobile Safari/537.36 /OS.Gatu v3.0"
    private const val PAGE_UA =
        "Mozilla/5.0 (Linux; Android 13; Pixel 5 Build/TQ3A.230901.001; wv) " +
            "AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/144.0.7559.132 " +
            "Safari/537.36 /OS.Gatu v3.0"
    private const val PLAYER_UA =
        "Mozilla/5.0 (Linux; Android 13; Pixel 5 Build/TQ3A.230901.001; wv) " +
            "AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/149.0.7827.91 " +
            "Safari/537.36 /OS.Gatu v3.0"
    private const val DESKTOP_UA =
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) " +
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36"
    private const val XRW_APP = "app.netmirror.netmirrornew"

    private class Ott(val code: String, val path: String, val site: String)

    private val catalogs = listOf(
        Ott("nf", "/mobile", "netmirror_nf"),
        Ott("hs", "/mobile/hs", "netmirror_hs"),
        Ott("pv", "/mobile/pv", "netmirror_pv"),
    )

    private data class MirrorHit(val id: String, val title: String)
    private data class MirrorEp(val id: String, val season: Int?, val episode: Int?)
    private data class MirrorSeason(val id: String)

    private class MirrorPost(
        val title: String,
        val year: String?,
        val movie: Boolean,
        val episodes: List<MirrorEp>,
        val seasons: List<MirrorSeason>,
        val morePages: Boolean,
        val pageCursor: String?,
    )

    @Volatile
    private var base: String? = null

    @Volatile
    private var cookieValue: String? = null

    @Volatile
    private var cookieAt = 0L
    private val cookieLock = Mutex()

    private fun userverFor(b: String): String {
        val host = JpNet.hostOf(b)
        return "https://userver.${host.substringAfter("net", host)}".let {
            if (host.startsWith("net")) "https://userver.$host" else "https://userver.net52.cc"
        }
    }

    private fun apiCookieHeader(cookie: String, ott: String): String =
        "t_hash_t=$cookie; ott=$ott; hd=on"

    private fun playlistHeaders(b: String, cookie: String, ott: String): Map<String, String> =
        mapOf(
            "Accept" to "*/*",
            "Accept-Language" to "en-IN,en-US;q=0.9,en;q=0.8",
            "Connection" to "keep-alive",
            "Cookie" to apiCookieHeader(cookie, ott),
            "Referer" to "$b/mobile/home?app=1",
            "Sec-Fetch-Dest" to "empty",
            "Sec-Fetch-Mode" to "cors",
            "Sec-Fetch-Site" to "same-origin",
            "User-Agent" to PLAYER_UA,
            "X-Requested-With" to XRW_APP,
        )

    private fun unixTime(): Long = System.currentTimeMillis() / 1000

    private suspend fun resolveBase(): String? {
        base?.let { return it }
        val cands = mutableListOf<String>()
        JpDomains.get("justplay_netmirror")?.let { cands.add(it) }
        JpDomains.get("netmirror")?.let { cands.add(it) }
        cands.addAll(DEFAULT_BASES)
        for (b in cands.distinct()) {
            try {
                val html = JpNet.get(
                    "$b/mobile/home?app=1", "$b/",
                    mapOf("User-Agent" to SOLVE_UA, "X-Requested-With" to XRW_APP),
                ) ?: continue
                if (Regex("""data-addhash\s*=\s*["'][^"']+""").containsMatchIn(html)) {
                    base = b
                    return b
                }
            } catch (_: Exception) {
            }
        }
        return null
    }

    private fun cookieFresh(at: Long): Boolean =
        !cookieValue.isNullOrEmpty() && System.currentTimeMillis() - at < 54_000_000L

    private suspend fun solvedCookie(b: String): String? {
        if (cookieFresh(cookieAt)) return cookieValue
        return cookieLock.withLock {
            if (cookieFresh(cookieAt)) return cookieValue
            val fresh = solveCookie(b)
            cookieValue = fresh
            cookieAt = if (fresh == null) 0L else System.currentTimeMillis()
            fresh
        }
    }

    private fun addhashOf(html: String): String =
        Regex(
            """<body[^>]*data-addhash\s*=\s*["']([^"']+)["']""",
            RegexOption.IGNORE_CASE,
        ).find(html)?.groupValues?.get(1)?.trim().orEmpty()

    private suspend fun solveCookie(b: String): String? {
        val headers = mapOf("User-Agent" to SOLVE_UA, "X-Requested-With" to XRW_APP)
        var addhash = try {
            JpNet.get("$b/mobile/home?app=1", "$b/", headers)?.let { addhashOf(it) }.orEmpty()
        } catch (_: Exception) {
            ""
        }
        if (addhash.isEmpty()) return null
        try {
            JpNet.get("${userverFor(b)}/?hee5=$addhash&a=y&t=${Math.random()}", "$b/", headers)
        } catch (_: Exception) {
        }
        val host = JpNet.hostOf(b)
        repeat(7) {
            delay(10_000L)
            val res = try {
                val jar = JpNet.newJar()
                val r = JpNet.postForm(
                    "$b/mobile/verify2.php",
                    mapOf("verify" to addhash),
                    "$b/mobile/home?app=1",
                    mapOf("User-Agent" to SOLVE_UA, "X-Requested-With" to "XMLHttpRequest"),
                    jar,
                )
                if (r != null && r.body.contains("\"statusup\":\"All Done\"")) {
                    JpNet.cookieOf(jar, host, "t_hash_t")?.takeIf { it.isNotBlank() }
                } else null
            } catch (_: Exception) {
                null
            }
            if (!res.isNullOrBlank()) return res
        }
        return null
    }

    private suspend fun search(b: String, cookie: String, ott: Ott, query: String): List<MirrorHit> {
        val text = JpNet.retry {
            try {
                JpNet.get(
                    "$b${ott.path}/search.php?s=${JpNet.urlEncode(query)}&t=${unixTime()}",
                    "$b/home",
                    mapOf(
                        "User-Agent" to DESKTOP_UA,
                        "Cookie" to apiCookieHeader(cookie, ott.code),
                    ),
                )
            } catch (_: Exception) {
                null
            }
        } ?: return emptyList()
        return try {
            val arr = JSONObject(text).optJSONArray("searchResult") ?: return emptyList()
            (0 until arr.length()).mapNotNull { i ->
                val o = arr.optJSONObject(i) ?: return@mapNotNull null
                val id = o.optString("id")
                val t = o.optString("t")
                if (id.isBlank() || t.isBlank()) return@mapNotNull null
                MirrorHit(id, t)
            }
        } catch (_: Exception) {
            emptyList()
        }
    }

    private fun parseEpisodes(arr: JSONArray?): List<MirrorEp> {
        if (arr == null) return emptyList()
        val out = mutableListOf<MirrorEp>()
        for (i in 0 until arr.length()) {
            val o = arr.optJSONObject(i) ?: continue
            val id = o.optString("id")
            if (id.isBlank()) continue
            out.add(
                MirrorEp(
                    id,
                    o.optString("s").removePrefix("S").toIntOrNull(),
                    o.optString("ep").removePrefix("E").toIntOrNull(),
                ),
            )
        }
        return out
    }

    private fun parseSeasons(arr: JSONArray?): List<MirrorSeason> {
        if (arr == null) return emptyList()
        val out = mutableListOf<MirrorSeason>()
        for (i in 0 until arr.length()) {
            val o = arr.optJSONObject(i) ?: continue
            val id = o.optString("id")
            if (id.isNotBlank()) out.add(MirrorSeason(id))
        }
        return out
    }

    private suspend fun fetchPost(b: String, cookie: String, ott: Ott, id: String): MirrorPost? {
        val text = JpNet.retry {
            try {
                JpNet.get(
                    "$b${ott.path}/post.php?id=${JpNet.urlEncode(id)}&t=${unixTime()}",
                    "$b/home",
                    mapOf(
                        "User-Agent" to DESKTOP_UA,
                        "Cookie" to apiCookieHeader(cookie, ott.code),
                    ),
                )
            } catch (_: Exception) {
                null
            }
        } ?: return null
        return try {
            val root = JSONObject(text)
            val title = root.optString("title")
            val arr = root.optJSONArray("episodes") ?: return null
            if (title.isBlank() || arr.length() == 0) return null
            MirrorPost(
                title,
                root.optString("year").takeIf { it.isNotBlank() },
                arr.optJSONObject(0) == null,
                parseEpisodes(arr),
                parseSeasons(root.optJSONArray("season")),
                root.optInt("nextPageShow", 0) == 1,
                root.optString("nextPageSeason").takeIf { it.isNotBlank() },
            )
        } catch (_: Exception) {
            null
        }
    }

    private suspend fun fetchEpisodePages(
        b: String,
        cookie: String,
        ott: Ott,
        idJson: String,
        seasonId: String,
        startPage: Int,
    ): List<MirrorEp> {
        val out = mutableListOf<MirrorEp>()
        var page = startPage
        var more = true
        while (more && page < 15) {
            val next = try {
                JSONObject(
                    JpNet.get(
                        "$b${ott.path}/episodes.php?s=${JpNet.urlEncode(seasonId)}" +
                            "&series=${JpNet.urlEncode(idJson)}&t=${unixTime()}&page=$page",
                        "$b/home",
                        mapOf(
                            "User-Agent" to DESKTOP_UA,
                            "Cookie" to apiCookieHeader(cookie, ott.code),
                        ),
                    ) ?: break,
                )
            } catch (_: Exception) {
                break
            }
            out.addAll(parseEpisodes(next.optJSONArray("episodes")))
            more = next.optInt("nextPageShow", 0) == 1
            page++
        }
        return out
    }

    private suspend fun emitPlaylist(
        b: String,
        site: String,
        cookie: String,
        ott: Ott,
        id: String,
        title: String,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ): Boolean {
        val headers = playlistHeaders(b, cookie, ott.code)
        val arr = fetchPlaylist(b, ott, id, title, headers, cookie) ?: return false
        if (emitSources(b, site, ott, arr, headers, subtitleCallback, callback)) return true
        cookieValue = null
        cookieAt = 0L
        val fresh = solvedCookie(b) ?: return false
        val freshHeaders = playlistHeaders(b, fresh, ott.code)
        val retry = fetchPlaylist(b, ott, id, title, freshHeaders, fresh) ?: return false
        return emitSources(b, site, ott, retry, freshHeaders, subtitleCallback, callback)
    }

    private suspend fun fetchPlaylist(
        b: String,
        ott: Ott,
        id: String,
        title: String,
        headers: Map<String, String>,
        cookie: String,
    ): JSONArray? {
        val text = JpNet.retry {
            try {
                JpNet.get(
                    "$b${ott.path}/playlist.php?id=${JpNet.urlEncode(id)}" +
                        "&t=${JpNet.urlEncode(title)}&tm=${unixTime()}",
                    "$b/mobile/home?app=1", headers,
                )
            } catch (_: Exception) {
                null
            }
        } ?: return null
        return try {
            JSONArray(text)
        } catch (_: Exception) {
            null
        }
    }

    private suspend fun emitSources(
        b: String,
        site: String,
        ott: Ott,
        arr: JSONArray,
        headers: Map<String, String>,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ): Boolean {
        val candidates = mutableListOf<Pair<String, String>>()
        for (i in 0 until arr.length()) {
            val item = arr.optJSONObject(i) ?: continue
            val tracks = item.optJSONArray("tracks")
            if (tracks != null) {
                for (j in 0 until tracks.length()) {
                    val track = tracks.optJSONObject(j) ?: continue
                    if (track.optString("kind") != "captions") continue
                    val file = track.optString("file").replace("\\", "")
                    if (!file.startsWith("http")) continue
                    subtitleCallback(HikariSubtitle(track.optString("label").ifBlank { "English" }, file))
                }
            }
            val sources = item.optJSONArray("sources") ?: continue
            for (j in 0 until sources.length()) {
                val src = sources.optJSONObject(j) ?: continue
                val file = src.optString("file")
                if (!file.startsWith("/")) continue
                candidates.add(file to src.optString("label"))
            }
        }
        if (candidates.isEmpty()) return false
        val gate = Semaphore(3)
        val verified = coroutineScope {
            candidates.map { (file, label) ->
                async(Dispatchers.IO) {
                    gate.acquire()
                    try {
                        playableFile(b, file, headers)?.let { it to label }
                    } finally {
                        gate.release()
                    }
                }
            }.mapNotNull { runCatching { it.await() }.getOrNull() }
        }
        for ((url, label) in verified) {
            callback(
                HikariStream(
                    name = JpLabels.buildLabel(site, label, ""),
                    url = url,
                    headers = headers,
                    isM3u8 = true,
                ),
            )
        }
        return verified.isNotEmpty()
    }

    private suspend fun playableFile(b: String, file: String, headers: Map<String, String>): String? {
        val url = b + file
        if (JpNet.m3u8Alive(url, headers)) return url
        if (file.contains("hp=yes")) {
            val stripped = file.replace("&hp=yes", "").replace("hp=yes&", "").replace("?hp=yes", "")
            val bare = b + stripped
            if (JpNet.m3u8Alive(bare, headers)) return bare
        }
        return null
    }

    private suspend fun resolvePost(
        b: String,
        cookie: String,
        ott: Ott,
        postId: String,
        post: MirrorPost,
        res: JpPlayLink,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ): Boolean {
        if (post.movie || res.season == null) {
            return emitPlaylist(b, ott.site, cookie, ott, postId, post.title, subtitleCallback, callback)
        }
        val idJson = """{"id":"$postId"}"""
        val latest = post.episodes.toMutableList()
        if (post.morePages && !post.pageCursor.isNullOrEmpty()) {
            latest.addAll(fetchEpisodePages(b, cookie, ott, idJson, post.pageCursor, 2))
        }
        latest.firstOrNull { it.season == res.season && it.episode == res.episode }?.let {
            return emitPlaylist(b, ott.site, cookie, ott, it.id, post.title, subtitleCallback, callback)
        }
        for (season in post.seasons.dropLast(1)) {
            val eps = fetchEpisodePages(b, cookie, ott, idJson, season.id, 1)
            val hit = eps.firstOrNull { it.season == res.season && it.episode == res.episode }
            if (hit != null) {
                return emitPlaylist(b, ott.site, cookie, ott, hit.id, post.title, subtitleCallback, callback)
            }
        }
        return false
    }

    private suspend fun resolveOtt(
        b: String,
        cookie: String,
        ott: Ott,
        res: JpPlayLink,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ) {
        val title = res.title ?: return
        val hits = search(b, cookie, ott, title)
            .filter { JpNet.titleMatches(it.title, title) }
        if (hits.isEmpty()) return
        var fallback: Pair<String, MirrorPost>? = null
        for (hit in hits.take(3)) {
            val post = fetchPost(b, cookie, ott, hit.id) ?: continue
            if (post.movie != (res.season == null)) continue
            if (fallback == null) fallback = hit.id to post
            if (!JpNet.yearMatches(post.year.orEmpty(), res.matchYear)) continue
            if (resolvePost(b, cookie, ott, hit.id, post, res, subtitleCallback, callback)) return
        }
        val spare = fallback ?: return
        resolvePost(b, cookie, ott, spare.first, spare.second, res, subtitleCallback, callback)
    }

    suspend fun invoke(
        res: JpPlayLink,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ) {
        try {
            val b = resolveBase() ?: return
            val cookie = solvedCookie(b) ?: return
            coroutineScope {
                catalogs.forEach { ott ->
                    async(Dispatchers.IO) {
                        try {
                            resolveOtt(b, cookie, ott, res, subtitleCallback, callback)
                        } catch (_: Exception) {
                        }
                    }
                }
            }
        } catch (_: Exception) {
        }
    }
}
