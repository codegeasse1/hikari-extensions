package com.hikari.ext.providers

import com.hikari.ext.HikariNet
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.json.JSONObject
import java.io.ByteArrayOutputStream
import java.net.HttpURLConnection
import java.net.URL
import java.net.URLEncoder
import java.util.concurrent.ConcurrentHashMap

/** Raw response for callers that must see status codes / headers (redirect chains). */
internal data class TmfResp(
    val code: Int,
    val headers: Map<String, List<String>>,
    val body: String,
)

internal fun TmfResp.header(name: String): String? =
    headers.entries.firstOrNull { it.key.equals(name, ignoreCase = true) }
        ?.value?.firstOrNull()?.trim()

internal object TmfNet {

    const val DESKTOP_UA =
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

    // the site blocks a bare user agent on a lot of networks, only the full
    // browser header set gets through
    fun browserHeaders(referer: String? = null): Map<String, String> {
        val h = LinkedHashMap<String, String>()
        h["User-Agent"] = DESKTOP_UA
        h["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
        h["Accept-Language"] = "en-US,en;q=0.9"
        h["sec-ch-ua"] = "\"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\""
        h["sec-ch-ua-mobile"] = "?0"
        h["sec-ch-ua-platform"] = "\"Windows\""
        h["Sec-Fetch-Dest"] = "document"
        h["Sec-Fetch-Mode"] = "navigate"
        h["Sec-Fetch-Site"] = if (referer != null) "same-origin" else "none"
        h["Sec-Fetch-User"] = "?1"
        h["Upgrade-Insecure-Requests"] = "1"
        referer?.let { h["Referer"] = it }
        return h
    }

    private val FALLBACK_DOMAINS = listOf(
        "https://themoviesflix.forum",
        "https://themoviesflixhq.com",
        "https://moviesflixhq.com",
        "https://themoviesflix.actor",
        "https://moviesflixi.com",
    )

    @Volatile private var activeDomain: String? = null
    private val domainLock = Any()

    // the firebase entry regularly points at a dead or cloudflare walled mirror, first answer wins
    suspend fun domain(): String {
        activeDomain?.let { return it }
        synchronized(domainLock) { activeDomain?.let { return it } }
        val candidates = try {
            val txt = HikariNet.getString("https://cloudstreampluginhelper-default-rtdb.firebaseio.com/.json")
            val fb = txt?.let { JSONObject(it) }?.optString("themoviesflix")
                ?.ifBlank { JSONObject(txt).optString("themoviesflix_url") }
                ?.ifBlank { JSONObject(txt).optString("themoviesflix_domain") }
                ?.trim()?.removeSuffix("/")
            listOfNotNull(fb?.takeIf { it.startsWith("http") }, *FALLBACK_DOMAINS.toTypedArray()).distinct()
        } catch (_: Exception) {
            FALLBACK_DOMAINS
        }
        for (candidate in candidates) {
            val code = try {
                HikariNet.fetch("$candidate/?s=the", browserHeaders())?.status
            } catch (_: Exception) {
                null
            }
            if (code != null && code in 200..299) {
                synchronized(domainLock) { activeDomain = candidate }
                return candidate
            }
        }
        return FALLBACK_DOMAINS.first()
    }

    fun originOf(url: String): String = try {
        val uri = java.net.URI(url)
        "${uri.scheme}://${uri.host}"
    } catch (_: Exception) {
        url
    }

    fun hostOf(url: String): String = try {
        URL(url).host?.lowercase() ?: ""
    } catch (_: Exception) {
        ""
    }

    fun urlEncode(s: String): String = URLEncoder.encode(s, "UTF-8")

    fun absolute(link: String, base: String): String =
        if (link.startsWith("http")) link else base.trimEnd('/') + "/" + link.removePrefix("/")

    // -- raw client (redirect chains, cookies, headers, range probes) -------------

    private val globalJar = ConcurrentHashMap<String, String>()

    fun newJar(): MutableMap<String, String> = HashMap()

    private fun jarKey(host: String, name: String) = "$host\u0000$name"

    private fun sendCookies(host: String, jar: MutableMap<String, String>): String? {
        val prefix = "$host\u0000"
        return jar.entries.filter { it.key.startsWith(prefix) }
            .joinToString("; ") { "${it.key.removePrefix(prefix)}=${it.value}" }
            .takeIf { it.isNotBlank() }
    }

    private fun harvest(host: String, headers: Map<String, List<String>>, jar: MutableMap<String, String>) {
        for ((k, vs) in headers) {
            if (!k.equals("Set-Cookie", ignoreCase = true)) continue
            for (raw in vs) {
                val pair = raw.substringBefore(";")
                val idx = pair.indexOf('=')
                if (idx > 0) jar[jarKey(host, pair.substring(0, idx).trim())] = pair.substring(idx + 1).trim()
            }
        }
    }

    suspend fun request(
        method: String,
        url: String,
        body: String? = null,
        contentType: String? = null,
        referer: String? = null,
        extraHeaders: Map<String, String> = emptyMap(),
        followRedirects: Boolean = true,
        jar: MutableMap<String, String>? = null,
    ): TmfResp? = withContext(Dispatchers.IO) {
        try {
            val host = hostOf(url)
            val cookies = jar ?: globalJar
            val conn = (URL(url).openConnection() as HttpURLConnection).apply {
                requestMethod = method
                connectTimeout = 20_000
                readTimeout = 25_000
                instanceFollowRedirects = followRedirects
                setRequestProperty("User-Agent", DESKTOP_UA)
                setRequestProperty("Accept-Encoding", "identity")
                if (referer != null) setRequestProperty("Referer", referer)
                for ((k, v) in extraHeaders) setRequestProperty(k, v)
                sendCookies(host, cookies)?.let { setRequestProperty("Cookie", it) }
                if (body != null) {
                    doOutput = true
                    if (contentType != null) setRequestProperty("Content-Type", contentType)
                    outputStream.use { it.write(body.toByteArray(Charsets.UTF_8)) }
                }
            }
            val code = conn.responseCode
            val headers = conn.headerFields.filterKeys { it != null }.mapKeys { it.key!! }
            harvest(host, headers, cookies)
            val stream = try {
                if (code in 200..399) conn.inputStream else conn.errorStream
            } catch (_: Exception) {
                try { conn.errorStream } catch (_: Exception) { null }
            }
            val bytes = stream?.use { ins ->
                val buf = ByteArrayOutputStream()
                val tmp = ByteArray(8192)
                while (true) {
                    val n = ins.read(tmp)
                    if (n < 0) break
                    buf.write(tmp, 0, n)
                }
                buf.toByteArray()
            } ?: ByteArray(0)
            conn.disconnect()
            TmfResp(code, headers, String(bytes, Charsets.UTF_8))
        } catch (_: Exception) {
            null
        }
    }

    suspend fun get(
        url: String,
        referer: String? = null,
        headers: Map<String, String> = emptyMap(),
        jar: MutableMap<String, String>? = null,
    ): String? {
        val r = request("GET", url, referer = referer, extraHeaders = headers, jar = jar) ?: return null
        return if (r.code in 200..299) r.body else null
    }

    suspend fun postJson(
        url: String,
        body: String,
        referer: String? = null,
        extraHeaders: Map<String, String> = emptyMap(),
    ): TmfResp? = request(
        "POST", url, body, "application/json; charset=utf-8",
        referer, extraHeaders,
    )

    /** Site pages: full browser headers + the app's WebView fallback. */
    suspend fun fetchPage(url: String, referer: String? = null): String? =
        HikariNet.getStringSmart(url, browserHeaders(referer))

    // -- drive pages ----------------------------------------------------------

    class DrivePage(
        val title: String,
        val quality: Int?,
        val info: String,
        val links: List<String>,
        val episodes: Map<Int, List<String>>,
    )

    private class CachedDrivePage(val savedAt: Long, val page: DrivePage?)

    private val driveCache = ConcurrentHashMap<String, CachedDrivePage>()
    private const val DRIVE_CACHE_MS = 5 * 60 * 1000L

    fun qualityOf(text: String): Int? =
        Regex("""(\d{3,4})[pP]\b""").find(text)?.groupValues?.get(1)?.toIntOrNull()
            ?: if (text.contains("2160", true) || text.contains("4K", true) || text.contains("UHD", true)) 2160 else null

    fun infoOf(title: String): String =
        Regex("""[\[{(]([^\]}]+)[\]}]""").findAll(title)
            .map { it.groupValues[1].trim() }
            .filter { it.isNotBlank() && !it.equals("links", true) }
            .joinToString(" · ")

    // links that belong to the drive page itself and never carry a file
    private val DRIVE_SELF = listOf(
        "nexdrive", "mobilejsr", "vglist", "w.org", "wordpress", "gmpg",
        "googleapis", "googletagmanager", "font-awesome", "schema", "category/",
        "t.me/+", "telegram", "moviesflix",
    )

    private fun isDriveJunk(href: String): Boolean = DRIVE_SELF.any { href.contains(it, true) }

    private val encodedRe = Regex("""const\s+encoded\s*=\s*"((?:[^"\\]|\\.)*)"""")

    fun decodeBlocks(html: String): List<String> {
        val out = ArrayList<String>()
        for (m in encodedRe.findAll(html)) {
            try {
                val clean = m.groupValues[1].replace("\\/", "/").replace("\\\\", "\\")
                val bytes = android.util.Base64.decode(clean, android.util.Base64.DEFAULT)
                val dec = String(bytes, Charsets.UTF_8)
                if (dec.isNotBlank()) out.add(dec)
            } catch (_: Exception) {
            }
        }
        return out
    }

    private val hrefRe = Regex("""<a[^>]+href="(https?://[^"]+)"""", RegexOption.IGNORE_CASE)
    private val epHeadRe = Regex("""Episodes?\s*:?\s*0*(\d+)""", RegexOption.IGNORE_CASE)
    private val h4Re = Regex("""<h4[^>]*>(.*?)</h4>""", setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE))

    fun parseDrivePage(html: String, pageUrl: String): DrivePage {
        val h1 = Regex("""<h1[^>]*>(.*?)</h1>""", setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE))
            .find(html)?.groupValues?.get(1)?.replace(Regex("<[^>]+>"), "")?.trim()
        val title = if (!h1.isNullOrBlank()) h1 else
            Regex("""<title>([^<]*)""", RegexOption.IGNORE_CASE).find(html)?.groupValues?.get(1)
                ?.substringBefore(" – ")?.substringBefore(" - ")?.trim().orEmpty()

        val decoded = decodeBlocks(html)
        val combined = decoded.joinToString("\n")

        val links = hrefRe.findAll(combined).map { it.groupValues[1].trim() }
            .filter { it.startsWith("http") && !isDriveJunk(it) }
            .distinct().toList()

        // old episode layout: each episode under an h4 header with its buttons below
        val episodes = mutableMapOf<Int, MutableList<String>>()
        val h4s = h4Re.findAll(combined).toList()
        for ((i, h4) in h4s.withIndex()) {
            val text = h4.groupValues[1].replace(Regex("<[^>]+>"), "").trim()
            if (!text.contains("Episode", true)) continue
            val epNum = epHeadRe.find(text)?.groupValues?.get(1)?.toIntOrNull() ?: continue
            val end = h4s.getOrNull(i + 1)?.range?.first ?: combined.length
            val seg = combined.substring(h4.range.first, end)
            val epLinks = hrefRe.findAll(seg).map { it.groupValues[1].trim() }
                .filter { it.startsWith("http") && !isDriveJunk(it) }
                .distinct().toList()
            if (epLinks.isNotEmpty()) {
                episodes.getOrPut(epNum) { mutableListOf() }.addAll(epLinks)
            }
        }
        return DrivePage(title, qualityOf(title), infoOf(title), links, episodes)
    }

    /** "= E25 To E28" style ranges from drive page titles. */
    fun rangeOf(title: String): IntRange? {
        val m = Regex("""E(\d+)\s*(?:To|-)\s*E?(\d+)""", RegexOption.IGNORE_CASE).find(title)
            ?: return null
        val a = m.groupValues[1].toIntOrNull() ?: return null
        val b = m.groupValues[2].toIntOrNull() ?: return null
        if (a < 1 || b < 1 || b - a > 500) return null
        return a..b
    }

    private suspend fun driveFetchOnce(url: String): TmfResp? {
        val jar = newJar()
        var current = url
        val seen = mutableSetOf<String>()
        repeat(12) {
            if (!seen.add(current)) return null
            val res = request(
                "GET", current,
                referer = "https://themoviesflix.forum/",
                extraHeaders = browserHeaders("https://themoviesflix.forum/").filterKeys { k ->
                    k != "Referer" && k != "User-Agent"
                },
                followRedirects = false,
                jar = jar,
            ) ?: return null
            val loc = res.header("location").orEmpty()
            if (loc.isEmpty()) {
                if (res.code != 200) return null
                if (res.body.contains("challenge-platform") || res.body.contains("Just a moment", true)) return null
                return res
            }
            current = when {
                loc.startsWith("http") -> loc
                loc.startsWith("/") -> originOf(current) + loc
                else -> current.trimEnd('/') + "/" + loc
            }
        }
        return null
    }

    private fun mirrorUrl(url: String): String = when {
        url.contains("mobilejsr.best") -> url.replace("mobilejsr.best", "mobilejsr.rest")
        url.contains("mobilejsr.rest") -> url.replace("mobilejsr.rest", "mobilejsr.best")
        url.contains("nexdrive.fit") -> url.replace("nexdrive.fit", "mobilejsr.rest")
        else -> url
    }

    // when one drive host has a bad day the mirror answers; zip pack pages are
    // skipped because no player can open an archive
    suspend fun fetchDrivePage(url: String): DrivePage? {
        val key = url
        driveCache[key]?.let { cached ->
            if (System.currentTimeMillis() - cached.savedAt < DRIVE_CACHE_MS) return cached.page
            driveCache.remove(key)
        }
        synchronized(driveCache) {
            driveCache[key]?.let { cached ->
                if (System.currentTimeMillis() - cached.savedAt < DRIVE_CACHE_MS) return cached.page
                driveCache.remove(key)
            }
        }
        val res = driveFetchOnce(key) ?: driveFetchOnce(mirrorUrl(key))
        val page = res?.let {
            val parsed = parseDrivePage(it.body, key)
            if (parsed.title.contains("zip", true)) null else parsed
        }
        driveCache[key] = CachedDrivePage(System.currentTimeMillis(), page)
        return page
    }

    /** One kilobyte range request; also reports content type for direct-file detection. */
    suspend fun probe(url: String, referer: String? = null): TmfResp? =
        request(
            "GET", url,
            referer = referer,
            extraHeaders = browserHeaders(referer).filterKeys { k ->
                k != "Referer" && k != "User-Agent"
            } + mapOf("Range" to "bytes=0-1023"),
        )

    /** True when the URL answers like a direct media file (206, or 200 + media type). */
    suspend fun probeFile(url: String, referer: String? = null): Boolean {
        val r = probe(url, referer) ?: return false
        if (r.code == 206) return true
        if (r.code != 200) return false
        val ct = (r.header("content-type") ?: "").lowercase()
        return ct.startsWith("video/") || ct.startsWith("audio/") ||
            ct.contains("octet-stream") || ct.contains("mkv") || ct.contains("mp4")
    }
}
