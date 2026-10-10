package com.hikari.ext.providers

import com.hikari.ext.HikariNet
import com.hikari.ext.HikariStream
import com.hikari.ext.HikariSubtitle
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import org.json.JSONObject
import java.io.ByteArrayOutputStream
import java.net.HttpURLConnection
import java.net.URI
import java.net.URL
import java.net.URLEncoder
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Semaphore

internal const val PLAY_UA =
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

internal data class JpResp(val code: Int, val headers: Map<String, List<String>>, val body: String)

internal fun JpResp.header(name: String): String? =
    headers.entries.firstOrNull { it.key.equals(name, ignoreCase = true) }
        ?.value?.firstOrNull()?.trim()

internal object JpNet {

    fun headers(referer: String? = null, extra: Map<String, String> = emptyMap()): Map<String, String> {
        val h = mutableMapOf("User-Agent" to PLAY_UA)
        if (referer != null) h["Referer"] = referer
        h.putAll(extra)
        return h
    }

    fun browserHeaders(referer: String? = null): Map<String, String> {
        val h = LinkedHashMap<String, String>()
        h["User-Agent"] = PLAY_UA
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

    fun docHeaders(referer: String?): Map<String, String> =
        browserHeaders(referer).filterKeys { it != "User-Agent" && it != "Referer" }

    fun urlEncode(s: String): String = URLEncoder.encode(s, "UTF-8")

    fun absolute(href: String, base: String): String = when {
        href.startsWith("http") -> href
        href.startsWith("//") -> "https:$href"
        href.startsWith("/") -> base.trimEnd('/') + href
        else -> base.trimEnd('/') + "/" + href
    }

    fun hostOf(url: String): String = try {
        URI(url).host?.lowercase() ?: ""
    } catch (_: Exception) {
        ""
    }

    fun getBaseUrl(url: String): String = try {
        val uri = URI(url)
        "${uri.scheme}://${uri.host}"
    } catch (_: Exception) {
        url
    }

    fun deEsc(s: String): String =
        s.replace("\\/", "/").replace("\\\"", "\"").replace("&amp;", "&")

    fun slugify(s: String): String {
        val cleaned = s.replace(Regex("[^\\p{L}\\p{Nd}\\s]"), "").trim()
        return Regex("\\s+").replace(cleaned, "-").lowercase()
    }

    fun rot13(input: String): String = buildString {
        for (c in input) {
            when (c) {
                in 'a'..'z' -> append('a' + (c - 'a' + 13) % 26)
                in 'A'..'Z' -> append('A' + (c - 'A' + 13) % 26)
                else -> append(c)
            }
        }
    }

    fun normalizeTitle(s: String?): String =
        (s ?: "").lowercase().replace(Regex("[^a-z0-9]"), "")

    fun seasonsOf(text: String): Set<Int>? {
        val seasons = mutableSetOf<Int>()
        Regex("(?i)Season\\s*(\\d{1,2})\\s*[-\\u2013]\\s*(\\d{1,2})").findAll(text).forEach {
            val a = it.groupValues[1].toIntOrNull() ?: return@forEach
            val b = it.groupValues[2].toIntOrNull() ?: return@forEach
            (a..b).forEach { s -> if (s in 1..50) seasons.add(s) }
        }
        Regex("(?i)\\bS(\\d{1,2})\\s*[-\\u2013]\\s*S?(\\d{1,2})\\b").findAll(text).forEach {
            val a = it.groupValues[1].toIntOrNull() ?: return@forEach
            val b = it.groupValues[2].toIntOrNull() ?: return@forEach
            (a..b).forEach { s -> if (s in 1..50) seasons.add(s) }
        }
        Regex("(?i)Season\\s*(\\d{1,2})").findAll(text).forEach {
            it.groupValues[1].toIntOrNull()?.let { s -> if (s in 1..50) seasons.add(s) }
        }
        Regex("""\bS(\d{1,2})\b""").findAll(text).forEach {
            it.groupValues[1].toIntOrNull()?.let { s -> if (s in 1..50) seasons.add(s) }
        }
        return seasons.ifEmpty { null }
    }

    fun titleMatches(postTitle: String?, query: String): Boolean {
        if (postTitle.isNullOrBlank()) return false
        val normQuery = normalizeTitle(query)
        if (normQuery.isBlank()) return false
        if (!normalizeTitle(postTitle).contains(normQuery)) return false
        if (normQuery.length < 4) return normalizeTitle(postTitle) == normQuery
        if (normQuery.length >= 10) return true
        var stripped = normalizeTitle(postTitle)
        for (prefix in listOf("download", "watch")) {
            if (stripped.startsWith(prefix) && stripped.length > prefix.length) {
                stripped = stripped.substring(prefix.length)
            }
        }
        return stripped.startsWith(normQuery)
    }

    fun yearMatches(text: String, year: Int?): Boolean {
        if (year == null) return true
        val years = Regex("(19|20)\\d{2}").findAll(text).mapNotNull { it.value.toIntOrNull() }.toList()
        if (years.isEmpty()) return true
        return years.any { kotlin.math.abs(it - year) <= 1 }
    }

    fun getIndexQuality(str: String?): Int {
        if (str.isNullOrBlank()) return 0
        Regex("""(\d{3,4})[pP]""").find(str)?.groupValues?.getOrNull(1)?.toIntOrNull()?.let { return it }
        val lower = str.lowercase()
        return when {
            lower.contains("8k") -> 4320
            lower.contains("4k") || lower.contains("uhd") -> 2160
            lower.contains("2k") -> 1440
            else -> 0
        }
    }

    suspend fun <T> retry(attempts: Int = 2, gapMs: Long = 600L, block: suspend () -> T?): T? {
        repeat(attempts) { i ->
            try {
                block()?.let { return it }
            } catch (_: Exception) {
            }
            if (i < attempts - 1) delay(gapMs)
        }
        return null
    }

    // -- raw client -----------------------------------------------------------

    private val globalJar = ConcurrentHashMap<String, String>()

    fun newJar(): MutableMap<String, String> = HashMap()

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
                if (idx > 0) jar["$host\u0000" + pair.substring(0, idx).trim()] = pair.substring(idx + 1).trim()
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
    ): JpResp? = withContext(Dispatchers.IO) {
        try {
            val host = hostOf(url)
            val cookies = jar ?: globalJar
            val conn = (URL(url).openConnection() as HttpURLConnection).apply {
                requestMethod = method
                connectTimeout = 15_000
                readTimeout = 20_000
                instanceFollowRedirects = followRedirects
                setRequestProperty("User-Agent", PLAY_UA)
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
            JpResp(code, headers, String(bytes, Charsets.UTF_8))
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
    ): JpResp? = request("POST", url, body, "application/json; charset=utf-8", referer, extraHeaders)

    suspend fun postForm(
        url: String,
        form: Map<String, String>,
        referer: String? = null,
        extraHeaders: Map<String, String> = emptyMap(),
        jar: MutableMap<String, String>? = null,
    ): JpResp? {
        val body = form.entries.joinToString("&") { "${urlEncode(it.key)}=${urlEncode(it.value)}" }
        return request("POST", url, body, "application/x-www-form-urlencoded; charset=UTF-8", referer, extraHeaders, jar = jar)
    }

    fun cookieOf(jar: MutableMap<String, String>, host: String, name: String): String? =
        jar["$host\u0000$name"]

    /** Site pages: full browser headers + the app's WebView fallback for WAF walls. */
    suspend fun fetchPage(url: String, referer: String? = null): String? =
        HikariNet.getStringSmart(url, browserHeaders(referer))

    // -- liveness ---------------------------------------------------------------

    private val verifiedUrls: MutableSet<String> = ConcurrentHashMap.newKeySet()
    private val verifyGate = Semaphore(8)

    suspend fun probe(url: String, referer: String? = null, headers: Map<String, String> = emptyMap()): Int? {
        return try {
            val h = browserHeaders(referer).toMutableMap()
            h.putAll(headers.filterKeys { !it.equals("Referer", true) && !it.equals("User-Agent", true) })
            h["Range"] = "bytes=0-1023"
            request("GET", url, referer = referer, extraHeaders = h)?.code
        } catch (_: Exception) {
            null
        }
    }

    fun linkIsVideo(url: String): Boolean {
        val clean = url.substringBefore("?").substringBefore("#").lowercase()
        return clean.endsWith(".mp4") || clean.endsWith(".mkv") || clean.endsWith(".avi") ||
            clean.endsWith(".webm") || clean.endsWith(".mov") || clean.endsWith(".m4a")
    }

    suspend fun m3u8Alive(url: String, headers: Map<String, String> = emptyMap()): Boolean {
        if (!url.startsWith("http")) return false
        return try {
            val r = request("GET", url, extraHeaders = headers) ?: return false
            r.code == 200 && r.body.trimStart().startsWith("#EXTM3U")
        } catch (_: Exception) {
            false
        }
    }

    suspend fun fileAlive(url: String, referer: String? = null): Boolean {
        if (!url.startsWith("http")) return false
        return probe(url, referer) in 200..299
    }

    suspend fun alive(
        url: String,
        isM3u8: Boolean,
        headers: Map<String, String> = emptyMap(),
        referer: String? = null,
    ): Boolean {
        if (!url.startsWith("http")) return false
        if (url in verifiedUrls) return true
        var ok = false
        val gate = verifyGate
        gate.acquire()
        try {
            if (url in verifiedUrls) return true
            val merged = if (referer != null && headers.keys.none { it.equals("Referer", true) }) {
                headers + mapOf("Referer" to referer)
            } else headers
            ok = try {
                if (isM3u8) m3u8Alive(url, merged)
                else probe(url, referer, merged)?.let { it in 200..399 } ?: false
            } catch (_: Exception) {
                false
            }
            if (ok) {
                if (verifiedUrls.size > 600) verifiedUrls.clear()
                verifiedUrls.add(url)
            }
        } finally {
            gate.release()
        }
        return ok
    }

    // -- redirect / cf helpers ----------------------------------------------------

    suspend fun followManually(url: String, referer: String?): JpResp? {
        var current = url
        val jar = newJar()
        val seen = mutableSetOf<String>()
        repeat(15) {
            if (!seen.add(current)) return null
            val res = request(
                "GET", current,
                referer = referer,
                extraHeaders = docHeaders(referer),
                followRedirects = false,
                jar = jar,
            ) ?: return null
            val loc = res.header("location").orEmpty()
            if (loc.isEmpty()) return res.takeIf { it.code == 200 }
            current = absolute(loc, current)
        }
        return null
    }

    private fun driveMirror(url: String): String = when {
        url.contains("nexdrive.fit") -> url.replace("nexdrive.fit", "mobilejsr.rest")
        url.contains("mobilejsr.rest") -> url.replace("mobilejsr.rest", "nexdrive.fit")
        url.contains("mobilejsr.best") -> url.replace("mobilejsr.best", "mobilejsr.rest")
        else -> url
    }

    private fun isCfChallenge(body: String): Boolean {
        val b = body.lowercase()
        return (b.contains("just a moment") && b.contains("challenge-platform")) ||
            b.contains("checking your browser") ||
            b.contains("checking if the site connection is secure")
    }

    /** Plain fetch first; when a WAF wall answers, retry through the real-WebView fetch. */
    suspend fun fetchWithCf(url: String, referer: String? = null): String? {
        val plain = try {
            get(url, referer, docHeaders(referer))
        } catch (_: Exception) {
            null
        }
        if (plain != null && !isCfChallenge(plain)) return plain
        return try {
            fetchPage(url, referer)?.takeIf { !isCfChallenge(it) }
        } catch (_: Exception) {
            null
        }
    }

    suspend fun fetchDrivePage(url: String, referer: String?): String? {
        for (candidate in listOf(url, driveMirror(url))) {
            fetchWithCf(candidate, referer)?.let { return it }
            try {
                followManually(candidate, referer)?.body?.let { return it }
            } catch (_: Exception) {
            }
        }
        return null
    }

    suspend fun resolveRedirectTarget(url: String, referer: String? = null): String? {
        var current = url
        repeat(7) {
            val res = request(
                "GET", current,
                referer = referer,
                extraHeaders = headers(referer),
                followRedirects = false,
            ) ?: return null
            val loc = res.header("location").orEmpty()
            if (loc.isEmpty()) return current.takeIf { it.startsWith("http") }
            current = absolute(loc, current)
        }
        return null
    }

    // id= links hide the target behind rot13(base64(base64())) layers plus a
    // blog ?re= hop; mirrors the source's decryptIdLink
    suspend fun decryptIdLink(url: String, referer: String? = null): String? {
        return try {
            val text = get(url, referer, headers(referer)) ?: return null
            val m1 = Regex("""s\('o','([A-Za-z0-9+/=]+)'""").findAll(text).map { it.groupValues[1] }.toList()
            val m2 = Regex("""ck\('_wp_http_\d+','([^']+)'""").findAll(text).map { it.groupValues[1] }.toList()
            val concat = (m1 + m2).joinToString("")
            if (concat.isBlank()) return null
            val decoded = b64d(rot13(b64d(b64d(concat) ?: return null) ?: return null) ?: return null)
                ?: return null
            val obj = try {
                JSONObject(decoded)
            } catch (_: Exception) {
                null
            }
            if (obj == null) {
                val direct = b64d(decoded) ?: decoded
                return direct.trim().takeIf { it.startsWith("http") }
            }
            val o = obj.optString("o").trim()
            val data = obj.optString("data").trim()
            val blog = obj.optString("blog_url").trim()
            if (data.isNotBlank() && blog.isNotBlank()) {
                val follow = request("GET", "$blog?re=$data", referer = url, extraHeaders = headers(url))
                val textBody = follow?.body?.replace(Regex("<[^>]+>"), " ")?.replace(Regex("\\s+"), " ")?.trim().orEmpty()
                return textBody.ifBlank { o }.ifBlank { null }
            }
            val target = o.ifBlank { obj.optString("l").trim() }
            if (target.startsWith("http")) return target
            val unbased = b64d(target) ?: target
            return unbased.trim().takeIf { it.startsWith("http") } ?: target.ifBlank { null }
        } catch (_: Exception) {
            null
        }
    }

    fun b64d(s: String): String? = try {
        var t = s.trim().replace("-", "+").replace("_", "/")
        while (t.length % 4 != 0) t += "="
        String(android.util.Base64.decode(t, android.util.Base64.DEFAULT), Charsets.UTF_8)
    } catch (_: Exception) {
        null
    }

    // -- emission -------------------------------------------------------------------

    private val MEDIA_CAPTURE = Regex("""(?i)\.(m3u8|mp4|mkv)(?:[?#]|$)""")

    /** Anything the host router does not know: capture what its player requests in a real WebView. */
    suspend fun emitSiteLink(
        site: String,
        url: String,
        label: String,
        quality: Int? = null,
        referer: String? = null,
        callback: (HikariStream) -> Unit,
    ) {
        if (url.isBlank()) return
        try {
            val found = HikariNet.resolveWithWebView(url, MEDIA_CAPTURE, timeoutMs = 45_000)
            for (r in found) {
                val q = quality?.let { " · ${it}p" } ?: ""
                callback(
                    HikariStream(
                        name = JpLabels.buildLabel(site, "WebView", "$label$q"),
                        url = r.url,
                        headers = r.headers,
                        isM3u8 = r.url.contains(".m3u8"),
                    )
                )
            }
        } catch (_: Exception) {
        }
    }

    suspend fun emitOwnLink(
        site: String,
        url: String,
        label: String,
        quality: Int? = null,
        referer: String? = null,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ) {
        if (url.isBlank()) return
        val host = hostOf(url)
        if (host.isEmpty()) return
        val routed: (suspend (String?, (HikariSubtitle) -> Unit, (HikariStream) -> Unit) -> Unit)? = when {
            host.endsWith("hubcloud.ist") || host.endsWith("hubcloud.foo") || host.contains("hubcloud.") ->
                { r, s, c -> JpHubCloud.resolve(url, r, s, c) }
            host.contains("gdflix") || host.contains("gdlink") ->
                { r, s, c -> JpGdFlix.resolve(url, r, s, c) }
            host.contains("hubdrive") -> { r, s, c -> JpHubDrive.resolve(url, r, s, c) }
            host.contains("hubcdn") -> { r, s, c -> JpHubCdn.resolve(url, r, s, c) }
            host.contains("hblinks") -> { r, s, c -> JpHbLinks.resolve(url, r, s, c) }
            host.contains("gofile") -> { r, s, c -> JpGoFile.resolve(url, r, s, c) }
            host.contains("fastdl") -> { r, s, c -> JpFastDl.resolve(url, r, s, c) }
            host.contains("vcloud") -> { r, s, c -> JpVCloud.resolve(url, r, s, c) }
            host.contains("vegadrive") || host.contains("m-drive.") -> { r, s, c -> JpVegaDrive.resolve(url, r, s, c) }
            host.contains("mcloud.") -> { r, s, c -> JpMCloud.resolve(url, r, s, c) }
            host.contains("filebee") || host.contains("filepress") || host.contains("fpgo") ->
                { r, s, c -> JpFilePress.resolve(url, r, s, c) }
            else -> null
        }
        if (routed == null) {
            emitSiteLink(site, url, label, quality, referer, callback)
            return
        }
        try {
            val collected = mutableListOf<HikariStream>()
            val subs = mutableListOf<HikariSubtitle>()
            routed(referer, { subs.add(it) }, { collected.add(it) })
            emitChecked(site, label, quality, collected, subs, subtitleCallback, callback)
        } catch (_: Exception) {
        }
    }

    suspend fun emitChecked(
        site: String,
        label: String,
        quality: Int?,
        links: List<HikariStream>,
        subs: List<HikariSubtitle>,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ) {
        if (links.isEmpty()) return
        val checked = withTimeoutOrNull(45_000L) {
            coroutineScope {
                links.map { link ->
                    async(Dispatchers.IO) {
                        if (alive(link.url, link.isM3u8, link.headers)) link else null
                    }
                }.mapNotNull { runCatching { it.await() }.getOrNull() }
            }
        } ?: return
        for (s in subs) subtitleCallback(s)
        for (link in checked) {
            val q = quality?.let { " · ${it}p" } ?: ""
            callback(
                link.copy(
                    name = JpLabels.buildLabel(site, link.name, "$label$q"),
                    subtitles = link.subtitles + subs,
                )
            )
        }
    }
}
