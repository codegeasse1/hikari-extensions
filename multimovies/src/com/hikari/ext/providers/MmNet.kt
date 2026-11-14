package com.hikari.ext.providers

import com.hikari.ext.HikariNet
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.ByteArrayOutputStream
import java.net.HttpURLConnection
import java.net.URL
import java.net.URLEncoder
import java.util.concurrent.ConcurrentHashMap

/**
 * Raw HTTP layer for the Multimovies port.
 *
 * Site pages (multimovies.garden, Cloudflare-guarded) go through
 * [HikariNet.getStringSmart] ( hardened stack + real-WebView fallback, the
 * Hikari replacement for the source's CF-dialog mmGet ). Everything else —
 * JSON APIs and POST flows that need status codes / Set-Cookie harvesting
 * (byse captcha flow) — uses [HttpURLConnection] directly with a manual
 * cookie jar, mirroring the source's MMNet on top of app.get/app.post.
 */
internal data class MmResp(
    val code: Int,
    val headers: Map<String, List<String>>,
    val body: String,
)

internal object MmNet {
    const val UA =
        "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36"
    private const val TIMEOUT_MS = 30_000

    // Hosts that keep per-session cookies (the byse player binds its captcha to them).
    private val cookieJar = ConcurrentHashMap<String, MutableMap<String, String>>()

    fun urlEncode(s: String): String = URLEncoder.encode(s, "UTF-8")

    fun abs(base: String, url: String): String {
        val u = url.trim()
        if (u.startsWith("http://") || u.startsWith("https://")) return u
        val b = base.trimEnd('/')
        return if (u.startsWith("/")) "$b$u" else "$b/$u"
    }

    fun deEsc(s: String): String =
        s.replace("\\/", "/").replace("\\\"", "\"").replace("&amp;", "&")

    fun hostOf(url: String): String = try {
        URL(url).host?.lowercase() ?: ""
    } catch (_: Exception) {
        ""
    }

    fun setCookie(host: String, name: String, value: String) {
        if (host.isBlank() || name.isBlank()) return
        cookieJar.getOrPut(host) { ConcurrentHashMap() }[name] = value
    }

    private fun cookieHeader(host: String): String? =
        cookieJar[host]?.takeIf { it.isNotEmpty() }
            ?.entries?.joinToString("; ") { "${it.key}=${it.value}" }

    private fun harvest(host: String, headers: Map<String, List<String>>) {
        for ((k, vs) in headers) {
            if (!k.equals("Set-Cookie", ignoreCase = true)) continue
            for (raw in vs) {
                val pair = raw.substringBefore(";")
                val idx = pair.indexOf('=')
                if (idx > 0) setCookie(host, pair.substring(0, idx).trim(), pair.substring(idx + 1).trim())
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
        useCookies: Boolean = true,
    ): MmResp? = withContext(Dispatchers.IO) {
        try {
            val host = hostOf(url)
            val conn = (URL(url).openConnection() as HttpURLConnection).apply {
                requestMethod = method
                connectTimeout = TIMEOUT_MS
                readTimeout = TIMEOUT_MS
                instanceFollowRedirects = followRedirects
                setRequestProperty("User-Agent", UA)
                setRequestProperty("Accept-Encoding", "identity")
                if (referer != null) setRequestProperty("Referer", referer)
                for ((k, v) in extraHeaders) setRequestProperty(k, v)
                if (useCookies) cookieHeader(host)?.let { setRequestProperty("Cookie", it) }
                if (body != null) {
                    doOutput = true
                    if (contentType != null) setRequestProperty("Content-Type", contentType)
                    outputStream.use { it.write(body.toByteArray(Charsets.UTF_8)) }
                }
            }
            val code = conn.responseCode
            val headers = conn.headerFields
                .filterKeys { it != null }
                .mapKeys { it.key!! }
            if (useCookies) harvest(host, headers)
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
            MmResp(code, headers, String(bytes, Charsets.UTF_8))
        } catch (_: Exception) {
            null
        }
    }

    suspend fun get(
        url: String,
        referer: String? = null,
        headers: Map<String, String> = emptyMap(),
    ): String? {
        val r = request("GET", url, referer = referer, extraHeaders = headers) ?: return null
        return if (r.code in 200..299) r.body else null
    }

    suspend fun postJson(
        url: String,
        body: String,
        referer: String? = null,
        extraHeaders: Map<String, String> = emptyMap(),
        useCookies: Boolean = false,
    ): Pair<Int, String>? {
        val r = request(
            "POST", url, body,
            "application/json; charset=utf-8", referer, extraHeaders,
            useCookies = useCookies,
        ) ?: return null
        return r.code to r.body
    }

    suspend fun postForm(
        url: String,
        form: Map<String, String>,
        referer: String? = null,
    ): String? {
        val body = form.entries.joinToString("&") { "${urlEncode(it.key)}=${urlEncode(it.value)}" }
        val r = request(
            "POST", url, body,
            "application/x-www-form-urlencoded; charset=UTF-8", referer,
        ) ?: return null
        return if (r.code in 200..299) r.body else null
    }

    /** Cloudflare-guarded site pages: hardened stack + real-WebView fallback. */
    suspend fun siteGet(url: String, referer: String? = null): String? {
        val h = mutableMapOf(
            "User-Agent" to UA,
            "Accept-Language" to "en-US,en;q=0.9",
        )
        if (referer != null) h["Referer"] = referer
        return HikariNet.getStringSmart(url, h)
    }
}
