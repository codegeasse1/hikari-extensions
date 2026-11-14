package com.hikari.ext.providers

import android.util.Base64
import com.hikari.ext.HikariNet
import org.json.JSONObject
import java.io.ByteArrayInputStream
import java.util.zip.GZIPInputStream
import java.util.zip.Inflater
import java.util.zip.InflaterInputStream

/**
 * Miruro secure-pipe transport: base64url({path,method,query+live+_t}) GET to
 * {domain}/api/secure/pipe, response auto-decoded (gzip/zlib/XOR). Plus the
 * Firebase domain refresh. Mirrors MiruroUtils pipe codec exactly.
 */
object MrPipe {
    const val DEFAULT_DOMAIN = "https://www.miruro.to"
    private const val FIREBASE_URL = "https://cloudstreampluginhelper-default-rtdb.firebaseio.com/.json"
    private const val CACHE_TTL_MS = 5 * 60 * 1000L

    @Volatile private var domain = DEFAULT_DOMAIN
    @Volatile private var lastDomainLoad = 0L

    fun workingDomain(): String = domain

    suspend fun refreshDomain() {
        val now = System.currentTimeMillis()
        if (now - lastDomainLoad < CACHE_TTL_MS) return
        lastDomainLoad = now
        try {
            val body = HikariNet.getString(FIREBASE_URL) ?: return
            val o = JSONObject(body)
            val v = o.optString("miruro").ifBlank { o.optString("miruro_url") }
                .ifBlank { o.optString("miruro_domain") }.trim().removeSuffix("/")
            if (v.startsWith("http")) domain = v
        } catch (t: Throwable) { }
    }

    private val XOR_KEY = byteArrayOf(
        0x71, 0x95.toByte(), 0x10, 0x34, 0xF8.toByte(), 0xFB.toByte(), 0xCF.toByte(), 0x53,
        0xD8.toByte(), 0x9D.toByte(), 0xB5.toByte(), 0x2C, 0xEB.toByte(), 0x3D, 0xC2.toByte(), 0x2C,
    )

    fun encodePipeRequest(payload: JSONObject): String {
        val bytes = payload.toString().toByteArray(Charsets.UTF_8)
        return Base64.encodeToString(bytes, Base64.URL_SAFE or Base64.NO_WRAP or Base64.NO_PADDING)
    }

    private fun xorDecrypt(data: ByteArray): ByteArray {
        val result = ByteArray(data.size)
        for (i in data.indices) result[i] = (data[i].toInt() xor XOR_KEY[i % XOR_KEY.size].toInt()).toByte()
        return result
    }

    private fun decompress(data: ByteArray): String {
        if (data.size > 2 && data[0] == 0x1f.toByte() && data[1] == 0x8b.toByte()) {
            GZIPInputStream(ByteArrayInputStream(data)).use { return it.readBytes().toString(Charsets.UTF_8) }
        }
        val isZlib = data.size > 1 &&
            (data[0].toInt() and 0x0f) == 0x08 &&
            (data[0].toInt() shr 4) <= 7 &&
            (((data[0].toInt() and 0xff) shl 8) or (data[1].toInt() and 0xff)) % 31 == 0
        val inflater = if (isZlib) Inflater() else Inflater(true)
        InflaterInputStream(ByteArrayInputStream(data), inflater).use { return it.readBytes().toString(Charsets.UTF_8) }
    }

    fun decodePipeResponseAuto(responseBody: String): String {
        val trimmed = responseBody.trim()
        if (trimmed.startsWith("{") || trimmed.startsWith("[")) return trimmed
        val padded = trimmed + "=".repeat((4 - trimmed.length % 4) % 4)
        val decoded = try {
            Base64.decode(padded, Base64.URL_SAFE)
        } catch (t: Throwable) {
            throw Exception("Cannot base64-decode pipe response")
        }
        try {
            return decompress(decoded)
        } catch (t: Throwable) { }
        try {
            return decompress(xorDecrypt(decoded))
        } catch (t: Throwable) { }
        throw Exception("Cannot decode pipe response")
    }

    /**
     * Pipe GET with Cloudflare-aware fetching: plain client first, WebView
     * render fallback (carries CF cookies) when blocked.
     */
    suspend fun pipeRequest(path: String, query: Map<String, Any?>): String {
        val enriched = query.toMutableMap()
        enriched["live"] = "true"
        enriched["_t"] = System.currentTimeMillis()
        val payload = JSONObject()
            .put("path", path)
            .put("method", "GET")
            .put("query", JSONObject(enriched.mapValues { it.value }))
            .put("body", JSONObject.NULL)
        val encoded = encodePipeRequest(payload)
        val pipeUrl = "${workingDomain()}/api/secure/pipe?e=$encoded"
        val headers = mapOf(
            "User-Agent" to "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36",
            "Accept" to "*/*",
            "Referer" to "${workingDomain()}/",
        )
        val webBody = HikariNet.getString(pipeUrl, headers)
            ?: HikariNet.getStringSmart(pipeUrl, headers)
            ?: throw Exception("Failed on ${workingDomain()} for /$path")
        return decodePipeResponseAuto(webBody)
    }
}
