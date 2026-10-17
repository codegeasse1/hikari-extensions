package com.hikari.ext.providers

import android.util.Base64
import java.net.URI
import java.security.MessageDigest
import java.security.SecureRandom
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import org.json.JSONObject

/**
 * Auth for the NetNaija-box mobile BFF (port of NetNaijaApp token logic).
 *
 * Every call carries x-client-token "<ms>,<md5(reversed ms)>" plus an
 * x-tr-signature HMAC-MD5 over the canonical request string, keyed by the
 * twice-base64-decoded client secret (same as the mobile client). The gateway
 * returns the anonymous JWT through the x-user *response header*, which the
 * HikariNet SDK does not expose (status+body only), so this port signs
 * requests without the bearer — the signature alone authenticates the client.
 */
internal object NnAppAuth {
    private const val SECRET_DEFAULT_B64 = "NzZpUmwwN3MweFNOOWpxbUVXQXQ3OUVCSlp1bElRSXNWNjRGWnIyTw=="
    private const val SECRET_ALT_B64 = "WHFuMm5uTzQxL0w5Mm8xaXVYaFNMSFRiWHZZNFo1Wlo2Mm04bVNMQQ=="

    const val MODERN_UA =
        "com.community.mbox.in/50020130 (Linux; U; Android 14; en_IN; Pixel 8; Build/UD1A.230803.041; Cronet/145.0.7582.0)"

    val deviceId: String = buildString {
        val r = SecureRandom()
        val hex = "0123456789abcdef"
        repeat(32) { append(hex[r.nextInt(16)]) }
    }

    val clientInfo: String =
        "{\"package_name\":\"com.community.mbox.in\",\"version_name\":\"4.0.03.0920.03\"," +
            "\"version_code\":50020130,\"os\":\"android\",\"os_version\":\"14\"," +
            "\"device_id\":\"$deviceId\",\"install_store\":\"official\"," +
            "\"gaid\":\"1b2212c1-dadf-43c3-a0c8-bd6ce48ae22d\",\"brand\":\"Google\",\"model\":\"Pixel 8\"}"

    fun md5Hex(input: ByteArray): String =
        MessageDigest.getInstance("MD5").digest(input).joinToString("") { "%02x".format(it) }

    fun clientToken(ts: Long = System.currentTimeMillis()): String {
        val s = ts.toString()
        return "$s,${md5Hex(s.reversed().toByteArray())}"
    }

    fun canonical(
        method: String, accept: String?, contentType: String?,
        url: String, body: String?, ts: Long,
    ): String {
        return try {
            val parsed = URI(url)
            val path = parsed.path ?: ""
            val rawQuery = parsed.rawQuery
            val query = if (!rawQuery.isNullOrBlank()) {
                rawQuery.split("&").mapNotNull {
                    val p = it.split("=")
                    if (p.isEmpty()) null else p[0] to (p.getOrNull(1) ?: "")
                }.sortedBy { it.first }.joinToString("&") { "${it.first}=${it.second}" }
            } else ""
            val canonicalUrl = if (query.isNotEmpty()) "$path?$query" else path
            val bodyHash: String
            val bodyLength: String
            if (body != null) {
                val bytes = body.toByteArray()
                val use = if (bytes.size > 102400) bytes.copyOfRange(0, 102400) else bytes
                bodyHash = md5Hex(use)
                bodyLength = bytes.size.toString()
            } else {
                bodyHash = ""
                bodyLength = ""
            }
            method.uppercase() + "\n" + (accept ?: "") + "\n" + (contentType ?: "") + "\n" +
                bodyLength + "\n" + ts + "\n" + bodyHash + "\n" + canonicalUrl
        } catch (_: Exception) {
            method.uppercase() + "\n" + (accept ?: "") + "\n" + (contentType ?: "") + "\n\n" +
                ts + "\n\n" + url
        }
    }

    fun signature(
        method: String, accept: String?, contentType: String?,
        url: String, body: String?, altKey: Boolean = false,
        ts: Long = System.currentTimeMillis(),
    ): String {
        return try {
            val canon = canonical(method, accept, contentType, url, body, ts)
            val twice = String(Base64.decode(if (altKey) SECRET_ALT_B64 else SECRET_DEFAULT_B64, Base64.DEFAULT))
            val secretBytes = Base64.decode(twice, Base64.DEFAULT)
            val mac = Mac.getInstance("HmacMD5")
            mac.init(SecretKeySpec(secretBytes, "HmacMD5"))
            val sig = Base64.encodeToString(mac.doFinal(canon.toByteArray()), Base64.NO_WRAP)
            "$ts|2|$sig"
        } catch (_: Exception) {
            "$ts|2|"
        }
    }

    fun headers(
        method: String, url: String,
        contentType: String = "application/json",
        accept: String = "application/json",
        body: String? = null,
    ): Map<String, String> {
        val ts = System.currentTimeMillis()
        return mapOf(
            "user-agent" to MODERN_UA,
            "accept" to accept,
            "content-type" to contentType,
            "connection" to "keep-alive",
            "x-client-token" to clientToken(ts),
            "x-tr-signature" to signature(method, accept, contentType, url, body, false, ts),
            "x-client-info" to clientInfo,
            "x-client-status" to "0",
        )
    }

    fun jwtExpiry(token: String): Long {
        return try {
            val payload = token.split(".").getOrNull(1) ?: return 0L
            var std = payload.replace("-", "+").replace("_", "/")
            std += "=".repeat((4 - (std.length % 4)) % 4)
            val json = String(Base64.decode(std, Base64.URL_SAFE))
            JSONObject(json).getLong("exp")
        } catch (_: Exception) {
            0L
        }
    }
}
