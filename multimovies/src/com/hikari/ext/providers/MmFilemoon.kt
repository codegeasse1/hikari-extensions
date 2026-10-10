package com.hikari.ext.providers

import com.hikari.ext.HikariStream
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.json.JSONArray
import org.json.JSONObject

/**
 * Byse-powered file hosts: challenge, ECDSA attestation, proof of work, then an
 * AES-GCM sealed playback payload whose key halves hide in a version indexed pair.
 * Port of the source's MMFilemoon; links are emitted as [HikariStream].
 */
internal object MmFilemoon {

    private data class Fingerprint(
        val token: String,
        val viewerId: String,
        val deviceId: String,
        val confidence: Double,
    )

    private data class PlaybackSource(val url: String, val label: String, val height: Int)

    // one cold session per host, the pow is too costly to repeat for every file
    private val sessionCache = HashMap<String, Fingerprint>()

    private fun clientBody(): JSONObject {
        val random = { MmCrypto.randomB64urlHash() }
        return JSONObject()
            .put("user_agent", MmNet.UA)
            .put("architecture", "x86")
            .put("bitness", "64")
            .put("platform", "Windows")
            .put("platform_version", "19.0.0")
            .put("model", "")
            .put("ua_full_version", "131.0.0.0")
            .put("brand_full_versions", JSONArray()
                .put(JSONObject().put("brand", "Chromium").put("version", "131.0.0.0"))
                .put(JSONObject().put("brand", "Not A(Brand").put("version", "99.0.0.0")))
            .put("pixel_ratio", 1)
            .put("screen_width", 1920)
            .put("screen_height", 1080)
            .put("color_depth", 24)
            .put("languages", JSONArray().put("en-US"))
            .put("timezone", "Asia/Kolkata")
            .put("hardware_concurrency", 8)
            .put("device_memory", 8)
            .put("touch_points", 0)
            .put("webgl_vendor", "Google Inc. (AMD)")
            .put("webgl_renderer", "ANGLE (AMD, Radeon RX 560 Series (0x000067FF) Direct3D11 vs_5_0 ps_5_0, D3D11)")
            .put("canvas_hash", random())
            .put("audio_hash", random())
            .put("webgl_params_hash", random())
            .put("fonts_hash", random())
            .put("codecs_hash", random())
            .put("media_devices", "ai1ao1vi1")
            .put("pointer_type", "fine,hover")
            .put("extra", JSONObject()
                .put("vendor", "Google Inc.")
                .put("appVersion", "Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"))
    }

    private fun fingerprintBody(fp: Fingerprint): JSONObject = JSONObject()
        .put("token", fp.token)
        .put("viewer_id", fp.viewerId)
        .put("device_id", fp.deviceId)
        .put("confidence", fp.confidence)

    private suspend fun obtainFingerprint(host: String, referer: String): Fingerprint? {
        synchronized(sessionCache) { sessionCache[host]?.let { return it } }

        val challenge = MmNet.postJson(
            "https://$host/api/videos/access/challenge",
            "{}",
            referer = referer,
            useCookies = true,
        )?.let { (code, text) ->
            if (code == 200) JSONObject(text) else null
        } ?: return null
        val challengeId = challenge.optString("challenge_id")
        val nonce = challenge.optString("nonce")
        if (challengeId.isBlank() || nonce.isBlank()) return null

        val key = MmCrypto.newP256Key()
        val attestBody = JSONObject()
            .put("viewer_id", "")
            .put("device_id", "")
            .put("challenge_id", challengeId)
            .put("nonce", nonce)
            .put("signature", key.signer(nonce))
            .put("public_key", JSONObject(key.jwk))
            .put("client", clientBody())
            .put("storage", JSONObject())
            .put("attributes", JSONObject().put("entropy", "high"))

        val attest = MmNet.postJson(
            "https://$host/api/videos/access/attest",
            attestBody.toString(),
            referer = referer,
            useCookies = true,
        )?.let { (code, text) ->
            if (code == 200) JSONObject(text) else null
        } ?: return null
        val token = attest.optString("token")
        if (token.isBlank()) return null

        val fp = Fingerprint(
            token = token,
            viewerId = attest.optString("viewer_id"),
            deviceId = attest.optString("device_id"),
            confidence = attest.optDouble("confidence", 0.5),
        )
        MmNet.setCookie(host, "byse_viewer_id", fp.viewerId)
        MmNet.setCookie(host, "byse_device_id", fp.deviceId)
        synchronized(sessionCache) { sessionCache[host] = fp }
        return fp
    }

    private fun assembleKey(version: String, parts: List<String>): ByteArray? {
        val v = version.trim().toIntOrNull() ?: return null
        if (v < 1 || v > 20) return null
        val first = v
        val second = 31 - v
        if (second < 1 || second > parts.size || first > parts.size) return null
        val picked = listOf(parts[first - 1], parts[second - 1])
            .filter { it.isNotBlank() }
            .map { MmCrypto.b64urlDecode(it) }
        if (picked.size != 2) return null
        return picked[0] + picked[1]
    }

    private fun parsePlayback(encrypted: JSONObject): List<PlaybackSource> {
        val iv = MmCrypto.b64urlDecode(encrypted.optString("iv"))
        val payload = MmCrypto.b64urlDecode(encrypted.optString("payload"))
        val version = encrypted.optString("version")
        val partsRaw = encrypted.optJSONArray("key_parts") ?: return emptyList()
        val parts = (0 until partsRaw.length()).map { partsRaw.optString(it) }
        val key = assembleKey(version, parts) ?: return emptyList()
        val plain = MmCrypto.aesGcmDecrypt(key, iv, payload) ?: return emptyList()
        val json = JSONObject(String(plain, Charsets.UTF_8))
        val out = mutableListOf<PlaybackSource>()
        val sources = json.optJSONArray("sources") ?: return emptyList()
        for (i in 0 until sources.length()) {
            val s = sources.optJSONObject(i) ?: continue
            val url = s.optString("url")
            if (url.isBlank()) continue
            val label = s.optString("label").ifBlank { "Filemoon" }
            out.add(PlaybackSource(url, label, s.optInt("height", 0)))
        }
        return out
    }

    // resolve every playable file hosted under a byse frontend, code is the file id
    suspend fun resolve(
        embedFrameUrl: String,
        code: String,
        labelPrefix: String,
        embedOrigin: String,
        callback: (HikariStream) -> Unit,
    ): Boolean {
        val playerHost = MmNet.hostOf(embedFrameUrl)
        if (playerHost.isBlank()) return false
        val referer = embedFrameUrl
        val fp = obtainFingerprint(playerHost, referer) ?: return false

        val captcha = MmNet.postJson(
            "https://$playerHost/api/videos/$code/embed/captcha",
            JSONObject().put("fingerprint", fingerprintBody(fp)).toString(),
            referer = referer,
            useCookies = true,
        )?.let { (code2, text) ->
            if (code2 == 200) JSONObject(text) else null
        } ?: return false
        val powToken = captcha.optString("pow_token")
        val powNonce = captcha.optString("pow_nonce")
        val difficulty = captcha.optInt("pow_difficulty", 0)
        if (powToken.isBlank() || powNonce.isBlank()) return false

        // the PoW search is pure integer work, keep it off the IO threads
        val solution = withContext(Dispatchers.Default) {
            MmCrypto.solvePow(powNonce, difficulty)
        } ?: return false

        val verify = MmNet.postJson(
            "https://$playerHost/api/videos/$code/embed/captcha/verify",
            JSONObject()
                .put("pow_token", powToken)
                .put("solution", solution)
                .put("fingerprint", fingerprintBody(fp))
                .toString(),
            referer = referer,
            useCookies = true,
        )?.let { (code2, text) ->
            if (code2 == 200) JSONObject(text) else null
        } ?: return false
        val captchaToken = verify.optString("token")
        if (captchaToken.isBlank()) return false

        val playback = MmNet.postJson(
            "https://$playerHost/api/videos/$code/embed/playback",
            JSONObject().put("fingerprint", fingerprintBody(fp)).toString(),
            referer = referer,
            extraHeaders = mapOf(
                "X-Captcha-Token" to captchaToken,
                "X-Embed-Origin" to embedOrigin,
                "X-Embed-Referer" to "https://$embedOrigin/e/$code",
                "X-Embed-Parent" to "https://$embedOrigin/e/$code",
            ),
            useCookies = true,
        )?.let { (code2, text) ->
            if (code2 == 200) JSONObject(text) else null
        } ?: return false
        val encrypted = playback.optJSONObject("playback") ?: return false
        val sources = parsePlayback(encrypted)
        for (src in sources) {
            callback(
                HikariStream(
                    name = "$labelPrefix Filemoon ${src.label}",
                    url = src.url,
                    isM3u8 = true,
                )
            )
        }
        return sources.isNotEmpty()
    }
}
