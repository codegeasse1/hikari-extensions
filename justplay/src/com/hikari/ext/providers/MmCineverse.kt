package com.hikari.ext.providers

import com.hikari.ext.HikariStream
import org.json.JSONObject

/**
 * The rozgarlelo hub fronts several mirror platforms; some it resolves server side
 * into a relay player page, others it redirects to their own player domains.
 * Port of the source's MMModiplay (MMCineverse + MMFilemoonBridge); jsoup replaced
 * with regex, and the non-redirecting proxy fetch uses [MmNet.request] with
 * followRedirects=false so the Location header stays visible.
 */
internal object MmCineverse {

    private const val HUB = "https://rozgarlelo.modiplay.xyz"

    // hosts running the shared api v1 player with the static protocol derived cipher
    private val apiV1Hosts = mapOf(
        "multimovies.rpmhub.site" to "RPM Share",
        "multimovies.embedseek.xyz" to "Seek Streaming",
        "multimovies.p2pplay.pro" to "Stream P2P",
        "server1.uns.bio" to "Upn Share",
    )

    private val APIV1_KEY = "kiemtienmua911ca".toByteArray(Charsets.UTF_8)
    private val APIV1_IV = "1234567890oiuytr".toByteArray(Charsets.UTF_8)

    private data class SubServer(val platform: String, val name: String, val code: String)

    private fun decryptApiV1(hex: String): JSONObject? {
        val data = try {
            MmCrypto.hexToBytes(hex)
        } catch (_: Exception) {
            return null
        }
        if (data.isEmpty() || data.size % 16 != 0) return null
        val plain = MmCrypto.aesCbcDecrypt(data, APIV1_KEY, APIV1_IV) ?: return null
        return try {
            JSONObject(String(plain, Charsets.UTF_8))
        } catch (_: Exception) {
            null
        }
    }

    suspend fun resolveApiV1(
        host: String,
        code: String,
        label: String,
        callback: (HikariStream) -> Unit,
    ): Boolean {
        val info = MmNet.get(
            "https://$host/api/v1/info?id=$code",
            referer = "https://$host/",
        )?.let { decryptApiV1(it) }
        val title = info?.optString("title").orEmpty()

        val videoHex = MmNet.get(
            "https://$host/api/v1/video?id=$code&w=1920&h=1080&r=$HUB",
            referer = "https://$host/",
        ) ?: return false
        val video = decryptApiV1(videoHex) ?: return false

        var added = false
        val cfNative = video.optString("cfNative")
        if (cfNative.isNotBlank()) {
            val quality = Regex("(\\d{3,4})p").find(title)?.groupValues?.get(1)
            callback(
                HikariStream(
                    name = label + (quality?.let { " ${it}p" } ?: ""),
                    url = cfNative,
                    isM3u8 = cfNative.contains(".m3u8"),
                )
            )
            added = true
        }
        val tiktok = video.optString("hlsVideoTiktok")
        if (tiktok.isNotBlank()) {
            val ttDomain = try {
                JSONObject(video.optString("streamingConfig"))
                    .getJSONObject("adjust")
                    .getJSONObject("Tiktok")
                    .optString("domain")
            } catch (_: Exception) {
                ""
            }
            val ttVersion = try {
                JSONObject(video.optString("streamingConfig"))
                    .getJSONObject("adjust")
                    .getJSONObject("Tiktok")
                    .getJSONObject("params")
                    .optString("v")
            } catch (_: Exception) {
                ""
            }
            val url = if (ttDomain.isNotBlank()) {
                "https://$host/hlsmod/$ttDomain$tiktok?v=$ttVersion"
            } else {
                "https://$host$tiktok"
            }
            callback(HikariStream(name = "$label Relay", url = url, isM3u8 = true))
            added = true
        }
        return added
    }

    private suspend fun resolveProxyPage(
        platform: String,
        code: String,
        displayName: String,
        labelPrefix: String,
        callback: (HikariStream) -> Unit,
    ): Boolean {
        val proxyUrl = "$HUB/proxy.php?p=${MmNet.urlEncode(platform)}&c=${MmNet.urlEncode(code)}" +
            "&title=&site_ref=https%3A%2F%2Fmultimovies.garden%2F&noredirect=1"

        val resp = MmNet.request(
            "GET", proxyUrl,
            referer = "$HUB/",
            extraHeaders = mapOf("User-Agent" to MmNet.UA),
            followRedirects = false,
        ) ?: return false

        if (resp.code in 300..399) {
            val location = resp.headers.entries
                .firstOrNull { it.key.equals("Location", ignoreCase = true) }
                ?.value?.firstOrNull() ?: return false
            val host = MmNet.hostOf(location)
            val hash = location.substringAfter("#", "")
            val name = apiV1Hosts[host]
            if (name != null && hash.isNotBlank()) {
                return resolveApiV1(host, hash, "$labelPrefix $name", callback)
            }
            if (host == "bysetayico.com" && hash.isBlank()) {
                val fileCode = location.substringAfterLast("/")
                return MmFilemoonBridge.resolveByse("$labelPrefix Filemoon", fileCode, callback)
            }
            return false
        }

        val html = resp.body
        val src = Regex("var\\s+src=\"([^\"]+)\"").find(html)?.groupValues?.get(1)
        if (src.isNullOrBlank()) return false
        val stream = MmNet.deEsc(src)
        callback(
            HikariStream(
                name = "$labelPrefix $displayName",
                url = stream,
                headers = mapOf("Referer" to "$HUB/"),
                isM3u8 = stream.contains(".m3u8"),
            )
        )
        return true
    }

    suspend fun resolve(
        embedUrl: String,
        labelPrefix: String,
        callback: (HikariStream) -> Unit,
    ): Boolean {
        val html = MmNet.get(embedUrl, referer = "https://multimovies.garden/") ?: return false

        // switchServer(embedUrl, platform, displayName, fileCode, title, element)
        val servers = Regex("switchServer\\('([^']*)','([^']*)','([^']*)','([^']*)'")
            .findAll(html)
            .map { SubServer(it.groupValues[2], it.groupValues[3], it.groupValues[4]) }
            .distinctBy { it.platform }
            .toList()

        // the default frame mirrors the first dropdown entry, keep the order stable
        val defaultFrame = Regex("<[^>]*playerFrame[^>]*>")
            .find(html)?.value
            ?.let { Regex("src=\"([^\"]+)\"").find(it)?.groupValues?.get(1) }
        val ordered = if (defaultFrame != null) {
            val defaultPlatform = Regex("[?&]p=([^&]+)").find(defaultFrame)?.groupValues?.get(1)
            servers.sortedByDescending { it.platform == defaultPlatform }
        } else servers

        var any = false
        for (server in ordered) {
            try {
                any = resolveProxyPage(
                    server.platform,
                    server.code,
                    server.name,
                    labelPrefix,
                    callback,
                ) || any
            } catch (_: Exception) {
                // a dead mirror must not take the rest down
            }
        }
        return any
    }
}

/** Translates bysetayico file links into the shared player domain before the heavy flow. */
internal object MmFilemoonBridge {

    suspend fun resolveByse(
        label: String,
        code: String,
        callback: (HikariStream) -> Unit,
    ): Boolean {
        val details = MmNet.get(
            "https://bysetayico.com/api/videos/$code/embed/details",
            referer = "https://bysetayico.com/e/$code",
        ) ?: return false
        val frameUrl = try {
            JSONObject(details).optString("embed_frame_url")
        } catch (_: Exception) {
            ""
        }
        if (frameUrl.isBlank()) return false
        return MmFilemoon.resolve(frameUrl, code, label, "bysetayico.com", callback)
    }
}
