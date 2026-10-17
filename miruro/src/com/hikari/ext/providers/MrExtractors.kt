package com.hikari.ext.providers

import com.hikari.ext.HikariNet
import com.hikari.ext.HikariStream
import com.hikari.ext.HikariSubtitle
import org.json.JSONObject

/**
 * Miruro embed extractors: MegaPlay/VidWish page flow (data-id ->
 * /stream/getSources -> m3u8 + vtt tracks) and a WebView click-to-capture
 * fallback. Mirrors MiruroExtractor without CloudStream helpers.
 */
object MrExtractors {
    private const val UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) Gecko/20100101 Firefox/140.0"
    private val m3u8Capture = Regex("(?i)\\.(m3u8|mp4)(?:\\?|$)")

    suspend fun megaplay(
        url: String,
        referer: String,
        mainUrl: String,
        label: String,
        subtitleOut: (HikariSubtitle) -> Unit,
        linkOut: (HikariStream) -> Unit,
    ) {
        val headers = mapOf(
            "User-Agent" to UA,
            "Accept" to "*/*",
            "X-Requested-With" to "XMLHttpRequest",
            "Referer" to "$mainUrl/",
        )
        try {
            val html = HikariNet.getString(url, headers)
                ?: HikariNet.getStringSmart(url, headers) ?: return
            val id = Regex("data-id=[\"'](\\d+)\"").find(html)?.groupValues?.get(1)
                ?: Regex("data-realid=[\"'](\\d+)\"").find(html)?.groupValues?.get(1)
                ?: Regex("/stream/s-\\d+/(\\d+)").find(url)?.groupValues?.get(1)
                ?: return
            val rawText = HikariNet.getString("$mainUrl/stream/getSources?id=$id", headers) ?: return
            val m3u8Url = extractM3u8(rawText) ?: return
            linkOut(
                HikariStream(
                    name = "$label Auto",
                    url = m3u8Url,
                    headers = headers,
                    isM3u8 = true,
                )
            )
            Regex("\"file\"\\s*:\\s*\"([^\"]+\\.vtt[^\"]*)\"").findAll(rawText).forEach { m ->
                subtitleOut(HikariSubtitle("English", m.groupValues[1]))
            }
        } catch (t: Throwable) {
            try {
                val hits = HikariNet.resolveWithWebView(
                    url, m3u8Capture, timeoutMs = 15_000,
                    script = "document.querySelector('.jw-icon-display')?.click();",
                )
                for (h in hits) {
                    if (!h.url.contains(".m3u8", ignoreCase = true)) continue
                    linkOut(HikariStream(name = "$label Auto", url = h.url, headers = headers, isM3u8 = true))
                }
            } catch (t2: Throwable) { }
        }
    }

    private fun extractM3u8(raw: String): String? {
        runCatching { JSONObject(raw) }.getOrNull()?.let { o ->
            o.optJSONObject("sources")?.optString("file")?.takeIf { it.startsWith("http") }?.let { return it }
        }
        Regex("\"(links\\.hls\\d+)\"").findAll(raw).toList().reversed().forEach { m ->
            val key = m.groupValues[1]
            val parts = key.split(".")
            if (parts.size == 2) {
                val pattern = "\"${parts[0]}\"\\s*:\\s*\\{[^}]*\"${parts[1]}\"\\s*:\\s*\"([^\"]+)\""
                Regex(pattern).find(raw)?.let { return it.groupValues[1] }
            }
        }
        Regex("\"file\"\\s*:\\s*\"(https?://[^\"]+\\.m3u8[^\"]*)\"").find(raw)?.let { return it.groupValues[1] }
        Regex("(https?://[^\\s\"\\\\<>]+\\.m3u8[^\\s\"\\\\<>]*)").find(raw)?.let { return it.groupValues[1] }
        return null
    }

    suspend fun webview(
        url: String,
        referer: String,
        label: String,
        linkOut: (HikariStream) -> Unit,
    ) {
        try {
            val hits = HikariNet.resolveWithWebView(
                url, m3u8Capture, timeoutMs = 20_000,
                script = "document.querySelector('button,[role=\"button\"],.jw-icon-display,.vds-play-button')?.click();",
            )
            for (h in hits) {
                if (h.url.contains(".m3u8", ignoreCase = true)) {
                    linkOut(HikariStream(name = "$label Auto", url = h.url, headers = mapOf("Referer" to url), isM3u8 = true))
                } else if (h.url.contains(".mp4", ignoreCase = true) && !h.url.contains(".txt")) {
                    linkOut(HikariStream(name = "$label MP4", url = h.url, headers = mapOf("Referer" to url)))
                }
            }
        } catch (t: Throwable) { }
    }
}
