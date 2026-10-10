package com.hikari.ext.providers

import com.hikari.ext.HikariNet
import com.hikari.ext.HikariStream
import com.hikari.ext.HikariSubtitle
import java.net.URL
import java.net.URLDecoder

/**
 * Embed router for AniKage sources (mirrors the bundle RaghavEmbeds routing):
 * MegaPlay family, inline m3u8 hosts, FlixCloud, packed JS hosts, and a
 * WebView-capture fallback for anything else.
 */
object AkEmbeds {

    private const val UA =
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

    private val m3u8Regex = Regex("https?://[^\\s\"'<>\\\\]+\\.m3u8[^\\s\"'<>\\\\]*")
    private val capture = Regex("https?://[^\"'\\s]+?\\.(?:m3u8|mp4)(?:[?#][^\"'\\s]*)?")

    fun hostOf(url: String): String = try { URL(url).host.lowercase() } catch (t: Throwable) { "" }

    private fun isFlixUrl(url: String): Boolean = hostOf(url).endsWith("flixcloud.cc")

    private val flixUrlRegex = Regex("https?://[^\\s\"'<>\\\\]*flixcloud\\.cc/[^\\s\"'<>\\\\]*")

    private fun flixUrls(html: String): List<String> =
        flixUrlRegex.findAll(html).map { it.value.trimEnd('.', ',', ';') }
            .filter { !it.contains("/v7/") && !it.contains("/api/") }
            .distinct().toList()

    suspend fun resolveEmbed(
        embedUrl: String,
        referer: String,
        label: String,
        sourceTag: String,
        audio: String?,
        subtitleOut: (HikariSubtitle) -> Unit,
        linkOut: (HikariStream) -> Unit,
    ): Boolean {
        if (embedUrl.isBlank()) return false
        val host = hostOf(embedUrl)
        return try {
            when {
                host.endsWith("megaplay.buzz") || host.endsWith("vidwish.live") ||
                    host.endsWith("vidtube.site") || host.contains("megaplay-") ->
                    resolveMegaPlayFamily(embedUrl, referer, label, sourceTag, subtitleOut, linkOut)

                host.endsWith("vivibebe.site") || host.endsWith("bibiemb.xyz") ->
                    resolveInlineM3u8(embedUrl, referer, label, subtitleOut, linkOut)

                host.endsWith("flixcloud.cc") ->
                    resolveFlix(embedUrl, referer, label, sourceTag, audio, subtitleOut, linkOut)

                host.endsWith("otakuhg.site") || host.endsWith("otakuvid.online") || host.endsWith("kwik.cx") ->
                    resolvePackedM3u8(embedUrl, referer, label, subtitleOut, linkOut)

                else -> resolveGeneric(embedUrl, referer, label, sourceTag, audio, subtitleOut, linkOut)
            }
        } catch (t: Throwable) {
            false
        }
    }

    private suspend fun resolveMegaPlayFamily(
        embedUrl: String,
        referer: String,
        label: String,
        sourceTag: String,
        subtitleOut: (HikariSubtitle) -> Unit,
        linkOut: (HikariStream) -> Unit,
    ): Boolean {
        val stream = AkMegaPlay.resolveStream(embedUrl, referer) ?: return false
        return AkMegaPlay.emitLinks(
            sourceTag, label, stream.m3u8, "https://${hostOf(embedUrl)}/",
            stream.subtitles, subtitleOut, linkOut,
        )
    }

    private suspend fun resolveInlineM3u8(
        embedUrl: String,
        referer: String,
        label: String,
        subtitleOut: (HikariSubtitle) -> Unit,
        linkOut: (HikariStream) -> Unit,
    ): Boolean {
        passSubtitle(embedUrl, subtitleOut)
        val html = fetchEmbedHtml(embedUrl, referer) ?: return false
        val m3u8 = m3u8Regex.find(html)?.value ?: return false
        if (!streamPlayable(m3u8, "https://${hostOf(embedUrl)}/")) return false
        return emitM3u8(label, m3u8, "https://${hostOf(embedUrl)}/", linkOut)
    }

    suspend fun resolveFlix(
        embedUrl: String,
        referer: String,
        label: String,
        sourceTag: String,
        audio: String?,
        subtitleOut: (HikariSubtitle) -> Unit,
        linkOut: (HikariStream) -> Unit,
    ): Boolean {
        val res = FxResolver.resolve(embedUrl, referer) ?: return false
        val proxyMaster = FxProxy.registerMaster(res.m3u8, res.masterContent, res.pkKey) ?: return false
        val hasEnglishAudio = res.masterContent.contains("LANGUAGE=\"en\"") ||
            res.masterContent.contains("NAME=\"English\"")
        val hasOtherAudio = Regex("TYPE=AUDIO[^\\n]*LANGUAGE=\"(?!en)[^\"]*\"")
            .containsMatchIn(res.masterContent) ||
            (res.masterContent.contains("TYPE=AUDIO") && !hasEnglishAudio)
        val lang = when {
            audio == "dub" && hasEnglishAudio -> "dub"
            audio == "sub" && (hasOtherAudio || !hasEnglishAudio) -> "sub"
            audio == "dub" && !hasEnglishAudio -> "sub"
            else -> "dub"
        }
        linkOut(
            HikariStream(
                name = label,
                url = "$proxyMaster?lang=$lang",
                headers = mapOf("Referer" to "https://flixcloud.cc/"),
                isM3u8 = true,
            )
        )
        for (sub in res.subtitles) {
            val ext = sub.format?.uppercase()
            subtitleOut(HikariSubtitle(if (ext != null) "${sub.language ?: "Subtitle"} ($ext)" else (sub.language ?: "Subtitle"), sub.url))
        }
        return true
    }

    private suspend fun resolvePackedM3u8(
        embedUrl: String,
        referer: String,
        label: String,
        subtitleOut: (HikariSubtitle) -> Unit,
        linkOut: (HikariStream) -> Unit,
    ): Boolean {
        passSubtitle(embedUrl, subtitleOut)
        val html = fetchEmbedHtml(embedUrl, referer) ?: return false
        var m3u8 = m3u8Regex.find(html)?.value
        if (m3u8 == null) m3u8 = AkJsPacker.parseAndUnpack(html)?.let { m3u8Regex.find(it)?.value }
        if (m3u8 == null) return false
        return emitM3u8(label, m3u8, "https://${hostOf(embedUrl)}/", linkOut)
    }

    private suspend fun resolveGeneric(
        embedUrl: String,
        referer: String,
        label: String,
        sourceTag: String,
        audio: String?,
        subtitleOut: (HikariSubtitle) -> Unit,
        linkOut: (HikariStream) -> Unit,
    ): Boolean {
        // Direct WebView capture first (player pages that fetch the stream via JS).
        try {
            val hits = HikariNet.resolveWithWebView(embedUrl, capture, timeoutMs = 30_000)
            var found = false
            for (h in hits) {
                if (h.url.isBlank()) continue
                // Raw FlixCloud masters are XOR-obfuscated and NEVER play directly
                // (ExoPlayer reports manifest-malformed). Real flix embeds go through
                // the localhost proxy; never leak the raw URL into the link list.
                if (isFlixUrl(h.url)) continue
                linkOut(
                    HikariStream(
                        name = label,
                        url = h.url,
                        headers = h.headers,
                        isM3u8 = h.url.contains(".m3u8", ignoreCase = true),
                    )
                )
                found = true
            }
            if (found) return true
        } catch (t: Throwable) { }
        val html = fetchEmbedHtml(embedUrl, referer) ?: return false
        // Wrapper pages that iframe a FlixCloud player: resolve the embed properly
        // through the localhost proxy instead of leaking the raw master URL.
        var flixOk = false
        for (flixEmbed in flixUrls(html)) {
            if (resolveFlix(flixEmbed, referer, label, sourceTag, audio, subtitleOut, linkOut)) flixOk = true
        }
        var m3u8 = m3u8Regex.find(html)?.value
        if (m3u8 == null) m3u8 = AkJsPacker.parseAndUnpack(html)?.let { m3u8Regex.find(it)?.value }
        if (m3u8 == null) return flixOk
        return emitM3u8(label, m3u8, "https://${hostOf(embedUrl)}/", linkOut) || flixOk
    }

    private suspend fun fetchEmbedHtml(embedUrl: String, referer: String): String? {
        val h = mapOf("User-Agent" to UA, "Referer" to referer)
        return HikariNet.getString(embedUrl, h) ?: HikariNet.getStringSmart(embedUrl, h)
    }

    suspend fun streamPlayable(m3u8Url: String, referer: String): Boolean {
        return try {
            val headers = mapOf("User-Agent" to UA, "Referer" to referer)
            val master = HikariNet.getString(m3u8Url, headers) ?: return false
            if (!master.contains("#EXTM3U")) return false
            val playlistUrl: String
            val playlist: String
            if (master.contains("#EXT-X-STREAM-INF")) {
                val variant = firstEntry(master) ?: return false
                playlistUrl = relativeTo(m3u8Url, variant)
                playlist = HikariNet.getString(playlistUrl, headers) ?: return false
                if (!playlist.contains("#EXTM3U")) return false
            } else {
                playlistUrl = m3u8Url
                playlist = master
            }
            val seg = firstEntry(playlist) ?: return true
            val probe = HikariNet.fetch(relativeTo(playlistUrl, seg), headers + mapOf("Range" to "bytes=0-0"))
            probe != null && (probe.status == 200 || probe.status == 206)
        } catch (t: Throwable) {
            false
        }
    }

    private fun firstEntry(playlist: String): String? =
        playlist.lineSequence().map { it.trim() }
            .firstOrNull { it.isNotEmpty() && !it.startsWith("#") }

    private fun relativeTo(baseUrl: String, entry: String): String {
        if (entry.startsWith("http://") || entry.startsWith("https://")) return entry
        return baseUrl.substringBeforeLast("/") + "/" + entry.removePrefix("./")
    }

    private fun emitM3u8(label: String, m3u8: String, referer: String, linkOut: (HikariStream) -> Unit): Boolean {
        // Never emit raw FlixCloud masters: XOR-obfuscated, unplayable without the proxy.
        if (isFlixUrl(m3u8)) return false
        linkOut(
            HikariStream(
                name = label,
                url = m3u8,
                headers = mapOf("User-Agent" to UA, "Referer" to referer),
                isM3u8 = true,
            )
        )
        return true
    }

    private fun passSubtitle(embedUrl: String, subtitleOut: (HikariSubtitle) -> Unit) {
        try {
            val query = URL(embedUrl).query ?: return
            val sub = Regex("(?:sub|caption_1|c1_file)=([^&]+)").find(query)?.groupValues?.get(1) ?: return
            val decoded = URLDecoder.decode(sub, "UTF-8")
            val label = Regex("(?:sub_1|c1_label)=([^&]+)").find(query)?.groupValues?.get(1)
                ?.let { URLDecoder.decode(it, "UTF-8") } ?: "English"
            subtitleOut(HikariSubtitle(label, decoded))
        } catch (t: Throwable) { }
    }
}
