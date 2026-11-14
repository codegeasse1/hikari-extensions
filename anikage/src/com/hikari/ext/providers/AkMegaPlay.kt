package com.hikari.ext.providers

import android.util.Base64
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariStream
import com.hikari.ext.HikariSubtitle
import org.json.JSONObject
import javax.crypto.Cipher
import javax.crypto.Mac
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec

/**
 * MegaPlay family resolver (megaplay.buzz / vidwish.live / vidtube.site):
 * page -> data-id -> /stream/getSourcesNew|getSources -> enc/file + tracks.
 * Ported from the bundle MegaPlayHelper (org.json instead of Jackson).
 */
object AkMegaPlay {
    private const val UA =
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

    class MegaPlayStream(val m3u8: String, val subtitles: List<Pair<String, String>>)

    fun audioTypeFromUrl(url: String): String? =
        Regex("/(dub|sub)(?:[/?#]|$)").find(url)?.groupValues?.get(1)

    suspend fun resolveStream(embedUrl: String, referer: String?): MegaPlayStream? {
        val host = Regex("https?://([^/]+)").find(embedUrl)?.groupValues?.get(1) ?: return null
        val pageHeaders = mapOf(
            "User-Agent" to UA,
            "Referer" to (referer ?: "https://$host/"),
        )
        val pageHtml = HikariNet.getString(embedUrl, pageHeaders)
            ?: HikariNet.getStringSmart(embedUrl, pageHeaders) ?: return null

        val streamId = Regex("data-id=[\"'](\\d+)\"").find(pageHtml)?.groupValues?.get(1)
            ?: Regex("data-realid=[\"'](\\d+)\"").find(pageHtml)?.groupValues?.get(1)
            ?: Regex("/stream/s-\\d+/(\\d+)/").find(embedUrl)?.groupValues?.get(1)
            ?: return null

        val audioType = audioTypeFromUrl(embedUrl)
            ?: Regex("type\\s*:\\s*['\"](dub|sub)['\"]").find(pageHtml)?.groupValues?.get(1)
            ?: "sub"

        val altHost = Regex("data-domain=[\"']([^\"']+)[\"']").find(pageHtml)?.groupValues?.get(1)
        val hosts = listOfNotNull(host, altHost?.takeIf { it != host }).distinct()

        for (apiHost in hosts) {
            val base = "https://$apiHost"
            val ajaxHeaders = mapOf(
                "User-Agent" to UA,
                "Accept" to "*/*",
                "X-Requested-With" to "XMLHttpRequest",
                "Origin" to base,
                "Referer" to embedUrl,
            )
            for (endpoint in listOf("getSourcesNew", "getSources")) {
                val text = HikariNet.getString("$base/stream/$endpoint?id=$streamId&type=$audioType", ajaxHeaders)
                    ?: continue
                val root = runCatching { JSONObject(text) }.getOrNull() ?: continue
                val streamUrl = extractStream(root, base) ?: continue
                return MegaPlayStream(streamUrl, parseSubtitleTracks(root, streamUrl))
            }
        }
        return null
    }

    private suspend fun extractStream(root: JSONObject, base: String): String? {
        val sources = root.opt("sources")
        when {
            sources is JSONObject -> sources.optString("file").takeIf { it.isNotBlank() }?.let { return migrateLegacyUrl(it) }
            sources is org.json.JSONArray && sources.length() > 0 ->
                sources.optJSONObject(0)?.optString("file")?.takeIf { it.isNotBlank() }?.let { return migrateLegacyUrl(it) }
        }
        val enc = root.optString("enc").takeIf { it.isNotBlank() } ?: return null
        return migrateLegacyUrl(AkMegaPlayCipher.resolveEncStreamUrl(enc, base) ?: return null)
    }

    private fun migrateLegacyUrl(url: String): String {
        if (!url.contains("https://cdn.imgnex.top/anime")) return url
        return url.replace("https://cdn.imgnex.top/anime", "https://megap.norami.top")
    }

    private fun parseSubtitleTracks(root: JSONObject, m3u8: String): List<Pair<String, String>> {
        val subs = mutableListOf<Pair<String, String>>()
        val origin = Regex("https?://[^/]+").find(m3u8)?.value ?: "https://megap.norami.top"
        val tracks = root.optJSONArray("tracks") ?: return subs
        for (i in 0 until tracks.length()) {
            val el = tracks.optJSONObject(i) ?: continue
            val kind = el.optString("kind")
            if (kind != "captions" && kind != "subtitles") continue
            val file = el.optString("file")
            if (file.isBlank()) continue
            val migrated = if (file.contains("https://cdn.imgnex.top/anime")) file.replace("https://cdn.imgnex.top/anime", origin) else file
            subs.add((el.optString("label").ifBlank { "English" }) to migrated)
        }
        return subs
    }

    private const val TOKEN_KEY = "MpCdnT0k3n!9f2K#xQ7vL5mR8wN1pY4s"
    private const val TOKEN_LIFETIME_SECONDS = 7L * 24L * 60L * 60L
    private val hexIdsRegex = Regex("/([a-f0-9]{32})/([a-f0-9]{32})/", RegexOption.IGNORE_CASE)

    private fun b64url(bytes: ByteArray): String =
        Base64.encodeToString(bytes, Base64.URL_SAFE or Base64.NO_WRAP or Base64.NO_PADDING)

    fun signUrl(url: String): String {
        val m = hexIdsRegex.find(url) ?: return url
        val expires = System.currentTimeMillis() / 1000L + TOKEN_LIFETIME_SECONDS
        val payload = "$expires|${m.groupValues[1].lowercase()}/${m.groupValues[2].lowercase()}"
        val mac = Mac.getInstance("HmacSHA256")
        mac.init(SecretKeySpec(TOKEN_KEY.toByteArray(Charsets.UTF_8), "HmacSHA256"))
        val signature = mac.doFinal(payload.toByteArray(Charsets.UTF_8))
        val token = "${b64url(payload.toByteArray(Charsets.UTF_8))}.${b64url(signature)}"
        val sep = if (url.contains('?')) "&" else "?"
        return "$url${sep}token=$token"
    }

    private data class VariantEntry(val url: String, val quality: Int?)

    private fun parseVariants(masterUrl: String, masterText: String): List<VariantEntry> {
        val base = masterUrl.substringBefore('?').let { it.substringBeforeLast('/') + "/" }
        val out = mutableListOf<VariantEntry>()
        val lines = masterText.lines()
        var i = 0
        while (i < lines.size) {
            val line = lines[i].trim()
            if (line.startsWith("#EXT-X-STREAM-INF:")) {
                val quality = Regex("RESOLUTION=(\\d+)x(\\d+)").find(line)?.groupValues?.get(2)?.toIntOrNull()
                var j = i + 1
                while (j < lines.size && (lines[j].isBlank() || lines[j].startsWith("#"))) j++
                if (j < lines.size) {
                    val uri = lines[j].trim()
                    if (uri.isNotEmpty()) {
                        out.add(VariantEntry(if (uri.startsWith("http")) uri else base + uri, quality))
                    }
                    i = j
                }
            }
            i++
        }
        return out
    }

    suspend fun emitLinks(
        source: String,
        label: String,
        m3u8: String,
        referer: String,
        subtitles: List<Pair<String, String>>,
        subtitleOut: (HikariSubtitle) -> Unit,
        linkOut: (HikariStream) -> Unit,
    ): Boolean {
        val playHeaders = mapOf("User-Agent" to UA, "Referer" to referer)
        val signedMaster = signUrl(m3u8)
        val masterText = HikariNet.getString(signedMaster, playHeaders)
        var found = false
        val variants = masterText?.let { parseVariants(m3u8, it) } ?: emptyList()
        if (variants.isNotEmpty()) {
            for (v in variants) {
                val suffix = v.quality?.let { " ${it}p" } ?: ""
                linkOut(
                    HikariStream(
                        name = "$label$suffix",
                        url = signUrl(v.url),
                        headers = playHeaders,
                        isM3u8 = true,
                    )
                )
                found = true
            }
        } else if (masterText != null && masterText.contains("#EXTM3U")) {
            linkOut(HikariStream(name = label, url = signedMaster, headers = playHeaders, isM3u8 = true))
            found = true
        }
        for ((subLabel, subUrl) in subtitles) subtitleOut(HikariSubtitle(subLabel, subUrl))
        return found
    }
}

object AkMegaPlayCipher {
    private const val FALLBACK_KEY_SEED = "i?LMTAx0Q6,:}50U"
    private const val FALLBACK_IV_SEED = "W0;27ToaUpl_P%'c"

    @Volatile private var cachedSeeds: Pair<String, String>? = null

    private val keyPairRegex = Regex("[A-Za-z]\\w*=\"([^\"]{16})\",[A-Za-z]\\w*=\"([^\"]{16})\"")
    private val fileRegex = Regex("\"file\"\\s*:\\s*\"([^\"]+)\"")

    private fun fallback() = Pair(FALLBACK_KEY_SEED, FALLBACK_IV_SEED)

    private suspend fun keySeedCandidates(baseUrl: String): List<Pair<String, String>> {
        cachedSeeds?.let { return listOf(it, fallback()) }
        val dynamic = try {
            val js = HikariNet.getString("$baseUrl/lib/newclient.min.js") ?: return listOf(fallback())
            keyPairRegex.find(js)?.groupValues?.let { g -> Pair(g[1], g[2]).also { cachedSeeds = it } }
        } catch (t: Throwable) {
            null
        }
        return listOfNotNull(dynamic, fallback())
    }

    private fun decryptToken(enc: String, keySeed: String, ivSeed: String): String? {
        return try {
            var b64 = enc.replace('-', '+').replace('_', '/')
            while (b64.length % 4 != 0) b64 += "="
            val cipherBytes = Base64.decode(b64, Base64.DEFAULT)
            val seedBytes = keySeed.toByteArray(Charsets.UTF_8)
            val keyBytes = ByteArray(32)
            System.arraycopy(seedBytes, 0, keyBytes, 0, minOf(32, seedBytes.size))
            val ivBytes = ByteArray(16)
            val ivSeedBytes = ivSeed.toByteArray(Charsets.UTF_8)
            System.arraycopy(ivSeedBytes, 0, ivBytes, 0, minOf(16, ivSeedBytes.size))
            val cipher = Cipher.getInstance("AES/CBC/PKCS5Padding")
            cipher.init(Cipher.DECRYPT_MODE, SecretKeySpec(keyBytes, "AES"), IvParameterSpec(ivBytes))
            String(cipher.doFinal(cipherBytes), Charsets.UTF_8)
        } catch (t: Throwable) {
            null
        }
    }

    suspend fun resolveEncStreamUrl(enc: String, baseUrl: String): String? {
        for ((keySeed, ivSeed) in keySeedCandidates(baseUrl)) {
            val plain = decryptToken(enc, keySeed, ivSeed) ?: continue
            fileRegex.find(plain)?.groupValues?.get(1)?.let { return it }
        }
        return null
    }
}
