package com.hikari.ext.providers

import com.hikari.ext.HikariNet
import org.json.JSONObject

/**
 * MegaPlay embed flow used by 9anime watch pages (port of NineAnimeExtractors
 * MegaPlayBaseExtractor + MegaPlayCipher): #megaplay-player stream id ->
 * getSourcesNew/getSources ajax -> legacy file URL or AES-CBC encrypted token
 * (seeds scraped from newclient.min.js, static fallback) -> master playlist +
 * caption tracks. Hikari types throughout; the master playlist is emitted
 * directly (the player expands variants).
 */
internal object NaMegaPlay {

    private const val FALLBACK_KEY_SEED = "i?LMTAx0Q6,:}50U"
    private const val FALLBACK_IV_SEED = "W0;27ToaUpl_P%'c"

    @Volatile
    private var cachedSeeds: Pair<String, String>? = null

    private val keyPairRe = Regex("""[A-Za-z]\w*="([^"]{16})",[A-Za-z]\w*="([^"]{16})"""")
    private val fileRe = Regex(""""file"\s*:\s*"([^"]+)"""")

    private suspend fun seeds(baseUrl: String): List<Pair<String, String>> {
        cachedSeeds?.let { return listOf(it, FALLBACK_KEY_SEED to FALLBACK_IV_SEED) }
        val dynamic = try {
            val js = HikariNet.getString(
                "$baseUrl/lib/newclient.min.js",
                mapOf("User-Agent" to NaNet.UA, "Referer" to "$baseUrl/"),
            ) ?: ""
            keyPairRe.find(js)?.groupValues?.let { g ->
                (g[1] to g[2]).also { cachedSeeds = it }
            }
        } catch (_: Exception) {
            null
        }
        return listOfNotNull(dynamic, FALLBACK_KEY_SEED to FALLBACK_IV_SEED)
    }

    private fun decryptToken(enc: String, keySeed: String, ivSeed: String): String? {
        return try {
            var b64 = enc.replace('-', '+').replace('_', '/')
            while (b64.length % 4 != 0) b64 += "="
            val cipherBytes = android.util.Base64.decode(b64, android.util.Base64.DEFAULT)
            val seedBytes = keySeed.toByteArray(Charsets.UTF_8)
            val keyBytes = ByteArray(32)
            System.arraycopy(seedBytes, 0, keyBytes, 0, minOf(32, seedBytes.size))
            val ivBytes = ByteArray(16)
            val ivSeedBytes = ivSeed.toByteArray(Charsets.UTF_8)
            System.arraycopy(ivSeedBytes, 0, ivBytes, 0, minOf(16, ivSeedBytes.size))
            val cipher = javax.crypto.Cipher.getInstance("AES/CBC/PKCS5Padding")
            cipher.init(
                javax.crypto.Cipher.DECRYPT_MODE,
                javax.crypto.spec.SecretKeySpec(keyBytes, "AES"),
                javax.crypto.spec.IvParameterSpec(ivBytes),
            )
            String(cipher.doFinal(cipherBytes), Charsets.UTF_8)
        } catch (_: Exception) {
            null
        }
    }

    private suspend fun resolveEnc(enc: String, baseUrl: String): String? {
        for ((keySeed, ivSeed) in seeds(baseUrl)) {
            val plain = decryptToken(enc, keySeed, ivSeed) ?: continue
            fileRe.find(plain)?.groupValues?.get(1)?.let { return it }
        }
        return null
    }

    data class Resolved(val url: String, val subs: List<Pair<String, String>>)

    suspend fun resolve(embedUrl: String, referer: String?): Resolved? {
        return try {
            val host = try {
                java.net.URI(embedUrl).let { "${it.scheme}://${it.host}" }
            } catch (_: Exception) {
                return null
            }
            val html = HikariNet.getString(
                embedUrl,
                mapOf("User-Agent" to NaNet.UA, "Referer" to (referer ?: "$host/")),
            ) ?: return null
            val playerBlock = Regex(
                """<[^>]*id="megaplay-player"[^>]*>""",
                RegexOption.IGNORE_CASE,
            ).find(html)?.value ?: return null
            fun attr(name: String): String =
                Regex("""$name="([^"]+)"""", RegexOption.IGNORE_CASE)
                    .find(playerBlock)?.groupValues?.get(1).orEmpty()
            val streamId = attr("data-id").ifBlank { attr("data-realid") }
                .ifBlank { Regex("""/stream/s-\d+/(\d+)/""").find(embedUrl)?.groupValues?.get(1).orEmpty() }
                .ifBlank { return null }
            val type = if (embedUrl.contains("/dub", ignoreCase = true)) "dub" else "sub"
            val ajax = mapOf(
                "User-Agent" to NaNet.UA,
                "Accept" to "*/*",
                "X-Requested-With" to "XMLHttpRequest",
                "Referer" to embedUrl,
            )
            var root: JSONObject? = null
            for (ep in listOf("getSourcesNew", "getSources")) {
                try {
                    val text = HikariNet.getString("$host/stream/$ep?id=$streamId&type=$type", ajax)
                        ?: continue
                    root = JSONObject(text)
                    break
                } catch (_: Exception) {
                }
            }
            val r = root ?: return null
            var file: String? = null
            try {
                val sources = r.opt("sources")
                file = when (sources) {
                    is JSONObject -> sources.optString("file").ifBlank { null }
                    is org.json.JSONArray -> sources.optJSONObject(0)?.optString("file")?.ifBlank { null }
                    else -> null
                }
            } catch (_: Exception) {
            }
            if (file.isNullOrBlank()) {
                val enc = r.optString("enc")
                if (enc.isBlank()) return null
                file = resolveEnc(enc, host) ?: return null
            }
            // legacy imgnex cdn paths carry an /anime prefix the megap hosts dropped
            val cdnOrigin = if (file.contains("cdn.imgnex.top")) null
            else Regex("""https?://[^/]+""").find(file)?.value
            if (file.contains("https://cdn.imgnex.top/anime")) {
                file = file.replace("https://cdn.imgnex.top/anime", cdnOrigin ?: "https://megap.norami.top")
            }
            val subs = mutableListOf<Pair<String, String>>()
            try {
                val tracks = r.optJSONArray("tracks")
                if (tracks != null) {
                    for (i in 0 until tracks.length()) {
                        val t = tracks.optJSONObject(i) ?: continue
                        val kind = t.optString("kind")
                        if (kind != "captions" && kind != "subtitles") continue
                        var f = t.optString("file")
                        if (f.isBlank()) continue
                        if (f.contains("https://cdn.imgnex.top/anime")) {
                            f = f.replace("https://cdn.imgnex.top/anime", cdnOrigin ?: "https://megap.norami.top")
                        }
                        subs.add(t.optString("label").ifBlank { "Unknown" } to f)
                    }
                }
            } catch (_: Exception) {
            }
            Resolved(file, subs)
        } catch (_: Exception) {
            null
        }
    }
}

/** Shared UA + tiny HTML helpers for the 9anime port. */
internal object NaNet {
    const val UA =
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

    fun strip(s: String): String =
        s.replace(Regex("<[^>]+>"), " ").replace(Regex("\\s+"), " ").trim()

    fun unesc(s: String): String = s.replace("&amp;", "&").replace("&#39;", "'")
        .replace("&#039;", "'").replace("&quot;", "\"").replace("&lt;", "<")
        .replace("&gt;", ">").replace("&nbsp;", " ").replace("&#8217;", "'")
        .replace("&#8211;", "-").replace("&#038;", "&").trim()

    fun b64d(s: String): String? = try {
        var t = s.trim().replace("-", "+").replace("_", "/")
        while (t.length % 4 != 0) t += "="
        String(android.util.Base64.decode(t, android.util.Base64.DEFAULT), Charsets.UTF_8)
    } catch (_: Exception) {
        null
    }
}
