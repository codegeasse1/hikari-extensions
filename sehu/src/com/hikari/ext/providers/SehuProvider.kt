package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.json.JSONArray
import org.json.JSONObject
import java.security.MessageDigest
import java.util.UUID
import javax.crypto.Cipher
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec

/**
 * SeHu (se0427hu.vip, brand "sehu") — video walls backed by a signed JSON
 * API at `https://api.jxdfm2.com`.
 *
 * Protocol (reverse-engineered from the site's own JS):
 *  - every request carries header x-req-info =
 *    channelId,,brandId,deviceId,deviceType,bundleId,t,version
 *    (e.g. "30,,sehu,H5-<id>,H5-pc,com.sh01.h523.vid,<ms>,1.0.0")
 *    with Content-Type text/plain and an AES-CBC-ZeroPadding body, except
 *    appConfig which posts an empty body.
 *  - key = t[-6:] + signKey[:4] + bundleId[:6] (16 chars);
 *    iv  = bundleId[-6:] + signKey[-4:] + deviceId[:6] (16 chars).
 *  - sign = MD5(sortedValuesConcat + signKey + endpoint).uppercase().
 *  - responses with code 10000 carry AES-encrypted data (same key/iv).
 *  - detail/search need a guest token: guestLogin {} -> token, sent as
 *    x-token. 20005 = token expired -> re-login once and retry.
 *
 * Catalogs come from the appConfig videoTypeList (level-1 video types);
 * video walls are queryTypeVideosH5 {typeId, pageNo 0-based}; search is
 * {keywords, pageNo 0-based, pageSize 13} -> searchList. Only
 * contentType == 1 items are videos (6 = actress profile, 3 = external ad).
 */
class SehuProvider : HikariProvider {

    override val id = "sehu"
    override val name = "SeHu"
    override val mainUrl = "https://www.se0427hu.vip:9527"
    override val description = "SeHu: Chinese, media originals, Japanese & Korean, Western, anime, Category III, AI face-swap, LGBTQ+ and commentary walls + search — direct m3u8."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.MOVIE)

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? = null

    companion object {
        private const val API = "https://api.jxdfm2.com"
        private const val SIGN_KEY = "52S8VB&uiFR^hprd"
        private const val BUNDLE = "com.sh01.h523.vid"
        private const val VERSION = "1.0.0"
        private const val BRAND = "sehu"
        private const val CHANNEL = "30"
        private const val DEVICE_TYPE = "H5-pc"
        private const val PAGE_SIZE = 13

        private const val EP_GUEST = "/ht/users/v2/guestLogin"
        private const val EP_LIST = "/ht/content/v2/queryTypeVideosH5"
        private const val EP_SEARCH = "/ht/content/v2/search"
        private const val EP_DETAIL = "/ht/content/v2/detail"

        private val deviceId: String by lazy {
            "H5-" + UUID.randomUUID().toString().replace("-", "").take(16)
        }
    }

    private val authMutex = Mutex()
    private var guestToken: String? = null

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("1", "Chinese", HikariMediaType.MOVIE),
        HikariCatalog("2", "Media Originals", HikariMediaType.MOVIE),
        HikariCatalog("3", "Japanese & Korean", HikariMediaType.MOVIE),
        HikariCatalog("10", "Commentary", HikariMediaType.MOVIE),
        HikariCatalog("4", "Western", HikariMediaType.MOVIE),
        HikariCatalog("5", "Anime", HikariMediaType.MOVIE),
        HikariCatalog("7", "AI Face-Swap", HikariMediaType.MOVIE),
        HikariCatalog("8", "LGBTQ+", HikariMediaType.MOVIE),
        HikariCatalog("6", "Category III", HikariMediaType.MOVIE),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val typeId = catalog.id.toIntOrNull() ?: return emptyList()
        val data = api(EP_LIST, mapOf("typeId" to typeId, "pageNo" to (page - 1).coerceAtLeast(0)), false)
            ?: return emptyList()
        return videosOf(data.optJSONArray("typeVideoList"))
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        val data = api(
            EP_SEARCH,
            mapOf("keywords" to q, "pageNo" to (page - 1).coerceAtLeast(0), "pageSize" to PAGE_SIZE),
            true,
        ) ?: return emptyList()
        return videosOf(data.optJSONArray("searchList"))
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val detail = detailOf(media.id) ?: return media
        val vd = detail.optJSONObject("videoDetail") ?: return media
        val desc = vd.optString("description").trim()
        val tags = vd.optString("tags").trim()
        val overview = listOf(desc, tags.takeIf { it.isNotBlank() }?.let { "Tags: $it" })
            .filter { it.isNotBlank() }
            .joinToString("\n\n")
        return media.copy(
            title = vd.optString("title").trim().takeIf { it.isNotBlank() } ?: media.title,
            posterUrl = vd.optString("img").trim().takeIf { it.isNotBlank() } ?: media.posterUrl,
            overview = overview,
        )
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val detail = detailOf(media.id) ?: return emptyList()
        val bundle = when (val p = detail.opt("playUrl")) {
            is JSONObject -> p
            is String -> try {
                JSONObject(p)
            } catch (e: Exception) {
                null
            }
            else -> null
        } ?: return emptyList()
        val m3u8 = bundle.optString("playUrl").trim()
        val mp4 = bundle.optString("downUrl").trim()
        val out = ArrayList<HikariStream>(2)
        if (m3u8.isNotBlank()) {
            out.add(
                HikariStream(
                    name = "SeHu HD",
                    url = m3u8,
                    headers = streamHeaders(),
                    isM3u8 = true,
                )
            )
        }
        if (mp4.isNotBlank()) {
            out.add(
                HikariStream(
                    name = "SeHu MP4",
                    url = mp4,
                    headers = streamHeaders(),
                    isM3u8 = mp4.lowercase().contains(".m3u8"),
                )
            )
        }
        return out
    }

    // ---- API plumbing ----

    private suspend fun detailOf(id: String): JSONObject? {
        val contentId = id.toIntOrNull() ?: return null
        return api(EP_DETAIL, mapOf("contentId" to contentId, "tryPlayFlag" to "0"), true)
    }

    private fun videosOf(arr: JSONArray?): List<HikariMedia> {
        if (arr == null) return emptyList()
        val out = ArrayList<HikariMedia>(arr.length())
        for (i in 0 until arr.length()) {
            val v = arr.optJSONObject(i) ?: continue
            if (v.optInt("contentType", 1) != 1) continue
            val title = v.optString("title").trim()
            if (title.isBlank()) continue
            val cid = when (val c = v.opt("contentId")) {
                is Number -> c.toLong().toString()
                is String -> c.trim().takeIf { it.isNotBlank() }
                else -> null
            } ?: continue
            out.add(
                HikariMedia(
                    id = cid,
                    title = title,
                    type = HikariMediaType.MOVIE,
                    posterUrl = v.optString("img").trim().takeIf { it.isNotBlank() },
                )
            )
        }
        return out
    }

    /**
     * Signed API call. Returns the decrypted data object, or null. On 20005
     * (auth error) with [needAuth], re-logs-in once and retries once.
     */
    private suspend fun api(endpoint: String, extra: Map<String, Any>, needAuth: Boolean): JSONObject? {
        val first = signedCall(endpoint, extra, needAuth)
        if (first != null) return first
        if (!needAuth) return null
        authMutex.withLock { guestToken = null }
        ensureGuest()
        return signedCall(endpoint, extra, true)
    }

    private suspend fun signedCall(endpoint: String, extra: Map<String, Any>, needAuth: Boolean): JSONObject? {
        val t = System.currentTimeMillis().toString()
        val key = t.takeLast(6) + SIGN_KEY.take(4) + BUNDLE.take(6)
        val iv = BUNDLE.takeLast(6) + SIGN_KEY.takeLast(4) + deviceId.take(6)
        val signInput = extra.toSortedMap().entries.joinToString("") { it.value.toString() } + SIGN_KEY + endpoint
        val body = LinkedHashMap<String, Any>(extra)
        body["sign"] = md5(signInput)
        val headers = HashMap<String, String>(4)
        headers["Content-Type"] = "text/plain"
        headers["x-req-info"] = listOf(CHANNEL, "", BRAND, deviceId, DEVICE_TYPE, BUNDLE, t, VERSION).joinToString(",")
        if (needAuth) {
            ensureGuest()
            guestToken?.let { headers["x-token"] = it }
        }
        val raw = try {
            val encrypted = aesEncryptToBase64(JSONObject(body).toString(), key, iv)
            HikariNet.postString(API + endpoint, encrypted, headers)
        } catch (e: Exception) {
            null
        } ?: return null
        return try {
            val res = JSONObject(raw)
            if (res.optInt("code", -1) != 10000) {
                if (res.optInt("code", -1) == 20005 && needAuth) {
                    authMutex.withLock { guestToken = null }
                }
                return null
            }
            val data = res.opt("data") ?: return null
            if (data is JSONObject) return data
            val text = data.toString()
            try {
                JSONObject(aesDecryptToString(text, key, iv))
            } catch (e: Exception) {
                try {
                    JSONObject(text)
                } catch (e2: Exception) {
                    null
                }
            }
        } catch (e: Exception) {
            null
        }
    }

    private suspend fun ensureGuest() {
        authMutex.withLock {
            if (!guestToken.isNullOrBlank()) return
        }
        val res = try {
            signedCall(EP_GUEST, emptyMap(), false)
        } catch (e: Exception) {
            null
        }
        val token = res?.optString("token").orEmpty()
        if (token.isNotBlank()) {
            authMutex.withLock { guestToken = token }
        }
    }

    private fun streamHeaders(): Map<String, String> = mapOf(
        "Referer" to "$mainUrl/",
        "User-Agent" to HikariNet.browserHeaders.getValue("User-Agent"),
    )

    private fun md5(s: String): String {
        val d = MessageDigest.getInstance("MD5").digest(s.toByteArray(Charsets.UTF_8))
        return buildString(d.size * 2) {
            for (b in d) {
                val v = b.toInt() and 0xFF
                if (v < 16) append('0')
                append(v.toString(16))
            }
        }.uppercase()
    }

    /** AES-128-CBC with manual ZeroPadding (CryptoJS-compatible). */
    private fun aesEncryptToBase64(plain: String, keyStr: String, ivStr: String): String {
        val data = plain.toByteArray(Charsets.UTF_8)
        var pad = 16 - (data.size % 16)
        if (pad == 0) pad = 16
        val padded = ByteArray(data.size + pad)
        System.arraycopy(data, 0, padded, 0, data.size)
        val cipher = Cipher.getInstance("AES/CBC/NoPadding")
        cipher.init(
            Cipher.ENCRYPT_MODE,
            SecretKeySpec(keyStr.toByteArray(Charsets.UTF_8), "AES"),
            IvParameterSpec(ivStr.toByteArray(Charsets.UTF_8)),
        )
        val out = cipher.doFinal(padded)
        return android.util.Base64.encodeToString(out, android.util.Base64.NO_WRAP)
    }

    private fun aesDecryptToString(b64: String, keyStr: String, ivStr: String): String {
        val raw = android.util.Base64.decode(b64.replace("\\s".toRegex(), ""), android.util.Base64.DEFAULT)
        val cipher = Cipher.getInstance("AES/CBC/NoPadding")
        cipher.init(
            Cipher.DECRYPT_MODE,
            SecretKeySpec(keyStr.toByteArray(Charsets.UTF_8), "AES"),
            IvParameterSpec(ivStr.toByteArray(Charsets.UTF_8)),
        )
        val padded = cipher.doFinal(raw)
        var end = padded.size
        while (end > 0 && padded[end - 1] == 0.toByte()) end--
        return String(padded, 0, end, Charsets.UTF_8).replace("\u0000", "")
    }
}
