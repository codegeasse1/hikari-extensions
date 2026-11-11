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
import org.json.JSONObject
import java.net.URLEncoder
import java.security.MessageDigest
import java.util.UUID
import javax.crypto.Cipher
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec

/**
 * PPJAV (PP福利大全, jjkswra10.ppjav.one) — Vue SPA backed by a signed JSON
 * API at `https://zdap.gkquu.cn:4438/zd/`.
 *
 * Protocol (reverse-engineered from the site's own JS):
 *  - every request carries headers token=<guid> sign=<MD5> timestamp=<ms>
 *    nonce=<16 chars> url=<site host>. GET sign = MD5(ts+guid+nonce+SALT);
 *    POST sign = MD5(sortedQuery+ts+guid+nonce+SALT).
 *  - the guid is just a client-generated UUID (localStorage on the web);
 *    POST /user/userLogin {deviceCode, source:"1", channel:"default_3"}
 *    registers it, after which all /sp calls return returnValue 1.
 *  - returnData may be AES-CBC-encrypted (key "mh_aes=19@#$@%#",
 *    iv "5e1y6w452uqw9jq8") or plain JSON.
 *
 *  - search:  GET /sp/getSearchList {plateId, searchName, page, size}
 *             -> {total, pageSize=30, videoList[{plateId, videoId,
 *                videoTitle, videoCover, videoUrlOne/Two/Three, duration}]}
 *  - catalog: GET /sp/getLabelVideoList {plateId, labelId, page, size}
 *             (labelId = first entry of /sp/getPlateLabelList, cached)
 *  - detail:  GET /sp/getVideoDetail {plateId, videoId}
 *             -> {videoDetail{... same fields ...}}
 *  The play URLs are direct m3u8/mp4 lines (twitter CDN + mirror hosts).
 *  Pages are sequential (1..N until videoList is empty) — unlimited.
 */
class PpJavProvider : HikariProvider {

    override val id = "ppjav"
    override val name = "PPJAV"
    override val mainUrl = "https://jjkswra10.ppjav.one"
    override val description = "PP福利大全 video walls + search — direct m3u8/mp4 lines, unlimited pages."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.MOVIE)

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? = null

    companion object {
        private const val API = "https://zdap.gkquu.cn:4438/zd"
        private const val SITE_HOST = "jjkswra10.ppjav.one"
        private const val SALT = "@1243asd31**21#"
        private const val AES_KEY = "mh_aes=19@#\$@%#"
        private const val AES_IV = "5e1y6w452uqw9jq8"
        private const val SEARCH_PLATE = "22"
        private const val PAGE_SIZE = 30
        private const val NONCE_CHARS = "ABCDEFGHJKMNPQRSTWXYZabcdefhijkmnprstwxyz2345678"

        private val authMutex = Mutex()
        private var guid: String? = null

        private val labelMutex = Mutex()
        private val labelCache = HashMap<String, String>()
    }

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("22", "成人视频", HikariMediaType.MOVIE),
        HikariCatalog("19", "成人天堂", HikariMediaType.MOVIE),
        HikariCatalog("5", "长视频", HikariMediaType.MOVIE),
        HikariCatalog("6", "AV", HikariMediaType.MOVIE),
        HikariCatalog("7", "动漫", HikariMediaType.MOVIE),
        HikariCatalog("4", "短视频", HikariMediaType.MOVIE),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val labelId = labelFor(catalog.id) ?: return emptyList()
        val data = signedGet(
            "/sp/getLabelVideoList",
            mapOf("plateId" to catalog.id, "labelId" to labelId, "page" to page.toString(), "size" to "25"),
        ) ?: return emptyList()
        return parseVideoList(data)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        val data = signedGet(
            "/sp/getSearchList",
            mapOf("plateId" to SEARCH_PLATE, "searchName" to q, "page" to page.toString(), "size" to PAGE_SIZE.toString()),
        ) ?: return emptyList()
        return parseVideoList(data)
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val (plateId, videoId) = splitId(media.id) ?: return media
        val data = signedGet("/sp/getVideoDetail", mapOf("plateId" to plateId, "videoId" to videoId))
            ?: return media
        val d = data.optJSONObject("videoDetail") ?: return media
        val title = d.optString("videoTitle").ifBlank { media.title }
        val poster = d.optString("videoCover").ifBlank { media.posterUrl ?: "" }
        val des = d.optString("videoDes")
        val overview = when {
            des.isNotBlank() -> des
            else -> listOf(
                d.optString("duration").takeIf { it.isNotBlank() },
                d.optString("nickName").takeIf { it.isNotBlank() },
            ).filterNotNull().joinToString(" · ").takeIf { it.isNotBlank() }
        }
        return media.copy(title = title, posterUrl = poster.ifBlank { null }, overview = overview)
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val (plateId, videoId) = splitId(media.id) ?: return emptyList()
        val data = signedGet("/sp/getVideoDetail", mapOf("plateId" to plateId, "videoId" to videoId))
            ?: return emptyList()
        val d = data.optJSONObject("videoDetail") ?: return emptyList()
        val lines = listOf(
            "线路1" to d.optString("videoUrlOne"),
            "线路2" to d.optString("videoUrlTwo"),
            "线路3" to d.optString("videoUrlThree"),
        ).filter { it.second.isNotBlank() }
        if (lines.isEmpty()) return emptyList()
        val headers = mapOf(
            "Referer" to "$mainUrl/",
            "User-Agent" to HikariNet.browserHeaders.getValue("User-Agent"),
        )
        val global = lines.size > 1
        return lines.mapIndexed { i, (name, url) ->
            HikariStream(
                name = when {
                    lines.size == 1 -> "PPJAV"
                    i == 0 && global -> "全球线路"
                    i == 1 && global -> "优化线路"
                    i == 2 -> "高清"
                    else -> name
                },
                url = url,
                headers = headers,
                isM3u8 = url.lowercase().contains(".m3u8"),
            )
        }
    }

    // ---- API plumbing ----

    private fun parseVideoList(data: JSONObject): List<HikariMedia> {
        val arr = data.optJSONArray("videoList") ?: return emptyList()
        val out = ArrayList<HikariMedia>(arr.length())
        for (i in 0 until arr.length()) {
            val v = arr.optJSONObject(i) ?: continue
            val plateId = v.opt("plateId")?.toString()
                ?.takeIf { it.isNotBlank() && it != "null" } ?: continue
            val videoId = v.optString("videoId").trim()
            if (videoId.isBlank()) continue
            val title = v.optString("videoTitle").trim().ifBlank { continue }
            out.add(
                HikariMedia(
                    id = "$plateId/$videoId",
                    title = title,
                    type = HikariMediaType.MOVIE,
                    posterUrl = v.optString("videoCover").takeIf { it.isNotBlank() },
                ),
            )
        }
        return out
    }

    private fun splitId(id: String): Pair<String, String>? {
        val i = id.indexOf('/')
        if (i <= 0 || i >= id.length - 1) return null
        return id.substring(0, i) to id.substring(i + 1)
    }

    /** First label tab of a plate (the default wall), memoized per process. */
    private suspend fun labelFor(plateId: String): String? {
        labelMutex.withLock { labelCache[plateId]?.let { return it } }
        val data = signedGet("/sp/getPlateLabelList", mapOf("plateId" to plateId)) ?: return null
        val labels = data.optJSONObject("plateCenter")?.optJSONArray("plateLabelList") ?: return null
        for (i in 0 until labels.length()) {
            val id = labels.optJSONObject(i)?.opt("exploreLabelId")?.toString()
            if (!id.isNullOrBlank() && id != "null") {
                labelMutex.withLock { labelCache[plateId] = id }
                return id
            }
        }
        return null
    }

    /**
     * Signed GET against the zd API. Returns returnData as JSONObject
     * (AES-decrypted when needed), or null on any failure. Re-registers the
     * device once when the server reports "login required" (1000).
     */
    private suspend fun signedGet(path: String, params: Map<String, String>): JSONObject? {
        ensureLogin() ?: return null
        var res = signedGetOnce(path, params)
        if (res != null && res.optInt("returnValue", -1) == 1000) {
            authMutex.withLock { guid = null }
            ensureLogin() ?: return null
            res = signedGetOnce(path, params)
        }
        if (res == null || res.optInt("returnValue", -1) != 1) return null
        return when (val data = res.opt("returnData")) {
            is JSONObject -> data
            is String -> runCatching { JSONObject(aesDecrypt(data)) }.getOrNull()
            else -> null
        }
    }

    private suspend fun signedGetOnce(path: String, params: Map<String, String>): JSONObject? {
        val g = guid ?: return null
        val ts = System.currentTimeMillis().toString()
        val nonce = randomNonce(16)
        val qs = params.toSortedMap().entries.joinToString("&") { (k, v) ->
            k + "=" + URLEncoder.encode(v, "UTF-8")
        }
        val sign = md5(ts + g + nonce + SALT)
        val headers = mapOf(
            "Content-Type" to "application/x-www-form-urlencoded",
            "token" to g,
            "sign" to sign,
            "timestamp" to ts,
            "nonce" to nonce,
            "url" to SITE_HOST,
        )
        val body = HikariNet.getString("$API$path?$qs", headers) ?: return null
        return runCatching { JSONObject(body) }.getOrNull()
    }

    /** Registers a fresh client UUID as this install's device identity. */
    private suspend fun ensureLogin(): String? {
        authMutex.withLock { guid?.let { return it } }
        val g = UUID.randomUUID().toString()
        val ts = System.currentTimeMillis().toString()
        val nonce = randomNonce(16)
        val params = mapOf("channel" to "default_3", "deviceCode" to g, "source" to "1")
        val sorted = params.toSortedMap().entries.joinToString("&") { (k, v) -> "$k=$v" }
        val sign = md5(sorted + ts + g + nonce + SALT)
        val headers = mapOf(
            "Content-Type" to "application/json",
            "token" to g,
            "sign" to sign,
            "timestamp" to ts,
            "nonce" to nonce,
            "url" to SITE_HOST,
        )
        val body = JSONObject(params).toString()
        val ok = try {
            val res = HikariNet.postString("$API/user/userLogin", body, headers)?.let { JSONObject(it) }
            res != null && res.optInt("returnValue", -1) == 1
        } catch (e: Exception) {
            false
        }
        if (!ok) return null
        authMutex.withLock { guid = g }
        return g
    }

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

    private fun randomNonce(len: Int): String {
        val rnd = java.util.Random()
        return (1..len).map { NONCE_CHARS[rnd.nextInt(NONCE_CHARS.length)] }.joinToString("")
    }

    private fun aesDecrypt(base64: String): String {
        val cipher = Cipher.getInstance("AES/CBC/PKCS5Padding")
        cipher.init(
            Cipher.DECRYPT_MODE,
            SecretKeySpec(AES_KEY.toByteArray(Charsets.UTF_8), "AES"),
            IvParameterSpec(AES_IV.toByteArray(Charsets.UTF_8)),
        )
        val raw = HikariNet.base64Decode(base64) ?: throw IllegalArgumentException("bad base64")
        return String(cipher.doFinal(raw), Charsets.UTF_8)
    }
}
