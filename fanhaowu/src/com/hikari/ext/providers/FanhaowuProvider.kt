package com.hikari.ext.providers

import android.util.Base64
import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.sync.withPermit
import org.json.JSONArray
import org.json.JSONObject
import java.net.URLEncoder
import javax.crypto.Cipher
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec

/**
 * 番号屋 (ulwr4q.fanhaowu1.cyou) — Nuxt SPA backed by a same-host JSON API
 * at `/api/` (same backend family as 91DJU: same cipher envelope).
 *
 * Protocol:
 *  - responses are either {code:0, data:{...}} or {cipher:"..."} where
 *    cipher is AES-CBC/base64 (key "a9yX32LpQvUt7wBc",
 *    iv "N7cPk2Bv38hWqFzM") decrypting to another {code:0, data} envelope.
 *  - list: GET /api/videos?kw=<q>&page=<n> (or ?category_id=<id>)
 *  - detail: GET /api/movie?id=<id> -> {info:{id, title, cover_url,
 *    play_url}}; play_url is "name$url[#name$url...]".
 *  - resolve: GET /api/parse_video?url=<detailUrl> -> {m3u8_url}.
 *  Cover images may be AES-encrypted at rest — decrypted in-extension
 *  (data: URIs); plain covers pass through untouched.
 */
class FanhaowuProvider : HikariProvider {

    override val id = "fanhaowu"
    override val name = "FanHaoWu"
    override val mainUrl = "https://ulwr4q.fanhaowu1.cyou"
    override val description = "FanHaoWu videos: latest wall, 20+ categories + search — direct m3u8, unlimited pages."
    override val version = 2
    override val tvTypes = setOf(HikariMediaType.MOVIE)

    companion object {
        private const val AES_KEY = "a9yX32LpQvUt7wBc"
        private const val AES_IV = "N7cPk2Bv38hWqFzM"
        private const val IMG_KEY = "f5d965df75336270"
        private const val IMG_IV = "97b60394abc2fbe1"
        private const val COVER_MEM_MAX = 64
        private val coverCache = object : LinkedHashMap<String, String>(COVER_MEM_MAX, 0.75f, true) {
            override fun removeEldestEntry(eldest: MutableMap.MutableEntry<String, String>): Boolean =
                size > COVER_MEM_MAX
        }
        private val coverCacheMutex = Mutex()
        private val coverPermits = Semaphore(6)
    }

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("latest", "最新视频", HikariMediaType.MOVIE),
        HikariCatalog("1319", "自拍偷拍", HikariMediaType.MOVIE),
        HikariCatalog("1315", "国产主播", HikariMediaType.MOVIE),
        HikariCatalog("1328", "中文字幕", HikariMediaType.MOVIE),
        HikariCatalog("1341", "国产传媒", HikariMediaType.MOVIE),
        HikariCatalog("1340", "熟女少妇", HikariMediaType.MOVIE),
        HikariCatalog("1339", "颜值正义", HikariMediaType.MOVIE),
        HikariCatalog("1336", "酒店探花", HikariMediaType.MOVIE),
        HikariCatalog("1348", "偷情少妇", HikariMediaType.MOVIE),
        HikariCatalog("1344", "家庭乱伦", HikariMediaType.MOVIE),
        HikariCatalog("1343", "SM调教", HikariMediaType.MOVIE),
        HikariCatalog("1342", "野战车震", HikariMediaType.MOVIE),
        HikariCatalog("1316", "日韩精品", HikariMediaType.MOVIE),
        HikariCatalog("1314", "日韩无码", HikariMediaType.MOVIE),
        HikariCatalog("1317", "欧美劲爆", HikariMediaType.MOVIE),
        HikariCatalog("1349", "日本有码", HikariMediaType.MOVIE),
        HikariCatalog("1332", "AV明星", HikariMediaType.MOVIE),
        HikariCatalog("1327", "AI换脸", HikariMediaType.MOVIE),
        HikariCatalog("1321", "巨乳尤物", HikariMediaType.MOVIE),
        HikariCatalog("1329", "人妻系列", HikariMediaType.MOVIE),
        HikariCatalog("1346", "学生空姐", HikariMediaType.MOVIE),
        HikariCatalog("1366", "cosplay", HikariMediaType.MOVIE),
        HikariCatalog("1365", "素人搭讪", HikariMediaType.MOVIE),
        HikariCatalog("1320", "伦理影片", HikariMediaType.MOVIE),
        HikariCatalog("1318", "成人动漫", HikariMediaType.MOVIE),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val path = if (catalog.id == "latest") {
            "/api/videos?page=$page"
        } else {
            "/api/videos?category_id=${catalog.id}&page=$page"
        }
        val data = apiGet(path) ?: return emptyList()
        return parseList(data.optJSONArray("list"))
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        val data = apiGet("/api/videos?kw=${URLEncoder.encode(q, "UTF-8")}&page=$page")
            ?: return emptyList()
        return parseList(data.optJSONArray("list"))
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val info = movieInfo(media.id) ?: return media
        val rawCover = info.optString("cover_url").takeIf { it.isNotBlank() }
        return media.copy(
            title = info.optString("title").ifBlank { media.title },
            posterUrl = (rawCover?.let { decryptCover(it) } ?: rawCover) ?: media.posterUrl,
        )
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val eps = episodesOf(media.id)
        return if (eps.isEmpty()) null else eps
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val target = episode?.id ?: episodesOf(media.id).firstOrNull()?.id
        val detailUrl = target?.takeIf { it.isNotBlank() } ?: return emptyList()
        val data = apiGet("/api/parse_video?url=${URLEncoder.encode(detailUrl, "UTF-8")}")
            ?: return emptyList()
        val m3u8 = data.optString("m3u8_url").trim()
        if (m3u8.isBlank()) return emptyList()
        return listOf(
            HikariStream(
                name = episode?.name?.takeIf { it.isNotBlank() } ?: "FanHaoWu",
                url = m3u8,
                headers = mapOf("Referer" to "$mainUrl/"),
                isM3u8 = true,
            ),
        )
    }

    // ---- API plumbing ----

    private suspend fun parseList(arr: JSONArray?): List<HikariMedia> {
        if (arr == null) return emptyList()
        data class Card(val id: String, val title: String, val cover: String?)
        val cards = ArrayList<Card>(arr.length())
        for (i in 0 until arr.length()) {
            val v = arr.optJSONObject(i) ?: continue
            val id = v.opt("id")?.toString()?.takeIf { it.isNotBlank() && it != "null" } ?: continue
            val title = v.optString("title").trim().ifBlank { continue }
            cards.add(Card(id, title, v.optString("cover_url").takeIf { it.isNotBlank() }))
        }
        val covers = coroutineScope {
            cards.map { card ->
                async {
                    val c = card.cover
                    if (c != null) coverPermits.withPermit { decryptCover(c) } ?: c else c
                }
            }.awaitAll()
        }
        return cards.mapIndexed { i, card ->
            HikariMedia(
                id = card.id,
                title = card.title,
                type = HikariMediaType.MOVIE,
                posterUrl = covers[i],
            )
        }
    }

    private suspend fun movieInfo(id: String): JSONObject? {
        val data = apiGet("/api/movie?id=${URLEncoder.encode(id, "UTF-8")}") ?: return null
        return data.optJSONObject("info")
    }

    private suspend fun episodesOf(id: String): List<HikariEpisode> {
        val info = movieInfo(id) ?: return emptyList()
        val bundle = info.optString("play_url")
        if (bundle.isBlank()) return emptyList()
        val out = ArrayList<HikariEpisode>()
        var n = 1
        for (part in bundle.split('#')) {
            val cut = part.indexOf('$')
            if (cut <= 0 || cut >= part.length - 1) continue
            val url = part.substring(cut + 1).trim()
            if (url.isBlank()) continue
            val name = part.substring(0, cut).trim().ifBlank { "正片" }
            out.add(HikariEpisode(number = n++, id = url, name = name))
        }
        return out
    }

    private suspend fun apiGet(path: String): JSONObject? {
        val raw = HikariNet.getString(
            "$mainUrl$path",
            mapOf("Accept" to "application/json"),
        ) ?: return null
        val root = runCatching { JSONObject(raw) }.getOrNull() ?: return null
        val env = when {
            root.has("cipher") -> {
                val dec = aesDecrypt(root.optString("cipher")) ?: return null
                runCatching { JSONObject(dec) }.getOrNull() ?: return null
            }
            else -> root
        }
        if (env.optInt("code", 0) != 0 && env.has("code")) return null
        return env.optJSONObject("data")
    }

    private fun aesDecrypt(base64: String): String? = try {
        val cipher = Cipher.getInstance("AES/CBC/PKCS5Padding")
        cipher.init(
            Cipher.DECRYPT_MODE,
            SecretKeySpec(AES_KEY.toByteArray(Charsets.UTF_8), "AES"),
            IvParameterSpec(AES_IV.toByteArray(Charsets.UTF_8)),
        )
        val raw = HikariNet.base64Decode(base64) ?: return null
        String(cipher.doFinal(raw), Charsets.UTF_8)
    } catch (e: Exception) {
        null
    }

    private suspend fun decryptCover(url: String): String? {
        coverCacheMutex.withLock { coverCache[url]?.let { return it } }
        val out = decryptCoverFresh(url)
        if (out != null) coverCacheMutex.withLock { coverCache[url] = out }
        return out
    }

    private suspend fun decryptCoverFresh(url: String): String? = try {
        val bytes = HikariNet.getBytes(url, mapOf("Referer" to "$mainUrl/", "User-Agent" to HikariNet.browserHeaders.getValue("User-Agent"), "Accept" to "image/*,*/*;q=0.8")) ?: return null
        if (isImageBytes(bytes)) return url
        val cipher = Cipher.getInstance("AES/CBC/NoPadding")
        cipher.init(
            Cipher.DECRYPT_MODE,
            SecretKeySpec(IMG_KEY.toByteArray(Charsets.UTF_8), "AES"),
            IvParameterSpec(IMG_IV.toByteArray(Charsets.UTF_8)),
        )
        val decrypted = cipher.doFinal(bytes)
        if (!isImageBytes(decrypted)) return null
        "data:image/jpeg;base64," + Base64.encodeToString(decrypted, Base64.NO_WRAP)
    } catch (e: Exception) {
        null
    }

    private fun isImageBytes(b: ByteArray): Boolean {
        if (b.size < 12) return false
        return (b[0] == 0xFF.toByte() && b[1] == 0xD8.toByte()) ||
            (b[0] == 0x89.toByte() && b[1] == 0x50.toByte() && b[2] == 0x4E.toByte() && b[3] == 0x47.toByte()) ||
            (b[0] == 0x47.toByte() && b[1] == 0x49.toByte() && b[2] == 0x46.toByte()) ||
            (b[0] == 0x52.toByte() && b[1] == 0x49.toByte() && b[2] == 0x46.toByte() && b[3] == 0x46.toByte())
    }
}
