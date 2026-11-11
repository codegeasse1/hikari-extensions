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
 * 91DJU (91短剧, bwkalr.91dju.cyou) — Nuxt SPA backed by a same-host JSON API
 * at `/api/`.
 *
 * Protocol (reverse-engineered from the site's own JS):
 *  - responses are either `{code:0, data:{...}}` or `{cipher:"..."}` where
 *    cipher is AES-CBC/base64 (key "a9yX32LpQvUt7wBc",
 *    iv "N7cPk2Bv38hWqFzM") decrypting to another {code:0, data} envelope.
 *  - search/list: GET /api/videos?kw=<q>&page=<n> (or no kw for latest,
 *    or ?category_id=<id>) -> {page, ps=20, total, pages, list[{id, title,
 *    cover_url, category, category_id, ...}]}
 *  - detail:      GET /api/movie?id=<id>
 *                 -> {info:{id, title, cover_url, play_url, play_from},
 *                     next, prev}; play_url is "ep$detailUrl[#ep$detailUrl...]".
 *  - resolve:     GET /api/parse_video?url=<detailUrl> -> {m3u8_url}
 *  Cover images come down AES-encrypted (same key/iv as the MRDS/51CG poster
 *  scheme) and are decrypted in-extension to data: URIs; plain covers pass
 *  through untouched.
 *  Pages are sequential (1..N until list is empty) — unlimited.
 */
class DjuProvider : HikariProvider {

    override val id = "91dju"
    override val name = "91DJU"
    override val mainUrl = "https://bwkalr.91dju.cyou"
    override val description = "91短剧 videos: latest wall, 10 category walls + search — direct m3u8, unlimited pages."
    override val version = 2
    override val tvTypes = setOf(HikariMediaType.MOVIE)

    companion object {
        private const val AES_KEY = "a9yX32LpQvUt7wBc"
        private const val AES_IV = "N7cPk2Bv38hWqFzM"
        /** Cover images are AES-encrypted at rest (same key/iv as the
         *  MRDS/51CG poster scheme — same operator backend): raw bytes must
         *  be decrypted to render. NoPadding: JPEGs are block-aligned. */
        private const val IMG_KEY = "f5d965df75336270"
        private const val IMG_IV = "97b60394abc2fbe1"
        /** Decrypted covers, keyed by source URL: a home load materializes
         *  hundreds of cards; re-downloading + re-decrypting every cover on
         *  every load would trash the heap. Bounded LRU. */
        private const val COVER_MEM_MAX = 64
        private val coverCache = object : LinkedHashMap<String, String>(COVER_MEM_MAX, 0.75f, true) {
            override fun removeEldestEntry(eldest: MutableMap.MutableEntry<String, String>): Boolean =
                size > COVER_MEM_MAX
        }
        private val coverCacheMutex = Mutex()
        /** Bounds concurrent cover decrypts (polite to the pic CDN). */
        private val coverPermits = Semaphore(6)
    }

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("latest", "Latest", HikariMediaType.MOVIE),
        HikariCatalog("2041", "AI短剧", HikariMediaType.MOVIE),
        HikariCatalog("2042", "AI漫剧", HikariMediaType.MOVIE),
        HikariCatalog("2039", "Ai魔改", HikariMediaType.MOVIE),
        HikariCatalog("2044", "Ai真人剧", HikariMediaType.MOVIE),
        HikariCatalog("2045", "会所按摩", HikariMediaType.MOVIE),
        HikariCatalog("2088", "熟女做爱", HikariMediaType.MOVIE),
        HikariCatalog("2090", "成人动漫", HikariMediaType.MOVIE),
        HikariCatalog("2093", "内射中出", HikariMediaType.MOVIE),
        HikariCatalog("2094", "家庭乱伦", HikariMediaType.MOVIE),
        HikariCatalog("2095", "三级片", HikariMediaType.MOVIE),
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

    /**
     * Episode rows come from the detail's play_url bundle
     * ("name$url#name$url..."); the actual m3u8 is resolved per episode in
     * [getStreams] so opening a series never pays for every episode upfront.
     */
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
                name = episode?.name?.takeIf { it.isNotBlank() } ?: "91DJU",
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
        // Covers are ciphertext until decrypted (see companion): resolve them
        // concurrently under a bound so a 20-card page stays well inside the
        // app's per-page budget.
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

    /**
     * GETs a same-host /api path and unwraps the envelope: either
     * {code:0, data} directly or {cipher} (AES-decrypted to the same shape).
     */
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

    /**
     * Decrypts an AES-encrypted cover served by the pic.* CDN (the site's own
     * loader does exactly this before handing bytes to its image view).
     * Results are memoized (bounded LRU). Returns null when the bytes are
     * already a plain image or the download/decrypt fails — callers then
     * fall back to the raw URL.
     */
    private suspend fun decryptCover(url: String): String? {
        coverCacheMutex.withLock { coverCache[url]?.let { return it } }
        val out = decryptCoverFresh(url)
        if (out != null) {
            coverCacheMutex.withLock { coverCache[url] = out }
        }
        return out
    }

    private suspend fun decryptCoverFresh(url: String): String? = try {
        val bytes = HikariNet.getBytes(url, mapOf("Referer" to "$mainUrl/")) ?: return null
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
