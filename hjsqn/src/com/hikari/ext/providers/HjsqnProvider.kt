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
import java.net.URLEncoder
import javax.crypto.Cipher
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec

/**
 * HaiLuan (hjsqn.com) — 海角 SNS wall: category walls + tag walls + search.
 *
 * - catalogs: /category/<slug>/ (5 walls), pages /category/<slug>/page/<n>/.
 * - cards: /archives/<id>/ anchors (title attr + inner title div,
 *   covers in z-image-loader-url, AES at rest — same dx-family key).
 * - detail: dplayer data-config JSON carries the video.url, a same-host
 *   /media/m3u8 proxy path wrapping the signed CDN m3u8 (exp+token).
 * - search: /search/<kw>/ (same wall markup).
 */
class HjsqnProvider : HikariProvider {

    override val id = "hjsqn"
    override val name = "HaiLuan"
    override val mainUrl = "https://hjsqn.com"
    override val description = "HaiLuan: original/lewd/eating-melon/adventure/yellow walls + search — direct m3u8, unlimited pages."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.MOVIE)

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? = null

    companion object {
        private const val IMG_KEY = "f5d965df75336270"
        private const val IMG_IV = "97b60394abc2fbe1"
        private val BROWSER_UA by lazy { HikariNet.browserHeaders.getValue("User-Agent") }
        private const val COVER_MEM_MAX = 64
        private val coverCache = object : LinkedHashMap<String, String>(COVER_MEM_MAX, 0.75f, true) {
            override fun removeEldestEntry(eldest: MutableMap.MutableEntry<String, String>): Boolean =
                size > COVER_MEM_MAX
        }
        private val coverCacheMutex = Mutex()
        private val coverPermits = Semaphore(6)

        private val ARCH_ID_RE = Regex("^(\\d+)/?\"")
        private val TITLE_ATTR_RE = Regex("title=\"([^\"]{2,150})\"")
        private val TITLE_DIV_RE =
            Regex("xqbj-list-rows-image-title[^>]*>\\s*([^<]{2,150})\\s*<")
        private val RANK_TITLE_RE =
            Regex("rank-card-title[^>]*>\\s*([^<]{2,150})\\s*<")
        private val COVER_RE = Regex("z-image-loader-url=\"([^\"]+)\"")
        private val PLAYER_URL_RE = Regex("\"url\":\"((?:\\\\.|[^\"\\\\])*)\"")
        private val OG_TITLE_RE =
            Regex("<meta[^>]*property=\"og:title\"[^>]*content=\"([^\"]+)\"")
        private val OG_IMAGE_RE =
            Regex("<meta[^>]*property=\"og:image\"[^>]*content=\"([^\"]+)\"")
        private val DESC_RE =
            Regex("<meta[^>]*name=\"description\"[^>]*content=\"([^\"]{10,600})\"")
    }

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("hjyc", "HaiJiao Originals", HikariMediaType.MOVIE),
        HikariCatalog("hjll", "HaiJiao Taboo", HikariMediaType.MOVIE),
        HikariCatalog("hjcg", "Gossip & Leaks", HikariMediaType.MOVIE),
        HikariCatalog("hjth", "HaiJiao Adventure", HikariMediaType.MOVIE),
        HikariCatalog("hjwh", "Webcam Models", HikariMediaType.MOVIE),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val path = if (page <= 1) {
            "/category/${catalog.id}/"
        } else {
            "/category/${catalog.id}/page/$page/"
        }
        val html = HikariNet.getStringSmart(mainUrl + path, referer()) ?: return emptyList()
        return resolveCovers(parseCards(html))
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        val enc = URLEncoder.encode(q, "UTF-8").replace("+", "%20")
        val path = if (page <= 1) "/search/$enc/" else "/search/$enc/page/$page/"
        val html = HikariNet.getStringSmart(mainUrl + path, referer()) ?: return emptyList()
        return resolveCovers(parseCards(html))
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val html = HikariNet.getStringSmart("$mainUrl/archives/${media.id}/", referer()) ?: return media
        val title = OG_TITLE_RE.find(html)?.groupValues?.get(1)?.let { unesc(it) }?.trim()
            .takeUnless { it.isNullOrBlank() }
            ?: Regex("<title>([^<]{2,150})</title>").find(html)?.groupValues?.get(1)
                ?.let { unesc(it) }?.split("_", "-", "|")?.firstOrNull()?.trim()
        val cover = OG_IMAGE_RE.find(html)?.groupValues?.get(1)?.let { unesc(it) }
        val desc = DESC_RE.find(html)?.groupValues?.get(1)?.let { unesc(it) }?.trim().orEmpty()
        return media.copy(
            title = title?.takeIf { it.isNotBlank() } ?: media.title,
            posterUrl = cover?.takeIf { it.isNotBlank() } ?: media.posterUrl,
            overview = desc,
        )
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val html = HikariNet.getStringSmart("$mainUrl/archives/${media.id}/", referer()) ?: return emptyList()
        val raw = PLAYER_URL_RE.find(html)?.groupValues?.get(1) ?: return emptyList()
        val path = raw.replace("\\/", "/").replace("\\u0026", "&")
        if (!path.startsWith("/media/m3u8")) return emptyList()
        return listOf(
            HikariStream(
                name = "HaiLuan",
                url = mainUrl + path,
                headers = mapOf(
                    "Referer" to "$mainUrl/",
                    "User-Agent" to BROWSER_UA,
                ),
                isM3u8 = true,
            )
        )
    }

    // ---- walls ----

    private data class Card(val id: String, val title: String, val cover: String?)

    private fun parseCards(html: String): List<Card> {
        val out = ArrayList<Card>()
        val seen = HashSet<String>()
        for (chunk in html.split("<a href=\"/archives/")) {
            if (chunk.length > 12000 || chunk.length < 60) continue
            val part = chunk.take(6000)
            val idm = ARCH_ID_RE.find(part) ?: continue
            val id = idm.groupValues[1]
            if (!seen.add(id)) continue
            val title = TITLE_ATTR_RE.find(part)?.groupValues?.get(1)?.let { unesc(it) }?.trim()
                ?: TITLE_DIV_RE.find(part)?.groupValues?.get(1)?.let { unesc(it) }?.trim()
                ?: RANK_TITLE_RE.find(part)?.groupValues?.get(1)?.let { unesc(it) }?.trim()
                ?.takeIf { it.isNotBlank() }
                ?: continue
            val cover = COVER_RE.find(part)?.groupValues?.get(1)
            out.add(Card(id, title, cover))
        }
        return out
    }

    private suspend fun resolveCovers(cards: List<Card>): List<HikariMedia> {
        if (cards.isEmpty()) return emptyList()
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

    // ---- shared helpers ----

    private fun referer(): Map<String, String> = mapOf(
        "Referer" to "$mainUrl/",
        "User-Agent" to BROWSER_UA,
    )

    private fun unesc(s: String): String = s
        .replace("&quot;", "\"").replace("&#34;", "\"").replace("&#39;", "'")
        .replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")

    private suspend fun decryptCover(url: String): String? {
        coverCacheMutex.withLock { coverCache[url]?.let { return it } }
        val out = decryptCoverFresh(url)
        if (out != null) {
            coverCacheMutex.withLock { coverCache[url] = out }
        }
        return out
    }

    private suspend fun decryptCoverFresh(url: String): String? = try {
        val bytes = HikariNet.getBytes(
            url,
            mapOf(
                "Referer" to "$mainUrl/",
                "User-Agent" to BROWSER_UA,
                "Accept" to "image/*,*/*;q=0.8",
            )
        ) ?: return null
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
