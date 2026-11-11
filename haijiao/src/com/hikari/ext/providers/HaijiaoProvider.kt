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
import org.json.JSONObject
import java.net.URLEncoder
import javax.crypto.Cipher
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec

/**
 * 海角社区 (7b24b.hazottkc.cc) — server-rendered walls + JSON-LD.
 *
 * - catalogs: /video/ (海角视频, movies), /drama/ (海角短剧, series).
 * - cards: video-list-page__link (title attr + data-src covers, AES at rest).
 * - video detail: dplayer data-config (url + pic).
 * - drama detail: JSON-LD TVSeries episodes → /drama/<id>/<n>/ pages whose
 *   inline JSON carries the episode m3u8.
 * - search: /search/<q>/ (same card markup).
 */
class HaijiaoProvider : HikariProvider {

    override val id = "haijiao"
    override val name = "海角社区"
    override val mainUrl = "https://7b24b.hazottkc.cc"
    override val description = "海角社区: 海角视频 + 海角短剧选集 + 搜索 — 直接 m3u8, 无限翻页."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.MOVIE, HikariMediaType.SERIES)

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        if (!media.id.startsWith("d:")) return null
        val eps = dramaEpisodes(media.id.removePrefix("d:"), null)
        return eps.ifEmpty { null }
    }

    companion object {
        private const val IMG_KEY = "f5d965df75336270"
        private const val IMG_IV = "97b60394abc2fbe1"
        private const val COVER_MEM_MAX = 64
        private val coverCache = object : LinkedHashMap<String, String>(COVER_MEM_MAX, 0.75f, true) {
            override fun removeEldestEntry(eldest: MutableMap.MutableEntry<String, String>): Boolean =
                size > COVER_MEM_MAX
        }
        private val coverCacheMutex = Mutex()
        private val coverPermits = Semaphore(6)

        private val HREF_RE = Regex("href=\"(/video/(\\d+)/)\"")
        private val TITLE_RE = Regex("title=\"([^\"]{2,150})\"")
        private val COVER_RE = Regex("(?:data-enc-src|data-src)=\"([^\"]+)\"")
        private val DPLAYER_RE = Regex("data-config=\"\\{&(quot;)[^}]*?&quot;url&quot;:&quot;([^&]+?)&")
        private val DPLAYER_PIC_RE = Regex("&quot;pic&quot;:&quot;([^&]+?)&")
        private val EP_URL_RE = Regex("\"url\"\\s*:\\s*\"(https?[^\"]+?\\.m3u8[^\"]*?)\"")
    }

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("video", "海角视频", HikariMediaType.MOVIE),
        HikariCatalog("drama", "海角短剧", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val seg = if (catalog.id == "drama") "drama" else "video"
        val path = if (page <= 1) "/$seg/" else "/$seg/page/$page/"
        val html = HikariNet.getStringSmart(mainUrl + path, referer()) ?: return emptyList()
        return if (catalog.id == "drama") parseDramaList(html) else resolveCovers(parseVideoCards(html))
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        val enc = URLEncoder.encode(q, "UTF-8").replace("+", "%20")
        val path = if (page <= 1) "/search/$enc/" else "/search/$enc/page/$page/"
        val html = HikariNet.getStringSmart(mainUrl + path, referer()) ?: return emptyList()
        return resolveCovers(parseVideoCards(html))
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        if (media.id.startsWith("d:")) {
            val id = media.id.removePrefix("d:")
            val html = HikariNet.getStringSmart("$mainUrl/drama/$id/", referer()) ?: return media
            val desc = Regex("<meta[^>]*name=\"description\"[^>]*content=\"([^\"]+)\"")
                .find(html)?.groupValues?.get(1)?.let { unesc(it) }?.trim().orEmpty()
            val cover = Regex("<meta[^>]*property=\"og:image\"[^>]*content=\"([^\"]+)\"")
                .find(html)?.groupValues?.get(1)?.let { unesc(it) }?.trim().orEmpty()
            return media.copy(
                posterUrl = (if (cover.isNotBlank() && !cover.contains("og-share")) cover else media.posterUrl),
                overview = desc,
            )
        }
        val html = HikariNet.getStringSmart("$mainUrl/video/${media.id}/", referer()) ?: return media
        val desc = Regex("<meta[^>]*name=\"description\"[^>]*content=\"([^\"]+)\"")
            .find(html)?.groupValues?.get(1)?.let { unesc(it) }?.trim().orEmpty()
        var cover = DPLAYER_PIC_RE.find(html)?.groupValues?.get(1)?.let { unesc(it) }?.trim().orEmpty()
        if (cover.isBlank()) cover = media.posterUrl ?: ""
        return media.copy(
            posterUrl = cover.ifBlank { null },
            overview = desc,
        )
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val m3u8 = if (media.id.startsWith("d:")) {
            val epUrl = episode?.id?.takeIf { it.startsWith("http") }
                ?: dramaEpisodes(media.id.removePrefix("d:"), null).firstOrNull()?.id
                ?: return emptyList()
            episodeM3u8(epUrl)
        } else {
            val html = HikariNet.getStringSmart("$mainUrl/video/${media.id}/", referer()) ?: return emptyList()
            videoM3u8(html)
        } ?: return emptyList()
        return listOf(
            HikariStream(
                name = episode?.name?.takeIf { it.isNotBlank() } ?: "海角社区",
                url = m3u8,
                headers = mapOf("Referer" to "$mainUrl/"),
                isM3u8 = true,
            )
        )
    }

    // ---- videos ----

    private data class Card(val id: String, val title: String, val cover: String?)

    private fun parseVideoCards(html: String): List<Card> {
        val names = jsonLdNames(html)
        val out = ArrayList<Card>()
        val seen = HashSet<String>()
        for (chunk in html.split("video-list-page__card")) {
            if (chunk.length > 6000) continue
            val part = chunk.take(3000)
            val href = HREF_RE.find(part) ?: continue
            val id = href.groupValues[2]
            if (!seen.add(id)) continue
            val title = TITLE_RE.find(part)?.groupValues?.get(1)?.let { unesc(it) }?.trim()
                ?.takeIf { it.isNotBlank() }
                ?: names["/video/$id/"] ?: continue
            val cover = COVER_RE.find(part)?.groupValues?.get(1)
            out.add(Card(id, title, cover))
        }
        if (out.isEmpty()) {
            for ((url, name) in names) {
                val id = Regex("/video/(\\d+)/").find(url)?.groupValues?.get(1) ?: continue
                if (seen.add(id)) out.add(Card(id, name, null))
            }
        }
        return out
    }

    private fun parseDramaList(html: String): List<HikariMedia> {
        val names = jsonLdNames(html)
        val out = ArrayList<HikariMedia>()
        val seen = HashSet<String>()
        // JSON-LD has every entry; covers come from the card markup below.
        for ((url, name) in names) {
            val id = Regex("/drama/(\\d+)/").find(url)?.groupValues?.get(1) ?: continue
            if (!seen.add(id)) continue
            val cover = dramaCover(html, id)
            out.add(
                HikariMedia(
                    id = "d:$id",
                    title = name,
                    type = HikariMediaType.SERIES,
                    posterUrl = cover,
                )
            )
        }
        return out
    }

    private fun dramaCover(html: String, id: String): String? {
        val i = html.indexOf("/drama/$id/")
        if (i < 0) return null
        val part = html.substring(i, (i + 3000).coerceAtMost(html.length))
        return COVER_RE.find(part)?.groupValues?.get(1)
    }

    private fun videoM3u8(detailHtml: String): String? {
        val m = DPLAYER_RE.find(detailHtml) ?: return null
        val u = unesc(m.groupValues[2]).replace("\\/", "/")
        return u.takeIf { it.startsWith("http") }
    }

    // ---- drama episodes ----

    private suspend fun dramaEpisodes(dramaId: String, html: String?): List<HikariEpisode> {
        val h = html ?: HikariNet.getStringSmart("$mainUrl/drama/$dramaId/", referer()) ?: return emptyList()
        val out = ArrayList<HikariEpisode>()
        var i = 0
        while (true) {
            i = h.indexOf("<script type=\"application/ld+json\">", i)
            if (i < 0) break
            val s = i + "<script type=\"application/ld+json\">".length
            val e = h.indexOf("</script>", s)
            if (e < 0) break
            runCatching {
                val root = JSONObject(h.substring(s, e))
                val series = findType(root, "TVSeries") ?: findType(root, "VideoObject")
                val eps = series?.optJSONArray("episode")
                if (eps != null) {
                    for (k in 0 until eps.length()) {
                        val ep = eps.optJSONObject(k) ?: continue
                        val url = ep.optString("url").trim()
                        if (url.isBlank()) continue
                        val n = ep.optInt("episodeNumber", out.size + 1)
                        val name = ep.optString("name").trim().ifBlank { "第${n}集" }
                        out.add(HikariEpisode(number = n, id = url, name = name))
                    }
                }
            }
            i = e + 9
        }
        return out
    }

    private fun findType(o: JSONObject, type: String): JSONObject? {
        if (o.optString("@type") == type) return o
        val g = o.optJSONArray("@graph")
        if (g != null) {
            for (k in 0 until g.length()) {
                val f = g.optJSONObject(k)?.let { findType(it, type) }
                if (f != null) return f
            }
        }
        o.optJSONObject("mainEntity")?.let { findType(it, type) }?.let { return it }
        return null
    }

    private suspend fun episodeM3u8(epUrl: String): String? {
        val html = HikariNet.getStringSmart(epUrl, referer()) ?: return null
        val m = EP_URL_RE.find(html) ?: return null
        return unesc(m.groupValues[1]).replace("\\/", "/").takeIf { it.startsWith("http") }
    }

    // ---- shared helpers ----

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

    private fun referer(): Map<String, String> = mapOf("Referer" to "$mainUrl/")

    private fun unesc(s: String): String = s
        .replace("&quot;", "\"").replace("&#34;", "\"").replace("&#39;", "'")
        .replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")

    private fun jsonLdNames(html: String): Map<String, String> {
        val out = HashMap<String, String>()
        var i = 0
        while (true) {
            i = html.indexOf("<script type=\"application/ld+json\">", i)
            if (i < 0) break
            val s = i + "<script type=\"application/ld+json\">".length
            val e = html.indexOf("</script>", s)
            if (e < 0) break
            runCatching {
                collectItems(JSONObject(html.substring(s, e)), out)
            }
            i = e + 9
        }
        return out
    }

    private fun collectItems(o: JSONObject, out: MutableMap<String, String>) {
        if (o.optString("@type") == "ItemList") {
            val arr = o.optJSONArray("itemListElement") ?: return
            for (k in 0 until arr.length()) {
                val it = arr.optJSONObject(k) ?: continue
                val inner = it.optJSONObject("item")
                val name = (inner?.optString("name") ?: it.optString("name")).trim()
                val url = (inner?.optString("url") ?: it.optString("url")).trim()
                if (name.isNotBlank() && url.isNotBlank()) out[url] = name
            }
            return
        }
        val g = o.optJSONArray("@graph")
        if (g != null) {
            for (k in 0 until g.length()) {
                g.optJSONObject(k)?.let { collectItems(it, out) }
            }
        }
        o.optJSONObject("mainEntity")?.let { collectItems(it, out) }
    }

    private suspend fun decryptCover(url: String): String? {
        coverCacheMutex.withLock { coverCache[url]?.let { return it } }
        val out = decryptCoverFresh(url)
        if (out != null) coverCacheMutex.withLock { coverCache[url] = out }
        return out
    }

    private suspend fun decryptCoverFresh(url: String): String? = try {
        val bytes = HikariNet.getBytes(url, referer()) ?: return null
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
