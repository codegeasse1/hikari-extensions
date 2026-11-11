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
 * 91探花 (911tanhua.com) — category walls as JSON-LD VideoObject lists.
 *
 * - catalogs: /category/<slug>/ (14 channels), pages /category/<slug>/<n>/.
 * - each VideoObject carries name + watch url + thumbnailUrl + description.
 * - watch page → twitter:player /embed/<hash>/ → data-url (or source) m3u8.
 * - search: /search/<q>/ (same wall markup).
 */
class TanhuaProvider : HikariProvider {

    override val id = "tanhua"
    override val name = "91探花"
    override val mainUrl = "https://911tanhua.com"
    override val description = "91探花: 国产探花 14 频道 + 搜索 — 直接 m3u8, 无限翻页."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.MOVIE)

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? = null

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

        private val EMBED_RE = Regex("<meta[^>]*name=\"twitter:player\"[^>]*content=\"([^\"]+)\"")
        private val DATA_URL_RE = Regex("data-url=\"([^\"]+\\.m3u8[^\"]*)\"")
        private val SOURCE_RE = Regex("<source[^>]*src=\"([^\"]+\\.m3u8[^\"]*)\"")
    }

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("tanhua-shipin", "探花视频", HikariMediaType.MOVIE),
        HikariCatalog("xingba-tanhua", "性吧探花", HikariMediaType.MOVIE),
        HikariCatalog("dingmei-tanhua", "顶美探花", HikariMediaType.MOVIE),
        HikariCatalog("mote-sipai", "模特私拍", HikariMediaType.MOVIE),
        HikariCatalog("jietou-dashan", "街头搭讪", HikariMediaType.MOVIE),
        HikariCatalog("jietou-chaodi", "街头抄底", HikariMediaType.MOVIE),
        HikariCatalog("wangbao-chigua", "网曝吃瓜", HikariMediaType.MOVIE),
        HikariCatalog("zhongkou-jiqing", "重口激情", HikariMediaType.MOVIE),
        HikariCatalog("sm-tiaojiao", "SM调教", HikariMediaType.MOVIE),
        HikariCatalog("zipai-toupai", "自拍偷拍", HikariMediaType.MOVIE),
        HikariCatalog("zhibo-luping", "直播录屏", HikariMediaType.MOVIE),
        HikariCatalog("zhifu-youhuo", "制服诱惑", HikariMediaType.MOVIE),
        HikariCatalog("jiudian-shexiangtou", "酒店摄像头", HikariMediaType.MOVIE),
        HikariCatalog("huisuo-anmo", "会所按摩", HikariMediaType.MOVIE),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val path = if (page <= 1) "/category/${catalog.id}/" else "/category/${catalog.id}/$page/"
        val html = HikariNet.getStringSmart(mainUrl + path, referer()) ?: return emptyList()
        return resolveCovers(parseVideos(html))
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        val enc = URLEncoder.encode(q, "UTF-8").replace("+", "%20")
        val path = if (page <= 1) "/search/$enc/" else "/search/$enc/$page/"
        val html = HikariNet.getStringSmart(mainUrl + path, referer()) ?: return emptyList()
        return resolveCovers(parseVideos(html))
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val html = HikariNet.getStringSmart("$mainUrl/watch/${media.id}/", referer()) ?: return media
        val detail = videoObject(html, media.id)
        val title = detail?.optString("name")?.trim().orEmpty()
        val desc = detail?.optString("description")?.trim().orEmpty()
        val cover = thumbs(detail).firstOrNull().orEmpty()
        return media.copy(
            title = title.ifBlank { media.title },
            posterUrl = cover.ifBlank { media.posterUrl },
            overview = desc,
        )
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val html = HikariNet.getStringSmart("$mainUrl/watch/${media.id}/", referer()) ?: return emptyList()
        val direct = videoObject(html, media.id)?.optString("contentUrl")?.trim()
            ?.takeIf { it.startsWith("http") }
        val m3u8 = direct ?: embedM3u8(html) ?: return emptyList()
        return listOf(
            HikariStream(
                name = "91探花",
                url = m3u8,
                headers = mapOf("Referer" to "$mainUrl/"),
                isM3u8 = m3u8.contains(".m3u8"),
            )
        )
    }

    // ---- walls ----

    private data class Card(val id: String, val title: String, val cover: String?, val desc: String)

    private fun parseVideos(html: String): List<Card> {
        val out = ArrayList<Card>()
        val seen = HashSet<String>()
        var i = 0
        while (true) {
            i = html.indexOf("<script type=\"application/ld+json\">", i)
            if (i < 0) break
            val s = i + "<script type=\"application/ld+json\">".length
            val e = html.indexOf("</script>", s)
            if (e < 0) break
            runCatching {
                val root = JSONObject(html.substring(s, e))
                collectVideos(root, out, seen)
            }
            i = e + 9
        }
        return out
    }

    private fun collectVideos(o: JSONObject, out: MutableList<Card>, seen: MutableSet<String>) {
        if (o.optString("@type") == "VideoObject") {
            val url = o.optString("url").ifBlank { o.optString("@id") }.trim()
            val id = Regex("/watch/([A-Za-z0-9]+)/?").find(url)?.groupValues?.get(1) ?: return
            if (!seen.add(id)) return
            val title = o.optString("name").trim().ifBlank { return }
            out.add(Card(id, title, thumbs(o).firstOrNull(), o.optString("description").trim()))
            return
        }
        if (o.optString("@type") == "ItemList") {
            val arr = o.optJSONArray("itemListElement") ?: return
            for (k in 0 until arr.length()) {
                val it = arr.optJSONObject(k) ?: continue
                val inner = it.optJSONObject("item")
                if (inner != null) {
                    collectVideos(inner, out, seen)
                } else {
                    val url = it.optString("url").trim()
                    val id = Regex("/watch/([A-Za-z0-9]+)/?").find(url)?.groupValues?.get(1) ?: continue
                    if (!seen.add(id)) continue
                    val title = it.optString("name").trim().ifBlank { continue }
                    out.add(Card(id, title, null, ""))
                }
            }
            return
        }
        val g = o.optJSONArray("@graph")
        if (g != null) {
            for (k in 0 until g.length()) {
                g.optJSONObject(k)?.let { collectVideos(it, out, seen) }
            }
        }
        o.optJSONObject("mainEntity")?.let { collectVideos(it, out, seen) }
    }

    private fun videoObject(html: String, id: String): JSONObject? {
        var i = 0
        while (true) {
            i = html.indexOf("<script type=\"application/ld+json\">", i)
            if (i < 0) return null
            val s = i + "<script type=\"application/ld+json\">".length
            val e = html.indexOf("</script>", s)
            if (e < 0) return null
            val found = runCatching {
                findVideo(JSONObject(html.substring(s, e)), id)
            }.getOrNull()
            if (found != null) return found
            i = e + 9
        }
    }

    private fun findVideo(o: JSONObject, id: String): JSONObject? {
        if (o.optString("@type") == "VideoObject") {
            val url = o.optString("url").ifBlank { o.optString("@id") }
            if (url.contains("/watch/$id")) return o
            return null
        }
        val g = o.optJSONArray("@graph")
        if (g != null) {
            for (k in 0 until g.length()) {
                val f = g.optJSONObject(k)?.let { findVideo(it, id) }
                if (f != null) return f
            }
        }
        o.optJSONObject("mainEntity")?.let { findVideo(it, id) }?.let { return it }
        return null
    }

    private fun thumbs(detail: JSONObject?): List<String> {
        if (detail == null) return emptyList()
        val t = detail.opt("thumbnailUrl") ?: return emptyList()
        if (t is String) return listOf(t).filter { it.isNotBlank() }
        if (t is org.json.JSONArray) {
            val out = ArrayList<String>()
            for (k in 0 until t.length()) {
                t.optString(k).takeIf { it.isNotBlank() }?.let { out.add(it) }
            }
            return out
        }
        return emptyList()
    }

    private suspend fun embedM3u8(watchHtml: String): String? {
        val m = EMBED_RE.find(watchHtml)?.groupValues?.get(1)?.let { unesc(it) } ?: return null
        if (!m.startsWith("http")) return null
        val ehtml = HikariNet.getStringSmart(m, referer()) ?: return null
        val raw = DATA_URL_RE.find(ehtml)?.groupValues?.get(1)
            ?: SOURCE_RE.find(ehtml)?.groupValues?.get(1)
            ?: return null
        return unesc(raw).takeIf { it.startsWith("http") }
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
                overview = card.desc.takeIf { it.isNotBlank() },
            )
        }
    }

    // ---- shared helpers ----

    private fun referer(): Map<String, String> = mapOf("Referer" to "$mainUrl/")

    private fun unesc(s: String): String = s
        .replace("&quot;", "\"").replace("&#34;", "\"").replace("&#39;", "'")
        .replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")

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
