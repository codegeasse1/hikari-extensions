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
 * 91三级片 (91sanji.com) — server-rendered category walls + JSON-LD.
 *
 * - catalogs: /category/<slug>/ (6 regions), pages /category/<slug>/<n>/.
 * - cards: video-card anchors (title attr + data-enc-src covers, AES at rest).
 * - detail: JSON-LD VideoObject (contentUrl m3u8, thumbnailUrl, description).
 * - search: /search/<q>/ (same wall markup).
 */
class SanjiProvider : HikariProvider {

    override val id = "sanji"
    override val name = "91三级片"
    override val mainUrl = "https://91sanji.com"
    override val description = "91三级片: 国产/香港/日本/韩国/欧美/东南亚三级 + 搜索 — 直接 m3u8, 无限翻页."
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

        private val HREF_RE = Regex("href=\"(/video/(\\d+)/)\"")
        private val TITLE_RE = Regex("title=\"([^\"]{2,150})\"")
        private val COVER_RE = Regex("(?:data-enc-src|data-src)=\"([^\"]+)\"")
        private val DURATION_RE = Regex("<span[^>]*class=\"duration\"[^>]*>([^<]{1,12})</span>")
    }

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("chinese-sex-movies", "国产三级片", HikariMediaType.MOVIE),
        HikariCatalog("hong-kong-sex-movies", "香港三级片", HikariMediaType.MOVIE),
        HikariCatalog("japanese-sex-movies", "日本三级片", HikariMediaType.MOVIE),
        HikariCatalog("korean-sex-movies", "韩国三级片", HikariMediaType.MOVIE),
        HikariCatalog("western-sex-movies", "欧美三级片", HikariMediaType.MOVIE),
        HikariCatalog("southeast-asian-sex-movies", "东南亚三级片", HikariMediaType.MOVIE),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val path = if (page <= 1) "/category/${catalog.id}/" else "/category/${catalog.id}/$page/"
        val html = HikariNet.getStringSmart(mainUrl + path, referer()) ?: return emptyList()
        return resolveCovers(parseCards(html))
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        val enc = URLEncoder.encode(q, "UTF-8").replace("+", "%20")
        val path = if (page <= 1) "/search/$enc/" else "/search/$enc/$page/"
        val html = HikariNet.getStringSmart(mainUrl + path, referer()) ?: return emptyList()
        return resolveCovers(parseCards(html))
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val html = HikariNet.getStringSmart("$mainUrl/video/${media.id}/", referer()) ?: return media
        val detail = videoObject(html)
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
        val html = HikariNet.getStringSmart("$mainUrl/video/${media.id}/", referer()) ?: return emptyList()
        val detail = videoObject(html)
        val m3u8 = detail?.optString("contentUrl")?.trim()?.takeIf { it.startsWith("http") }
            ?: embedM3u8(html) ?: return emptyList()
        return listOf(
            HikariStream(
                name = "91三级片",
                url = m3u8,
                headers = mapOf("Referer" to "$mainUrl/"),
                isM3u8 = m3u8.contains(".m3u8"),
            )
        )
    }

    // ---- walls ----

    private data class Card(val id: String, val title: String, val cover: String?)

    private fun parseCards(html: String): List<Card> {
        val names = jsonLdNames(html)
        val out = ArrayList<Card>()
        val seen = HashSet<String>()
        for (chunk in html.split("<article class=\"video-card\"")) {
            if (chunk.length > 8000) continue
            val part = chunk.take(4000)
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

    // ---- detail ----

    private fun videoObject(html: String): JSONObject? {
        var i = 0
        while (true) {
            i = html.indexOf("<script type=\"application/ld+json\">", i)
            if (i < 0) return null
            val s = i + "<script type=\"application/ld+json\">".length
            val e = html.indexOf("</script>", s)
            if (e < 0) return null
            val found = runCatching {
                findType(JSONObject(html.substring(s, e)), "VideoObject")
            }.getOrNull()
            if (found != null) return found
            i = e + 9
        }
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

    private suspend fun embedM3u8(detailHtml: String): String? {
        val m = Regex("<meta[^>]*property=\"og:video\"[^>]*content=\"([^\"]+)\"")
            .find(detailHtml)?.groupValues?.get(1)?.let { unesc(it) } ?: return null
        if (!m.startsWith("http")) return null
        val ehtml = HikariNet.getStringSmart(m, referer()) ?: return null
        val raw = Regex("data-url=\"([^\"]+\\.m3u8[^\"]*)\"").find(ehtml)?.groupValues?.get(1)
            ?: Regex("<source[^>]*src=\"([^\"]+\\.m3u8[^\"]*)\"").find(ehtml)?.groupValues?.get(1)
            ?: return null
        return unesc(raw).takeIf { it.startsWith("http") }
    }

    // ---- shared helpers ----

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
