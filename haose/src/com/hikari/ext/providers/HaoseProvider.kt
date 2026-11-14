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
 * 好色TV (aside.fusyeset.cc) — server-rendered video walls.
 *
 * - catalogs: /video/ (成人视频), /videos/latest/ (最新更新), /tag/<slug>/
 *   (pagination per section via the page's own data-page-base-url).
 * - cards: video-card-v2 (title attr + data-src/data-enc-src covers, which
 *   are AES-encrypted at rest on the pic CDN — same scheme as 91DJU).
 * - detail: twitter:player → /videos/embed/<id>/ → data-url m3u8.
 * - search: /search/<q>/ (same wall markup).
 */
class HaoseProvider : HikariProvider {

    override val id = "haose"
    override val name = "HaoSeTV"
    override val mainUrl = "https://aside.fusyeset.cc"
    override val description = "HaoSeTV: adult video walls + tag categories + search — direct m3u8, unlimited pages."
    override val version = 2
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

        private val pagerBaseCache = HashMap<String, String>()
        private val pagerMutex = Mutex()

        private val HREF_RE = Regex("href=\"(/video/(\\d+)/)\"")
        private val TITLE_RE = Regex("title=\"([^\"]{2,150})\"")
        private val COVER_RE = Regex("(?:data-enc-src|data-src)=\"([^\"]+)\"")
        private val PAGER_BASE_RE = Regex("data-page-base-url=\"([^\"]+)\"")
        private val EMBED_RE = Regex("<meta[^>]*name=\"twitter:player\"[^>]*content=\"([^\"]+)\"")
        private val DATA_URL_RE = Regex("data-url=\"([^\"]+\\.m3u8[^\"]*)\"")
        private val SOURCE_RE = Regex("<source[^>]*src=\"([^\"]+\\.m3u8[^\"]*)\"")
    }

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("video", "成人视频", HikariMediaType.MOVIE),
        HikariCatalog("latest", "最新更新", HikariMediaType.MOVIE),
        HikariCatalog("tag:cosplay", "Cosplay", HikariMediaType.MOVIE),
        HikariCatalog("tag:黑料", "黑料", HikariMediaType.MOVIE),
        HikariCatalog("tag:吃瓜", "吃瓜", HikariMediaType.MOVIE),
        HikariCatalog("tag:乱伦", "乱伦", HikariMediaType.MOVIE),
        HikariCatalog("tag:自慰", "自慰", HikariMediaType.MOVIE),
        HikariCatalog("tag:巨乳", "巨乳", HikariMediaType.MOVIE),
        HikariCatalog("tag:性感", "性感", HikariMediaType.MOVIE),
        HikariCatalog("tag:美腿", "美腿", HikariMediaType.MOVIE),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val first = firstPath(catalog.id)
        if (page <= 1) {
            val (cards, base) = listPage(first)
            if (base != null) pagerMutex.withLock { pagerBaseCache[catalog.id] = base }
            return resolveCovers(cards)
        }
        var base = pagerMutex.withLock { pagerBaseCache[catalog.id] }
        if (base == null) {
            val (_, b) = listPage(first)
            base = b ?: defaultBase(catalog.id)
            val bb = base
            pagerMutex.withLock { pagerBaseCache[catalog.id] = bb }
        }
        val (cards, b2) = listPage(base + page + "/")
        if (b2 != null) pagerMutex.withLock { pagerBaseCache[catalog.id] = b2 }
        return resolveCovers(cards)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        val enc = URLEncoder.encode(q, "UTF-8").replace("+", "%20")
        val key = "search:$enc"
        val first = "/search/$enc/"
        if (page <= 1) {
            val (cards, base) = listPage(first)
            if (base != null) pagerMutex.withLock { pagerBaseCache[key] = base }
            return resolveCovers(cards)
        }
        var base = pagerMutex.withLock { pagerBaseCache[key] }
        if (base == null) {
            val (_, b) = listPage(first)
            base = b ?: (first + "page/")
            val bb = base
            pagerMutex.withLock { pagerBaseCache[key] = bb }
        }
        val (cards, b2) = listPage(base + page + "/")
        if (b2 != null) pagerMutex.withLock { pagerBaseCache[key] = b2 }
        return resolveCovers(cards)
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val html = HikariNet.getStringSmart("$mainUrl/video/${media.id}/", referer()) ?: return media
        val title = Regex("<meta[^>]*property=\"og:title\"[^>]*content=\"([^\"]+)\"")
            .find(html)?.groupValues?.get(1)?.let { unesc(it) }?.trim().orEmpty()
        val desc = Regex("<meta[^>]*property=\"og:description\"[^>]*content=\"([^\"]+)\"")
            .find(html)?.groupValues?.get(1)?.let { unesc(it) }?.trim().orEmpty()
        var cover = Regex("<meta[^>]*name=\"twitter:image\"[^>]*content=\"([^\"]+)\"")
            .find(html)?.groupValues?.get(1)?.let { unesc(it) }?.trim().orEmpty()
        if (cover.isBlank()) cover = media.posterUrl ?: ""
        return media.copy(
            title = title.ifBlank { media.title },
            posterUrl = cover.ifBlank { null },
            overview = desc,
        )
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val html = HikariNet.getStringSmart("$mainUrl/video/${media.id}/", referer()) ?: return emptyList()
        val embed = EMBED_RE.find(html)?.groupValues?.get(1)?.let { unesc(it) } ?: return emptyList()
        val ehtml = HikariNet.getStringSmart(embed, referer()) ?: return emptyList()
        val raw = DATA_URL_RE.find(ehtml)?.groupValues?.get(1)
            ?: SOURCE_RE.find(ehtml)?.groupValues?.get(1)
            ?: return emptyList()
        val m3u8 = unesc(raw)
        if (!m3u8.startsWith("http")) return emptyList()
        return listOf(
            HikariStream(
                name = "HaoSeTV",
                url = m3u8,
                headers = mapOf("Referer" to "$mainUrl/"),
                isM3u8 = true,
            )
        )
    }

    // ---- walls ----

    private fun firstPath(id: String): String = when {
        id == "video" -> "/video/"
        id == "latest" -> "/videos/latest/"
        id.startsWith("tag:") -> "/tag/${id.removePrefix("tag:")}/"
        else -> "/video/"
    }

    private fun defaultBase(id: String): String = when {
        id == "video" -> "/video/page/"
        id == "latest" -> "/videos/latest/page/"
        id.startsWith("tag:") -> "/tag/${id.removePrefix("tag:")}/video/"
        else -> "/video/page/"
    }

    private data class Card(val id: String, val title: String, val cover: String?)

    private suspend fun listPage(path: String): Pair<List<Card>, String?> {
        val html = HikariNet.getStringSmart(mainUrl + path, referer()) ?: return Pair(emptyList(), null)
        val base = PAGER_BASE_RE.find(html)?.groupValues?.get(1)
        return Pair(parseCards(html), base)
    }

    private fun parseCards(html: String): List<Card> {
        val names = jsonLdNames(html)
        val out = ArrayList<Card>()
        val seen = HashSet<String>()
        for (chunk in html.split("video-card-v2__thumb")) {
            if (chunk.length > 6000) continue
            val href = HREF_RE.find(chunk.take(3000)) ?: continue
            val id = href.groupValues[2]
            if (!seen.add(id)) continue
            val title = TITLE_RE.find(chunk.take(3000))?.groupValues?.get(1)?.let { unesc(it) }?.trim()
                ?.takeIf { it.isNotBlank() }
                ?: names["/video/$id/"] ?: continue
            val cover = COVER_RE.find(chunk.take(3000))?.groupValues?.get(1)
            out.add(Card(id, title, cover))
        }
        if (out.isEmpty()) {
            // Fallback: bare JSON-LD names (covers resolved in getMeta).
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

    // ---- shared helpers ----

    private fun referer(): Map<String, String> = mapOf("Referer" to "$mainUrl/")

    private fun unesc(s: String): String = s
        .replace("&quot;", "\"").replace("&#34;", "\"").replace("&#39;", "'")
        .replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")

    /** detail/list JSON-LD: url -> name. */
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
