package com.hikari.ext.providers

import android.util.Base64
import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.net.URLEncoder
import javax.crypto.Cipher
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec

/**
 * 51CG (51cg1.com) — daily-contest / gossip blacklist video site (same Mirages
 * theme + DPlayer + AES-encrypted pic.xustgq.cn posters as MRDS).
 *
 * Native Hikari extension (no CloudStream dependency). Plain server-rendered HTML:
 *  - home:           `https://51cg1.com/`           → `.../page/<n>/`
 *  - categories:     `https://51cg1.com/category/<slug>/` → `.../<n>/`
 *  - search:         `https://51cg1.com/search/<q>/` → `.../<n>/`
 *  - a post page (`/archives/<id>/`) embeds a DPlayer whose `data-config`
 *    JSON holds the signed HLS m3u8 stream.
 *  - post images live on `pic.xustgq.cn` AES-encrypted and must be decrypted.
 *
 * getCatalog/search take a page argument, so the app's own pagination is
 * unlimited (page 1..N until the site returns no more cards).
 */
class C51CGProvider : HikariProvider {

    override val id = "51cg"
    override val name = "51CG"
    override val mainUrl = "https://51cg1.com"
    override val description = "Daily contest & gossip videos from 51cg1.com — 27 category walls plus search, direct HLS streams."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.MOVIE)


    companion object {
        private const val BASE = "https://51cg1.com"
        private const val CACHE_TTL_MS = 600_000L
        private val htmlCache = HashMap<String, Pair<Long, String>>()
        private val cacheMutex = Mutex()
        private val translateCache = HashMap<String, String>()
        private val translateMutex = Mutex()
        private const val IMG_KEY = "f5d965df75336270"
        private const val IMG_IV = "97b60394abc2fbe1"
    }

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("home", "Home", HikariMediaType.MOVIE),
        HikariCatalog("wpcz", "Today's Melon", HikariMediaType.MOVIE),
        HikariCatalog("xsxy", "Campus Student", HikariMediaType.MOVIE),
        HikariCatalog("whhl", "Internet Celebrity", HikariMediaType.MOVIE),
        HikariCatalog("rdsj", "Hot Melon", HikariMediaType.MOVIE),
        HikariCatalog("mrdg", "Melon List", HikariMediaType.MOVIE),
        HikariCatalog("bkdg", "Must Watch", HikariMediaType.MOVIE),
        HikariCatalog("cbdj", "AI Adult Drama", HikariMediaType.MOVIE),
        HikariCatalog("ysyl", "Watching Fun", HikariMediaType.MOVIE),
        HikariCatalog("mrds", "Daily Contest", HikariMediaType.MOVIE),
        HikariCatalog("lldd", "Ethics Morality", HikariMediaType.MOVIE),
        HikariCatalog("gcjq", "Chinese Drama", HikariMediaType.MOVIE),
        HikariCatalog("thjx", "Selected Visits", HikariMediaType.MOVIE),
        HikariCatalog("whhj", "Web Yellow Collection", HikariMediaType.MOVIE),
        HikariCatalog("snsn", "Slutty Men Women", HikariMediaType.MOVIE),
        HikariCatalog("whmx", "Celebrity Scandal", HikariMediaType.MOVIE),
        HikariCatalog("hwcg", "Overseas Melon", HikariMediaType.MOVIE),
        HikariCatalog("rrcg", "Everyone Melon", HikariMediaType.MOVIE),
        HikariCatalog("ldcg", "Cadre Leader", HikariMediaType.MOVIE),
        HikariCatalog("jpll", "Sweet Girl", HikariMediaType.MOVIE),
        HikariCatalog("qubk", "Melon Watching", HikariMediaType.MOVIE),
        HikariCatalog("dcbq", "Flirting", HikariMediaType.MOVIE),
        HikariCatalog("zzs", "51 Knowledge", HikariMediaType.MOVIE),
        HikariCatalog("cgxw", "Melon News", HikariMediaType.MOVIE),
        HikariCatalog("yczq", "Original Blogger", HikariMediaType.MOVIE),
        HikariCatalog("51djc", "51 Theater", HikariMediaType.MOVIE),
        HikariCatalog("sjb", "World Cup", HikariMediaType.MOVIE),
        HikariCatalog("51hd", "Past Activities", HikariMediaType.MOVIE),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val url = when {
            catalog.id == "home" -> if (page <= 1) "$BASE/" else "$BASE/page/$page/"
            else -> if (page <= 1) "$BASE/category/${catalog.id}/" else "$BASE/category/${catalog.id}/$page/"
        }
        val html = getCached(url) ?: return emptyList()
        return parseCards(html)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        val encoded = URLEncoder.encode(q, "UTF-8").replace("+", "%20")
        val url = if (page <= 1) "$BASE/search/$encoded/" else "$BASE/search/$encoded/$page/"
        val html = getCached(url) ?: return emptyList()
        return parseCards(html)
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val page = getCached(BASE + media.id) ?: return media
        val rawTitle = Regex("<title>([^<]*)</title>").find(page)?.groupValues?.get(1)
            ?.substringBefore("-")?.trim() ?: media.title
        var poster = Regex("data-xkrkllgl=\"([^\"]+)\"")
            .find(page)?.groupValues?.get(1)
        if (poster.isNullOrBlank()) {
            poster = Regex("loadBannerDirect\\s*\\(\\s*['\"]([^'\"]+)['\"]")
                .find(page)?.groupValues?.get(1)
        }
        if (poster.isNullOrBlank()) {
            poster = Regex("meta itemprop=\"image\" content=\"([^\"]+)\"")
                .find(page)?.groupValues?.get(1)
        }
        if (poster != null && poster.contains("pic.xustgq.cn")) {
            poster = decryptImage(poster) ?: poster
        }
        val synopsis = Regex("meta name=\"description\" content=\"([^\"]*)\"")
            .find(page)?.groupValues?.get(1)
        return media.copy(
            title = translate(rawTitle) ?: media.title,
            posterUrl = poster ?: media.posterUrl,
            overview = translate(synopsis)?.takeIf { it.isNotBlank() },
        )
    }

        /** Melon-list (mrdg) posts are TOP10 collection pages: the post page itself
     *  has no embedded video - it lists the 10 ranked videos as .btn-primary
     *  buttons. Surface them as episodes so each rank is a tappable video
     *  (mirrors the CloudStream provider's melon-list handling). */
    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val page = getCached(BASE + media.id) ?: return null
        val seg = page.indexOf("post-content")
        val body = if (seg >= 0) page.substring(seg) else page
        val out = LinkedHashMap<String, HikariEpisode>()
        var number = 1
        val re = Regex("<a[^>]+href=\"(/archives/\\d+/)\"[^>]*class=\"[^>]*btn[^>]*\"[^>]*>([\\s\\S]*?)</a>")
        for (m in re.findAll(body)) {
            val href = m.groupValues[1]
            if (out.containsKey(href)) continue
            var raw = stripTags(m.groupValues[2]).trim()
            raw = raw.substringBefore("点击查看详情").trim().trimEnd('→', '｜').trim()
            out[href] = HikariEpisode(
                number = number,
                id = href,
                name = translate(raw) ?: raw,
            )
            number++
        }
        return if (out.isEmpty()) null else out.values.toList()
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        var html = getCached(BASE + media.id) ?: return emptyList()
        var urls = extractM3u8(html)
        if (urls.isEmpty() && episode != null && episode.id.startsWith("/")) {
            html = getCached(BASE + episode.id) ?: return emptyList()
            urls = extractM3u8(html)
        }
        if (urls.isEmpty() && episode == null) {
            getEpisodes(media)?.firstOrNull()?.let { first ->
                getCached(BASE + first.id)?.let { urls = extractM3u8(it) }
            }
        }
        if (urls.isEmpty()) return emptyList()

        val ua = HikariNet.browserHeaders.getValue("User-Agent")
        val referer = "$BASE/"

        val headers = mapOf("Referer" to referer, "User-Agent" to ua)
        return urls.mapIndexed { i, u ->
            HikariStream(
                name = if (urls.size == 1) "51CG" else "Video ${i + 1}",
                url = u,
                headers = headers,
                isM3u8 = true,
            )
        }
    }

    /** Extracts every HLS stream URL from a post page (escaped-slash configs
     *  first, then plain URLs). */
    private fun extractM3u8(html: String): List<String> {
        val urls = LinkedHashSet<String>()
        val escaped = Regex("https?:\\\\?/\\\\?/[^\\s\"'<>]+?\\.m3u8[^\\s\"'<>]*")
        escaped.findAll(html).forEach { urls.add(it.value.replace("\\/", "/").replace("&amp;", "&")) }
        if (urls.isEmpty()) {
            val plain = Regex("https?://[^\\s\"'<>]+\\.m3u8[^\\s\"'<>]*")
            plain.findAll(html).forEach { urls.add(it.value) }
        }
        return urls.toList()
    }

    // ---- HTML helpers ----

    /** Fetches a page once per CACHE_TTL_MS (home builds many rows). */
    private suspend fun getCached(url: String): String? {
        val now = System.currentTimeMillis()
        cacheMutex.withLock {
            htmlCache[url]?.let { (t, html) -> if (now - t < CACHE_TTL_MS) return html }
        }
        val html = HikariNet.getStringSmart(url) ?: return null
        cacheMutex.withLock {
            if (htmlCache.size > 60) htmlCache.clear()
            htmlCache[url] = now to html
        }
        return html
    }

    /** Extracts the video-card grid from a list/search page. Skips ad articles. */
    private suspend fun parseCards(html: String): List<HikariMedia> {
        val out = LinkedHashMap<String, HikariMedia>()
        val articleRe = Regex("<article(?![^>]*ad-item)[^>]*>([\\s\\S]*?)</article>")
        for (m in articleRe.findAll(html)) {
            val block = m.groupValues[1]
            if (!block.contains("post-card")) continue
            val href = Regex("<a[^>]+href=\"([^\"]+)\"").find(block)?.groupValues?.get(1) ?: continue
            if (!href.contains("/archives/")) continue
            val rawTitle = Regex("class=\"[^\"]*post-card-title[^\"]*\"[^>]*>([\\s\\S]*?)</h2>")
                .find(block)?.groupValues?.get(1)?.let { stripTags(it) }?.trim()
                ?: continue
            var poster = Regex("loadBannerDirect\\s*\\(\\s*['\"]([^'\"]+)['\"]")
                .find(block)?.groupValues?.get(1)
            if (poster != null && poster.contains("pic.xustgq.cn")) {
                poster = decryptImage(poster) ?: poster
            }
            out[href] = HikariMedia(
                id = href,
                title = translate(rawTitle) ?: rawTitle,
                type = HikariMediaType.MOVIE,
                posterUrl = poster,
            )
        }
        return out.values.toList()
    }

    /** Decrypts the AES-encrypted poster bytes served by pic.xustgq.cn. */
    private suspend fun decryptImage(url: String): String? = try {
        val bytes = HikariNet.getBytes(url, mapOf("Referer" to "$BASE/")) ?: return null
        val key = SecretKeySpec(IMG_KEY.toByteArray(), "AES")
        val iv = IvParameterSpec(IMG_IV.toByteArray())
        val cipher = Cipher.getInstance("AES/CBC/PKCS5Padding")
        cipher.init(Cipher.DECRYPT_MODE, key, iv)
        val decrypted = cipher.doFinal(bytes)
        val ext = url.substringAfterLast(".", "jpeg").substringBefore("?")
        "data:image/$ext;base64," + Base64.encodeToString(decrypted, Base64.NO_WRAP)
    } catch (e: Exception) {
        null
    }

    /** Best-effort Chinese→English title/synopsis translation (cached). */
    private suspend fun translate(text: String?): String? {
        if (text.isNullOrBlank()) return text
        if (!isTranslateEnabled()) return text
        translateMutex.withLock { translateCache[text]?.let { return it } }
        val result = try {
            val encoded = URLEncoder.encode(text, "UTF-8")
            val url = "https://translate.googleapis.com/translate_a/single?client=gtx&sl=auto&tl=en&dt=t&q=$encoded"
            val resp = HikariNet.getString(url) ?: text
            Regex("\"([^\"]+)\"").find(resp)?.groupValues?.get(1) ?: text
        } catch (e: Exception) {
            text
        }
        translateMutex.withLock { translateCache[text] = result }
        return result
    }

    private suspend fun isTranslateEnabled(): Boolean {
        val flag = try {
            val json = HikariNet.getString(
                "https://gist.githubusercontent.com/codegeasse1/02333c773cbd933b02e1779e6a1222fe/raw/config.json"
            ) ?: "{}"
            Regex("\"translate\"\\s*:\\s*(true|false)").find(json)
                ?.groupValues?.get(1)?.toBoolean() ?: true
        } catch (e: Exception) {
            true
        }
        return flag
    }

    private fun stripTags(s: String): String =
        s.replace(Regex("<[^>]*>"), "").replace("&amp;", "&").trim()
}
