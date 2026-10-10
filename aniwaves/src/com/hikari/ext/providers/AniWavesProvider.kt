package com.hikari.ext.providers

import android.util.Base64
import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import com.hikari.ext.HikariSubtitle
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import org.json.JSONObject
import java.net.URLEncoder
import javax.crypto.Cipher
import javax.crypto.Mac
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec

/**
 * AniWaves (aniwaves.ru) — anime via HTML grids + ajax episode/server/sources.
 *
 * Faithful native port of KSHITIJ8473/raghav AniWaves (CloudStream MainAPI),
 * verified live Oct 2026:
 *  - Firebase-hosted domain table (refreshDomain) with https://aniwaves.ru fallback.
 *  - Rows: /updated/page/N, /trending/page/N, /filter?lang=sub&page=N,
 *    /filter?lang=dub&page=N, /newest/page/N (all `.ani.items` grids with
 *    a.item cards), plus Movies via /filter?type[]=movie&page=N (verified).
 *  - Search: /filter?keyword=.
 *  - Cards: a.item[href=/watch/slug-id] > .poster img[src] + .info > .name
 *    (+ .meta dots: type, year); filter pages use div.item cards with
 *    a.name[href], .genre links, ep-status sub/dub counts.
 *  - Detail: h1.title (+data-jp), .poster img, .hotest .image div style bg,
 *    .synopsis, .bmeta Premiered/Genre/Type/Status rows, #watch-main[data-id].
 *  - Episodes: GET {host}/ajax/episode/list/{animeId} (X-Requested-With) ->
 *    {status,result: li a[data-ids][data-num][data-sub][data-dub] html}.
 *  - Servers: GET {host}/ajax/server/list?servers={dataIds} ->
 *    .type[data-type] > li[data-link-id][data-sv-id] ->
 *    GET {host}/ajax/sources?id={linkId}&asi=0&autoPlay=0 -> {result:{url}}.
 *  - Embeds: megaplay natively (MegaPlayHelper port w/ enc + signUrl +
 *    legacy migration), filemoon-likes via WebView capture, rest via WebView.
 */
class AniWavesProvider : HikariProvider {

    override val id = "aniwaves"
    override val name = "AniWaves"
    override val mainUrl = "https://aniwaves.ru"
    override val description = "Anime from aniwaves.ru — recently updated, trending, sub, dub, new releases and movies."
    override val version = 2
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val serverNames = mapOf(
        "4" to "Vidplay",
        "1" to "BYFMS",
        "2" to "DGHG",
        "12" to "MyCloud",
        "14" to "DatSaV",
    )

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("updated", "Recently Updated", HikariMediaType.SERIES),
        HikariCatalog("trending", "Trending", HikariMediaType.SERIES),
        HikariCatalog("sub", "Latest Sub", HikariMediaType.SERIES),
        HikariCatalog("dub", "Latest Dub", HikariMediaType.SERIES),
        HikariCatalog("newest", "New Releases", HikariMediaType.SERIES),
        HikariCatalog("movies", "Movies", HikariMediaType.MOVIE),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        AwApi.refreshDomain()
        val p = page.coerceAtLeast(1)
        val url = when (catalog.id) {
            "trending" -> "${AwApi.host()}/trending/page/$p"
            "sub" -> "${AwApi.host()}/filter?lang=sub&page=$p"
            "dub" -> "${AwApi.host()}/filter?lang=dub&page=$p"
            "newest" -> "${AwApi.host()}/newest/page/$p"
            "movies" -> "${AwApi.host()}/filter?type%5B%5D=movie&page=$p"
            else -> "${AwApi.host()}/updated/page/$p"
        }
        val html = HikariNet.getStringSmart(url, AwApi.pageHeaders()) ?: return emptyList()
        return AwApi.parseCards(html)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        AwApi.refreshDomain()
        val p = page.coerceAtLeast(1)
        val html = HikariNet.getStringSmart("${AwApi.host()}/filter?keyword=${URLEncoder.encode(q, "UTF-8")}&page=$p", AwApi.pageHeaders()) ?: return emptyList()
        return AwApi.parseCards(html)
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        AwApi.refreshDomain()
        val d = AwApi.detail(media.id) ?: return media
        val type = when (d.type.lowercase()) {
            "movie" -> HikariMediaType.MOVIE
            else -> HikariMediaType.SERIES
        }
        return media.copy(
            id = d.url,
            title = d.title,
            type = type,
            posterUrl = d.poster ?: media.posterUrl,
            backdropUrl = d.background ?: media.backdropUrl,
            overview = d.plot ?: media.overview,
            genres = d.genres.takeIf { it.isNotEmpty() } ?: media.genres,
            year = d.year ?: media.year,
        )
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        AwApi.refreshDomain()
        val d = AwApi.detail(media.id) ?: return null
        val eps = AwApi.episodes(d.url, d.animeId)
        if (eps.isEmpty()) return null
        val out = ArrayList<HikariEpisode>()
        for (e in eps) {
            if (e.hasSub) out.add(HikariEpisode(number = e.num, id = "${d.url}|sub|${d.animeId}|${e.num}|${e.dataIds}", name = e.name ?: "Episode ${e.num}"))
            if (e.hasDub) out.add(HikariEpisode(number = e.num, id = "${d.url}|dub|${d.animeId}|${e.num}|${e.dataIds}", name = (e.name ?: "Episode ${e.num}") + " (Dub)"))
        }
        return out.ifEmpty { null }
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        AwApi.refreshDomain()
        val ref = episode?.id ?: return emptyList()
        val parts = ref.split("|")
        if (parts.size < 5) return emptyList()
        val watchUrl = parts[0]
        val kind = parts[1]
        val dataIds = parts[4].replace("&amp;", "&")
        val servers = AwApi.serverList(watchUrl, dataIds, kind)
        if (servers.isEmpty()) return emptyList()
        val out = java.util.Collections.synchronizedList(ArrayList<HikariStream>())
        val seenEmbeds = java.util.Collections.synchronizedSet(HashSet<String>())
        coroutineScope {
            servers.map { (linkId, displayName, targetType) ->
                async {
                    try {
                        val embedUrl = AwApi.sourceUrl(watchUrl, linkId) ?: return@async
                        if (!seenEmbeds.add(embedUrl)) return@async
                        out.addAll(resolveEmbed(embedUrl, watchUrl, displayName, targetType))
                    } catch (t: Throwable) { }
                }
            }.awaitAll()
        }
        return out.distinctBy { it.url }
    }

    private suspend fun resolveEmbed(embedUrl: String, watchUrl: String, displayName: String, targetType: String): List<HikariStream> {
        val label = "AniWaves $displayName (${targetType.uppercase()})"
        val host = Regex("""https?://([^/]+)""").find(embedUrl)?.groupValues?.get(1).orEmpty()
        if (host.contains("megaplay")) {
            val stream = AwMega.resolveStream(embedUrl, watchUrl) ?: return emptyList()
            return AwMega.emitLinks(label, stream, embedUrl)
        }
        if (host.contains("echovideo") || host.contains("weneverbeenfree.com") || host.contains("filemoon") ||
            host.contains("mfw09.org") || host.contains("myvidplay.com") || host.contains("playmogo")) {
            val base = Regex("""https?://[^/]+""").find(embedUrl)?.value ?: AwApi.host()
            return HikariNet.resolveWithWebView(embedUrl, AwApi.MEDIA_CAPTURE, timeoutMs = 60_000).map {
                HikariStream(name = "$label via $base", url = it.url, headers = it.headers.ifEmpty { mapOf("Referer" to embedUrl) }, isM3u8 = it.url.contains(".m3u8"))
            }
        }
        val captured = HikariNet.resolveWithWebView(embedUrl, AwApi.MEDIA_CAPTURE, timeoutMs = 45_000)
        return captured.map {
            HikariStream(name = label, url = it.url, headers = it.headers.ifEmpty { mapOf("Referer" to watchUrl) }, isM3u8 = it.url.contains(".m3u8") || it.url.contains("master.txt"))
        }
    }
}

// ---------------------------------------------------------------------------

private data class AwDetail(val url: String, val title: String, val poster: String?, val background: String?, val plot: String?, val genres: List<String>, val year: Int?, val type: String, val animeId: String)
private data class AwEp(val num: Int, val name: String?, val dataIds: String, val hasSub: Boolean, val hasDub: Boolean)

private object AwApi {
    const val DEFAULT_HOST = "https://aniwaves.ru"
    const val UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    private const val DOMAIN_URL = "https://cloudstreampluginhelper-default-rtdb.firebaseio.com/.json"

    @Volatile private var base = DEFAULT_HOST
    @Volatile private var lastDomain = 0L

    val MEDIA_CAPTURE = Regex("""https?://[^"' \s]+\.m3u8[^"' \s]*|https?://[^"' \s]*master\.txt[^"' \s]*|https?://[^"' \s]+\.mp4[^"' \s]*""")

    fun host(): String = base

    suspend fun refreshDomain() {
        val now = System.currentTimeMillis()
        if (now - lastDomain < 5 * 60 * 1000L) return
        lastDomain = now
        try {
            val txt = HikariNet.getString(DOMAIN_URL) ?: return
            val o = JSONObject(txt)
            val v = o.optString("aniwaves").ifBlank { o.optString("aniwaves_url") }.trim().removeSuffix("/")
            if (v.startsWith("http")) base = v
        } catch (t: Throwable) { }
    }

    fun pageHeaders(): Map<String, String> = mapOf(
        "User-Agent" to UA,
        "Accept" to "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer" to "$base/",
    )
    private fun ajaxHeaders(referer: String): Map<String, String> = mapOf(
        "User-Agent" to UA, "Referer" to referer, "X-Requested-With" to "XMLHttpRequest",
    )

    // -- cards ------------------------------------------------------------------
    private val anchorCardRe = Regex("""<a[^>]*class="[^"]*\bitem\b[^"]*"[^>]*href="([^"]+)"[^>]*>(.*?)</a>""", setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE))
    private val divCardRe = Regex("""<div[^>]*class="[^"]*\bitem\b[^"]*"[^>]*>(.*?)</div>\s*</div>\s*</div>""", setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE))
    private val nameRe = Regex("""class="[^"]*\bname\b[^"]*"[^>]*>(.*?)</(?:a|div)>""", setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE))
    private val hrefInRe = Regex("""<a[^>]*href="([^"]+)"""", RegexOption.IGNORE_CASE)
    private val imgRe = Regex("""<img[^>]+src="([^"]+)"""", RegexOption.IGNORE_CASE)
    private val dotRe = Regex("""<span[^>]*class="[^"]*\bdot\b[^"]*"[^>]*>(.*?)</span>""", setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE))
    private val rightRe = Regex("""<div[^>]*class="[^"]*\bright\b[^"]*"[^>]*>([^<]*)<""", RegexOption.IGNORE_CASE)

    private fun stripTags(s: String): String = s.replace(Regex("<[^>]+>"), "").replace(Regex("\\s+"), " ").trim()

    private fun cardToMedia(href: String, inner: String): HikariMedia? {
        if (!href.startsWith("/watch/") && !href.contains("/watch/")) return null
        val url = if (href.startsWith("http")) href else base + href
        val title = nameRe.find(inner)?.groupValues?.get(1)?.let { stripTags(it) }?.takeIf { it.isNotBlank() } ?: return null
        var poster = imgRe.find(inner)?.groupValues?.get(1)
        if (poster != null && poster.startsWith("//")) poster = "https:$poster"
        val dots = dotRe.findAll(inner).map { stripTags(it.groupValues[1]) }.toList()
        val right = rightRe.find(inner)?.groupValues?.get(1)?.trim()
        val typeToken = (right ?: dots.getOrNull(1) ?: "").lowercase()
        val type = when {
            typeToken.contains("movie") -> HikariMediaType.MOVIE
            typeToken.contains("ova") || typeToken.contains("ona") || typeToken.contains("special") -> HikariMediaType.SERIES
            else -> HikariMediaType.SERIES
        }
        val year = dots.mapNotNull { Regex("""\b(19\d\d|20\d\d)\b""").find(it)?.groupValues?.get(1)?.toIntOrNull() }.firstOrNull()
        return HikariMedia(id = url, title = title, type = type, posterUrl = poster, year = year)
    }

    fun parseCards(html: String): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        val seen = HashSet<String>()
        val grid = Regex("""<(?:div|ul)[^>]*class="[^"]*\bani\b[^"]*\bitems\b[^"]*"[^>]*>(.*?)</(?:div|ul)>""", setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE))
            .find(html)?.groupValues?.get(1) ?: html
        for (m in anchorCardRe.findAll(grid)) {
            val media = cardToMedia(m.groupValues[1], m.groupValues[2]) ?: continue
            if (seen.add(media.id)) out.add(media)
        }
        for (m in divCardRe.findAll(grid)) {
            val inner = m.groupValues[1]
            val href = hrefInRe.find(inner)?.groupValues?.get(1) ?: continue
            val media = cardToMedia(href, inner) ?: continue
            if (seen.add(media.id)) out.add(media)
        }
        return out
    }

    // -- detail -------------------------------------------------------------------
    private val h1Re = Regex("""<h1[^>]*class="[^"]*title[^"]*"[^>]*>([^<]+)<""", RegexOption.IGNORE_CASE)
    private val posterRe = Regex("""<div[^>]*class="[^"]*\bposter\b[^"]*"[^>]*>.*?<img[^>]+src="([^"]+)"""", setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE))
    private val bgRe = Regex("""class="[^"]*image[^"]*"[^>]*style="[^"]*url\('([^']+)'\)""", RegexOption.IGNORE_CASE)
    private val synopsisRe = Regex("""<div[^>]*class="[^"]*synopsis[^"]*"[^>]*>(.*?)</div>\s*</div>""", setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE))
    private val metaRowRe = Regex("""<div[^>]*>\s*(Premiered|Genre|Type|Status)[^<]*<(?:span|a)[^>]*>(.*?)</(?:span|a)>""", setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE))
    private val watchMainRe = Regex("""id="watch-main"[^>]*data-id="(\d+)"""")
    private val idSuffixRe = Regex("""-(\d+)(?:[/?#]|$)""")

    suspend fun detail(pageUrl: String): AwDetail? {
        val url = if (pageUrl.startsWith("http")) pageUrl else base + pageUrl
        val html = HikariNet.getStringSmart(url, pageHeaders()) ?: return null
        val title = h1Re.find(html)?.groupValues?.get(1)?.trim()?.takeIf { it.isNotBlank() } ?: return null
        var poster = posterRe.find(html)?.groupValues?.get(1)
        if (poster != null && poster.startsWith("//")) poster = "https:$poster"
        val bg = bgRe.find(html)?.groupValues?.get(1)?.let { if (it.startsWith("//")) "https:$it" else it }
        val plot = synopsisRe.find(html)?.groupValues?.get(1)?.let { stripTags(it) }?.takeIf { it.isNotBlank() }
        var year: Int? = null
        var type = ""
        val genres = ArrayList<String>()
        for (m in metaRowRe.findAll(html)) {
            when (m.groupValues[1]) {
                "Premiered" -> year = Regex("""\b(19\d\d|20\d\d)\b""").find(m.groupValues[2])?.groupValues?.get(1)?.toIntOrNull()
                "Genre" -> Regex("""<a[^>]*>([^<]+)<""").findAll(m.groupValues[2]).forEach { genres.add(it.groupValues[1].trim()) }
                "Type" -> type = stripTags(m.groupValues[2])
            }
        }
        val animeId = watchMainRe.find(html)?.groupValues?.get(1)
            ?: idSuffixRe.find(url)?.groupValues?.get(1) ?: return null
        return AwDetail(url, title, poster?.takeIf { it.startsWith("http") }, bg, plot, genres, year, type, animeId)
    }

    // -- episodes -------------------------------------------------------------------
    private val epLinkRe = Regex("""<li[^>]*>.*?<a[^>]*data-ids="([^"]+)"[^>]*data-num="(\d+)"[^>]*>""", setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE))
    private fun attr(tag: String, name: String): String = Regex("""$name="([^"]*)"""").find(tag)?.groupValues?.get(1) ?: ""

    suspend fun episodes(pageUrl: String, animeId: String): List<AwEp> {
        val body = HikariNet.getStringSmart("$base/ajax/episode/list/$animeId", ajaxHeaders(pageUrl)) ?: return emptyList()
        val epsHtml = try { JSONObject(body).optString("result").takeIf { it.isNotBlank() } } catch (t: Throwable) { null } ?: return emptyList()
        val out = ArrayList<AwEp>()
        val seen = HashSet<Int>()
        for (m in epLinkRe.findAll(epsHtml)) {
            val tag = m.value
            val num = m.groupValues[2].toIntOrNull() ?: continue
            if (!seen.add(num)) continue
            val liTitle = Regex("""<li[^>]*title="([^"]*)"""").find(tag)?.groupValues?.get(1)
                ?.substringAfter("GMT - ", "")?.trim()?.takeIf { it.isNotEmpty() }
            out.add(AwEp(num, liTitle, m.groupValues[1], attr(tag, "data-sub") == "1", attr(tag, "data-dub") == "1"))
        }
        return out.sortedBy { it.num }
    }

    // -- servers ----------------------------------------------------------------------
    private val typeSectionRe = Regex("""class="[^"]*\btype\b[^"]*"[^>]*data-type="([^"]+)"[^>]*>(.*?)</(?:div|ul)>\s*</(?:div|ul)>""", setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE))
    private val liRe = Regex("""<li[^>]*data-link-id="([^"]+)"[^>]*data-sv-id="([^"]*)"[^>]*>(.*?)</li>""", setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE))
    private val serverNames = mapOf("4" to "Vidplay", "1" to "BYFMS", "2" to "DGHG", "12" to "MyCloud", "14" to "DatSaV")

    suspend fun serverList(watchUrl: String, dataIds: String, kind: String): List<Triple<String, String, String>> {
        val body = HikariNet.getStringSmart(
            "$base/ajax/server/list?servers=${URLEncoder.encode(dataIds, "UTF-8")}",
            ajaxHeaders(watchUrl),
        ) ?: return emptyList()
        val html = try { JSONObject(body).optString("result").takeIf { it.isNotBlank() } } catch (t: Throwable) { null } ?: return emptyList()
        val targets = if (kind == "dub") listOf("dub") else listOf("sub", "ssub")
        val out = ArrayList<Triple<String, String, String>>()
        for (tm in typeSectionRe.findAll(html)) {
            if (tm.groupValues[1] !in targets) continue
            for (lm in liRe.findAll(tm.groupValues[2])) {
                val linkId = lm.groupValues[1]
                if (linkId.isBlank()) continue
                val svId = lm.groupValues[2]
                val name = stripTags(lm.groupValues[3]).takeIf { it.isNotBlank() } ?: "Server"
                out.add(Triple(linkId, serverNames[svId] ?: name, tm.groupValues[1]))
            }
        }
        return out
    }

    suspend fun sourceUrl(watchUrl: String, linkId: String): String? {
        val body = HikariNet.getStringSmart(
            "$base/ajax/sources?id=${URLEncoder.encode(linkId, "UTF-8")}&asi=0&autoPlay=0",
            ajaxHeaders(watchUrl),
        ) ?: return null
        return try {
            JSONObject(body).optJSONObject("result")?.optString("url")?.takeIf { it.isNotBlank() }
        } catch (t: Throwable) { null }
    }
}

// MegaPlay helper (ported from AniWaves MegaPlayHelper: cipher + enc + signUrl
// + legacy URL migration + subtitle origin migration). okhttp -> HikariNet.
private object AwMega {
    const val UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    private const val FALLBACK_KEY_SEED = "i?LMTAx0Q6,:}50U"
    private const val FALLBACK_IV_SEED = "W0;27ToaUpl_P%'c"
    private const val TOKEN_KEY = "MpCdnT0k3n!9f2K#xQ7vL5mR8wN1pY4s"
    private const val TOKEN_LIFETIME = 7L * 24L * 60L * 60L
    private val keyPairRe = Regex("""[A-Za-z]\w*="([^"]{16})",[A-Za-z]\w*="([^"]{16})"""")
    private val fileRe = Regex(""""file"\s*:\s*"([^"]+)"""")
    private val hexIdsRe = Regex("""/([a-f0-9]{32})/([a-f0-9]{32})/""", RegexOption.IGNORE_CASE)
    private val resRe = Regex("""RESOLUTION=(\d+)x(\d+)""")

    class Stream(val m3u8: String, val subtitles: List<Pair<String, String>>)

    @Volatile private var cachedSeeds: Pair<String, String>? = null

    private suspend fun seeds(base: String): List<Pair<String, String>> {
        cachedSeeds?.let { return listOf(it, FALLBACK_KEY_SEED to FALLBACK_IV_SEED) }
        val dynamic = try {
            HikariNet.getString("$base/lib/newclient.min.js", mapOf("User-Agent" to UA))
                ?.let { keyPairRe.find(it)?.groupValues?.let { g -> (g[1] to g[2]).also { cachedSeeds = it } } }
        } catch (t: Throwable) { null }
        return listOfNotNull(dynamic, FALLBACK_KEY_SEED to FALLBACK_IV_SEED)
    }

    private fun decryptToken(enc: String, keySeed: String, ivSeed: String): String? {
        return try {
            var b64 = enc.replace('-', '+').replace('_', '/')
            while (b64.length % 4 != 0) b64 += "="
            val cipherBytes = Base64.decode(b64, Base64.DEFAULT)
            val keyBytes = ByteArray(32)
            val sb = keySeed.toByteArray(Charsets.UTF_8)
            System.arraycopy(sb, 0, keyBytes, 0, minOf(32, sb.size))
            val ivBytes = ByteArray(16)
            val ib = ivSeed.toByteArray(Charsets.UTF_8)
            System.arraycopy(ib, 0, ivBytes, 0, minOf(16, ib.size))
            val cipher = Cipher.getInstance("AES/CBC/PKCS5Padding")
            cipher.init(Cipher.DECRYPT_MODE, SecretKeySpec(keyBytes, "AES"), IvParameterSpec(ivBytes))
            String(cipher.doFinal(cipherBytes), Charsets.UTF_8)
        } catch (t: Throwable) { null }
    }

    suspend fun resolveEnc(enc: String, base: String): String? {
        for ((k, iv) in seeds(base)) {
            val plain = decryptToken(enc, k, iv) ?: continue
            fileRe.find(plain)?.groupValues?.get(1)?.let { return migrate(it) }
        }
        return null
    }

    private fun migrate(url: String): String {
        if (!url.contains("https://cdn.imgnex.top/anime")) return url
        return url.replace("https://cdn.imgnex.top/anime", "https://megap.norami.top")
    }

    suspend fun resolveStream(embedUrl: String, referer: String?): Stream? {
        val host = Regex("""https?://([^/]+)""").find(embedUrl)?.groupValues?.get(1) ?: return null
        val pageHeaders = mapOf("User-Agent" to UA, "Referer" to (referer ?: "https://$host/"))
        val html = HikariNet.getStringSmart(embedUrl, pageHeaders) ?: return null
        val streamId = Regex("""data-id=["'](\d+)["']""").find(html)?.groupValues?.get(1)
            ?: Regex("""data-realid=["'](\d+)["']""").find(html)?.groupValues?.get(1)
            ?: Regex("""/stream/s-\d+/(\d+)/""").find(embedUrl)?.groupValues?.get(1)
            ?: return null
        val audio = Regex("""/(dub|sub)(?:[/?#]|$)""").find(embedUrl)?.groupValues?.get(1)
            ?: Regex("""type\s*:\s*['"](dub|sub)['"]""").find(html)?.groupValues?.get(1)
            ?: "sub"
        val altHost = Regex("""data-domain=["']([^"']+)["']""").find(html)?.groupValues?.get(1)
        val hosts = listOfNotNull(host, altHost?.takeIf { it != host }).distinct()
        for (apiHost in hosts) {
            val apiBase = "https://$apiHost"
            val ajax = mapOf("User-Agent" to UA, "Accept" to "*/*", "X-Requested-With" to "XMLHttpRequest", "Origin" to apiBase, "Referer" to embedUrl)
            for (endpoint in listOf("getSourcesNew", "getSources")) {
                val body = HikariNet.getStringSmart("$apiBase/stream/$endpoint?id=$streamId&type=$audio", ajax) ?: continue
                val root = try { JSONObject(body) } catch (t: Throwable) { continue }
                val streamUrl = extractStream(root, apiBase) ?: continue
                val origin = Regex("""https?://[^/]+""").find(streamUrl)?.value ?: "https://megap.norami.top"
                val subs = ArrayList<Pair<String, String>>()
                root.optJSONArray("tracks")?.let { arr ->
                    for (i in 0 until arr.length()) {
                        val e = arr.optJSONObject(i) ?: continue
                        val kind = e.optString("kind")
                        if (kind != "captions" && kind != "subtitles") continue
                        var f = e.optString("file")
                        if (f.isBlank()) continue
                        if (f.contains("https://cdn.imgnex.top/anime")) f = f.replace("https://cdn.imgnex.top/anime", origin)
                        subs.add((e.optString("label").ifBlank { "English" }) to f)
                    }
                }
                return Stream(streamUrl, subs)
            }
        }
        return null
    }

    private suspend fun extractStream(root: JSONObject, base: String): String? {
        val sources = root.opt("sources")
        val plain = when {
            sources == null || sources == JSONObject.NULL -> null
            sources is JSONObject -> sources.optString("file").takeIf { it.isNotBlank() }
            sources is org.json.JSONArray && sources.length() > 0 -> sources.optJSONObject(0)?.optString("file")?.takeIf { it.isNotBlank() }
            else -> null
        }
        if (!plain.isNullOrBlank()) return migrate(plain)
        val enc = root.optString("enc").takeIf { it.isNotBlank() } ?: return null
        return resolveEnc(enc, base)
    }

    private fun b64url(bytes: ByteArray): String =
        Base64.encodeToString(bytes, Base64.URL_SAFE or Base64.NO_WRAP or Base64.NO_PADDING)

    fun signUrl(url: String): String {
        val m = hexIdsRe.find(url) ?: return url
        val expires = System.currentTimeMillis() / 1000L + TOKEN_LIFETIME
        val payload = "$expires|${m.groupValues[1].lowercase()}/${m.groupValues[2].lowercase()}"
        val mac = Mac.getInstance("HmacSHA256")
        mac.init(SecretKeySpec(TOKEN_KEY.toByteArray(Charsets.UTF_8), "HmacSHA256"))
        val sig = mac.doFinal(payload.toByteArray(Charsets.UTF_8))
        val token = "${b64url(payload.toByteArray(Charsets.UTF_8))}.${b64url(sig)}"
        return "$url${if (url.contains('?')) "&" else "?"}token=$token"
    }

    suspend fun emitLinks(label: String, stream: Stream, referer: String): List<HikariStream> {
        val playHeaders = mapOf("User-Agent" to UA, "Referer" to referer)
        val signedMaster = signUrl(stream.m3u8)
        val masterText = HikariNet.getString(signedMaster, playHeaders)
        val variants = masterText?.let { parseVariants(stream.m3u8, it) } ?: emptyList()
        val subs = stream.subtitles.filter { it.second.startsWith("http") }.map { HikariSubtitle(it.first.ifBlank { "English" }, it.second) }
        if (variants.isNotEmpty()) {
            return variants.map { v ->
                val suffix = v.second?.let { " $it" } ?: ""
                HikariStream(name = "$label$suffix", url = signUrl(v.first), headers = playHeaders, subtitles = subs, isM3u8 = true)
            }
        }
        if (masterText != null && masterText.contains("#EXTM3U")) {
            return listOf(HikariStream(name = label, url = signedMaster, headers = playHeaders, subtitles = subs, isM3u8 = true))
        }
        return emptyList()
    }

    private fun parseVariants(masterUrl: String, masterText: String): List<Pair<String, Int?>> {
        val base = masterUrl.substringBefore('?').substringBeforeLast('/') + "/"
        val out = ArrayList<Pair<String, Int?>>()
        val lines = masterText.lines()
        var i = 0
        while (i < lines.size) {
            val line = lines[i].trim()
            if (line.startsWith("#EXT-X-STREAM-INF:")) {
                val q = resRe.find(line)?.groupValues?.get(2)?.toIntOrNull()
                var j = i + 1
                while (j < lines.size && (lines[j].isBlank() || lines[j].startsWith("#"))) j++
                if (j < lines.size) {
                    val uri = lines[j].trim()
                    if (uri.isNotEmpty()) out.add((if (uri.startsWith("http")) uri else base + uri) to q)
                    i = j
                }
            }
            i++
        }
        return out
    }
}
