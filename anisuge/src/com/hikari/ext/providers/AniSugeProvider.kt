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
 * AniSuge (anisuge.tv) — anime sub & dub via home grids + ajax servers.
 *
 * Faithful native port of KSHITIJ8473/raghav AniSuge (CloudStream MainAPI),
 * verified live Oct 2026:
 *  - Firebase-hosted domain table (refreshDomain) with https://anisuge.tv fallback.
 *  - Home rows scrape /home sections: Recently Updated (section.pt-4),
 *    Recently Added (section.added), Just Completed (section.complete),
 *    Most Viewed (section.mostview) — .item grids {a[href], .name, img}.
 *  - Extra rows use the site's own filter sorts (verified in /filter form):
 *    /filter?sort=score (Top Rated) and /filter?sort=latest-updated, same grids.
 *  - Search: /filter?keyword=, same grids.
 *  - Detail scrapes .watch-wrap[data-id] (mangaId fallback), title/poster/
 *    banner/plot/year/genres; episodes via ajax/episode/list/{id}?vrf= (vrf =
 *    rc4+shift+rot13 of the id, ported verbatim) -> {result: html with
 *    a[data-ids] carrying data-sub/data-dub/data-mal/data-slug/data-timestamp}.
 *  - Servers: ajax/server/list?servers= -> server-type/server list ->
 *    ajax/server?get= -> {result:{url}}; embeds: plyr.php# b64, megaplay
 *    clones (native MegaPlay port w/ enc + signUrl), others via WebView
 *    m3u8/mp4 capture; mapper (mapper.nekostream.site) + pahe downloads via
 *    WebView capture.
 */
class AniSugeProvider : HikariProvider {
    override val id = "anisuge"
    override val name = "AniSuge"
    override val mainUrl = "https://anisuge.tv"
    override val description = "Anime from anisuge.tv — recently updated, recently added, just completed, most viewed, top rated, sub & dub."
    override val version = 2
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("recent", "Recently Updated", HikariMediaType.SERIES),
        HikariCatalog("added", "Recently Added", HikariMediaType.SERIES),
        HikariCatalog("complete", "Just Completed", HikariMediaType.SERIES),
        HikariCatalog("mostview", "Most Viewed", HikariMediaType.SERIES),
        HikariCatalog("top-rated", "Top Rated", HikariMediaType.SERIES),
        HikariCatalog("latest-updated", "Latest Updated", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        SgApi.refreshDomain()
        val p = page.coerceAtLeast(1)
        return when (catalog.id) {
            "top-rated" -> SgApi.filterItems("${SgApi.host()}/filter?sort=score&page=$p")
            "latest-updated" -> SgApi.filterItems("${SgApi.host()}/filter?sort=latest-updated&page=$p")
            else -> if (p > 1) emptyList() else SgApi.homeSection(catalog.id)
        }
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        SgApi.refreshDomain()
        val pageParam = if (page > 1) "&page=$page" else ""
        return SgApi.filterItems("${SgApi.host()}/filter?keyword=${URLEncoder.encode(q, "UTF-8")}$pageParam")
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        SgApi.refreshDomain()
        val d = SgApi.detail(media.id) ?: return media
        return media.copy(
            id = d.url,
            title = d.title,
            posterUrl = d.poster ?: media.posterUrl,
            backdropUrl = d.banner ?: media.backdropUrl,
            overview = d.plot ?: media.overview,
            genres = d.genres.takeIf { it.isNotEmpty() } ?: media.genres,
            year = d.year ?: media.year,
        )
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        SgApi.refreshDomain()
        val d = SgApi.detail(media.id) ?: return null
        val eps = SgApi.episodes(d.pageUrl, d.dataId)
        if (eps.isEmpty()) return null
        val out = ArrayList<HikariEpisode>()
        for (e in eps) {
            if (e.hasSub) out.add(HikariEpisode(number = e.num, id = "${d.pageUrl}|${d.dataId}|${e.num}|${e.dataIds}|${e.malId}|${e.slug}|${e.timestamp}|sub", name = e.name))
            if (e.hasDub) out.add(HikariEpisode(number = e.num, id = "${d.pageUrl}|${d.dataId}|${e.num}|${e.dataIds}|${e.malId}|${e.slug}|${e.timestamp}|dub", name = "${e.name} (Dub)"))
        }
        return out.ifEmpty { null }
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        SgApi.refreshDomain()
        val ref = episode?.id ?: return emptyList()
        val parts = ref.split("|")
        if (parts.size < 8) return emptyList()
        val baseUrl = parts[0]
        val dataIds = parts[3]
        val malId = parts[4]
        val slug = parts[5].ifBlank { parts[2] }
        val timestamp = parts[6]
        val sel = parts[7]
        val out = java.util.Collections.synchronizedList(ArrayList<HikariStream>())
        coroutineScope {
            val jobs = ArrayList<suspend () -> Unit>()
            jobs.add { out.addAll(SgApi.legacyServers(baseUrl, dataIds, sel)) }
            if (malId.isNotBlank() && timestamp.isNotBlank()) {
                jobs.add { out.addAll(SgApi.mapperSources(baseUrl, malId, slug, timestamp, sel)) }
            }
            jobs.map { async { it() } }.awaitAll()
        }
        return out.distinctBy { it.url }
    }
}

// ---------------------------------------------------------------------------

private data class SgDetail(val pageUrl: String, val dataId: String, val title: String, val poster: String?, val banner: String?, val plot: String?, val genres: List<String>, val year: Int?)
private data class SgEp(val num: Int, val name: String, val dataIds: String, val malId: String, val slug: String, val timestamp: String, val hasSub: Boolean, val hasDub: Boolean)

private object SgApi {
    const val DEFAULT_HOST = "https://anisuge.tv"
    const val UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    const val UA2 = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    private const val DOMAIN_URL = "https://cloudstreampluginhelper-default-rtdb.firebaseio.com/.json"
    private const val MAPPER_API = "https://mapper.nekostream.site/api/mal/"

    @Volatile private var base = DEFAULT_HOST
    @Volatile private var lastDomain = 0L

    val MEDIA_CAPTURE = Regex("""https?://[^"' \s]+?\.(?:m3u8|mp4)(?:[?#][^"' \s]*)?""")

    fun host(): String = base

    suspend fun refreshDomain() {
        val now = System.currentTimeMillis()
        if (now - lastDomain < 5 * 60 * 1000L) return
        lastDomain = now
        try {
            val txt = HikariNet.getString(DOMAIN_URL) ?: return
            val o = JSONObject(txt)
            val v = o.optString("anisuge").ifBlank { o.optString("anisuge_url") }.trim().removeSuffix("/")
            if (v.startsWith("http")) base = v
        } catch (t: Throwable) { }
    }

    private fun pageHeaders(referer: String? = null): Map<String, String> = mapOf(
        "User-Agent" to UA, "Referer" to (referer ?: "$base/"),
    )
    private fun ajaxHeaders(referer: String): Map<String, String> = mapOf(
        "User-Agent" to UA, "Referer" to referer, "X-Requested-With" to "XMLHttpRequest",
    )

    // -- grids --------------------------------------------------------------
    private val itemBlockRe = Regex("""<(?:div|a)[^>]*class="[^"]*\bitem\b[^"]*"[^>]*>(.*?)</(?:div|a)>""", setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE))
    private val hrefRe = Regex("""<a[^>]+href="([^"]+)"""", RegexOption.IGNORE_CASE)
    private val nameRe = Regex("""class="[^"]*\bname\b[^"]*"[^>]*>([^<]+)<""", RegexOption.IGNORE_CASE)
    private val imgRe = Regex("""<img[^>]+(?:data-src|src)="([^"]+)"""", RegexOption.IGNORE_CASE)

    private fun parseItems(html: String): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        val seen = HashSet<String>()
        for (m in itemBlockRe.findAll(html)) {
            val block = m.groupValues[1]
            val href = hrefRe.find(m.value)?.groupValues?.get(1)
                ?: hrefRe.find(block)?.groupValues?.get(1) ?: continue
            val url = if (href.startsWith("http")) href else base + href
            if (!seen.add(url)) continue
            val title = nameRe.find(block)?.groupValues?.get(1)?.trim()?.takeIf { it.isNotBlank() } ?: continue
            var poster = imgRe.find(block)?.groupValues?.get(1)
            if (poster != null && !poster.startsWith("http")) poster = base + poster
            out.add(HikariMedia(id = url, title = title, type = HikariMediaType.SERIES, posterUrl = poster))
        }
        return out
    }

    private val sectionRes = mapOf(
        "recent" to Regex("""<section[^>]*class="[^"]*\bpt-4\b[^"]*"[^>]*>(.*?)</section>""", setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE)),
        "added" to Regex("""<section[^>]*class="[^"]*\badded\b[^"]*"[^>]*>(.*?)</section>""", setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE)),
        "complete" to Regex("""<section[^>]*class="[^"]*\bcomplete\b[^"]*"[^>]*>(.*?)</section>""", setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE)),
        "mostview" to Regex("""<section[^>]*class="[^"]*\bmostview\b[^"]*"[^>]*>(.*?)</section>""", setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE)),
    )

    suspend fun homeSection(kind: String): List<HikariMedia> {
        val html = HikariNet.getStringSmart("$base/home", pageHeaders()) ?: return emptyList()
        val re = sectionRes[kind] ?: return emptyList()
        val section = re.find(html)?.groupValues?.get(1)
        if (section != null) {
            val items = parseItems(section)
            if (items.isNotEmpty()) return items
        }
        // Fallback: first grid on the page (recently updated renders first).
        if (kind == "recent") {
            val items = parseItems(html)
            if (items.isNotEmpty()) return items
        }
        return emptyList()
    }

    suspend fun filterItems(url: String): List<HikariMedia> {
        val html = HikariNet.getStringSmart(url, pageHeaders()) ?: return emptyList()
        return parseItems(html)
    }

    // -- detail ---------------------------------------------------------------
    private val watchWrapRe = Regex("""class="[^"]*watch-wrap[^"]*"[^>]*data-id="(\d+)"""")
    private val mangaIdRe = Regex("""mangaId\s*=\s*(\d+)""")
    private val h1Re = Regex("""<h1[^>]*class="[^"]*title[^"]*"[^>]*>([^<]+)<""", RegexOption.IGNORE_CASE)
    private val ogTitleRe = Regex("""<meta[^>]+property="og:title"[^>]+content="([^"]+)"""", RegexOption.IGNORE_CASE)
    private val ogImageRe = Regex("""<meta[^>]+property="og:image"[^>]+content="([^"]+)"""", RegexOption.IGNORE_CASE)
    private val ogDescRe = Regex("""<meta[^>]+property="og:description"[^>]+content="([^"]+)"""", RegexOption.IGNORE_CASE)
    private val mediaBgRe = Regex("""class="[^"]*media-bg[^"]*"[^>]*style="[^"]*url\(['"]?([^'")]+)""", RegexOption.IGNORE_CASE)
    private val descFullRe = Regex("""<div[^>]*class="[^"]*description[^"]*"[^>]*>.*?<div[^>]*class="[^"]*full[^"]*"[^>]*>(.*?)</div>""", setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE))
    private val yearRe = Regex("""\b(19\d\d|20\d\d)\b""")
    private val genreLinkRe = Regex("""<a[^>]+href="[^"]*/genre/[^"]*"[^>]*>([^<]+)<""", RegexOption.IGNORE_CASE)

    private fun stripTags(s: String): String = s.replace(Regex("<[^>]+>"), " ").replace(Regex("\\s+"), " ").trim()

    suspend fun detail(pageUrl: String): SgDetail? {
        val url = if (pageUrl.startsWith("http")) pageUrl else base + pageUrl
        val html = HikariNet.getStringSmart(url, pageHeaders()) ?: return null
        val dataId = watchWrapRe.find(html)?.groupValues?.get(1)
            ?: mangaIdRe.find(html)?.groupValues?.get(1) ?: return null
        val title = h1Re.find(html)?.groupValues?.get(1)?.trim()
            ?: ogTitleRe.find(html)?.groupValues?.get(1)?.substringBefore(" Episode")?.trim()
            ?: return null
        val poster = ogImageRe.find(html)?.groupValues?.get(1)?.takeIf { it.startsWith("http") }
        val banner = mediaBgRe.find(html)?.groupValues?.get(1)?.let { if (it.startsWith("http")) it else base + it }
        val plot = descFullRe.find(html)?.groupValues?.get(1)?.let { stripTags(it) }?.takeIf { it.isNotBlank() }
            ?: ogDescRe.find(html)?.groupValues?.get(1)?.trim()
        val metaIdx = html.indexOf("Premiered:")
        val year = (if (metaIdx >= 0) yearRe.find(html.substring(metaIdx, minOf(html.length, metaIdx + 2000))) else yearRe.find(html))?.groupValues?.get(1)?.toIntOrNull()
        val genres = genreLinkRe.findAll(html).map { it.groupValues[1].trim() }.filter { it.isNotBlank() }.distinct().toList()
        return SgDetail(url, dataId, title, poster, banner, plot, genres, year)
    }

    // -- vrf + episodes ---------------------------------------------------------
    private fun rc4(key: ByteArray, input: ByteArray): ByteArray {
        val s = IntArray(256) { it }
        var j = 0
        for (i in 0..255) {
            j = (j + s[i] + (key[i % key.size].toInt() and 0xFF)) and 0xFF
            val t = s[i]; s[i] = s[j]; s[j] = t
        }
        var i = 0; j = 0
        val out = ByteArray(input.size)
        for (x in input.indices) {
            i = (i + 1) and 0xFF
            j = (j + s[i]) and 0xFF
            val t = s[i]; s[i] = s[j]; s[j] = t
            out[x] = ((input[x].toInt() and 0xFF) xor s[(s[i] + s[j]) and 0xFF]).toByte()
        }
        return out
    }

    private fun shiftCharcode(t: String): ByteArray {
        val out = ByteArray(t.length)
        for (r in t.indices) {
            var s = t[r].code
            when (r % 8) {
                1 -> s += 3
                7 -> s += 5
                2 -> s -= 4
                4 -> s -= 2
                6 -> s += 4
                0 -> s -= 3
                3 -> s += 2
                5 -> s += 5
            }
            out[r] = s.toByte()
        }
        return out
    }

    private fun rot13(s: String): String {
        val sb = StringBuilder()
        for (c in s) {
            when (c) {
                in 'a'..'z' -> { var o = c.code + 13; if (o > 'z'.code) o -= 26; sb.append(o.toChar()) }
                in 'A'..'Z' -> { var o = c.code + 13; if (o > 'Z'.code) o -= 26; sb.append(o.toChar()) }
                else -> sb.append(c)
            }
        }
        return sb.toString()
    }

    fun vrf(input: String): String {
        val encoded = URLEncoder.encode(input, "UTF-8").replace("+", "%20")
        val rc4Bytes = rc4("ysJhV6U27FVIjjuk".toByteArray(Charsets.UTF_8), encoded.toByteArray(Charsets.UTF_8))
        val b64 = Base64.encodeToString(rc4Bytes, Base64.URL_SAFE or Base64.NO_WRAP)
        val shifted = Base64.encodeToString(shiftCharcode(b64), Base64.URL_SAFE or Base64.NO_WRAP)
        return rot13(shifted)
    }

    private val epLinkRe = Regex("""<a[^>]*data-ids="([^"]+)"[^>]*>(.*?)</a>""", setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE))
    private fun attr(tag: String, name: String): String {
        return Regex("""$name="([^"]*)"""").find(tag)?.groupValues?.get(1) ?: ""
    }

    suspend fun episodes(pageUrl: String, dataId: String): List<SgEp> {
        val vrf = vrf(dataId)
        val body = HikariNet.getStringSmart("$base/ajax/episode/list/$dataId?vrf=${URLEncoder.encode(vrf, "UTF-8")}", ajaxHeaders(pageUrl)) ?: return emptyList()
        val epsHtml = try {
            JSONObject(body).optString("result").takeIf { it.isNotBlank() } ?: return emptyList()
        } catch (t: Throwable) { return emptyList() }
        val out = ArrayList<SgEp>()
        for (m in epLinkRe.findAll(epsHtml)) {
            val tag = m.value
            val dataIds = m.groupValues[1]
            val inner = stripTags(m.groupValues[2])
            val num = inner.toIntOrNull()
                ?: attr(tag, "data-slug").toIntOrNull()
                ?: attr(tag, "data-num").filter { it.isDigit() }.toIntOrNull()
                ?: 1
            val name = attr(tag, "data-num").takeIf { it.isNotBlank() } ?: "Episode $num"
            out.add(SgEp(
                num = num, name = name, dataIds = dataIds,
                malId = attr(tag, "data-mal").trim(),
                slug = attr(tag, "data-slug").takeIf { it.isNotBlank() } ?: num.toString(),
                timestamp = attr(tag, "data-timestamp").trim(),
                hasSub = attr(tag, "data-sub") == "1",
                hasDub = attr(tag, "data-dub") == "1",
            ))
        }
        return out
    }

    // -- servers ------------------------------------------------------------------
    private val serverTypeRe = Regex("""<div[^>]*class="[^"]*server-type[^"]*"[^>]*data-type="([^"]+)"[^>]*>(.*?)</div>\s*</div>""", setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE))
    private val serverRe = Regex("""data-link-id="([^"]+)"[^>]*>.*?<span[^>]*>([^<]*)<""", setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE))

    suspend fun legacyServers(baseUrl: String, dataIds: String, sel: String): List<HikariStream> {
        val out = ArrayList<HikariStream>()
        val listBody = HikariNet.getStringSmart(
            "$baseUrl/ajax/server/list?servers=${URLEncoder.encode(dataIds, "UTF-8")}",
            ajaxHeaders("$baseUrl/watch/"),
        ) ?: return out
        val listHtml = try { JSONObject(listBody).optString("result") } catch (t: Throwable) { null }?.takeIf { it.isNotBlank() } ?: return out
        val wanted = ArrayList<Pair<String, String>>()
        for (tm in serverTypeRe.findAll(listHtml)) {
            val typeAttr = tm.groupValues[1]
            val match = if (sel == "sub") typeAttr == "sub" || typeAttr == "hsub" || typeAttr == "h-sub" || typeAttr == "raw"
            else typeAttr == "dub" || typeAttr == "adub" || typeAttr == "a-dub"
            if (!match) continue
            for (sm in serverRe.findAll(tm.groupValues[2])) {
                wanted.add((sm.groupValues[2].trim().ifBlank { "Server" }) to sm.groupValues[1])
            }
        }
        if (wanted.isEmpty()) return out
        coroutineScope {
            wanted.map { (serverName, linkId) ->
                async {
                    try {
                        val infoBody = HikariNet.getStringSmart(
                            "$baseUrl/ajax/server?get=${URLEncoder.encode(linkId, "UTF-8")}",
                            ajaxHeaders("$baseUrl/watch/"),
                        ) ?: return@async emptyList<HikariStream>()
                        val playerUrl = JSONObject(infoBody).optJSONObject("result")?.optString("url")?.takeIf { it.isNotBlank() } ?: return@async emptyList()
                        resolveEmbed(playerUrl, baseUrl, serverName)
                    } catch (t: Throwable) { emptyList() }
                }
            }.awaitAll().forEach { out.addAll(it) }
        }
        return out
    }

    suspend fun resolveEmbed(playerUrl: String, baseUrl: String, serverName: String): List<HikariStream> {
        if (playerUrl.contains("plyr.php#")) {
            val b64 = playerUrl.substringAfter("#").substringBefore("#")
            val decoded = try { String(Base64.decode(b64, Base64.DEFAULT), Charsets.UTF_8) } catch (t: Throwable) { "" }
            if (decoded.isNotBlank()) {
                return listOf(HikariStream(
                    name = "AniSuge $serverName",
                    url = decoded,
                    headers = mapOf("Referer" to "https://gogoanime.me.uk/"),
                    isM3u8 = decoded.contains(".m3u8"),
                ))
            }
        }
        val embedHost = Regex("""https?://([^/]+)""").find(playerUrl)?.groupValues?.get(1) ?: ""
        if (embedHost.contains("megaplay") || embedHost.contains("vidwish") || embedHost.contains("vidtube") ||
            embedHost.contains("vidstream") || embedHost.contains("vidplay")) {
            val stream = SgMega.resolveStream(playerUrl, "$baseUrl/") ?: return emptyList()
            return SgMega.emitLinks("AniSuge $serverName", stream, "https://$embedHost/")
        }
        val captured = HikariNet.resolveWithWebView(
            playerUrl, MEDIA_CAPTURE,
            timeoutMs = 45_000,
            script = """document.querySelector('button,[role="button"],.jw-icon-display,.vds-play-button')?.click();""",
        )
        return captured.map {
            HikariStream(
                name = "AniSuge $serverName",
                url = it.url,
                headers = it.headers.ifEmpty { mapOf("Referer" to playerUrl) },
                isM3u8 = it.url.contains(".m3u8"),
            )
        }
    }

    // -- mapper ---------------------------------------------------------------------
    suspend fun mapperSources(baseUrl: String, malId: String, slug: String, timestamp: String, sel: String): List<HikariStream> {
        val out = ArrayList<HikariStream>()
        val body = HikariNet.getStringSmart("$MAPPER_API$malId/$slug/$timestamp", mapOf("User-Agent" to UA2, "Accept" to "application/json")) ?: return out
        val root = try { JSONObject(body) } catch (t: Throwable) { return out }
        val keys = root.keys()
        while (keys.hasNext()) {
            val providerKey = keys.next()
            val prov = root.optJSONObject(providerKey) ?: continue
            val entry = prov.optJSONObject(if (sel == "dub") "dub" else "sub") ?: continue
            val display = when (providerKey.lowercase()) {
                "gogoanime" -> "Vidstream"
                "anivibe" -> "vibe-Stream"
                "animepahe" -> "Kiwi-Stream"
                else -> providerKey.replaceFirstChar { it.uppercase() }
            }
            val streamUrl = entry.optString("url").takeIf { it.isNotBlank() }
            if (streamUrl != null) {
                var embedUrl: String? = null
                try {
                    val infoBody = HikariNet.getStringSmart(
                        "$baseUrl/ajax/server?get=${URLEncoder.encode(streamUrl, "UTF-8")}",
                        ajaxHeaders("$baseUrl/watch/"),
                    )
                    if (infoBody != null) embedUrl = JSONObject(infoBody).optJSONObject("result")?.optString("url")?.takeIf { it.isNotBlank() }
                } catch (t: Throwable) { }
                if (embedUrl.isNullOrBlank()) embedUrl = streamUrl
                try { out.addAll(resolveEmbed(embedUrl, baseUrl, display)) } catch (t: Throwable) { }
            }
            val dl = entry.optJSONObject("download")
            if (dl != null) {
                val dk = dl.keys()
                while (dk.hasNext()) {
                    val q = dk.next()
                    val paheUrl = dl.optString(q)
                    if (!paheUrl.startsWith("http")) continue
                    val captured = HikariNet.resolveWithWebView(paheUrl, MEDIA_CAPTURE, timeoutMs = 45_000)
                    for (r in captured) {
                        out.add(HikariStream(
                            name = "AniSuge $display $q",
                            url = r.url,
                            headers = r.headers.ifEmpty { mapOf("Referer" to paheUrl) },
                            isM3u8 = r.url.contains(".m3u8"),
                        ))
                    }
                }
            }
        }
        return out
    }
}

// MegaPlay clones (ported from AniSugeResolver MegaPlayResolver/Cipher).
private object SgMega {
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
            val js = HikariNet.getString("$base/lib/newclient.min.js", mapOf("User-Agent" to UA))
            js?.let { keyPairRe.find(it)?.groupValues?.let { g -> (g[1] to g[2]).also { cachedSeeds = it } } }
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
            fileRe.find(plain)?.groupValues?.get(1)?.let { return it }
        }
        return null
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
                val subs = ArrayList<Pair<String, String>>()
                root.optJSONArray("tracks")?.let { arr ->
                    for (i in 0 until arr.length()) {
                        val e = arr.optJSONObject(i) ?: continue
                        val kind = e.optString("kind")
                        if (kind != "captions" && kind != "subtitles") continue
                        val f = e.optString("file")
                        if (f.isBlank()) continue
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
        if (!plain.isNullOrBlank()) return plain
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
        if (variants.isEmpty()) {
            return listOf(HikariStream(name = label, url = signedMaster, headers = playHeaders, subtitles = subs, isM3u8 = true))
        }
        return variants.map { v ->
            val suffix = v.second?.let { " $it" } ?: ""
            HikariStream(name = "$label$suffix", url = signUrl(v.first), headers = playHeaders, subtitles = subs, isM3u8 = true)
        }
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
