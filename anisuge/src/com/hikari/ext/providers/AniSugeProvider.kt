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
import org.json.JSONObject
import java.net.URLEncoder

/**
 * AniSuge (anisuge.tv) — anime sub & dub.
 * Ported from KSHITIJ8473/raghav AniSuge to native Hikari.
 * Home/search are HTML grids; episodes via ajax/episode/list with VRF token;
 * streams via ajax/server endpoints with WebView fallback for embeds.
 */
class AniSugeProvider : HikariProvider {
    override val id = "anisuge"
    override val name = "AniSuge"
    override val mainUrl = "https://anisuge.tv"
    override val description = "Anime from anisuge.tv — recently updated, recently added, just completed, most viewed, sub & dub."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    companion object {
        private const val DEFAULT_BASE = "https://anisuge.tv"
        private const val DOMAIN_URL = "https://cloudstreampluginhelper-default-rtdb.firebaseio.com/.json"
        private const val CACHE_TTL = 600_000L
        private val cache = HashMap<String, Pair<Long, String>>()
        private val mutex = Mutex()
        @Volatile private var base = DEFAULT_BASE
        @Volatile private var lastDomain = 0L
        private val UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        private val streamCapture = Regex("""https?://[^"'\\s]+?\.(?:m3u8|mp4)(?:[?#][^"'\\s]*)?""")
    }

    private suspend fun refreshDomain() {
        val now = System.currentTimeMillis()
        if (now - lastDomain < 5 * 60 * 1000L) return
        try {
            val txt = HikariNet.getString(DOMAIN_URL) ?: return
            val o = JSONObject(txt)
            val k = o.keys()
            while (k.hasNext()) {
                val key = k.next()
                if (key.equals("anisuge", true) || key.equals("anisuge_url", true)) {
                    val v = o.optString(key).trim().removeSuffix("/")
                    if (v.startsWith("http")) { base = v; break }
                }
            }
        } catch (_: Exception) {} finally { lastDomain = now }
    }

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("recent", "Recently Updated", HikariMediaType.SERIES),
        HikariCatalog("added", "Recently Added", HikariMediaType.SERIES),
        HikariCatalog("complete", "Just Completed", HikariMediaType.SERIES),
        HikariCatalog("mostview", "Most Viewed", HikariMediaType.SERIES)
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        if (page > 1) return emptyList()
        refreshDomain()
        val html = getCached("$base/home") ?: return emptyList()
        // cut to requested section when present
        val sectionHtml = when (catalog.id) {
            "recent" -> cutSection(html, "Recently Updated") ?: html
            "added" -> cutSection(html, "Recently Added") ?: ""
            "complete" -> cutSection(html, "Just Completed") ?: cutSection(html, "Complete") ?: ""
            "mostview" -> cutSection(html, "Most Viewed") ?: cutSection(html, "Mostview") ?: ""
            else -> html
        }
        if (sectionHtml.isBlank()) return emptyList()
        return parseItems(sectionHtml)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank() || page > 1) return emptyList()
        refreshDomain()
        val enc = URLEncoder.encode(query.trim(), "UTF-8")
        val html = getCached("$base/filter?keyword=$enc") ?: return emptyList()
        return parseItems(html)
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        refreshDomain()
        val url = media.id.takeIf { it.startsWith("http") } ?: return media
        val html = getCached(url) ?: return media
        val title = Regex("""<h1[^>]*class="[^"]*title[^"]*"[^>]*>([\s\S]*?)</h1>""").find(html)?.groupValues?.get(1)?.let { strip(it) }
            ?: Regex("""<meta[^>]+property="og:title"[^>]+content="([^"]+)"""").find(html)?.groupValues?.get(1)?.substringBefore(" Episode")?.trim()
            ?: media.title
        val poster = Regex("""<meta[^>]+property="og:image"[^>]+content="([^"]+)"""").find(html)?.groupValues?.get(1)?.takeIf { it.startsWith("http") } ?: media.posterUrl
        val plot = Regex("""<div[^>]*class="[^"]*description[^"]*"[^>]*>([\s\S]*?)</div>""").find(html)?.groupValues?.get(1)?.let { strip(it).replace("more+", "").trim() }
            ?: Regex("""<meta[^>]+property="og:description"[^>]+content="([^"]+)"""").find(html)?.groupValues?.get(1)
        val genres = Regex("""href="[^"]*/genre/[^"]*"[^>]*>([^<]+)</a>""").findAll(html).map { strip(it.groupValues[1]) }.filter { it.isNotBlank() }.toList()
        val year = Regex("""\b(19\d\d|20\d\d)\b""").find(html.substring(0, minOf(html.length, 20000)))?.groupValues?.get(1)?.toIntOrNull()
        return media.copy(title = title, posterUrl = poster ?: media.posterUrl, overview = plot ?: media.overview, genres = genres.ifEmpty { media.genres }, year = year ?: media.year)
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        refreshDomain()
        val url = media.id.takeIf { it.startsWith("http") } ?: return null
        val html = getCached(url) ?: return null
        val dataId = Regex("""class="[^"]*watch-wrap[^"]*"[^>]*data-id="(\d+)"""").find(html)?.groupValues?.get(1)
            ?: Regex("""mangaId\s*=\s*(\d+)""").find(html)?.groupValues?.get(1) ?: return null
        val vrf = generateVrf(dataId)
        val epsTxt = HikariNet.getString("$base/ajax/episode/list/$dataId?vrf=$vrf", mapOf("User-Agent" to UA, "Referer" to url, "X-Requested-With" to "XMLHttpRequest")) ?: return null
        val epsHtml = try { JSONObject(epsTxt).optString("result", "") } catch (_: Exception) { "" }
        if (epsHtml.isBlank()) return null
        val sub = ArrayList<HikariEpisode>()
        val dub = ArrayList<HikariEpisode>()
        for (m in Regex("""<a[^>]+data-ids="([^"]+)"[^>]*>([\s\S]*?)</a>""").findAll(epsHtml)) {
            val tag = m.value
            val dataIds = Regex("""data-ids="([^"]+)"""").find(tag)?.groupValues?.get(1) ?: continue
            val epNum = Regex("""data-slug="(\d+)"""").find(tag)?.groupValues?.get(1)?.toIntOrNull()
                ?: tag.substringAfter(">").substringBefore("<").trim().toIntOrNull()
                ?: Regex("""data-num="[^"]*?(\d+)""").find(tag)?.groupValues?.get(1)?.toIntOrNull() ?: continue
            val epTitle = Regex("""data-num="([^"]+)"""").find(tag)?.groupValues?.get(1)?.takeIf { it.isNotBlank() } ?: "Episode $epNum"
            val malId = Regex("""data-mal="([^"]*)"""").find(tag)?.groupValues?.get(1) ?: ""
            val slug = Regex("""data-slug="([^"]+)"""").find(tag)?.groupValues?.get(1)?.takeIf { it.isNotBlank() } ?: epNum.toString()
            val ts = Regex("""data-timestamp="([^"]*)"""").find(tag)?.groupValues?.get(1) ?: ""
            val hasSub = tag.contains("""data-sub="1"""")
            val hasDub = tag.contains("""data-dub="1"""")
            if (hasSub) sub.add(HikariEpisode(number = epNum, id = "$base|$dataId|$epNum|$dataIds|$malId|$slug|$ts|sub", name = epTitle))
            if (hasDub) dub.add(HikariEpisode(number = epNum, id = "$base|$dataId|$epNum|$dataIds|$malId|$slug|$ts|dub", name = epTitle))
        }
        return (sub + dub).ifEmpty { null }
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        refreshDomain()
        val raw = episode?.id ?: return emptyList()
        val parts = raw.split("|")
        if (parts.size < 8) return emptyList()
        val baseUrl = parts[0]
        val dataId = parts[1]
        val dataIds = parts[3]
        val audio = parts[7].ifBlank { "sub" }
        val referer = media.id.takeIf { it.startsWith("http") } ?: baseUrl
        // legacy server list
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        try {
            val vrf = generateVrf(dataIds)
            // server list endpoint mirrors site js (best effort)
            val srvTxt = HikariNet.getString("$baseUrl/ajax/server/list/$dataId?vrf=$vrf", mapOf("User-Agent" to UA, "Referer" to referer, "X-Requested-With" to "XMLHttpRequest"))
            if (srvTxt != null) {
                val srvHtml = try { JSONObject(srvTxt).optString("result", "") } catch (_: Exception) { "" }
                val ids = Regex("""data-link-id="([^"]+)"""").findAll(srvHtml).map { it.groupValues[1] }.distinct().toList()
                for (lid in ids) {
                    try {
                        val enc = URLEncoder.encode(lid, "UTF-8")
                        val infoTxt = HikariNet.getString("$baseUrl/ajax/server/$lid?vrf=${generateVrf(lid)}", mapOf("User-Agent" to UA, "Referer" to referer, "X-Requested-With" to "XMLHttpRequest")) ?: continue
                        val embed = try { JSONObject(infoTxt).optJSONObject("result")?.optString("url") ?: JSONObject(infoTxt).optString("result") } catch (_: Exception) { "" }
                        if (embed.isNullOrBlank() || !embed.startsWith("http")) continue
                        val hits = try { HikariNet.resolveWithWebView(embed, streamCapture, timeoutMs = 30_000) } catch (_: Throwable) { emptyList() }
                        for (h in hits) {
                            if (h.url.isBlank() || !seen.add(h.url)) continue
                            out.add(HikariStream(name = "AniSuge $audio", url = h.url, headers = h.headers, isM3u8 = h.url.contains(".m3u8", true)))
                        }
                        if (out.isNotEmpty() && out.size >= 4) break
                    } catch (_: Exception) {}
                }
            }
        } catch (_: Exception) {}
        // fallback: direct WebView on episode page for m3u8
        if (out.isEmpty()) {
            try {
                val hits = HikariNet.resolveWithWebView(referer, streamCapture, timeoutMs = 30_000)
                for (h in hits) {
                    if (h.url.isBlank() || !seen.add(h.url)) continue
                    out.add(HikariStream(name = "AniSuge $audio", url = h.url, headers = h.headers, isM3u8 = h.url.contains(".m3u8", true)))
                }
            } catch (_: Throwable) {}
        }
        // keep type label
        return out.map { it.copy(name = "${it.name} (${audio.uppercase()})") }
    }

    private suspend fun getCached(url: String): String? {
        val now = System.currentTimeMillis()
        mutex.withLock { cache[url]?.let { (t, h) -> if (now - t < CACHE_TTL) return h } }
        val html = HikariNet.getString(url, mapOf("User-Agent" to UA, "Referer" to "$base/")) ?: HikariNet.getStringSmart(url, mapOf("User-Agent" to UA))
        if (html == null) return null
        mutex.withLock { if (cache.size > 60) cache.clear(); cache[url] = now to html }
        return html
    }

    private fun parseItems(html: String): List<HikariMedia> {
        val out = LinkedHashMap<String, HikariMedia>()
        for (m in Regex("""<a[^>]+href="([^"]+)"[^>]*>([\s\S]{0,4000}?)(?:</a>)""").findAll(html)) {
            val href = m.groupValues[1]
            if (!href.contains("/watch/") && !href.contains("/anime/")) continue
            if (href.contains("/genre/") || href.contains("/filter")) continue
            val block = m.groupValues[2]
            if (!block.contains("<img") && !block.contains("class=\"name\"")) continue
            val full = when {
                href.startsWith("http") -> href
                href.startsWith("/") -> base + href
                else -> "$base/$href"
            }
            var title = Regex("""class="[^"]*name[^"]*"[^>]*>([^<]+)</""").find(block)?.groupValues?.get(1)?.trim()
            if (title.isNullOrBlank()) continue
            title = strip(title)
            val poster = Regex("""<img[^>]+(?:data-src|src)="([^"]+)"""").find(block)?.groupValues?.get(1)?.let {
                when {
                    it.startsWith("http") -> it
                    it.startsWith("//") -> "https:$it"
                    it.startsWith("/") -> base + it
                    else -> it
                }
            }
            out[full] = HikariMedia(id = full, title = title, type = HikariMediaType.SERIES, posterUrl = poster?.takeIf { it.startsWith("http") })
            if (out.size >= 60) break
        }
        return out.values.toList()
    }

    private fun cutSection(html: String, header: String): String? {
        val idx = html.indexOf(header)
        if (idx < 0) return null
        val start = html.lastIndexOf("<section", idx).takeIf { it >= 0 } ?: idx
        val end = html.indexOf("</section>", idx).takeIf { it >= 0 }?.plus(10) ?: (idx + 30000).coerceAtMost(html.length)
        return html.substring(start, end)
    }

    private fun strip(s: String): String = s.replace(Regex("""<[^>]+>"""), " ").replace("&amp;", "&").replace("&quot;", "\"").replace("&#039;", "'").replace("&nbsp;", " ").replace(Regex("""\s+"""), " ").trim()

    // VRF (ported from site js)
    private fun rc4(key: ByteArray, input: ByteArray): ByteArray {
        val s = IntArray(256) { it }
        var j = 0
        for (i in 0..255) {
            j = (j + s[i] + (key[i % key.size].toInt() and 0xFF)) and 0xFF
            val t = s[i]; s[i] = s[j]; s[j] = t
        }
        var i = 0; j = 0
        val res = ByteArray(input.size)
        for (x in input.indices) {
            i = (i + 1) and 0xFF; j = (j + s[i]) and 0xFF
            val t = s[i]; s[i] = s[j]; s[j] = t
            res[x] = ((input[x].toInt() and 0xFF) xor s[(s[i] + s[j]) and 0xFF]).toByte()
        }
        return res
    }
    private fun shiftCharcode(t: String): ByteArray {
        val r = ByteArray(t.length)
        for (idx in t.indices) {
            var s = t[idx].code
            when (idx % 8) {
                1 -> s += 3; 7 -> s += 5; 2 -> s -= 4; 4 -> s -= 2; 6 -> s += 4; 0 -> s -= 3; 3 -> s += 2; 5 -> s += 5
            }
            r[idx] = s.toByte()
        }
        return r
    }
    private fun rot13(s: String): String {
        val b = StringBuilder()
        for (c in s) {
            when (c) {
                in 'a'..'z' -> { var o = c.code + 13; if (o > 'z'.code) o -= 26; b.append(o.toChar()) }
                in 'A'..'Z' -> { var o = c.code + 13; if (o > 'Z'.code) o -= 26; b.append(o.toChar()) }
                else -> b.append(c)
            }
        }
        return b.toString()
    }
    private fun generateVrf(input: String): String {
        val enc = URLEncoder.encode(input, "UTF-8").replace("+", "%20")
        val rc4b = rc4("ysJhV6U27FVIjjuk".toByteArray(Charsets.UTF_8), enc.toByteArray(Charsets.UTF_8))
        val b64 = Base64.encodeToString(rc4b, Base64.URL_SAFE or Base64.NO_WRAP)
        val shifted = shiftCharcode(b64)
        return rot13(Base64.encodeToString(shifted, Base64.URL_SAFE or Base64.NO_WRAP))
    }
}
