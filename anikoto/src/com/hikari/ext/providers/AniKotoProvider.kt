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
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.json.JSONObject
import java.net.URLEncoder
import javax.crypto.Cipher
import javax.crypto.Mac
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec

/**
 * AniKoto (anikototv.to) — anime with sub & dub, ajax episode/server lists.
 * Ported from KSHITIJ8473/raghav AniKotoAnime to native Hikari (no CloudStream).
 */
class AniKotoProvider : HikariProvider {
    override val id = "anikoto"
    override val name = "AniKoto"
    override val mainUrl = "https://anikototv.to"
    override val description = "Anime from anikototv.to — latest updated, most viewed, ongoing, movies, sub & dub, direct HLS."
    override val version = 2
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    companion object {
        private const val DEFAULT_BASE = "https://anikototv.to"
        private const val DOMAIN_URL = "https://cloudstreampluginhelper-default-rtdb.firebaseio.com/.json"
        private const val CACHE_TTL = 600_000L
        private val htmlCache = HashMap<String, Pair<Long, String>>()
        private val cacheMutex = Mutex()
        @Volatile private var base = DEFAULT_BASE
        @Volatile private var lastDomainFetch = 0L

        private val browserHeaders = mapOf(
            "User-Agent" to "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36",
            "Accept" to "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language" to "en-US,en;q=0.5"
        )
        private fun ajaxHeaders(referer: String) = mapOf(
            "User-Agent" to (browserHeaders["User-Agent"] ?: ""),
            "X-Requested-With" to "XMLHttpRequest",
            "Accept" to "application/json, text/javascript, */*; q=0.01",
            "Referer" to referer
        )
        private val streamCapture = Regex("""https?://[^"'\\s]+?\.(?:m3u8|mp4)(?:[?#][^"'\\s]*)?""")
    }

    private suspend fun refreshDomain() {
        val now = System.currentTimeMillis()
        if (now - lastDomainFetch < 5 * 60 * 1000L) return
        try {
            val txt = HikariNet.getString(DOMAIN_URL) ?: return
            val obj = JSONObject(txt)
            val keys = obj.keys()
            while (keys.hasNext()) {
                val k = keys.next()
                if (k.equals("anikoto", true) || k.equals("anikoto_url", true) || k.equals("anikoto_domain", true)) {
                    val v = obj.optString(k).trim().removeSuffix("/")
                    if (v.startsWith("http")) { base = v; break }
                }
            }
        } catch (_: Exception) {} finally { lastDomainFetch = now }
    }

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("latest-updated", "Latest Updated", HikariMediaType.SERIES, rawType = "/latest-updated"),
        HikariCatalog("most-viewed", "Most Popular", HikariMediaType.SERIES, rawType = "/most-viewed"),
        HikariCatalog("ongoing", "Ongoing", HikariMediaType.SERIES, rawType = "/status/currently-airing"),
        HikariCatalog("tv", "TV Series", HikariMediaType.SERIES, rawType = "/type/tv"),
        HikariCatalog("ova", "OVA / Specials", HikariMediaType.SERIES, rawType = "/type/ova"),
        HikariCatalog("movies", "Movies", HikariMediaType.MOVIE, rawType = "/type/movie")
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        refreshDomain()
        val path = catalog.rawType.ifBlank { catalog.id }
        val url = "$base$path?page=$page"
        val html = getCached(url) ?: return emptyList()
        return parseCards(html)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank()) return emptyList()
        if (page > 1) return emptyList()
        refreshDomain()
        val enc = URLEncoder.encode(query.trim(), "UTF-8")
        val html = getCached("$base/filter?keyword=$enc", useSmart = true) ?: return emptyList()
        return parseCards(html)
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        refreshDomain()
        val url = media.id.takeIf { it.startsWith("http") } ?: return media
        val html = getCached(url) ?: return media
        val title = Regex("""<h1[^>]*class="[^"]*title[^"]*"[^>]*>([\s\S]*?)</h1>""").find(html)?.groupValues?.get(1)?.let { stripTags(it) }
            ?: Regex("""<h1[^>]*>([\s\S]*?)</h1>""").find(html)?.groupValues?.get(1)?.let { stripTags(it) } ?: media.title
        val poster = Regex("""<div[^>]*class="[^"]*poster[^"]*"[^>]*>[\s\S]*?<img[^>]+(?:data-src|src)="([^"]+)"""").find(html)?.groupValues?.get(1)
            ?: Regex("""<img[^>]+itemprop="image"[^>]+src="([^"]+)"""").find(html)?.groupValues?.get(1)
            ?: media.posterUrl
        val desc = Regex("""<div[^>]*class="[^"]*synopsis[^"]*"[^>]*>([\s\S]*?)</div>""").find(html)?.groupValues?.get(1)?.let { stripTags(it) }
        val genres = Regex("""href="[^"]*/genre/[^"]*"[^>]*>([^<]+)</a>""").findAll(html).map { stripTags(it.groupValues[1]) }.filter { it.isNotBlank() }.toList()
        val isMovie = html.contains("/type/movie") || Regex("""Movie""", RegexOption.IGNORE_CASE).find(html?.let { Regex("""<div[^>]*class="[^"]*bmeta[^"]*"[^>]*>([\s\S]*?)</div>""").find(it)?.groupValues?.get(1) ?: "" } ?: "") != null
        return media.copy(title = title, posterUrl = poster?.takeIf { it.startsWith("http") || it.startsWith("//") }?.let { if (it.startsWith("//")) "https:$it" else it } ?: media.posterUrl, overview = desc ?: media.overview, genres = genres.ifEmpty { media.genres }, type = if (isMovie) HikariMediaType.MOVIE else HikariMediaType.SERIES)
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        refreshDomain()
        val url = media.id.takeIf { it.startsWith("http") } ?: return null
        val html = getCached(url) ?: return null
        val animeId = Regex("""id="watch-main"[^>]*data-id="(\d+)"""").find(html)?.groupValues?.get(1)
            ?: Regex("""data-id=['"](\d+)['"]""").find(html)?.groupValues?.get(1)
        val sub = ArrayList<HikariEpisode>()
        val dub = ArrayList<HikariEpisode>()
        if (animeId != null) {
            try {
                val json = HikariNet.getString("$base/ajax/episode/list/$animeId", ajaxHeaders(url)) ?: ""
                val inner = jsonResultString(json)
                if (inner.isNotBlank()) {
                    for (m in Regex("""<a[^>]+data-ids="([^"]+)"[^>]*>([\s\S]*?)</a>""").findAll(inner)) {
                        val tag = m.value
                        val serverIds = Regex("""data-ids="([^"]+)"""").find(tag)?.groupValues?.get(1) ?: continue
                        if (serverIds.isBlank()) continue
                        val num = Regex("""data-num="(\d+)"""").find(tag)?.groupValues?.get(1)?.toIntOrNull()
                        val hasSub = tag.contains("""data-sub="1"""")
                        val hasDub = tag.contains("""data-dub="1"""")
                        var epName = Regex("""<span[^>]*class="[^"]*d-title[^"]*"[^>]*>([\s\S]*?)</span>""").find(tag)?.groupValues?.get(1)?.let { stripTags(it) }
                        if (epName.isNullOrBlank()) epName = Regex("""title="([^"]+)"""").find(tag)?.groupValues?.get(1)?.trim()
                        if (epName.isNullOrBlank()) epName = "Episode ${num ?: ""}"
                        if (hasSub || !hasDub) sub.add(HikariEpisode(number = num ?: (sub.size + 1), id = "anikoto|$url|$serverIds|sub", name = epName))
                        if (hasDub) dub.add(HikariEpisode(number = num ?: (dub.size + 1), id = "anikoto|$url|$serverIds|dub", name = epName))
                    }
                }
            } catch (_: Exception) {}
        }
        if (sub.isEmpty() && dub.isEmpty()) {
            val links = Regex("""<a[^>]+href="([^"]*?/ep-\d+[^"]*)"[^>]*>([^<]*)</a>""").findAll(html).toList()
            links.forEachIndexed { i, m ->
                val href = m.groupValues[1]
                val full = if (href.startsWith("http")) href else base + href
                val name = stripTags(m.groupValues[2]).ifBlank { "Episode ${i + 1}" }
                sub.add(HikariEpisode(number = i + 1, id = full, name = name))
            }
        }
        return (sub + dub).ifEmpty { null }
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        refreshDomain()
        val raw = episode?.id ?: return emptyList()
        if (raw.startsWith("anikoto|")) {
            val parts = raw.split("|", limit = 4)
            if (parts.size < 4) return emptyList()
            return resolveServers(parts[2], parts[1], parts[3])
        }
        // fallback: episode page -> try ajax list by ep number
        return try {
            val html = getCached(raw) ?: return emptyList()
            val animeId = Regex("""id="watch-main"[^>]*data-id="(\d+)"""").find(html)?.groupValues?.get(1)
                ?: Regex("""data-id=['"](\d+)['"]""").find(html)?.groupValues?.get(1) ?: return emptyList()
            val epNum = Regex("""/ep-(\d+)""").find(raw)?.groupValues?.get(1)?.toIntOrNull() ?: 1
            val json = HikariNet.getString("$base/ajax/episode/list/$animeId", ajaxHeaders(raw)) ?: return emptyList()
            val inner = jsonResultString(json)
            if (inner.isBlank()) return emptyList()
            val tags = Regex("""<a[^>]+data-ids="([^"]+)"[^>]*>""").findAll(inner).toList()
            val chosen = tags.firstOrNull { Regex("""data-num="(\d+)"""").find(it.value)?.groupValues?.get(1)?.toIntOrNull() == epNum } ?: tags.firstOrNull() ?: return emptyList()
            val serverIds = Regex("""data-ids="([^"]+)"""").find(chosen.value)?.groupValues?.get(1) ?: return emptyList()
            val audio = if (chosen.value.contains("""data-dub="1"""")) "dub" else "sub"
            resolveServers(serverIds, raw, audio)
        } catch (_: Exception) { emptyList() }
    }

    private suspend fun resolveServers(serverIds: String, referer: String, audioType: String): List<HikariStream> {
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        val enc = try { URLEncoder.encode(serverIds, "UTF-8") } catch (_: Exception) { serverIds }
        val listJson = HikariNet.getString("$base/ajax/server/list?servers=$enc", ajaxHeaders(referer)) ?: return emptyList()
        val listHtml = jsonResultString(listJson)
        if (listHtml.isBlank()) return emptyList()
        val sel = if (audioType == "dub") listOf("dub") else listOf("sub", "hsub")
        val entries = ArrayList<Pair<String, String>>()
        for (m in Regex("""<li[^>]+data-link-id="([^"]+)"[^>]*>([\s\S]*?)</li>""").findAll(listHtml)) {
            val block = m.value
            val isDubBlock = block.contains("data-type=\"dub\"") || listHtml.substring(0, m.range.first).substringAfterLast("data-type=\"").substringBefore("\"") == "dub"
            // simpler: accept all, prefer matching type
            val lid = m.groupValues[1]
            if (lid.isBlank()) continue
            val name = stripTags(m.groupValues[2]).ifBlank { "Server" }
            // filter by surrounding type div if present
            val before = listHtml.substring(0, m.range.first)
            val typeSeg = before.substringAfterLast("data-type=\"").substringBefore("\"")
            if (typeSeg.isNotEmpty() && !sel.any { it.equals(typeSeg, true) } && entries.isNotEmpty()) continue
            entries.add(lid to name)
        }
        val distinct = entries.distinctBy { it.first }.ifEmpty {
            Regex("""data-link-id="([^"]+)"""").findAll(listHtml).map { it.groupValues[1] to "Server" }.distinctBy { it.first }.toList()
        }
        for ((linkId, serverName) in distinct) {
            try {
                val encId = try { URLEncoder.encode(linkId, "UTF-8") } catch (_: Exception) { linkId }
                val sj = HikariNet.getString("$base/ajax/server?get=$encId", ajaxHeaders(referer)) ?: continue
                var embed = jsonResultUrl(sj) ?: continue
                if (embed.startsWith("//")) embed = "https:$embed"
                if (embed.startsWith("/")) embed = base + embed
                if (audioType == "dub" && embed.contains("/sub")) embed = embed.replace("/sub", "/dub")
                if (audioType == "sub" && embed.contains("/dub")) embed = embed.replace("/dub", "/sub")
                // hash m3u8 in fragment
                val hashM3u8 = getHashM3u8(embed)
                if (hashM3u8 != null && seen.add(hashM3u8)) {
                    out.add(HikariStream(name = "AniKoto $serverName", url = hashM3u8, headers = mapOf("Referer" to embed, "Origin" to "https://mewcdn.online"), isM3u8 = true))
                    continue
                }
                val host = Regex("""https?://([^/]+)""").find(embed)?.groupValues?.get(1) ?: ""
                val isMega = host.contains("megaplay", true) || host.contains("vidwish", true) || host.contains("vidtube", true)
                if (isMega) {
                    val mp = MegaPlay.resolve(embed, referer)
                    if (mp != null) {
                        val variants = MegaPlay.variants(mp.first, mapOf("Referer" to "https://$host/", "User-Agent" to (browserHeaders["User-Agent"] ?: "")))
                        val subs = mp.second.map { HikariSubtitle(it.first, it.second) }
                        if (variants.isEmpty()) {
                            if (seen.add(mp.first)) {
                                out.add(HikariStream(name = "AniKoto $serverName", url = mp.first, headers = mapOf("Referer" to "https://$host/", "User-Agent" to (browserHeaders["User-Agent"] ?: "")), subtitles = subs, isM3u8 = true))
                            }
                        } else {
                            for (v in variants) {
                                if (!seen.add(v.first)) continue
                                val suffix = v.second?.let { " $it" } ?: ""
                                out.add(HikariStream(name = "AniKoto $serverName$suffix", url = v.first, headers = mapOf("Referer" to "https://$host/", "User-Agent" to (browserHeaders["User-Agent"] ?: "")), subtitles = subs, isM3u8 = true))
                            }
                        }
                    }
                } else {
                    val hits = try { HikariNet.resolveWithWebView(embed, streamCapture, timeoutMs = 30_000) } catch (_: Throwable) { emptyList() }
                    for (h in hits) {
                        if (h.url.isBlank() || !seen.add(h.url)) continue
                        out.add(HikariStream(name = "AniKoto $serverName", url = h.url, headers = h.headers, isM3u8 = h.url.contains(".m3u8", true)))
                    }
                }
            } catch (_: Exception) {}
        }
        return out
    }

    private fun getHashM3u8(url: String): String? {
        val frag = url.substringAfter("#", "").substringBefore("#").takeIf { it.isNotBlank() } ?: return null
        return try {
            val dec = android.util.Base64.decode(frag, android.util.Base64.DEFAULT).toString(Charsets.UTF_8)
            proxyHost(dec).takeIf { it.startsWith("http") && it.contains(".m3u8") }
        } catch (_: Exception) { null }
    }

    private fun proxyHost(url: String): String {
        var u = url
        // mirrors serve the same files with permissive headers (full source list)
        val pairs = listOf(
            "vibeplayer.site" to "nanobyte.bigdreamsmalldih.site",
            "vault-01.uwucdn.top" to "uwu1.bigdreamsmalldih.site",
            "vault-02.uwucdn.top" to "uwu2.bigdreamsmalldih.site",
            "vault-03.uwucdn.top" to "uwu3.bigdreamsmalldih.site",
            "vault-04.uwucdn.top" to "uwu4.bigdreamsmalldih.site",
            "vault-05.uwucdn.top" to "uwu5.bigdreamsmalldih.site",
            "vault-06.uwucdn.top" to "uwu6.bigdreamsmalldih.site",
            "vault-07.uwucdn.top" to "uwu7.bigdreamsmalldih.site",
            "vault-08.uwucdn.top" to "uwu8.bigdreamsmalldih.site",
            "vault-09.uwucdn.top" to "uwu9.bigdreamsmalldih.site",
            "vault-10.uwucdn.top" to "uwu10.bigdreamsmalldih.site",
            "vault-11.uwucdn.top" to "uwu11.bigdreamsmalldih.site",
            "vault-12.uwucdn.top" to "uwu12.bigdreamsmalldih.site",
            "vault-13.uwucdn.top" to "uwu13.bigdreamsmalldih.site",
            "vault-14.uwucdn.top" to "uwu14.bigdreamsmalldih.site",
            "vault-15.uwucdn.top" to "uwu15.bigdreamsmalldih.site",
            "vault-16.uwucdn.top" to "uwu16.bigdreamsmalldih.site",
            "vault-99.uwucdn.top" to "uwu17.bigdreamsmalldih.site",
            "vault-10.owocdn.top" to "10.bigdreamsmalldih.site",
            "vault-11.owocdn.top" to "11.bigdreamsmalldih.site",
            "vault-12.owocdn.top" to "12.bigdreamsmalldih.site",
            "vault-13.owocdn.top" to "13.bigdreamsmalldih.site",
            "vault-14.owocdn.top" to "14.bigdreamsmalldih.site",
            "vault-15.owocdn.top" to "15.bigdreamsmalldih.site",
            "vault-16.owocdn.top" to "16.bigdreamsmalldih.site",
            "vault-99.owocdn.top" to "99.bigdreamsmalldih.site"
        )
        for ((a, b) in pairs) u = u.replace(a, b)
        return u
    }

    private fun jsonResultString(json: String): String {
        return try {
            val o = JSONObject(json)
            if (o.optInt("status", 0) != 200) "" else o.optString("result", "")
        } catch (_: Exception) { "" }
    }
    private fun jsonResultUrl(json: String): String? {
        return try {
            val o = JSONObject(json)
            if (o.optInt("status", 0) != 200) null else o.optJSONObject("result")?.optString("url")
        } catch (_: Exception) { null }
    }

    private suspend fun getCached(url: String, useSmart: Boolean = false): String? {
        val now = System.currentTimeMillis()
        cacheMutex.withLock { htmlCache[url]?.let { (t, h) -> if (now - t < CACHE_TTL) return h } }
        val html = if (useSmart) HikariNet.getStringSmart(url, browserHeaders) else HikariNet.getString(url, browserHeaders) ?: HikariNet.getStringSmart(url, browserHeaders)
        if (html == null) return null
        cacheMutex.withLock { if (htmlCache.size > 60) htmlCache.clear(); htmlCache[url] = now to html }
        return html
    }

    private fun parseCards(html: String): List<HikariMedia> {
        val out = LinkedHashMap<String, HikariMedia>()
        // split item blocks
        for (chunk in html.split(Regex("""<div[^>]*class="[^"]*item[^"]*"[^>]*>""")).drop(1)) {
            val block = chunk.substring(0, minOf(chunk.length, 8000))
            val href = Regex("""<a[^>]+href="([^"]*?/watch/[^"]*)"""").find(block)?.groupValues?.get(1)
                ?: Regex("""<a[^>]+href="([^"]+)"[^>]*>""").find(block)?.groupValues?.get(1) ?: continue
            if (href.isBlank()) continue
            val full = when {
                href.startsWith("http") -> href
                href.startsWith("/") -> base + href
                else -> "$base/$href"
            }
            var title = Regex("""<a[^>]*class="[^"]*name[^"]*"[^>]*>([\s\S]*?)</a>""").find(block)?.groupValues?.get(1)?.let { stripTags(it) }
            if (title.isNullOrBlank()) title = Regex("""title="([^"]+)"""").find(block)?.groupValues?.get(1)?.trim()
            if (title.isNullOrBlank()) continue
            val poster = Regex("""<img[^>]+(?:data-src|src)="([^"]+)"""").find(block)?.groupValues?.get(1)?.let {
                when {
                    it.startsWith("http") -> it
                    it.startsWith("//") -> "https:$it"
                    it.startsWith("/") -> base + it
                    else -> it
                }
            }
            val isMovie = block.contains("Movie", true)
            out[full] = HikariMedia(id = full, title = title, type = if (isMovie) HikariMediaType.MOVIE else HikariMediaType.SERIES, posterUrl = poster?.takeIf { it.startsWith("http") })
        }
        return out.values.toList()
    }

    private fun stripTags(s: String): String = s.replace(Regex("""<[^>]+>"""), " ").replace("&amp;", "&").replace("&quot;", "\"").replace("&#039;", "'").replace("&nbsp;", " ").replace(Regex("""\s+"""), " ").trim()
}

/** MegaPlay embed resolver (ported, HikariNet + org.json). */
private object MegaPlay {
    private const val UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    private const val FALLBACK_KEY = "i?LMTAx0Q6,:}50U"
    private const val FALLBACK_IV = "W0;27ToaUpl_P%'c"
    private const val TOKEN_KEY = "MpCdnT0k3n!9f2K#xQ7vL5mR8wN1pY4s"
    private const val TOKEN_LIFE = 7L * 24L * 60L * 60L

    suspend fun resolve(embedUrl: String, referer: String?): Pair<String, List<Pair<String, String>>>? {
        val host = Regex("""https?://([^/]+)""").find(embedUrl)?.groupValues?.get(1) ?: return null
        val page = HikariNet.getString(embedUrl, mapOf("User-Agent" to UA, "Referer" to (referer ?: "https://$host/"))) ?: return null
        val streamId = Regex("""data-id=['"](\d+)['"]""").find(page)?.groupValues?.get(1)
            ?: Regex("""data-realid=['"](\d+)['"]""").find(page)?.groupValues?.get(1)
            ?: Regex("""/stream/s-\d+/(\d+)/""").find(embedUrl)?.groupValues?.get(1) ?: return null
        val audio = Regex("""/(dub|sub)(?:[/?#]|$)""").find(embedUrl)?.groupValues?.get(1)
            ?: Regex("""type\s*:\s*['"](dub|sub)['"]""").find(page)?.groupValues?.get(1) ?: "sub"
        val alt = Regex("""data-domain=['"]([^"']+)['"]""").find(page)?.groupValues?.get(1)
        val hosts = listOfNotNull(host, alt?.takeIf { it != host }).distinct()
        for (h in hosts) {
            val b = "https://$h"
            val ah = mapOf("User-Agent" to UA, "Accept" to "*/*", "X-Requested-With" to "XMLHttpRequest", "Origin" to b, "Referer" to embedUrl)
            for (ep in listOf("getSourcesNew", "getSources")) {
                val txt = HikariNet.getString("$b/stream/$ep?id=$streamId&type=$audio", ah) ?: continue
                val root = runCatching { JSONObject(txt) }.getOrNull() ?: continue
                val url = extractStream(root, b) ?: continue
                return signUrl(url) to parseSubs(root)
            }
        }
        return null
    }

    private suspend fun extractStream(root: JSONObject, base: String): String? {
        if (root.has("sources")) {
            val s = root.opt("sources")
            if (s is JSONObject) s.optString("file").takeIf { it.isNotBlank() }?.let { return it }
            if (s is org.json.JSONArray && s.length() > 0) s.optJSONObject(0)?.optString("file")?.takeIf { it.isNotBlank() }?.let { return it }
        }
        val enc = root.optString("enc").takeIf { it.isNotBlank() } ?: return null
        return resolveEnc(enc, base)
    }

    private suspend fun resolveEnc(enc: String, base: String): String? {
        val seeds = mutableListOf<Pair<String, String>>()
        try {
            val js = HikariNet.getString("$base/lib/newclient.min.js")
            if (js != null) {
                Regex("""[A-Za-z]\w*="([^"]{16})",[A-Za-z]\w*="([^"]{16})"""").find(js)?.let { seeds.add(it.groupValues[1] to it.groupValues[2]) }
            }
        } catch (_: Exception) {}
        seeds.add(FALLBACK_KEY to FALLBACK_IV)
        for ((k, iv) in seeds) {
            decrypt(enc, k, iv)?.let { plain ->
                Regex(""""file"\s*:\s*"([^"]+)"""").find(plain)?.groupValues?.get(1)?.let { return it }
            }
        }
        return null
    }

    private fun decrypt(enc: String, kSeed: String, ivSeed: String): String? {
        return try {
            var b64 = enc.replace('-', '+').replace('_', '/')
            while (b64.length % 4 != 0) b64 += "="
            val cipherBytes = Base64.decode(b64, Base64.DEFAULT)
            val kb = ByteArray(32); System.arraycopy(kSeed.toByteArray(Charsets.UTF_8), 0, kb, 0, minOf(32, kSeed.length))
            val ivb = ByteArray(16); System.arraycopy(ivSeed.toByteArray(Charsets.UTF_8), 0, ivb, 0, minOf(16, ivSeed.length))
            val c = Cipher.getInstance("AES/CBC/PKCS5Padding")
            c.init(Cipher.DECRYPT_MODE, SecretKeySpec(kb, "AES"), IvParameterSpec(ivb))
            String(c.doFinal(cipherBytes), Charsets.UTF_8)
        } catch (_: Exception) { null }
    }

    private fun parseSubs(root: JSONObject): List<Pair<String, String>> {
        val out = ArrayList<Pair<String, String>>()
        val arr = root.optJSONArray("tracks") ?: return out
        for (i in 0 until arr.length()) {
            val o = arr.optJSONObject(i) ?: continue
            val kind = o.optString("kind")
            if (kind != "captions" && kind != "subtitles") continue
            val f = o.optString("file").takeIf { it.isNotBlank() } ?: continue
            out.add((o.optString("label").ifBlank { "English" }) to f)
        }
        return out
    }

    fun signUrl(url: String): String {
        val m = Regex("""/([a-f0-9]{32})/([a-f0-9]{32})/""", RegexOption.IGNORE_CASE).find(url) ?: return url
        val exp = System.currentTimeMillis() / 1000L + TOKEN_LIFE
        val payload = "$exp|${m.groupValues[1].lowercase()}/${m.groupValues[2].lowercase()}"
        return try {
            val mac = Mac.getInstance("HmacSHA256")
            mac.init(SecretKeySpec(TOKEN_KEY.toByteArray(Charsets.UTF_8), "HmacSHA256"))
            val sig = mac.doFinal(payload.toByteArray(Charsets.UTF_8))
            fun b64u(b: ByteArray) = Base64.encodeToString(b, Base64.URL_SAFE or Base64.NO_WRAP or Base64.NO_PADDING)
            val tok = "${b64u(payload.toByteArray(Charsets.UTF_8))}.${b64u(sig)}"
            url + (if (url.contains("?")) "&" else "?") + "token=$tok"
        } catch (_: Exception) { url }
    }

    suspend fun variants(m3u8: String, headers: Map<String, String>): List<Pair<String, Int?>> {
        val text = HikariNet.getString(signUrl(m3u8), headers) ?: return emptyList()
        if (!text.contains("#EXTM3U")) return emptyList()
        val base = m3u8.substringBefore('?').substringBeforeLast('/') + "/"
        val out = ArrayList<Pair<String, Int?>>()
        val lines = text.lines()
        var i = 0
        while (i < lines.size) {
            val line = lines[i].trim()
            if (line.startsWith("#EXT-X-STREAM-INF:")) {
                val q = Regex("""RESOLUTION=(\d+)x(\d+)""").find(line)?.groupValues?.get(2)?.toIntOrNull()
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
        return out.map { (u, q) -> signUrl(u) to q }
    }
}
