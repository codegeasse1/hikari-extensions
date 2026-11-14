package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import android.util.Base64
import java.net.URLEncoder
import org.json.JSONArray
import org.json.JSONObject

/**
 * RareAnimesIndia / Rare Toons India (rareanimes.mov) — Hindi-dubbed anime.
 *
 * Native port of KSHITIJ8473/raghav RareAnimesIndia: 6 category rows
 * (/hindi/category/<slug>/, verified live), article cards (herald thumbnail
 * anchor + img, verified against the live markup), /search/<term>/ search,
 * entry-content episodes (Episode N headers, codedew zipper/zipcloud buttons,
 * store archives, hub season/movie sub-pages), and the full resolver fleet —
 * argon embeds (JuicyCodes, verbatim port), argon downloads, streambeta,
 * hubcloud, gofile, pixeldrain, direct files — with WebView capture fallback.
 * CF-walled hosts go through the rendered fetch.
 */
class RareAnimesIndiaProvider : HikariProvider {

    override val id = "rareanimesindia"
    override val name = "Rare Toons India"
    override var mainUrl = "https://www.rareanimes.mov"
    override val description = "Hindi-dubbed anime direct downloads from Rare Toons India."
    override val version = 2
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private companion object {
        const val FB_RTDB = "https://cloudstreampluginhelper-default-rtdb.firebaseio.com/.json"
        const val STORE_HOST = "store.animetoonhindi.com"
        const val CODEDEW_HOST = "codedew.com"
        const val ARGON_HOST = "argon.razorshell.space"
        const val HUBCLOUD_HOST = "hubcloud.ist"
        const val PIXELDRAIN_HOST = "pixeldrain.net"
        const val UA =
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        val EPISODE_HEADER = Regex("""^(?:Episode|EP)\s*[.\-]?\s*(\d{1,3})\b""", RegexOption.IGNORE_CASE)
        val SEASON_MARKER = Regex("""^\s*Seasons?\s*[-–:.]?\s*(\d{1,2})\s*$""", RegexOption.IGNORE_CASE)
        val HUB_SEASON = Regex("""\bSeasons?\s*[-–:.]?\s*(\d{1,2})""", RegexOption.IGNORE_CASE)
        val HUB_MOVIE = Regex("""\bMovie\s*[-–:.]?\s*(\d{1,3})""", RegexOption.IGNORE_CASE)
        val CATS = listOf(
            "hindi-dub" to "Hindi Dub",
            "cartoon-network" to "Cartoon Network",
            "disney-xd" to "Disney XD",
            "hungama" to "Hungama",
            "marvel-hq" to "Marvel HQ",
            "pokemon" to "Pokemon",
        )
    }

    private val pageHeaders = mapOf(
        "User-Agent" to UA,
        "Accept" to "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer" to "https://www.rareanimes.mov/",
    )
    private val capture = Regex("""https?://[^"' \s<>]+\.m3u8[^"' \s<>]*|https?://[^"' \s<>]+\.mp4[^"' \s<>]*|https?://[^"' \s<>]+\.mkv[^"' \s<>]*""")

    private suspend fun refreshDomain() {
        try {
            val o = JSONObject(HikariNet.getString(FB_RTDB) ?: return)
            val d = o.optString("rareanimes").ifBlank { o.optString("rareanimes_url") }
                .ifBlank { o.optString("rareanimes_domain") }
            if (d.isNotBlank()) mainUrl = d.trimEnd('/')
        } catch (_: Exception) {
        }
    }

    private suspend fun get(url: String, headers: Map<String, String> = pageHeaders): String? =
        HikariNet.getStringSmart(url, headers) ?: HikariNet.getString(url, headers)

    // ------------------------------------------------------------------ rows

    override fun catalogs(): List<HikariCatalog> =
        CATS.map { (slug, name) -> HikariCatalog("cat:$slug", name, HikariMediaType.SERIES) }

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        refreshDomain()
        val slug = catalog.id.removePrefix("cat:")
        val p = page.coerceAtLeast(1)
        val url = if (p <= 1) "$mainUrl/hindi/category/$slug/"
        else "$mainUrl/hindi/category/$slug/page/$p/"
        val html = get(url) ?: return emptyList()
        return parseCards(html)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank()) return emptyList()
        refreshDomain()
        val q = URLEncoder.encode(query.trim(), "UTF-8")
        val p = page.coerceAtLeast(1)
        val urls = listOf(
            if (p <= 1) "$mainUrl/search/$q/" else "$mainUrl/page/$p/?s=$q",
            "$mainUrl/?s=$q",
        )
        for (u in urls) {
            val html = get(u) ?: continue
            val cards = parseCards(html)
            if (cards.isNotEmpty()) return cards
        }
        return emptyList()
    }

    private fun parseCards(html: String): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        val seen = HashSet<String>()
        val host = mainUrl.replace("https://", "").replace("http://", "")
        // Article-scoped first (herald thumbnail markup, verified live).
        for (a in Regex("""<article[\s\S]*?</article>""", RegexOption.IGNORE_CASE).findAll(html)) {
            val block = a.value
            val link = Regex("""<a[^>]+href="([^"]+)"[^>]*>""", RegexOption.IGNORE_CASE).find(block)
                ?.groupValues?.get(1) ?: continue
            if (!link.contains(host) && !link.startsWith(mainUrl)) continue
            val title = Regex("""<a[^>]+title="([^"]+)"""", RegexOption.IGNORE_CASE).find(block)
                ?.groupValues?.get(1)?.let { unesc(it) }
                ?: Regex("""alt="([^"]+)"""").find(block)?.groupValues?.get(1)?.let { unesc(it) }
                ?: continue
            if (title.length < 3) continue
            val img = Regex("""<img[^>]+src="([^"]+)"""", RegexOption.IGNORE_CASE).find(block)
                ?.groupValues?.get(1)?.takeIf { it.startsWith("http") }
            if (!seen.add(link)) continue
            out.add(HikariMedia(id = link, title = title, type = HikariMediaType.SERIES, posterUrl = img))
            if (out.size >= 60) return out
        }
        if (out.isNotEmpty()) return out
        // Generic anchor+img fallback.
        for (m in Regex("""<a[^>]+href="([^"]+)"[^>]*>([\s\S]*?)</a>""", RegexOption.IGNORE_CASE).findAll(html)) {
            val href = m.groupValues[1]
            if (href.startsWith("#") || href.contains("?s=")) continue
            val inner = m.groupValues[2]
            if (!inner.contains("<img")) continue
            val title = Regex("""alt="([^"]+)"""").find(inner)?.groupValues?.get(1)?.let { unesc(it) }
                ?.takeIf { it.length > 2 } ?: continue
            val img = Regex("""(?:data-src|src)="([^"]+)"""", RegexOption.IGNORE_CASE).find(inner)
                ?.groupValues?.get(1)?.takeIf { it.startsWith("http") }
            val url = abs(href)
            if (!url.contains(host)) continue
            if (!seen.add(url)) continue
            out.add(HikariMedia(id = url, title = title, type = HikariMediaType.SERIES, posterUrl = img))
            if (out.size >= 60) break
        }
        return out
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val html = get(media.id) ?: return media
        val title = Regex("""<meta[^>]+property="og:title"[^>]+content="([^"]+)"""").find(html)
            ?.groupValues?.get(1)?.let { unesc(it) } ?: media.title
        val poster = Regex("""<meta[^>]+property="og:image"[^>]+content="([^"]+)"""").find(html)
            ?.groupValues?.get(1)?.takeIf { it.startsWith("http") } ?: media.posterUrl
        val paras = Regex("""<div[^>]*class="[^"]*entry-content[^"]*"[^>]*>([\s\S]*?)</div>\s*(?:</div>)?""", RegexOption.IGNORE_CASE)
            .find(html)?.groupValues?.get(1)
            ?.let { Regex("""<p[^>]*>([\s\S]*?)</p>""", RegexOption.IGNORE_CASE).findAll(it).map { p -> unesc(strip(p.groupValues[1])) }.toList() }
            .orEmpty()
        val plot = paras.firstOrNull {
            it.startsWith("Synopsis", true) || it.startsWith("Storyline", true) || it.startsWith("Story:", true)
        }?.substringAfter(":")?.trim()?.takeIf { it.length > 10 }
            ?: paras.filter { it.length > 120 }.maxByOrNull { it.length }
        val genres = Regex("""<a[^>]+href="[^"]*category[^"]*"[^>]*>([^<]+)</a>""", RegexOption.IGNORE_CASE)
            .findAll(html).map { unesc(it.groupValues[1]) }.filter { it.isNotBlank() }.distinct().take(8).toList()
        return media.copy(
            title = title, posterUrl = poster, backdropUrl = poster,
            overview = plot ?: media.overview, genres = genres.ifEmpty { media.genres },
        )
    }

    // --------------------------------------------------------------- episodes

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val html = get(media.id) ?: return null
        val content = Regex("""<div[^>]*class="[^"]*entry-content[^"]*"[^>]*>([\s\S]*)</body>""", RegexOption.IGNORE_CASE)
            .find(html)?.groupValues?.get(1)
            ?: Regex("""<div[^>]*class="[^"]*entry-content[^"]*"[^>]*>([\s\S]*?)(?:<div[^>]*class="[^"]*(?:comments|related)[^"]*")""", RegexOption.IGNORE_CASE)
                .find(html)?.groupValues?.get(1)
            ?: html
        // 1) Hub pages: h2/h3/h4 headings linking internal season/movie posts.
        val hub = parseHub(content)
        if (hub.count { it.kind == "season" || it.kind == "movie" } >= 2) {
            return hub.mapIndexed { idx, h ->
                val num = h.num ?: (idx + 1)
                val eid = "rai:sub:" + Base64.encodeToString(h.url.toByteArray(), Base64.URL_SAFE or Base64.NO_WRAP)
                HikariEpisode(number = num, id = eid, name = h.text, image = media.posterUrl)
            }.sortedBy { it.number }
        }
        // 2) Direct episode sections with codedew/archive/file buttons.
        val out = ArrayList<HikariEpisode>()
        var current = -1
        var season = 1
        val pending = LinkedHashMap<Int, MutableList<Pair<String, String>>>()
        val order = ArrayList<Int>()
        val blocks = Regex("""<(p|div|li|h\d)[^>]*>([\s\S]*?)</\1>""", RegexOption.IGNORE_CASE)
            .findAll(content).map { strip(it.groupValues[2]) to it.groupValues[2] }.toList()
        for ((text, raw) in blocks) {
            if (text.isBlank()) continue
            SEASON_MARKER.find(text)?.let { season = it.groupValues[1].toIntOrNull() ?: season }
            val h = EPISODE_HEADER.find(text)
            if (h != null) {
                current = h.groupValues[1].toIntOrNull() ?: current
                if (current > 0 && !pending.containsKey(current)) {
                    pending[current] = ArrayList()
                    order.add(current)
                }
            }
            for (a in Regex("""<a[^>]+href="([^"]+)"[^>]*>([\s\S]*?)</a>""", RegexOption.IGNORE_CASE).findAll(raw)) {
                val href = a.groupValues[1]
                val label = unesc(strip(a.groupValues[2]))
                if (label.isBlank()) continue
                val isBtn = href.contains("$CODEDEW_HOST/zipper/") || href.contains("$CODEDEW_HOST/zipcloud/") ||
                    href.contains("$STORE_HOST/archives/") || href.endsWith(".mp4") ||
                    href.endsWith(".mkv") || label.contains("download", true) ||
                    label.contains("720p") || label.contains("1080p") || label.contains("480p")
                if (!isBtn) continue
                val ep = if (current > 0) current else -1
                if (ep > 0) pending.getOrPut(ep) {
                    order.add(ep)
                    ArrayList()
                }.add(label to href)
                else pending.getOrPut(-order.size - 1000) {
                    order.add(-order.size - 1000)
                    ArrayList()
                }.add(label to href)
            }
        }
        var n = 0
        for (key in order.distinct()) {
            val vars = pending[key] ?: continue
            if (vars.isEmpty()) continue
            n++
            val num = if (key > 0 && key < 1000) key else n
            val payload = JSONObject()
            val arr = JSONArray()
            for ((label, u) in vars.distinctBy { it.second }) {
                arr.put(JSONObject().put("n", label).put("u", u))
            }
            payload.put("v", arr)
            val eid = "rai:ep:" + Base64.encodeToString(payload.toString().toByteArray(), Base64.URL_SAFE or Base64.NO_WRAP)
            out.add(HikariEpisode(number = num, season = season, id = eid, name = "Episode $num", image = media.posterUrl))
        }
        if (out.isNotEmpty()) return out.sortedWith(compareBy({ it.season }, { it.number }))
        // 3) Any file link at all → one entry per file.
        val files = Regex("""<a[^>]+href="([^"]+\.(?:mp4|mkv))"[^>]*>([\s\S]*?)</a>""", RegexOption.IGNORE_CASE)
            .findAll(content).map {
                it.groupValues[1] to unesc(strip(it.groupValues[2])).ifBlank { "Download" }
            }.distinctBy { it.first }.toList()
        if (files.isNotEmpty()) {
            return files.mapIndexed { idx, (u, label) ->
                val payload = JSONObject().put("v", JSONArray().put(JSONObject().put("n", label).put("u", u)))
                val eid = "rai:ep:" + Base64.encodeToString(payload.toString().toByteArray(), Base64.URL_SAFE or Base64.NO_WRAP)
                HikariEpisode(number = idx + 1, id = eid, name = "Episode ${idx + 1} — $label", image = media.posterUrl)
            }
        }
        return null
    }

    private data class HubLink(val kind: String, val num: Int?, val text: String, val url: String)

    private fun parseHub(content: String): List<HubLink> {
        val out = ArrayList<HubLink>()
        val seen = HashSet<String>()
        val host = mainUrl.replace("https://", "").replace("http://", "")
        for (h in Regex("""<h[2-4][^>]*>([\s\S]*?)</h[2-4]>""", RegexOption.IGNORE_CASE).findAll(content)) {
            val inner = h.value
            val a = Regex("""<a[^>]+href="([^"]+)"[^>]*>([\s\S]*?)</a>""", RegexOption.IGNORE_CASE).find(inner)
                ?: continue
            val href = a.groupValues[1]
            if (!href.contains(host)) continue
            if (!seen.add(href)) continue
            val linkText = unesc(strip(a.groupValues[2])).ifBlank { unesc(strip(inner)) }
            if (linkText.isBlank()) continue
            val sn = HUB_SEASON.find(linkText)?.groupValues?.get(1)?.toIntOrNull()
                ?: HUB_SEASON.find(unesc(strip(inner)))?.groupValues?.get(1)?.toIntOrNull()
            val mn = if (sn == null) {
                HUB_MOVIE.find(linkText)?.groupValues?.get(1)?.toIntOrNull()
            } else null
            val kind = when {
                sn != null -> "season"
                mn != null -> "movie"
                else -> "extra"
            }
            out.add(HubLink(kind, sn ?: mn, linkText, href))
        }
        return out
    }

    // ---------------------------------------------------------------- streams

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val ref = episode?.id ?: return emptyList()
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        try {
            if (ref.startsWith("rai:sub:")) {
                val subUrl = String(Base64.decode(ref.removePrefix("rai:sub:"), Base64.URL_SAFE))
                val html = get(subUrl) ?: return emptyList()
                val vars = collectVariants(html)
                for ((label, u) in vars) resolveTarget(u, label, out, seen, 0)
            } else if (ref.startsWith("rai:ep:")) {
                val payload = JSONObject(String(Base64.decode(ref.removePrefix("rai:ep:"), Base64.URL_SAFE)))
                val arr = payload.optJSONArray("v") ?: return emptyList()
                for (i in 0 until arr.length()) {
                    val o = arr.optJSONObject(i) ?: continue
                    resolveTarget(o.optString("u"), o.optString("n", "RAI"), out, seen, 0)
                }
            } else if (ref.startsWith("rai:")) {
                // Legacy ids: direct file URLs joined with commas.
                for (u in ref.removePrefix("rai:").split(",")) {
                    if (u.startsWith("http") && (u.contains(".mp4") || u.contains(".mkv") || u.contains(".m3u8"))) {
                        if (!seen.add(u)) continue
                        out.add(HikariStream(name = labelFor(u), url = u, isM3u8 = u.contains("m3u8")))
                    }
                }
                if (out.isNotEmpty()) return out
            }
        } catch (_: Exception) {
        }
        return out
    }

    private fun collectVariants(html: String): List<Pair<String, String>> {
        val vars = ArrayList<Pair<String, String>>()
        val seen = HashSet<String>()
        for (a in Regex("""<a[^>]+href="([^"]+)"[^>]*>([\s\S]*?)</a>""", RegexOption.IGNORE_CASE).findAll(html)) {
            val href = a.groupValues[1]
            val label = unesc(strip(a.groupValues[2]))
            if (label.isBlank() || !seen.add(href)) continue
            if (href.contains("$CODEDEW_HOST/zipper/") || href.contains("$CODEDEW_HOST/zipcloud/") ||
                href.contains("$STORE_HOST/archives/") || href.endsWith(".mp4") || href.endsWith(".mkv")
            ) vars.add(label to href)
        }
        return vars
    }

    private suspend fun resolveTarget(url: String, label: String, out: MutableList<HikariStream>, seen: HashSet<String>, depth: Int) {
        if (depth > 3 || url.isBlank()) return
        try {
            when {
                url.contains("$ARGON_HOST/embed/") -> resolveArgon(url.substringAfter("/embed/").substringBefore("/").substringBefore("?"), label, out, seen)
                url.contains("$ARGON_HOST/downlead/") || url.contains("$ARGON_HOST/download/") ->
                    resolveArgonDownload(url.substringAfter("/downlead/").substringAfter("/download/").substringBefore("/").substringBefore("?"), label, out, seen)
                url.contains("$CODEDEW_HOST/streambeta") -> resolveStreamBeta(url, label, out, seen)
                url.contains("pixeldrain") -> {
                    pixelDrainId(url)?.let { id ->
                        val api = "https://$PIXELDRAIN_HOST/api/file/$id"
                        if (seen.add(api)) out.add(HikariStream(name = "PixelDrain [$label]", url = api))
                    }
                }
                url.contains("$HUBCLOUD_HOST/drive/") ->
                    resolveHubCloud(url.substringAfter("/drive/").substringBefore("/").substringBefore("?"), label, out, seen)
                url.contains("gofile.io") || url.contains("gofile/") ->
                    resolveGoFile(url.substringAfterLast("/").substringBefore("?"), label, out, seen)
                url.contains("$STORE_HOST/archives/") || (url.contains(mainUrl.replace("https://", "")) && !url.endsWith(".mp4") && !url.endsWith(".mkv")) -> {
                    // Archive / internal page: take up to 3 episode links and recurse.
                    val html = get(url) ?: return
                    parseArchiveLinks(html).take(3).forEach { (_, epUrl) ->
                        resolveCodedewOrDirect(epUrl, label, out, seen, depth + 1)
                    }
                }
                url.contains("$CODEDEW_HOST/zipper/") || url.contains("$CODEDEW_HOST/zipcloud/") ->
                    resolveCodedewOrDirect(url, label, out, seen, depth)
                url.endsWith(".mp4") || url.endsWith(".mkv") || url.contains(".m3u8") -> {
                    if (seen.add(url)) out.add(HikariStream(name = "RAI $label", url = url, isM3u8 = url.contains("m3u8")))
                }
                else -> {
                    // Unknown page: WebView capture for direct files/manifests.
                    for (r in HikariNet.resolveWithWebView(url, capture, timeoutMs = 45_000)) {
                        if (!seen.add(r.url)) continue
                        out.add(HikariStream(name = "RAI $label", url = r.url, headers = r.headers, isM3u8 = r.url.contains("m3u8")))
                    }
                }
            }
        } catch (_: Exception) {
        }
    }

    private suspend fun resolveCodedewOrDirect(url: String, label: String, out: MutableList<HikariStream>, seen: HashSet<String>, depth: Int) {
        try {
            val html = get(url, pageHeaders + ("Referer" to "https://$CODEDEW_HOST/")) ?: return
            // Codedew zip pages land on argon/hubcloud/pixeldrain/direct targets.
            val targets = LinkedHashSet<String>()
            for (m in Regex("""https?://[^"' \s<>]+""").findAll(html)) {
                val u = m.value
                if (u.contains(ARGON_HOST) || u.contains(HUBCLOUD_HOST) || u.contains("pixeldrain") ||
                    u.contains("gofile") || u.endsWith(".mp4") || u.endsWith(".mkv") || u.contains(".m3u8")
                ) targets.add(u)
            }
            // Meta-refresh / JS redirect.
            Regex("""(?:url=|window\.location[^"']*?["'])(https?://[^"' \s<>]+)""", RegexOption.IGNORE_CASE)
                .find(html)?.let { targets.add(it.groupValues[1]) }
            if (targets.isEmpty()) {
                for (r in HikariNet.resolveWithWebView(url, capture, timeoutMs = 45_000)) {
                    if (!seen.add(r.url)) continue
                    out.add(HikariStream(name = "RAI $label", url = r.url, headers = r.headers, isM3u8 = r.url.contains("m3u8")))
                }
                return
            }
            for (t in targets.take(4)) resolveTarget(t, label, out, seen, depth + 1)
        } catch (_: Exception) {
        }
    }

    private suspend fun resolveArgon(code: String, label: String, out: MutableList<HikariStream>, seen: HashSet<String>) {
        try {
            val embed = get(
                "https://$ARGON_HOST/embed/$code",
                pageHeaders + mapOf("Referer" to "https://$CODEDEW_HOST/", "Accept" to "text/html,*/*;q=0.8"),
            ) ?: return
            val config = RaiJuicy.decodeFromHtml(embed)
            val m3u8 = config?.let {
                Regex(""""file"\s*:\s*"([^"]*\.m3u8)"""").find(it)?.groupValues?.get(1)
            }?.replace("\\/", "/")
            if (!m3u8.isNullOrBlank() && m3u8.startsWith("http") && seen.add(m3u8)) {
                // The config file is an HLS playlist — direct link plays it.
                out.add(HikariStream(name = "Argon HLS [$label]", url = m3u8, isM3u8 = true))
            }
            // Quality download links inside the decoded config.
            if (config != null) {
                for (m in Regex(""""link"\s*:\s*"(https?://[^"]+)"""").findAll(config)) {
                    val u = m.groupValues[1].replace("\\/", "/")
                    if (!u.startsWith("http") || !seen.add(u)) continue
                    out.add(HikariStream(name = "Argon ${labelFor(u)} [$label]", url = u, isM3u8 = u.contains("m3u8")))
                }
            }
            if (out.isEmpty()) resolveArgonDownload(code, label, out, seen)
        } catch (_: Exception) {
        }
    }

    private suspend fun resolveArgonDownload(code: String, label: String, out: MutableList<HikariStream>, seen: HashSet<String>) {
        try {
            val embedUrl = "https://$ARGON_HOST/embed/$code"
            val dl = get(
                "https://$ARGON_HOST/downlead/$code/",
                pageHeaders + ("Referer" to embedUrl),
            ) ?: get("https://$ARGON_HOST/download/$code/", pageHeaders + ("Referer" to embedUrl)) ?: return
            for (m in Regex(""""(?:link|file|url)"\s*:\s*"(https?://[^"]+)"""").findAll(dl)) {
                val u = m.groupValues[1].replace("\\/", "/")
                if (!u.startsWith("http") || !seen.add(u)) continue
                if (u.contains("favicon") || u.endsWith(".png") || u.endsWith(".jpg")) continue
                out.add(HikariStream(name = "Argon ${labelFor(u)} [$label]", url = u, isM3u8 = u.contains("m3u8")))
            }
            for (m in Regex("""<a[^>]+href="([^"]+)"[^>]*>([\s\S]{0,60}?)</a>""", RegexOption.IGNORE_CASE).findAll(dl)) {
                val u = m.groupValues[1]
                if (!u.startsWith("http") || !seen.add(u)) continue
                if (!(u.endsWith(".mp4") || u.endsWith(".mkv") || u.contains(".m3u8"))) continue
                out.add(HikariStream(name = "Argon ${labelFor(u)} [$label]", url = u, isM3u8 = u.contains("m3u8")))
            }
        } catch (_: Exception) {
        }
    }

    private suspend fun resolveStreamBeta(pageUrl: String, label: String, out: MutableList<HikariStream>, seen: HashSet<String>) {
        try {
            val id = Regex("[?&]url=([^&]+)").find(pageUrl)?.groupValues?.get(1) ?: pageUrl
            val html = get("https://$CODEDEW_HOST/streambeta/?url=" + URLEncoder.encode(id, "UTF-8")) ?: return
            val arr = Regex("""playerSources\s*=\s*(\[.*?\])\s*;""", RegexOption.DOT_MATCHES_ALL)
                .find(html)?.groupValues?.get(1) ?: return
            val arrJson = try {
                JSONArray(arr)
            } catch (_: Exception) {
                return
            }
            for (i in 0 until arrJson.length()) {
                val o = arrJson.optJSONObject(i) ?: continue
                val name = o.optString("name", "Server")
                val stream = o.optString("streamUrl").takeIf { it.startsWith("http") }
                if (stream != null && seen.add(stream)) {
                    out.add(HikariStream(name = "WatchNow $name [$label]", url = stream, isM3u8 = stream.contains("m3u8")))
                }
                val dl = o.optString("url").takeIf { it.startsWith("http") } ?: continue
                val pd = if (dl.contains("pixeldra")) pixelDrainId(dl) else null
                val target = if (pd != null) "https://$PIXELDRAIN_HOST/api/file/$pd" else dl
                if (seen.add(target)) out.add(HikariStream(name = "WatchNow $name [$label]", url = target))
            }
        } catch (_: Exception) {
        }
    }

    private suspend fun resolveHubCloud(id: String, label: String, out: MutableList<HikariStream>, seen: HashSet<String>) {
        try {
            val page = get("https://$HUBCLOUD_HOST/drive/$id") ?: return
            val resolver = Regex("""var\s+url\s*=\s*'([^']+)'""").find(page)?.groupValues?.get(1)
                ?: return
            val body = get(resolver, pageHeaders + ("Referer" to "https://$HUBCLOUD_HOST/")) ?: return
            Regex("""<a[^>]*href="(https?://[^"]+)"[^>]*id="fsl"""").find(body)?.let { m ->
                val fsl = unesc(m.groupValues[1])
                if (fsl.startsWith("http") && !fsl.contains("hubcloud.cx/favicon") && seen.add(fsl)) {
                    out.add(HikariStream(name = "HubCloud FSL [$label]", url = fsl))
                }
            }
            if (out.isEmpty()) {
                Regex("""href="(https?://[^"]*cloudflarestorage\.com/[^"]+)"""").find(body)?.let { m ->
                    val u = unesc(m.groupValues[1])
                    if (seen.add(u)) out.add(HikariStream(name = "HubCloud FSL [$label]", url = u))
                }
            }
            Regex("""https://pixel\.hubcloud\.ist/\?id=[^"' \s<>]+""").find(body)?.let { pm ->
                var hop: String? = pm.value
                for (h in 0 until 3) {
                    val cur = hop ?: break
                    val b2 = get(cur, pageHeaders + ("Referer" to "https://$HUBCLOUD_HOST/")) ?: break
                    val pd = pixelDrainId(b2)
                    if (pd != null) {
                        val api = "https://$PIXELDRAIN_HOST/api/file/$pd"
                        if (seen.add(api)) out.add(HikariStream(name = "HubCloud Pixel [$label]", url = api))
                        break
                    }
                    hop = Regex("""https?://[^"' \s<>]+""").findAll(b2)
                        .map { it.value }.firstOrNull { it.contains("pixel") || it.endsWith(".mp4") || it.endsWith(".mkv") }
                    if (hop == null) break
                }
            }
        } catch (_: Exception) {
        }
    }

    private suspend fun resolveGoFile(code: String, label: String, out: MutableList<HikariStream>, seen: HashSet<String>) {
        try {
            val acc = HikariNet.getJson(
                "https://api.gofile.io/accounts",
                mapOf("User-Agent" to UA, "Origin" to "https://gofile.io", "Referer" to "https://gofile.io/"),
            )
            void(acc)
            val guest = HikariNet.postString(
                "https://api.gofile.io/accounts/website", "{}",
                mapOf("User-Agent" to UA, "Origin" to "https://gofile.io", "Referer" to "https://gofile.io/"),
                "application/json",
            )?.let { JSONObject(it).optJSONObject("data")?.optString("token") }
            val content = HikariNet.getJson(
                "https://api.gofile.io/contents/$code?pageSize=100&sortField=name&sortDirection=1",
                mapOf("User-Agent" to UA, "Authorization" to "Bearer ${guest ?: ""}"),
            ) ?: return
            val children = content.optJSONObject("data")?.optJSONObject("children") ?: return
            for (k in children.keys()) {
                val child = children.optJSONObject(k) ?: continue
                val link = child.optString("link")
                if (link.isBlank()) continue
                val name = child.optString("name", "GoFile")
                if (!seen.add(link)) continue
                out.add(HikariStream(name = "GoFile $name [$label]", url = link))
            }
        } catch (_: Exception) {
        }
    }

    private fun void(@Suppress("UNUSED_PARAMETER") o: Any?) = Unit

    private fun parseArchiveLinks(html: String): List<Pair<String, String>> {
        val out = ArrayList<Pair<String, String>>()
        val seen = HashSet<String>()
        for (a in Regex("""<a[^>]+href="([^"]+)"[^>]*>([\s\S]*?)</a>""", RegexOption.IGNORE_CASE).findAll(html)) {
            val href = abs(a.groupValues[1])
            val label = unesc(strip(a.groupValues[2]))
            if (!href.startsWith("http") || !seen.add(href)) continue
            if (EPISODE_HEADER.containsMatchIn(label) || Regex("""\bS\d+\s*E\d+""", RegexOption.IGNORE_CASE).containsMatchIn(label) ||
                href.contains(CODEDEW_HOST) || href.contains(mainUrl.replace("https://", ""))
            ) out.add(label.ifBlank { "Episode" } to href)
        }
        return out
    }

    private fun pixelDrainId(s: String): String? =
        Regex("""pixeldrain[^/]*/(?:u/|api/file/)?([A-Za-z0-9]+)""").find(s)?.groupValues?.get(1)
            ?: Regex("""/u/([A-Za-z0-9]+)""").find(s)?.groupValues?.get(1)

    private fun labelFor(u: String): String {
        for (q in listOf("2160p", "1080p", "720p", "480p", "360p")) if (u.contains(q, true)) return q
        if (u.endsWith(".mkv")) return "MKV"
        return "HD"
    }

    private fun abs(href: String): String {
        if (href.startsWith("http")) return href
        if (href.startsWith("//")) return "https:$href"
        return if (href.startsWith("/")) "$mainUrl$href" else "$mainUrl/$href"
    }

    private fun unesc(s: String): String = s.replace("&amp;", "&").replace("&#39;", "'")
        .replace("&#039;", "'").replace("&quot;", "\"").replace("&lt;", "<")
        .replace("&gt;", ">").replace("&nbsp;", " ").trim()

    private fun strip(s: String): String =
        s.replace(Regex("<[^>]+>"), " ").replace(Regex("\\s+"), " ").trim()
}
