package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import org.json.JSONObject
import java.net.URLEncoder

/**
 * ToonHub (toonhub4u.live, fallback .co) — Hindi-dubbed toons and anime
 * (Pokemon, Doraemon, Shinchan, Ben 10, Cartoon Network, Disney), verified
 * live Oct 2026:
 *  - Category rows: /category/channel-list/cartoon-network|disney|disney-xd-india/,
 *    /category/animated/animation-movies/, /category/animated-series/; cards are
 *    <li class="post-item"> with post-thumb img + h2.post-title > a.
 *  - Detail posts carry per-episode download toggles with FilePress(filebee),
 *    Mega, GDFlix and HubCloud links (+ size tags).
 *  - Playback: HubCloud drive flow + FilePress JSON API (both vendored from the
 *    RareAnimesIndia / TheMoviesFlix ports) + GDFlix GDPlayer scrape + WebView
 *    capture fallback. Mega links are skipped (need a Mega API key).
 */
class ToonHubProvider : HikariProvider {

    override val id = "toonhub"
    override val name = "ToonHub"
    override val mainUrl = "https://toonhub4u.live"
    override val description = "Hindi-dubbed toons & anime episodes and movies — Cartoon Network, Disney, Pokemon, Doraemon."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val UA =
        "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36"

    companion object {
        val BASES = listOf("https://toonhub4u.live", "https://toonhub4u.co")
        var base = BASES[0]
        const val HUBCLOUD_HOST = "hubcloud.ist"
        const val FILEBEE_API = "https://filebee.xyz/api"
    }

    private fun h(referer: String = "$base/"): Map<String, String> =
        mapOf("User-Agent" to UA, "Referer" to referer)

    private suspend fun getFirst(paths: List<String>): String? {
        for (b in BASES) {
            for (p in paths) {
                val t = HikariNet.getString(b + p, h("$b/"))
                if (t != null) {
                    base = b
                    return t
                }
            }
        }
        return null
    }

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("latest", "Latest Posts", HikariMediaType.SERIES),
        HikariCatalog("cn", "Cartoon Network", HikariMediaType.SERIES),
        HikariCatalog("disney", "Disney", HikariMediaType.SERIES),
        HikariCatalog("dxd", "Disney XD India", HikariMediaType.SERIES),
        HikariCatalog("movies", "Animation Movies", HikariMediaType.MOVIE),
        HikariCatalog("series", "Animated Series", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val p = page.coerceAtLeast(1)
        val path = when (catalog.id) {
            "cn" -> "/category/channel-list/cartoon-network/"
            "disney" -> "/category/channel-list/disney/"
            "dxd" -> "/category/channel-list/disney-xd-india/"
            "movies" -> "/category/animated/animation-movies/"
            "series" -> "/category/animated-series/"
            else -> "/"
        }
        val pagePath = if (p > 1) {
            if (path == "/") "/page/$p/" else path.trimEnd('/') + "/page/$p/"
        } else path
        val html = getFirst(listOf(pagePath)) ?: return emptyList()
        return parsePostCards(html)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank() || page > 1) return emptyList()
        val html = getFirst(listOf("/?s=${URLEncoder.encode(q, "UTF-8")}")) ?: return emptyList()
        return parsePostCards(html)
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val html = HikariNet.getString(media.id, h()) ?: return media
        val ogTitle = Regex("<meta[^>]+property=\"og:title\"[^>]+content=\"([^\"]+)\"")
            .find(html)?.groupValues?.get(1)?.let { unescape(it) }?.substringBefore(" » ")
        val ogImage = Regex("<meta[^>]+property=\"og:image\"[^>]+content=\"([^\"]+)\"")
            .find(html)?.groupValues?.get(1)
        val ogDesc = Regex("<meta[^>]+name=\"description\"[^>]+content=\"([^\"]+)\"")
            .find(html)?.groupValues?.get(1)?.let { unescape(it) }
        return media.copy(
            title = ogTitle?.takeIf { it.isNotBlank() } ?: media.title,
            posterUrl = ogImage?.takeIf { it.startsWith("http") } ?: media.posterUrl,
            backdropUrl = ogImage?.takeIf { it.startsWith("http") } ?: media.backdropUrl,
            overview = ogDesc?.takeIf { it.isNotBlank() && !it.startsWith("Download Your Favourite") },
        )
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val html = HikariNet.getString(media.id, h()) ?: return null
        val blocks = parseDownloadBlocks(html)
        if (blocks.isEmpty()) {
            // Single-movie post with no per-episode toggles.
            return listOf(HikariEpisode(number = 1, id = media.id + "#full", name = media.title, image = media.posterUrl))
        }
        return blocks.mapIndexed { idx, b ->
            HikariEpisode(
                number = b.episode ?: (idx + 1),
                id = media.id + "#ep$idx",
                name = b.title,
                image = media.posterUrl,
            )
        }
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val pageUrl = (episode?.id ?: media.id).substringBefore("#")
        val epIdx = (episode?.id ?: "").substringAfter("#ep", "").toIntOrNull()
        val html = HikariNet.getString(pageUrl, h()) ?: return emptyList()
        val blocks = parseDownloadBlocks(html)
        val target = if (epIdx != null && epIdx in blocks.indices) listOf(blocks[epIdx]) else blocks
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        for (b in target) {
            for ((kind, url) in b.links) {
                when {
                    kind == "hubcloud" -> resolveHubCloud(
                        url.substringAfter("/drive/").substringBefore("/").substringBefore("?"),
                        b.title, out, seen,
                    )
                    kind == "filepress" -> resolveFilePress(url, b.title, out, seen)
                    kind == "gdflix" -> resolveGdPlayer(url, b.title, pageUrl, out, seen)
                }
            }
        }
        if (out.isEmpty()) {
            // WebView capture over the post page (embedded watch players).
            try {
                val hits = HikariNet.resolveWithWebView(
                    pageUrl,
                    Regex("https?://[^\"'\\s]+?\\.(?:m3u8|mp4)(?:[?#][^\"'\\s]*)?"),
                    timeoutMs = 45_000,
                )
                for (hit in hits) {
                    if (hit.url.isBlank() || !seen.add(hit.url)) continue
                    out.add(HikariStream(name = "ToonHub", url = hit.url, headers = hit.headers, isM3u8 = hit.url.contains(".m3u8", true)))
                }
            } catch (t: Throwable) { }
        }
        return out
    }

    // ------------------------------------------------------------------
    // Parsing helpers.
    // ------------------------------------------------------------------

    data class DlBlock(val title: String, val episode: Int?, val links: List<Pair<String, String>>)

    fun parsePostCards(html: String): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        val seen = HashSet<String>()
        val itemRe = Regex("<li class=\"post-item[^\"]*\">(.*?)</li>", RegexOption.DOT_MATCHES_ALL)
        for (item in itemRe.findAll(html)) {
            val block = item.groupValues[1]
            val href = Regex("<h2 class=\"post-title\"><a href=\"([^\"]+)\"").find(block)?.groupValues?.get(1)
                ?: Regex("<a[^>]+class=\"post-thumb\"[^>]*href=\"([^\"]+)\"").find(block)?.groupValues?.get(1)
                ?: Regex("href=\"([^\"]+)\"[^>]*class=\"post-thumb\"").find(block)?.groupValues?.get(1)
                ?: continue
            if (!href.startsWith("http") || !seen.add(href)) continue
            val title = Regex("<h2 class=\"post-title\"><a[^>]*>(.*?)</a>").find(block)?.groupValues?.get(1)
                ?.let { stripTags(unescape(it)) }?.trim()?.takeIf { it.isNotBlank() } ?: continue
            val img = Regex("<img[^>]+src=\"([^\"]+)\"").find(block)?.groupValues?.get(1)
            val isMovie = title.contains("movie", true) || href.contains("movie", true)
            out.add(
                HikariMedia(
                    id = href,
                    title = unescape(title).replace(Regex("\\s*Download\\s+480p.*$", RegexOption.IGNORE_CASE), "").trim(),
                    type = if (isMovie) HikariMediaType.MOVIE else HikariMediaType.SERIES,
                    posterUrl = img?.takeIf { it.startsWith("http") },
                )
            )
        }
        return out
    }

    fun parseDownloadBlocks(html: String): List<DlBlock> {
        val out = ArrayList<DlBlock>()
        val toggleRe = Regex("<div class=\"mks_toggle([^>]*>.*?)</div>\\s*</div>", RegexOption.DOT_MATCHES_ALL)
        var idx = 0
        for (t in toggleRe.findAll(html)) {
            val block = t.groupValues[1]
            val links = ArrayList<Pair<String, String>>()
            for (a in Regex("<a\\s+href=\"([^\"]+)\"[^>]*>([^<]*)</a>").findAll(block)) {
                val url = a.groupValues[1]
                val label = a.groupValues[2].lowercase()
                when {
                    url.contains("hubcloud.") -> links.add("hubcloud" to url)
                    url.contains("filebee.xyz") || url.contains("filepress") -> links.add("filepress" to url)
                    url.contains("gdflix.") -> links.add("gdflix" to url)
                    label.contains("filepress") -> links.add("filepress" to url)
                    label.contains("hubcloud") -> links.add("hubcloud" to url)
                }
            }
            if (links.isEmpty()) continue
            val head = block.substring(0, 400.coerceAtMost(block.length))
            val epNum = Regex("[Ee]pisode\\s+(\\d+)").find(head)?.groupValues?.get(1)?.toIntOrNull()
                ?: Regex("E(\\d{1,4})\\b").find(head)?.groupValues?.get(1)?.toIntOrNull()
            val size = Regex("\\[Size:\\s*([^\\]]+)\\]").find(block)?.groupValues?.get(1)?.trim()
            val title = "Episode ${epNum ?: ++idx}" + (if (size != null) " [$size]" else "")
            out.add(DlBlock(title, epNum, links.distinct()))
        }
        return out
    }

    private fun stripTags(s: String): String =
        s.replace(Regex("<[^>]+>"), " ").replace(Regex("\\s+"), " ").trim()

    private fun unescape(s: String): String =
        s.replace("&#8217;", "'").replace("&#038;", "&").replace("&amp;", "&")
            .replace("&#8230;", "...").replace("&quot;", "\"").replace("&#8211;", "-")

    // ------------------------------------------------------------------
    // Hub resolvers (vendored patterns from the RAI / TMF ports).
    // ------------------------------------------------------------------

    private suspend fun resolveHubCloud(id: String, label: String, out: MutableList<HikariStream>, seen: HashSet<String>) {
        try {
            if (id.isBlank()) return
            val page = HikariNet.getString("https://$HUBCLOUD_HOST/drive/$id", h("https://$HUBCLOUD_HOST/")) ?: return
            val resolver = Regex("""var\s+url\s*=\s*'([^']+)'""").find(page)?.groupValues?.get(1) ?: return
            val body = HikariNet.getString(resolver, h("https://$HUBCLOUD_HOST/")) ?: return
            Regex("""<a[^>]*href="(https?://[^"]+)"[^>]*id="fsl"""").find(body)?.let { m ->
                val fsl = m.groupValues[1]
                if (fsl.startsWith("http") && !fsl.contains("hubcloud.cx/favicon") && seen.add(fsl)) {
                    out.add(HikariStream(name = "ToonHub HubCloud [$label]", url = fsl))
                }
            }
            if (out.isEmpty()) {
                Regex("""href="(https?://[^"]*cloudflarestorage\.com/[^"]+)"""").find(body)?.let { m ->
                    if (seen.add(m.groupValues[1])) {
                        out.add(HikariStream(name = "ToonHub HubCloud [$label]", url = m.groupValues[1]))
                    }
                }
            }
            Regex("""https://pixel\.hubcloud\.ist/\?id=[^"' \s<>]+""").find(body)?.let { pm ->
                val b2 = HikariNet.getString(pm.value, h("https://$HUBCLOUD_HOST/")) ?: return
                val pd = Regex(""""id"\s*:\s*"([a-zA-Z0-9]+)"|/file/([a-zA-Z0-9]+)""").find(b2)?.let {
                    it.groupValues[1].ifBlank { it.groupValues[2] }
                }
                if (!pd.isNullOrBlank()) {
                    val api = "https://pixeldrain.dev/api/file/$pd"
                    if (seen.add(api)) out.add(HikariStream(name = "ToonHub Pixel [$label]", url = api))
                }
            }
        } catch (t: Throwable) { }
    }

    private suspend fun resolveFilePress(url: String, label: String, out: MutableList<HikariStream>, seen: HashSet<String>) {
        try {
            val id = Regex("""/file/([a-f0-9]{16,40})""").find(url)?.groupValues?.get(1) ?: return
            val info = HikariNet.getJson("$FILEBEE_API/file/get/$id", h("https://filebee.xyz/"))?.optJSONObject("data") ?: return
            val name = info.optString("name")
            if (name.endsWith(".zip", true) || name.endsWith(".rar", true)) return
            val payload = JSONObject().put("captchaValue", "").put("id", id)
            for (method in listOf("dotFlixDownlaod", "telegramDownload")) {
                val res = HikariNet.postString(
                    "$FILEBEE_API/file/downlaod/", payload.put("method", method).toString(),
                    h("https://filebee.xyz/") + mapOf("Accept" to "application/json", "Origin" to "https://filebee.xyz"),
                    "application/json",
                ) ?: continue
                val data = JSONObject(res).optString("data")
                if (data.startsWith("http") && seen.add(data)) {
                    out.add(HikariStream(name = "ToonHub FilePress [$label]", url = data))
                    break
                }
            }
        } catch (t: Throwable) { }
    }

    private suspend fun resolveGdPlayer(url: String, label: String, referer: String, out: MutableList<HikariStream>, seen: HashSet<String>) {
        try {
            val page = HikariNet.getString(url, h(referer)) ?: return
            for (m in Regex("https?://[^\\s\"'<>\\\\]+\\.(?:m3u8|mp4)[^\\s\"'<>\\\\]*").findAll(page)) {
                val u = m.value
                if (u.startsWith("http") && !u.contains("gdflix.dev") && seen.add(u)) {
                    out.add(HikariStream(name = "ToonHub GDFlix [$label]", url = u, headers = h(url), isM3u8 = u.contains(".m3u8")))
                }
            }
            if (out.isNotEmpty()) return
            val hits = HikariNet.resolveWithWebView(
                url,
                Regex("https?://[^\"'\\s]+?\\.(?:m3u8|mp4)(?:[?#][^\"'\\s]*)?"),
                timeoutMs = 45_000,
            )
            for (hit in hits) {
                if (hit.url.isBlank() || !seen.add(hit.url)) continue
                out.add(HikariStream(name = "ToonHub GDFlix [$label]", url = hit.url, headers = hit.headers, isM3u8 = hit.url.contains(".m3u8", true)))
            }
        } catch (t: Throwable) { }
    }
}
