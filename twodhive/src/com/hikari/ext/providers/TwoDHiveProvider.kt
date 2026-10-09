package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import java.net.URLEncoder

/**
 * 2DHive (2dhive.com) — anime grid + EpisodeBrowser + MegaPlay embeds.
 * Ported from KSHITIJ8473/raghav TwoDHive to native Hikari.
 *
 * Detail pages carry an astro-island EpisodeBrowser (base64 props:
 * totalEpisodes, episodeMeta) plus /episode?anime={malId}&ep_num= links and
 * a /api/anime/summary?malId= quick endpoint. Playback goes through
 * https://megaplay.buzz/stream/mal/{malId}/{ep}/{sub|dub} (data-id ->
 * /stream/getSourcesNew) with WebView capture fallback.
 */
class TwoDHiveProvider : HikariProvider {

    override val id = "twodhive"
    override val name = "2Dhive"
    override val mainUrl = "https://2dhive.com"
    override val description = "Anime from 2DHive — trending, latest, movies, sub & dub via MegaPlay."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val megaBase = "https://megaplay.buzz"
    private val pageHeaders = mapOf(
        "User-Agent" to "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36",
        "Accept" to "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer" to "$mainUrl/",
    )
    private val capture = Regex("""https?://[^"' \s]+\.m3u8[^"' \s]*|https?://[^"' \s]+\.mp4[^"' \s]*""")
    private val anchorRe = Regex("""<a[^>]+href="([^"]+)"[^>]*>([\s\S]*?)</a>""", RegexOption.IGNORE_CASE)
    private val imgRe = Regex("""(?:data-src|data-original|src)="([^"]+)"""", RegexOption.IGNORE_CASE)

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("trending", "Trending", HikariMediaType.SERIES),
        HikariCatalog("latest", "Latest Added", HikariMediaType.SERIES),
        HikariCatalog("movies", "Movies", HikariMediaType.MOVIE),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val p = page.coerceAtLeast(1)
        val base = when (catalog.id) {
            "movies" -> "$mainUrl/movies"
            "latest" -> "$mainUrl/latest"
            else -> "$mainUrl/trending"
        }
        val html = HikariNet.getStringSmart(if (p <= 1) base else "$base?page=$p", pageHeaders)
            ?: HikariNet.getStringSmart(if (p <= 1) mainUrl else "$mainUrl?page=$p", pageHeaders)
            ?: return emptyList()
        return parseGrid(html)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank()) return emptyList()
        val q = URLEncoder.encode(query.trim(), "UTF-8")
        val html = HikariNet.getStringSmart("$mainUrl/search?q=$q&page=${page.coerceAtLeast(1)}", pageHeaders)
            ?: HikariNet.getStringSmart("$mainUrl/?s=$q", pageHeaders)
            ?: return emptyList()
        return parseGrid(html)
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val html = HikariNet.getStringSmart(media.id, pageHeaders) ?: return media
        val title = Regex("""<meta[^>]+property="og:title"[^>]+content="([^"]+)"""").find(html)?.groupValues?.get(1)?.let { unesc(it) } ?: media.title
        val poster = Regex("""<meta[^>]+property="og:image"[^>]+content="([^"]+)"""").find(html)?.groupValues?.get(1)?.takeIf { it.startsWith("http") } ?: media.posterUrl
        return media.copy(title = title, posterUrl = poster, backdropUrl = poster)
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val html = HikariNet.getStringSmart(media.id, pageHeaders) ?: return null
        val malId = Regex("""malId["']?\s*[:=]\s*["']?(\d+)""").find(html)?.groupValues?.get(1)
            ?: Regex("""/episode\?anime=(\d+)""").find(html)?.groupValues?.get(1)
            ?: Regex("""mal/(\d+)""").find(html)?.groupValues?.get(1)
            ?: return anchorFallback(html, media)
        var total = Regex("""/episode\?[^"']*ep_num=(\d+)""").findAll(html).mapNotNull { it.groupValues[1].toIntOrNull() }.maxOrNull() ?: 0
        // EpisodeBrowser island props (base64 JSON with totalEpisodes).
        val island = Regex("""<astro-island[^>]*EpisodeBrowser[^>]*props="([^"]+)"""", RegexOption.IGNORE_CASE).find(html)?.groupValues?.get(1)
        if (island != null) {
            try {
                val dec = HikariNet.base64Decode(island)?.toString(Charsets.UTF_8).orEmpty()
                Regex(""""totalEpisodes"\s*:\s*(\d+)""").find(dec)?.groupValues?.get(1)?.toIntOrNull()?.let {
                    if (it > total) total = it
                }
            } catch (t: Throwable) { }
        }
        if (total <= 0) total = 1
        if (total > 3000) total = 3000
        // Dub availability: page mentions dub or megaplay dub path.
        val hasDub = html.contains("dub", true)
        val out = ArrayList<HikariEpisode>()
        for (n in 1..total) {
            out.add(HikariEpisode(number = n, id = "twodhive:$malId|$n|sub", name = "Episode $n", image = media.posterUrl))
            if (hasDub) out.add(HikariEpisode(number = n, id = "twodhive:$malId|$n|dub", name = "Episode $n (Dub)", image = media.posterUrl))
        }
        return out
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val ref = episode?.id ?: return emptyList()
        val parts = ref.substringAfter("twodhive:").split("|")
        if (parts.size < 3) return emptyList()
        val malId = parts[0]
        val epNum = parts[1]
        val type = parts[2]
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        val playerUrl = "$megaBase/stream/mal/$malId/$epNum/$type"
        try {
            val html = HikariNet.getString(playerUrl, pageHeaders + mapOf("Referer" to "$mainUrl/"))
            if (html != null) {
                val pid = Regex("""data-id=["'](\d+)["']""").find(html)?.groupValues?.get(1)
                    ?: Regex("""data-realid=["'](\d+)["']""").find(html)?.groupValues?.get(1)
                if (pid != null) {
                    val srcBody = HikariNet.getString(
                        "$megaBase/stream/getSourcesNew?id=$pid&type=$type",
                        pageHeaders + mapOf("Referer" to playerUrl, "X-Requested-With" to "XMLHttpRequest")
                    )
                    if (srcBody != null) {
                        for (m in capture.findAll(srcBody)) {
                            if (!seen.add(m.value)) continue
                            out.add(HikariStream(name = "2DHive ${type.uppercase()}", url = m.value, isM3u8 = m.value.contains("m3u8")))
                        }
                        if (out.isNotEmpty()) return out
                    }
                }
            }
        } catch (t: Throwable) { }
        for (r in HikariNet.resolveWithWebView(playerUrl, capture, timeoutMs = 60_000)) {
            if (!seen.add(r.url)) continue
            out.add(HikariStream(name = "2DHive ${type.uppercase()}", url = r.url, headers = r.headers, isM3u8 = r.url.contains("m3u8")))
        }
        return out
    }

    private fun parseGrid(html: String): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        val seen = HashSet<String>()
        for (m in anchorRe.findAll(html)) {
            val href = m.groupValues[1]
            if (href.contains("/episode") || href.contains("?s=") || href.startsWith("#")) continue
            val inner = m.groupValues[2]
            if (!inner.contains("<img")) continue
            val img = imgRe.find(inner)?.groupValues?.get(1) ?: ""
            val title = Regex("""alt="([^"]+)"""").find(inner)?.groupValues?.get(1)?.let { unesc(it) }?.takeIf { it.length > 2 } ?: continue
            val url = abs(href)
            if (!seen.add(url)) continue
            out.add(HikariMedia(id = url, title = title, type = HikariMediaType.SERIES, posterUrl = img.takeIf { it.startsWith("http") }))
            if (out.size >= 60) break
        }
        return out
    }

    private fun anchorFallback(html: String, media: HikariMedia): List<HikariEpisode>? {
        val nums = Regex("""/episode\?[^"']*ep_num=(\d+)""").findAll(html).mapNotNull { it.groupValues[1].toIntOrNull() }.toSet()
        if (nums.isEmpty()) return null
        return nums.sorted().map { n -> HikariEpisode(number = n, id = "${media.id}|$n|sub", name = "Episode $n", image = media.posterUrl) }
    }

    private fun abs(href: String): String {
        if (href.startsWith("http")) return href
        return if (href.startsWith("/")) "$mainUrl$href" else "$mainUrl/$href"
    }
    private fun unesc(s: String): String = s.replace("&amp;", "&").replace("&#39;", "'").replace("&#039;", "'").replace("&quot;", "\"").replace("&lt;", "<").replace("&gt;", ">").replace("&nbsp;", " ").trim()
}
