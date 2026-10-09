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
 * 9anime (9anime.org.lv) — anime WordPress grid + base64 mirror embeds.
 * Ported from KSHITIJ8473/raghav NineAnime to native Hikari.
 *
 * Cards are generic article grids; detail pages list episodes in .eplister
 * (newest first, reversed to oldest-first); watch pages carry a
 * select.mirror whose options are base64 iframes (gogo/newplayer wrappers
 * one level deeper, plyr.php#base64, megaplay/vidmoly embeds) resolved via
 * WebView m3u8 capture.
 */
class NineAnimeProvider : HikariProvider {

    override val id = "nineanime"
    override val name = "9anime"
    override val mainUrl = "https://9anime.org.lv"
    override val description = "Anime from 9anime — latest, popular, movies, sub & dub mirrors."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val pageHeaders = mapOf(
        "User-Agent" to "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36",
        "Accept" to "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer" to "$mainUrl/",
    )
    private val capture = Regex("""https?://[^"' \s]+\.m3u8[^"' \s]*|https?://[^"' \s]+\.mp4[^"' \s]*""")
    private val anchorRe = Regex("""<a[^>]+href="([^"]+)"[^>]*>([\s\S]*?)</a>""", RegexOption.IGNORE_CASE)
    private val imgRe = Regex("""(?:data-src|data-original|src)="([^"]+)"""", RegexOption.IGNORE_CASE)

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("latest", "Latest Episodes", HikariMediaType.SERIES),
        HikariCatalog("popular", "Most Popular", HikariMediaType.SERIES),
        HikariCatalog("movies", "Movies", HikariMediaType.MOVIE),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val p = page.coerceAtLeast(1)
        val base = when (catalog.id) {
            "popular" -> "$mainUrl/most-popular"
            "movies" -> "$mainUrl/movies"
            else -> "$mainUrl/latest"
        }
        val html = HikariNet.getStringSmart(if (p <= 1) base else "$base/page/$p", pageHeaders)
            ?: HikariNet.getStringSmart(if (p <= 1) mainUrl else "$mainUrl/page/$p", pageHeaders)
            ?: return emptyList()
        return parseCards(html)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank()) return emptyList()
        val q = query.trim().replace(" ", "+")
        val p = page.coerceAtLeast(1)
        val html = HikariNet.getStringSmart("$mainUrl/?s=$q${if (p > 1) "&paged=$p" else ""}", pageHeaders)
            ?: HikariNet.getStringSmart("$mainUrl/search?q=${URLEncoder.encode(query.trim(), "UTF-8")}", pageHeaders)
            ?: return emptyList()
        return parseCards(html)
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val html = HikariNet.getStringSmart(media.id, pageHeaders) ?: return media
        val title = Regex("""<meta[^>]+property="og:title"[^>]+content="([^"]+)"""").find(html)?.groupValues?.get(1)?.let { unesc(it) } ?: media.title
        val poster = Regex("""<meta[^>]+property="og:image"[^>]+content="([^"]+)"""").find(html)?.groupValues?.get(1)?.takeIf { it.startsWith("http") } ?: media.posterUrl
        val desc = Regex("""<meta[^>]+(?:name|property)="description"[^>]+content="([^"]+)"""").find(html)?.groupValues?.get(1)?.let { unesc(it) }
        return media.copy(title = title, posterUrl = poster, backdropUrl = poster, overview = desc ?: media.overview)
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val html = HikariNet.getStringSmart(media.id, pageHeaders) ?: return null
        val out = ArrayList<HikariEpisode>()
        // .eplister rows: epl-num + epl-title + link.
        for (chunk in html.split(Regex("""<li[^>]*>""")).drop(1)) {
            if (!chunk.contains("epl-num") && !chunk.contains("epl-title")) continue
            val href = Regex("""<a[^>]+href="([^"]+)"""").find(chunk)?.groupValues?.get(1) ?: continue
            val num = Regex("""epl-num">([^<]+)<""").find(chunk)?.groupValues?.get(1)?.trim()?.toIntOrNull()
                ?: Regex("""(\d+)""").find(chunk)?.groupValues?.get(1)?.toIntOrNull() ?: continue
            val name = Regex("""epl-title">([\s\S]*?)<""").find(chunk)?.groupValues?.get(1)?.let { unesc(strip(it)) }?.takeIf { it.isNotBlank() } ?: "Episode $num"
            out.add(HikariEpisode(number = num, id = abs(href), name = name, image = media.posterUrl))
        }
        if (out.isNotEmpty()) return out.sortedBy { it.number }
        // Generic episode anchors.
        for (m in anchorRe.findAll(html)) {
            val href = m.groupValues[1]
            if (!href.contains("/episode") && !href.contains("/watch")) continue
            val label = unesc(strip(m.groupValues[2]))
            if (label.isBlank()) continue
            val num = Regex("""(\d+)""").find(label)?.groupValues?.get(1)?.toIntOrNull() ?: continue
            out.add(HikariEpisode(number = num, id = abs(href), name = label, image = media.posterUrl))
        }
        if (out.isNotEmpty()) return out.sortedBy { it.number }
        // Single movie/OVA.
        if (Regex("""<iframe[^>]+src="http|select[^>]*mirror""").containsMatchIn(html)) {
            return listOf(HikariEpisode(number = 1, id = media.id, name = media.title, image = media.posterUrl))
        }
        return null
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val pageUrl = episode?.id?.takeIf { it.startsWith("http") } ?: media.id
        val html = HikariNet.getStringSmart(pageUrl, pageHeaders) ?: return emptyList()
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        val embeds = ArrayList<String>()
        // select.mirror base64 options.
        val mirror = Regex("""<select[^>]*mirror[^>]*>([\s\S]*?)</select>""", RegexOption.IGNORE_CASE).find(html)?.groupValues?.get(1)
        if (mirror != null) {
            for (opt in Regex("""<option[^>]*value="([^"]+)"""").findAll(mirror)) {
                val dec = HikariNet.base64Decode(opt.groupValues[1])?.toString(Charsets.UTF_8) ?: continue
                val src = Regex("""src="([^"]+)"""").find(dec)?.groupValues?.get(1) ?: continue
                if (src.startsWith("http")) embeds.add(src)
            }
        }
        for (m in Regex("""<iframe[^>]+src="([^"]+)"""").findAll(html)) {
            if (m.groupValues[1].startsWith("http")) embeds.add(m.groupValues[1])
        }
        for (embed in embeds.distinct()) {
            // gogo newplayer wraps the real embed one level deeper.
            var target = embed
            if (target.contains("newplayer.php")) {
                try {
                    val ph = HikariNet.getString(target, pageHeaders + mapOf("Referer" to pageUrl))
                    if (ph != null) {
                        Regex("""<iframe[^>]+src="([^"]+)"""").find(ph)?.groupValues?.get(1)?.let {
                            if (it.startsWith("http")) target = it
                        }
                    }
                } catch (t: Throwable) { }
            }
            // plyr.php#base64 inline file.
            if (target.contains("plyr.php#")) {
                try {
                    val b64 = target.substringAfter("#").substringBefore("#")
                    val dec = HikariNet.base64Decode(b64)?.toString(Charsets.UTF_8).orEmpty()
                    val file = Regex("""https?://[^"' \s]+\.(?:m3u8|mp4)[^"' \s]*""").find(dec)?.value
                    if (file != null && seen.add(file)) {
                        out.add(HikariStream(name = "9anime", url = file, isM3u8 = file.contains("m3u8")))
                        continue
                    }
                } catch (t: Throwable) { }
            }
            for (r in HikariNet.resolveWithWebView(target, capture, timeoutMs = 60_000)) {
                if (!seen.add(r.url)) continue
                out.add(HikariStream(name = "9anime", url = r.url, headers = r.headers, isM3u8 = r.url.contains("m3u8")))
            }
            if (out.isNotEmpty()) return out
        }
        return out
    }

    private fun parseCards(html: String): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        val seen = HashSet<String>()
        for (m in anchorRe.findAll(html)) {
            val href = m.groupValues[1]
            if (href.contains("?s=") || href.contains("/episode") || href.startsWith("#")) continue
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

    private fun abs(href: String): String {
        if (href.startsWith("http")) return href
        return if (href.startsWith("/")) "$mainUrl$href" else "$mainUrl/$href"
    }
    private fun unesc(s: String): String = s.replace("&amp;", "&").replace("&#39;", "'").replace("&#039;", "'").replace("&quot;", "\"").replace("&lt;", "<").replace("&gt;", ">").replace("&nbsp;", " ").trim()
    private fun strip(s: String): String = s.replace(Regex("<[^>]+>"), " ").replace(Regex("\\s+"), " ").trim()
}
