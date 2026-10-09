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
 * RareAnimesIndia (rareanimes.mov) — Hindi-dubbed direct-download anime.
 * Ported from KSHITIJ8473/raghav RareAnimesIndia to native Hikari.
 *
 * Pages list episodes as "Episode N" section headers followed by quality
 * download buttons (archive/linker pages + direct hubs). getStreams returns
 * the direct file URLs (mp4/mkv) for the selected episode/variant.
 */
class RareAnimesIndiaProvider : HikariProvider {

    override val id = "rareanimesindia"
    override val name = "Rare Toons India"
    override val mainUrl = "https://www.rareanimes.mov"
    override val description = "Hindi-dubbed anime direct downloads from Rare Toons India."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val pageHeaders = mapOf(
        "User-Agent" to "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36",
        "Accept" to "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer" to "$mainUrl/",
    )
    private val epHeader = Regex("""^(?:Episode|EP)\s*[.\-]?\s*(\d{1,3})\b""", RegexOption.IGNORE_CASE)
    private val anchorRe = Regex("""<a[^>]+href="([^"]+)"[^>]*>([\s\S]*?)</a>""", RegexOption.IGNORE_CASE)

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("latest", "Latest Added", HikariMediaType.SERIES),
        HikariCatalog("movies", "Movies", HikariMediaType.MOVIE),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val p = page.coerceAtLeast(1)
        val url = if (p <= 1) mainUrl else "$mainUrl/page/$p"
        val html = HikariNet.getStringSmart(url, pageHeaders) ?: return emptyList()
        return parseCards(html)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank()) return emptyList()
        val q = URLEncoder.encode(query.trim(), "UTF-8")
        val html = HikariNet.getStringSmart("$mainUrl/?s=$q", pageHeaders) ?: return emptyList()
        return parseCards(html)
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val html = HikariNet.getStringSmart(media.id, pageHeaders) ?: return media
        val title = Regex("""<meta[^>]+property="og:title"[^>]+content="([^"]+)"""").find(html)?.groupValues?.get(1)?.let { unesc(it) } ?: media.title
        val poster = Regex("""<meta[^>]+property="og:image"[^>]+content="([^"]+)"""").find(html)?.groupValues?.get(1)?.takeIf { it.startsWith("http") } ?: media.posterUrl
        return media.copy(title = title, posterUrl = poster, backdropUrl = poster)
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val html = HikariNet.getStringSmart(media.id, pageHeaders) ?: return null
        val text = strip(html)
        // Direct "Episode N" sections with following download anchors.
        val anchors = anchorRe.findAll(html).map { abs(it.groupValues[1]) to unesc(strip(it.groupValues[2])) }.toList()
        val out = ArrayList<HikariEpisode>()
        var currentEp = 0
        var currentName = ""
        // Walk visible text lines for headers, attach subsequent file links.
        val pending = ArrayList<Pair<String, String>>()
        for (m in anchorRe.findAll(html)) {
            val href = abs(m.groupValues[1])
            val label = unesc(strip(m.groupValues[2]))
            if (!href.startsWith("http")) continue
            if (href.contains(mainUrl) && (href.contains("/episode") || epHeader.containsMatchIn(label))) {
                val n = epHeader.find(label)?.groupValues?.get(1)?.toIntOrNull()
                    ?: Regex("""(\d{1,3})""").find(label)?.groupValues?.get(1)?.toIntOrNull() ?: continue
                currentEp = n
                currentName = label
                continue
            }
            if (href.endsWith(".mp4") || href.endsWith(".mkv") || label.contains("download", true) || label.contains("720p") || label.contains("1080p") || label.contains("480p")) {
                val ep = if (currentEp > 0) currentEp else pending.size + 1
                pending.add(href to (if (currentName.isNotBlank()) "$currentName — $label" else label.ifBlank { "Episode $ep" }))
            }
        }
        // Group file links by episode order when no headers present.
        if (pending.isNotEmpty() && currentEp == 0) {
            for ((idx, p) in pending.withIndex()) {
                out.add(HikariEpisode(number = idx + 1, id = "rai:${media.id}|${idx}|${p.first}", name = "Episode ${idx + 1}", image = media.posterUrl))
            }
            // Store variant links inside episode ids is lossy; instead keep direct mapping below via page re-parse.
            return out
        }
        // Header-based: one episode per header, variants appended.
        val byEp = LinkedHashMap<Int, MutableList<String>>()
        var ep = 0
        for (m in anchorRe.findAll(html)) {
            val label = unesc(strip(m.groupValues[2]))
            val h = epHeader.find(label)
            if (h != null && abs(m.groupValues[1]).contains(mainUrl)) {
                ep = h.groupValues[1].toIntOrNull() ?: ep
                byEp.getOrPut(ep) { ArrayList() }
            } else {
                val href = abs(m.groupValues[1])
                if (ep > 0 && (href.endsWith(".mp4") || href.endsWith(".mkv") || label.contains("download", true))) {
                    byEp.getOrPut(ep) { ArrayList() }.add(href)
                }
            }
        }
        for ((num, variants) in byEp) {
            val id = if (variants.isEmpty()) "rai:${media.id}|ep|$num" else "rai:${variants.joinToString(",")}|ep|$num"
            out.add(HikariEpisode(number = num, id = id, name = "Episode $num", image = media.posterUrl))
        }
        if (out.isNotEmpty()) return out.sortedBy { it.number }
        // Fallback: every download link is its own entry.
        val files = anchors.filter { it.first.endsWith(".mp4") || it.first.endsWith(".mkv") }
        if (files.isNotEmpty()) {
            return files.mapIndexed { idx, f -> HikariEpisode(number = idx + 1, id = "rai:${f.first}|ep|${idx + 1}", name = f.second.ifBlank { "Episode ${idx + 1}" }, image = media.posterUrl) }
        }
        if (text.contains("download", true)) {
            return listOf(HikariEpisode(number = 1, id = "rai:${media.id}|ep|1", name = media.title, image = media.posterUrl))
        }
        return null
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val ref = episode?.id ?: return emptyList()
        if (!ref.startsWith("rai:")) return emptyList()
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        val inner = ref.removePrefix("rai:")
        // Direct file URLs embedded in the id.
        for (u in inner.split(",")) {
            if (u.startsWith("http") && (u.contains(".mp4") || u.contains(".mkv") || u.contains(".m3u8"))) {
                if (!seen.add(u)) continue
                out.add(HikariStream(name = labelFor(u), url = u, isM3u8 = u.contains("m3u8")))
            }
        }
        if (out.isNotEmpty()) return out
        // Otherwise re-parse the detail page around the episode header.
        val pageUrl = inner.split("|")[0].takeIf { it.startsWith("http") } ?: media.id
        val epNum = episode.number
        val html = HikariNet.getStringSmart(pageUrl, pageHeaders) ?: return emptyList()
        for (m in anchorRe.findAll(html)) {
            val href = abs(m.groupValues[1])
            if (!(href.endsWith(".mp4") || href.endsWith(".mkv") || href.contains(".m3u8"))) continue
            if (!seen.add(href)) continue
            out.add(HikariStream(name = "Episode $epNum — ${labelFor(href)}", url = href, isM3u8 = href.contains("m3u8")))
        }
        return out
    }

    private fun parseCards(html: String): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        val seen = HashSet<String>()
        val imgRe = Regex("""(?:data-src|data-original|src)="([^"]+)"""", RegexOption.IGNORE_CASE)
        for (m in anchorRe.findAll(html)) {
            val href = m.groupValues[1]
            if (!href.startsWith("http") && !href.startsWith("/")) continue
            if (href.contains("?s=") || href.contains("#")) continue
            val inner = m.groupValues[2]
            if (!inner.contains("<img")) continue
            val img = imgRe.find(inner)?.groupValues?.get(1) ?: ""
            val title = Regex("""alt="([^"]+)"""").find(inner)?.groupValues?.get(1)?.let { unesc(it) }?.takeIf { it.length > 2 } ?: continue
            val url = abs(href)
            if (!url.contains(mainUrl.replace("https://", "").replace("http://", "")) && !url.startsWith(mainUrl)) continue
            if (!seen.add(url)) continue
            out.add(HikariMedia(id = url, title = title, type = HikariMediaType.SERIES, posterUrl = img.takeIf { it.startsWith("http") }))
            if (out.size >= 60) break
        }
        return out
    }

    private fun labelFor(u: String): String {
        for (q in listOf("1080p", "720p", "480p", "360p", "2160p", "4K")) if (u.contains(q, true)) return "RAI $q"
        if (u.endsWith(".mkv")) return "RAI MKV"
        return "RAI"
    }

    private fun abs(href: String): String {
        if (href.startsWith("http")) return href
        return if (href.startsWith("/")) "$mainUrl$href" else "$mainUrl/$href"
    }
    private fun unesc(s: String): String = s.replace("&amp;", "&").replace("&#39;", "'").replace("&#039;", "'").replace("&quot;", "\"").replace("&lt;", "<").replace("&gt;", ">").replace("&nbsp;", " ").trim()
    private fun strip(s: String): String = s.replace(Regex("<[^>]+>"), " ").replace(Regex("\\s+"), " ").trim()
}
