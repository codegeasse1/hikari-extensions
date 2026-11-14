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
 * OHLI24 (ani.ohli24.com) — Korean anime info hub (Laravel/Inertia), verified
 * live Oct 2026:
 *  - Home renders .aw-sec sections (e.g. "방영중" airing) of .aw-box cards:
 *    <a class="aw-poster" href="/c/<title>"> + <img src="/storage/covers/...">
 *    Section heads (.oh-sec-head href) link to full listing pages.
 *  - Detail pages /c/<title> carry episode/watch links + the shared allforu
 *    player (/player/index.php?data= + &do=getVideo).
 *  - Search: /bbs/search.php?srows=24&gr_id=&sfl=wr_subject&stx=<q> (JSON in
 *    the data-page script + SSR cards); autocomplete panel also queries it.
 */
class Ohli24Provider : HikariProvider {

    override val id = "ohli24"
    override val name = "OHLI24"
    override val mainUrl = "https://ani.ohli24.com"
    override val description = "신작 애니 추천 및 애니메이션 정보 허브 — 방영중, 분기별 추천, 명작 순위."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val UA =
        "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36"

    companion object {
        var base = "https://ani.ohli24.com"
    }

    private fun h(referer: String = "$base/", inertia: Boolean = false): Map<String, String> {
        val m = HashMap<String, String>()
        m["User-Agent"] = UA
        m["Referer"] = referer
        m["Accept-Language"] = "ko-KR,ko;q=0.9"
        if (inertia) {
            m["X-Inertia"] = "true"
            m["X-Inertia-Version"] = ""
            m["X-Requested-With"] = "XMLHttpRequest"
        }
        return m
    }

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("airing", "방영중", HikariMediaType.SERIES),
        HikariCatalog("latest", "최신 업데이트", HikariMediaType.SERIES),
        HikariCatalog("quarter", "분기별 추천", HikariMediaType.SERIES),
        HikariCatalog("top", "명작 순위", HikariMediaType.SERIES),
        HikariCatalog("movies", "극장판", HikariMediaType.MOVIE),
        HikariCatalog("completed", "완결", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val p = page.coerceAtLeast(1)
        // Section listing pages discovered on home heads; fall back to home
        // section slices + search-driven listing when a path 404s.
        val candidates = when (catalog.id) {
            "airing" -> listOf("/%EB%B0%A9%EC%98%81%EC%A4%91?page=$p", "/?section=airing&page=$p", "/")
            "quarter" -> listOf("/%EB%B6%84%EA%B8%B0%EB%B3%84?page=$p", "/?section=quarter&page=$p", "/")
            "top" -> listOf("/%EC%88%9C%EC%9C%84?page=$p", "/?section=rank&page=$p", "/")
            "movies" -> listOf("/%EA%B7%B9%EC%9E%A5%ED%8C%90?page=$p", "/?section=movie&page=$p", "/")
            "completed" -> listOf("/%EC%99%84%EA%B2%B0?page=$p", "/?section=completed&page=$p", "/")
            else -> listOf("/?page=$p", "/")
        }
        for (path in candidates) {
            val html = HikariNet.getStringSmart("$base$path", h()) ?: continue
            if (catalog.id == "latest" || path == "/") {
                val secs = parseSections(html)
                val flat = secs.values.flatten()
                if (flat.isNotEmpty()) {
                    if (catalog.id != "latest") {
                        val hit = secs.entries.firstOrNull { it.key.contains(sectionName(catalog.id)) }?.value
                        if (hit != null && hit.isNotEmpty()) return hit
                    }
                    return flat.distinctBy { it.id }
                }
            }
            val cards = parseCards(html)
            if (cards.isNotEmpty()) return cards
        }
        return emptyList()
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        val p = page.coerceAtLeast(1)
        val enc = URLEncoder.encode(q, "UTF-8")
        val tries = listOf(
            "$base/bbs/search.php?srows=24&gr_id=&sfl=wr_subject&stx=$enc&page=$p",
            "$base/bbs/search.php?sfl=wr_subject&stx=$enc",
        )
        for (u in tries) {
            // Inertia JSON first (props carry the result list), then SSR HTML.
            val js = HikariNet.getString(u, h(u, inertia = true))
            if (js != null) {
                val cards = parseCards(js)
                if (cards.isNotEmpty()) return cards
            }
            val html = HikariNet.getStringSmart(u, h()) ?: continue
            val cards = parseCards(html)
            if (cards.isNotEmpty()) return cards
        }
        return emptyList()
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val html = HikariNet.getStringSmart(media.id, h()) ?: return media
        val ogTitle = Regex("<meta[^>]+property=\"og:title\"[^>]+content=\"([^\"]+)\"").find(html)?.groupValues?.get(1)?.let { unescape(it) }
        val ogImage = Regex("<meta[^>]+property=\"og:image\"[^>]+content=\"([^\"]+)\"").find(html)?.groupValues?.get(1)
        val ogDesc = Regex("<meta[^>]+(?:property=\"og:description\"|name=\"description\")[^>]+content=\"([^\"]+)\"").find(html)?.groupValues?.get(1)?.let { unescape(it) }
        val pageTitle = Regex("<title[^>]*>(.*?)</title>", RegexOption.DOT_MATCHES_ALL).find(html)?.groupValues?.get(1)?.let { stripTags(it) }?.substringBefore(" - ")?.trim()
        return media.copy(
            title = ogTitle?.takeIf { it.isNotBlank() } ?: pageTitle?.takeIf { it.isNotBlank() } ?: media.title,
            posterUrl = ogImage?.takeIf { it.startsWith("http") } ?: media.posterUrl,
            backdropUrl = ogImage?.takeIf { it.startsWith("http") } ?: media.backdropUrl,
            overview = ogDesc?.takeIf { it.isNotBlank() },
        )
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val html = HikariNet.getStringSmart(media.id, h()) ?: return null
        val eps = parseEpisodes(html, media)
        if (eps.isNotEmpty()) return eps
        val hasPlayer = html.contains("/player/index.php?data=")
        if (hasPlayer) {
            return listOf(HikariEpisode(number = 1, id = media.id + "#ep1", name = media.title, image = media.posterUrl))
        }
        return listOf(HikariEpisode(number = 1, id = media.id + "#full", name = media.title, image = media.posterUrl))
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val ref = (episode?.id ?: media.id).substringBefore("#")
        val html = HikariNet.getStringSmart(ref, h()) ?: return emptyList()
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        val playerUrls = Regex("(/player/index\\.php\\?data=[^\"'<>\\s&]+(?:&[^\"'<>\\s]*)?)").findAll(html)
            .map { it.groupValues[1] }.distinct().toList()
        for (pu in playerUrls) {
            val abs = if (pu.startsWith("http")) pu else base + pu
            resolveSaltPlayer(abs, ref, out, seen)
        }
        if (out.isEmpty()) {
            val frames = Regex("<iframe[^>]+src=[\"']([^\"']+)[\"']").findAll(html)
                .map { it.groupValues[1] }.filter { it.startsWith("http") }.distinct().toList()
            for (f in frames) {
                resolveEmbed(f, ref, out, seen)
            }
        }
        if (out.isEmpty()) {
            for (m in Regex("https?://[^\\s\"'<>\\\\]+\\.(?:m3u8|mp4)[^\\s\"'<>\\\\]*").findAll(html)) {
                if (m.value.startsWith("http") && seen.add(m.value)) {
                    out.add(HikariStream(name = "OHLI24", url = m.value, headers = mapOf("User-Agent" to UA, "Referer" to ref), isM3u8 = m.value.contains(".m3u8")))
                }
            }
        }
        if (out.isEmpty()) {
            try {
                val hits = HikariNet.resolveWithWebView(
                    ref,
                    Regex("https?://[^\"'\\s]+?\\.(?:m3u8|mp4)(?:[?#][^\"'\\s]*)?"),
                    timeoutMs = 45_000,
                )
                for (hit in hits) {
                    if (hit.url.isBlank() || !seen.add(hit.url)) continue
                    out.add(HikariStream(name = "OHLI24", url = hit.url, headers = hit.headers, isM3u8 = hit.url.contains(".m3u8", true)))
                }
            } catch (t: Throwable) { }
        }
        return out
    }

    // ------------------------------------------------------------------
    // Parsing helpers.
    // ------------------------------------------------------------------

    private fun sectionName(id: String): String = when (id) {
        "airing" -> "방영중"
        "quarter" -> "분기"
        "top" -> "순위"
        "movies" -> "극장"
        "completed" -> "완결"
        else -> ""
    }

    fun parseSections(html: String): Map<String, List<HikariMedia>> {
        val out = LinkedHashMap<String, MutableList<HikariMedia>>()
        val secRe = Regex("<section[^>]*class=\"[^\"]*aw-sec[^\"]*\"[^>]*>(.*?)</section>", RegexOption.DOT_MATCHES_ALL)
        for (s in secRe.findAll(html)) {
            val block = s.groupValues[1]
            val title = Regex("<h2[^>]*class=\"[^\"]*oh-sec-title[^\"]*\"[^>]*>(.*?)</h2>", RegexOption.DOT_MATCHES_ALL)
                .find(block)?.groupValues?.get(1)?.let { stripTags(it) }?.trim()?.takeIf { it.isNotBlank() } ?: "Section"
            out[title] = parseCards(block).toMutableList()
        }
        return out
    }

    fun parseCards(html: String): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        val seen = HashSet<String>()
        // .aw-box cards: <a class="aw-poster" href="/c/..."> + img + alt/title.
        val boxRe = Regex("<a[^>]*class=\"[^\"]*aw-poster[^\"]*\"[^>]*href=\"([^\"]+)\"[^>]*>(.*?)</a>", RegexOption.DOT_MATCHES_ALL)
        var anyBox = false
        for (m in boxRe.findAll(html)) {
            anyBox = true
            var href = m.groupValues[1]
            if (href.startsWith("/")) href = base + href
            if (!href.startsWith("http") || !seen.add(href)) continue
            val inner = m.groupValues[2]
            val title = Regex("alt=\"([^\"]+)\"").find(inner)?.groupValues?.get(1)?.let { unescape(it) }?.trim()
                ?.takeIf { it.isNotBlank() }
                ?: Regex("title=\"([^\"]+)\"").find(m.value)?.groupValues?.get(1)?.let { unescape(it) }?.trim()
                ?: href.substringAfter("/c/").substringBefore("?").trim().takeIf { it.isNotBlank() }
                ?: continue
            var img = Regex("<img[^>]+src=\"([^\"]+)\"").find(inner)?.groupValues?.get(1)
            if (img != null && img.startsWith("/")) img = base + img
            out.add(HikariMedia(id = href, title = title, type = HikariMediaType.SERIES, posterUrl = img?.takeIf { it.startsWith("http") }))
            if (out.size > 300) break
        }
        if (anyBox) return out
        // Generic /c/ detail links.
        val linkRe = Regex("<a[^>]+href=\"((?:https://ani\\.ohli24\\.com)?/c/[^\"]+)\"[^>]*>(.*?)</a>", RegexOption.DOT_MATCHES_ALL)
        for (m in linkRe.findAll(html)) {
            var href = m.groupValues[1]
            if (href.startsWith("/")) href = base + href
            if (!seen.add(href)) continue
            val title = stripTags(unescape(m.groupValues[2])).trim().takeIf { it.isNotBlank() && it.length < 140 }
                ?: href.substringAfter("/c/").trim().takeIf { it.isNotBlank() } ?: continue
            out.add(HikariMedia(id = href, title = title, type = HikariMediaType.SERIES))
            if (out.size > 300) break
        }
        return out
    }

    fun parseEpisodes(html: String, media: HikariMedia): List<HikariEpisode> {
        val out = ArrayList<HikariEpisode>()
        val seen = HashSet<String>()
        val epRe = Regex("<a[^>]+href=\"([^\"]+)\"[^>]*>([^<]{1,80})</a>")
        var idx = 0
        for (m in epRe.findAll(html)) {
            var url = m.groupValues[1]
            if (url.startsWith("/")) url = base + url
            if (!url.startsWith("http")) continue
            val label = stripTags(unescape(m.groupValues[2])).trim()
            val looksEp = url.contains("/e/", true) || url.contains("episode", true) || url.contains("ep-", true) ||
                Regex("\\d+\\s*[화회]|EP\\.?\\s*\\d+|Episode\\s+\\d+", RegexOption.IGNORE_CASE).containsMatchIn(label)
            if (!looksEp) continue
            if (!seen.add(url)) continue
            idx++
            val n = Regex("(\\d+)").findAll(label).mapNotNull { it.groupValues[1].toIntOrNull() }.lastOrNull() ?: idx
            out.add(HikariEpisode(number = n, id = url, name = label.ifBlank { "Episode $n" }, image = media.posterUrl))
            if (out.size > 500) break
        }
        return out.sortedBy { it.number }
    }

    private suspend fun resolveSaltPlayer(playerUrl: String, referer: String, out: MutableList<HikariStream>, seen: HashSet<String>) {
        val page = HikariNet.getString(playerUrl, mapOf("User-Agent" to UA, "Referer" to referer))
        if (page != null) {
            val data = Regex("[?&]data=([^\"'&\\s]+)").find(playerUrl)?.groupValues?.get(1)
            if (data != null) {
                for (doParam in listOf("getVideo", "getVideoSource")) {
                    val api = playerUrl.substringBefore("?") + "?data=$data&do=$doParam"
                    val txt = HikariNet.getString(api, mapOf("User-Agent" to UA, "Referer" to playerUrl, "X-Requested-With" to "XMLHttpRequest"))
                    if (txt != null) {
                        for (m in Regex("https?://[^\\s\"'<>\\\\]+\\.(?:m3u8|mp4)[^\\s\"'<>\\\\]*").findAll(txt)) {
                            if (m.value.startsWith("http") && seen.add(m.value)) {
                                out.add(HikariStream(name = "OHLI24", url = m.value, headers = mapOf("User-Agent" to UA, "Referer" to playerUrl), isM3u8 = m.value.contains(".m3u8")))
                            }
                        }
                        if (out.isNotEmpty()) return
                    }
                }
            }
            for (m in Regex("https?://[^\\s\"'<>\\\\]+\\.(?:m3u8|mp4)[^\\s\"'<>\\\\]*").findAll(page)) {
                if (m.value.startsWith("http") && seen.add(m.value)) {
                    out.add(HikariStream(name = "OHLI24", url = m.value, headers = mapOf("User-Agent" to UA, "Referer" to playerUrl), isM3u8 = m.value.contains(".m3u8")))
                }
            }
            if (out.isNotEmpty()) return
        }
        try {
            val hits = HikariNet.resolveWithWebView(
                playerUrl,
                Regex("https?://[^\"'\\s]+?\\.(?:m3u8|mp4)(?:[?#][^\"'\\s]*)?"),
                timeoutMs = 45_000,
            )
            for (hit in hits) {
                if (hit.url.isBlank() || !seen.add(hit.url)) continue
                out.add(HikariStream(name = "OHLI24", url = hit.url, headers = hit.headers, isM3u8 = hit.url.contains(".m3u8", true)))
            }
        } catch (t: Throwable) { }
    }

    private suspend fun resolveEmbed(embedUrl: String, referer: String, out: MutableList<HikariStream>, seen: HashSet<String>) {
        val page = HikariNet.getString(embedUrl, mapOf("User-Agent" to UA, "Referer" to referer))
        if (page != null) {
            for (m in Regex("https?://[^\\s\"'<>\\\\]+\\.(?:m3u8|mp4)[^\\s\"'<>\\\\]*").findAll(page)) {
                if (m.value.startsWith("http") && seen.add(m.value)) {
                    out.add(HikariStream(name = "OHLI24", url = m.value, headers = mapOf("User-Agent" to UA, "Referer" to embedUrl), isM3u8 = m.value.contains(".m3u8")))
                }
            }
            if (out.isNotEmpty()) return
        }
        try {
            val hits = HikariNet.resolveWithWebView(
                embedUrl,
                Regex("https?://[^\"'\\s]+?\\.(?:m3u8|mp4)(?:[?#][^\"'\\s]*)?"),
                timeoutMs = 45_000,
            )
            for (hit in hits) {
                if (hit.url.isBlank() || !seen.add(hit.url)) continue
                out.add(HikariStream(name = "OHLI24", url = hit.url, headers = hit.headers, isM3u8 = hit.url.contains(".m3u8", true)))
            }
        } catch (t: Throwable) { }
    }

    private fun stripTags(s: String): String =
        s.replace(Regex("<[^>]+>"), " ").replace(Regex("\\s+"), " ").trim()

    private fun unescape(s: String): String =
        s.replace("&#039;", "'").replace("&#39;", "'").replace("&amp;", "&")
            .replace("&quot;", "\"").replace("&#8230;", "...")
}
