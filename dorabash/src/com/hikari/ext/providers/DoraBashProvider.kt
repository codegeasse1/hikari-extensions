package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import java.net.URLEncoder
import android.util.Base64

/**
 * DoraBash (dorabash.in) — Indian Doraemon / Doraemons / Dorami catalog in
 * Hindi-English, verified live Oct 2026:
 *  - Taxonomy rows: /anime-type/tv|specials|short-movie|movie/ + /genre-slug/ rows
 *    plus the home "Latest Episodes" watch-card grid.
 *  - Watch pages /watch/<slug>[-episode-N]/ carry server options as
 *    <span data-embed-id="<b64 label>:<b64 url>"> (Abyss = player.abyssplayer.com,
 *    Moon = bysewepoin.com) plus a default player iframe and Mediafire
 *    download links.
 *  - Playback: Abyss/Moon embeds resolved via WebView media capture (the same
 *    technique the NineAnime port uses for byse moon players); Mediafire file
 *    links emitted as direct MP4 fallback.
 */
class DoraBashProvider : HikariProvider {

    override val id = "dorabash"
    override val name = "DoraBash"
    override val mainUrl = "https://dorabash.in"
    override val description = "Doraemon, Dorami & Doraemons episodes and movies — sub & Hindi dub."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val UA =
        "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36"

    companion object {
        var base = "https://dorabash.in"
    }

    private fun h(referer: String = "$base/"): Map<String, String> =
        mapOf("User-Agent" to UA, "Referer" to referer)

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("latest", "Latest Episodes", HikariMediaType.SERIES),
        HikariCatalog("movies", "Movies", HikariMediaType.MOVIE),
        HikariCatalog("tv", "TV Seasons", HikariMediaType.SERIES),
        HikariCatalog("specials", "Specials", HikariMediaType.SERIES),
        HikariCatalog("short", "Short Movies", HikariMediaType.MOVIE),
        HikariCatalog("comedy", "Comedy", HikariMediaType.SERIES),
        HikariCatalog("family", "Family", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val p = page.coerceAtLeast(1)
        val path = when (catalog.id) {
            "movies" -> "anime-type/movie"
            "tv" -> "anime-type/tv"
            "specials" -> "anime-type/specials"
            "short" -> "anime-type/short-movie"
            "comedy" -> "genre/comedy"
            "family" -> "genre/family"
            else -> ""
        }
        val url = if (path.isBlank()) {
            if (p > 1) "$base/page/$p/" else "$base/"
        } else {
            if (p > 1) "$base/$path/page/$p/" else "$base/$path/"
        }
        val html = HikariNet.getString(url, h()) ?: return emptyList()
        return parseWatchCards(html)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank() || page > 1) return emptyList()
        val html = HikariNet.getString("$base/?s=${URLEncoder.encode(q, "UTF-8")}", h())
            ?: HikariNet.getString("$base/search/${URLEncoder.encode(q, "UTF-8")}/", h())
            ?: return emptyList()
        return parseWatchCards(html)
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val html = HikariNet.getString(media.id, h()) ?: return media
        val ogTitle = Regex("<meta[^>]+property=\"og:title\"[^>]+content=\"([^\"]+)\"")
            .find(html)?.groupValues?.get(1)?.let { unescape(it) }
        val ogImage = Regex("<meta[^>]+property=\"og:image\"[^>]+content=\"([^\"]+)\"")
            .find(html)?.groupValues?.get(1)
        val ogDesc = Regex("<meta[^>]+property=\"og:description\"[^>]+content=\"([^\"]+)\"")
            .find(html)?.groupValues?.get(1)?.let { unescape(it) }
        return media.copy(
            title = ogTitle?.substringBefore(" Episode ")?.takeIf { it.isNotBlank() } ?: media.title,
            posterUrl = ogImage?.takeIf { it.startsWith("http") } ?: media.posterUrl,
            backdropUrl = ogImage?.takeIf { it.startsWith("http") } ?: media.backdropUrl,
            overview = ogDesc?.takeIf { it.isNotBlank() },
        )
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        // Watch pages are single playable entries (movies or episodes).
        val n = Regex("-episode-(\\d+)").find(media.id)?.groupValues?.get(1)?.toIntOrNull()
            ?: Regex("[Ee]pisode\\s+(\\d+)").find(media.title)?.groupValues?.get(1)?.toIntOrNull()
            ?: 1
        return listOf(HikariEpisode(number = n, id = media.id, name = media.title, image = media.posterUrl))
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val pageUrl = episode?.id ?: media.id
        val html = HikariNet.getString(pageUrl, h()) ?: return emptyList()
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        // 1) Server options: data-embed-id="<b64 label>:<b64 url>".
        val embedRe = Regex("data-embed-id=\"([^\"]+)\"")
        for (m in embedRe.findAll(html)) {
            val parts = m.groupValues[1].split(":")
            if (parts.size < 2) continue
            val label = try {
                String(Base64.decode(parts[0], Base64.DEFAULT), Charsets.UTF_8)
            } catch (t: Throwable) { "Server" }
            val target = try {
                String(Base64.decode(parts[1], Base64.DEFAULT), Charsets.UTF_8).trim()
            } catch (t: Throwable) { "" }
            if (!target.startsWith("http")) continue
            resolveEmbedPage(target, "DoraBash $label", pageUrl, out, seen)
        }
        // 2) Default player iframe.
        for (m in Regex("<iframe[^>]+(?:src|data-litespeed-src|data-src)=[\"']([^\"']+)[\"']").findAll(html)) {
            val src = m.groupValues[1]
            if (!src.startsWith("http")) continue
            if (src.contains("abyssplayer.com") || src.contains("bysewepoin.com") || src.contains("bysesayeveum.com")) {
                resolveEmbedPage(src, "DoraBash Player", pageUrl, out, seen)
            } else {
                val direct = Regex("https?://[^\\s\"'<>\\\\]+\\.(?:m3u8|mp4)[^\\s\"'<>\\\\]*").find(src)?.value
                if (direct != null && seen.add(direct)) {
                    out.add(HikariStream(name = "DoraBash Player", url = direct, headers = mapOf("Referer" to pageUrl, "User-Agent" to UA), isM3u8 = direct.contains(".m3u8")))
                }
            }
        }
        // 3) Mediafire download links as direct MP4 fallback.
        for (m in Regex("https?://(?:www\\.)?mediafire\\.com/file/[^\\s\"'<>]+").findAll(html)) {
            val mf = m.value
            if (seen.add("mf:$mf")) {
                val direct = resolveMediafire(mf)
                if (direct != null && seen.add(direct)) {
                    out.add(HikariStream(name = "DoraBash Mediafire", url = direct, headers = mapOf("User-Agent" to UA), isM3u8 = false))
                }
            }
        }
        return out
    }

    // ------------------------------------------------------------------
    // Parsing helpers.
    // ------------------------------------------------------------------

    fun parseWatchCards(html: String): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        val seen = HashSet<String>()
        val linkRe = Regex("<a\\s+[^>]*href=\"([^\"]*?/watch/[^\"]+/)[\"?]")
        for (m in linkRe.findAll(html)) {
            var url = m.groupValues[1]
            if (url.startsWith("/")) url = base + url
            if (!url.startsWith("http") || !seen.add(url)) continue
            val block = html.substring(m.range.first, (m.range.first + 800).coerceAtMost(html.length))
            val title = Regex("title=\"([^\"]{3,200})\"").find(m.value)?.groupValues?.get(1)?.let { unescape(it) }
                ?: Regex("aria-label=\"([^\"]{3,200})\"").find(block)?.groupValues?.get(1)?.let { unescape(it) }
                ?: slugToTitle(url)
            val img = Regex("<img[^>]+src=\"([^\"]+)\"").find(block)?.groupValues?.get(1)
            val isMovie = !url.contains("-episode-") || url.contains("movie", true)
            out.add(
                HikariMedia(
                    id = url,
                    title = title.trim(),
                    type = if (isMovie) HikariMediaType.MOVIE else HikariMediaType.SERIES,
                    posterUrl = img?.takeIf { it.startsWith("http") },
                )
            )
        }
        return out
    }

    private fun slugToTitle(url: String): String {
        val slug = url.trimEnd('/').substringAfterLast("/").substringBefore("-episode-")
        return slug.split("-").joinToString(" ") { w -> w.replaceFirstChar { it.uppercase() } }
    }

    private fun unescape(s: String): String =
        s.replace("&#8217;", "'").replace("&#8211;", "-").replace("&#038;", "&")
            .replace("&amp;", "&").replace("&#8230;", "...").replace("&quot;", "\"")

    // ------------------------------------------------------------------
    // Embed resolvers.
    // ------------------------------------------------------------------

    private suspend fun resolveEmbedPage(target: String, label: String, referer: String, out: MutableList<HikariStream>, seen: HashSet<String>) {
        // Abyss / byse moon players render via JS: capture what the page plays.
        val timeout = if (target.contains("byse")) 90_000L else 60_000L
        try {
            val hits = HikariNet.resolveWithWebView(
                target,
                Regex("https?://[^\"'\\s]+?\\.(?:m3u8|mp4)(?:[?#][^\"'\\s]*)?"),
                timeoutMs = timeout,
            )
            for (hit in hits) {
                if (hit.url.isBlank() || !seen.add(hit.url)) continue
                out.add(
                    HikariStream(
                        name = label,
                        url = hit.url,
                        headers = hit.headers,
                        isM3u8 = hit.url.contains(".m3u8", ignoreCase = true),
                    )
                )
            }
            if (out.isNotEmpty()) return
        } catch (t: Throwable) { }
        // Static fallback: any media URL in the embed HTML.
        val html = HikariNet.getString(target, h(referer)) ?: return
        for (m in Regex("https?://[^\\s\"'<>\\\\]+\\.(?:m3u8|mp4)[^\\s\"'<>\\\\]*").findAll(html)) {
            val u = m.value
            if (u.startsWith("http") && seen.add(u)) {
                out.add(HikariStream(name = label, url = u, headers = h(target), isM3u8 = u.contains(".m3u8")))
            }
        }
    }

    private suspend fun resolveMediafire(pageUrl: String): String? {
        // Mediafire file pages embed the direct download as kNO = "..." / aria-label download button.
        val html = HikariNet.getString(pageUrl, h(pageUrl)) ?: return null
        val direct = Regex("\"(https?://download[^\"']+\\.mediafire\\.com[^\"']+)\"").find(html)?.groupValues?.get(1)
            ?: Regex("aria-label=\"Download file\"[^>]+href=\"([^\"]+)\"").find(html)?.groupValues?.get(1)
            ?: Regex("id=\"downloadButton\"[^>]+href=\"([^\"]+)\"").find(html)?.groupValues?.get(1)
        return direct?.takeIf { it.startsWith("http") }
    }
}
