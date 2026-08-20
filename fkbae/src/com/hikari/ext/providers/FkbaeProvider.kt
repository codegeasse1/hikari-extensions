package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.net.URLEncoder

/**
 * FkBAE (fkbae.to) — user-uploaded nude clips & pics from Snapchat, Telegram,
 * Instagram and Discord. A WordPress site, scraped from its rendered pages:
 *  - 20 category walls (`/snapchat-nudes/`, `/leaked-snapchat/`, …) plus the
 *    home feed, all paginated with `/page/N/`,
 *  - a post page embeds the player iframe — `snstrhls.php` (fluid player, an
 *    HLS `<source>`) for HLS posts, or `snstr.php` (JWPlayer, `file:"…"`) for
 *    direct-`stream.fkbae.to/<id>.mp4` posts. Both streams are signed with
 *    `?token=…&expires=…` and play headerless, so no WebView needed,
 *  - search is WordPress `/?s=<query>` (also paginated with `/page/N/`).
 */
class FkbaeProvider : HikariProvider {

    override val id = "fkbae"
    override val name = "FkBAE"
    override val mainUrl = "https://fkbae.to"
    override val description = "User-uploaded nude videos from Snapchat, Telegram, Instagram & Discord — 20 category walls plus search, with direct HLS & MP4 streams."
    override val version = 4
    override val tvTypes = setOf(HikariMediaType.MOVIE)

    /** Browser-ish headers for scraping. fkbae.to (Cloudflare) serves its real
     *  pages to browsers; the plain okhttp client can get a challenge page on
     *  the player endpoint instead — so [getCached] goes through
     *  getStringSmart, which re-fetches in a real WebView when that happens. */
    private val pageHeaders = mapOf(
        "Accept" to "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language" to "en-US,en;q=0.9",
    )

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? = null

    companion object {
        private const val BASE = "https://fkbae.to"
        private const val CACHE_TTL_MS = 600_000L

        private val htmlCache = HashMap<String, Pair<Long, String>>()
        private val cacheMutex = Mutex()

        // The 20 category walls on the categories page (slug → label).
        private val categories = listOf(
            "snapchat-nudes" to "Nudes",
            "leaked-snapchat" to "Leaked",
            "snapchat-sex" to "Sex",
            "dirty-snapchat" to "Dirty",
            "snapchat-fuck" to "Fuck",
            "snapchat-xxx" to "XXX",
            "premium-snapchat" to "Girls with Premium Content",
            "snapchat-boobs" to "Boobs",
            "snapchat-sluts" to "Sluts",
            "snapchat-sexy" to "Sexy",
            "snapchat-lesbians" to "Lesbians",
            "snapchat-masturbation" to "Masturbation",
            "snapchat-asian" to "Asian",
            "snapchat-pussy" to "Pussy",
            "snapchat-milf" to "MILF",
            "snapchat-ass" to "Ass",
            "snapchat-horny" to "Horny",
            "snapchat-cum" to "Cum",
            "snapchat-blowjob" to "Blowjob",
            "snapchat-anal" to "Anal",
        )
    }

    override fun catalogs(): List<HikariCatalog> = buildList {
        add(HikariCatalog("latest", "Latest", HikariMediaType.MOVIE))
        for ((slug, label) in categories) {
            add(HikariCatalog("cat_$slug", label, HikariMediaType.MOVIE, rawType = slug))
        }
    }

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val slug = catalog.rawType.ifBlank { "" }
        val base = if (catalog.id == "latest" || slug.isBlank()) {
            "$BASE/"
        } else {
            "$BASE/$slug/"
        }
        val url = if (page <= 1) {
            base
        } else if (catalog.id == "latest" || slug.isBlank()) {
            "$BASE/page/$page/"
        } else {
            "$BASE/$slug/page/$page/"
        }
        return parseGrid(getCached(url) ?: return emptyList())
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        val url = if (page <= 1) "$BASE/?s=${encode(q)}" else "$BASE/page/$page/?s=${encode(q)}"
        return parseGrid(getCached(url) ?: return emptyList())
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val page = getCached("$BASE/${media.id}/") ?: return media
        val ogTitle = metaProperty(page, "og:title")?.let { unescapeEntities(it) }
        val title = ogTitle
            ?.replace(Regex("""\s*-\s*FKBAE\s*$"""), "")
            ?.takeIf { it.isNotBlank() } ?: media.title
        val description = metaProperty(page, "og:description")?.let { unescapeEntities(it) }
        val poster = metaProperty(page, "og:image") ?: media.posterUrl
        val published = metaProperty(page, "article:published_time")
        val year = published?.take(4)?.toIntOrNull() ?: media.year
        return media.copy(
            title = title,
            posterUrl = poster,
            overview = description?.takeIf { it.isNotBlank() },
            year = year,
        )
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val page = getCached("$BASE/${media.id}/") ?: return emptyList()
        // The post embeds the player iframe: snstrhls.php (fluid player, an
        // HLS <source>) for HLS posts, snstr.php (JWPlayer, file:"…") for the
        // direct-MP4 posts — grab whichever one this post uses.
        val playerUrl = Regex("""<iframe[^>]*src="([^"]*snstr(?:hls)?\.php\?fileid=[A-Za-z0-9]+)""")
            .find(page)?.groupValues?.get(1)
            ?.let { if (it.startsWith("http")) it else "$BASE$it" }
        if (playerUrl.isNullOrBlank()) return emptyList()
        val player = getCached(playerUrl) ?: return emptyList()
        // The player page serves exactly one source: an m3u8 playlist (fluid
        // player <source>) for HLS posts, a direct stream.fkbae.to/<id>.mp4
        // (JWPlayer file:"…") for the rest. Grab whichever it is, with a
        // generic https URL fallback for anything else.
        val source = Regex("""<source[^>]*src="(https://[^"]+)"""")
            .find(player)?.groupValues?.get(1)
            ?.let { unescapeEntities(it) }
            ?: Regex("""file\s*:\s*"(https://[^"]+)"""")
                .find(player)?.groupValues?.get(1)
                ?.let { unescapeEntities(it) }
            ?: Regex("""https://[^\s"'<>]+\.(?:m3u8|mp4)[^\s"'<>]*""")
                .find(player)?.groupValues?.get(0)
                ?.let { unescapeEntities(it) }
        if (source.isNullOrBlank()) return emptyList()
        val isHls = source.contains(".m3u8", ignoreCase = true)
        return listOf(
            HikariStream(
                name = if (isHls) "HLS" else "MP4",
                url = source,
                headers = emptyMap(),
                isM3u8 = isHls,
            )
        )
    }

    // ------------------------------------------------------------------
    //  HTML helpers
    // ------------------------------------------------------------------

    /** Parses the Fl Builder post grid: `post-<id>` cards with a `rel="bookmark"` link + thumbnail. */
    private fun parseGrid(html: String): List<HikariMedia> {
        val cardRe = Regex(
            """fl-post-grid-post[^>]*post-(\d+)[^>]*>\s*<div class="fl-post-grid-image">\s*<a href="https://fkbae\.to/\d+/"[^>]*rel="bookmark" title="([^"]*)"[^>]*>\s*<img[^>]*src="([^"]+)" """
        )
        val out = LinkedHashMap<String, HikariMedia>()
        for (m in cardRe.findAll(html)) {
            val id = m.groupValues[1]
            if (id.isBlank()) continue
            val title = unescapeEntities(m.groupValues[2]).takeIf { it.isNotBlank() } ?: continue
            out[id] = HikariMedia(
                id = id,
                title = title,
                type = HikariMediaType.MOVIE,
                posterUrl = m.groupValues[3].takeIf { it.startsWith("http") },
            )
        }
        return out.values.toList()
    }

    private fun metaProperty(html: String, prop: String): String? =
        Regex("""<meta[^>]*property="$prop"[^>]*content="([^"]*)" """").find(html)?.groupValues?.get(1)

    private fun unescapeEntities(s: String): String = s
        .replace("&#8217;", "'")
        .replace("&#8211;", "–")
        .replace("&#8212;", "—")
        .replace("&#8230;", "…")
        .replace("&#x27;", "'")
        .replace("&#39;", "'")
        .replace("&quot;", "\"")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace(Regex("""\s+"""), " ")
        .trim()

    private fun encode(s: String): String =
        URLEncoder.encode(s, "UTF-8").replace("+", "%20")

    private suspend fun getCached(url: String): String? {
        val now = System.currentTimeMillis()
        cacheMutex.withLock {
            htmlCache[url]?.let { (t, html) ->
                if (now - t < CACHE_TTL_MS) return html
            }
        }
        // getStringSmart: plain okhttp first; if the CDN answers with a
        // challenge page (or refuses entirely) it re-fetches in a real WebView
        // and returns the rendered HTML — the same browser that fkbae.to serves.
        val html = HikariNet.getStringSmart(url, pageHeaders) ?: return null
        cacheMutex.withLock {
            if (htmlCache.size > 40) htmlCache.clear()
            htmlCache[url] = now to html
        }
        return html
    }
}
