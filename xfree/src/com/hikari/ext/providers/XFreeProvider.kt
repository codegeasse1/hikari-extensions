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
 * XFree (xfree.com) — TikTok-style NSFW reels & shorts.
 *
 * Pure server-side-rendered Nuxt pages, no API needed:
 *  - browse walls: `/` (trending), `/all`, `/gay`, `/trans` and
 *    `/playlist/<id>-<slug>` — all paginate with `?page=N`,
 *  - a video page `/video?id=<id>&title=<slug>` embeds the direct MP4 in its
 *    `og:video` meta tag (CDN-signed), plus title/poster/likes in the other
 *    og tags,
 *  - search: `/search?q=<query>` (also paginated with `?page=N`).
 */
class XFreeProvider : HikariProvider {

    override val id = "xfree"
    override val name = "XFree"
    override val mainUrl = "https://www.xfree.com"
    override val description = "TikTok-style porn reels & shorts — trending, all, gay, trans and curated playlists with direct MP4 streams."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.MOVIE)

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? = null

    companion object {
        private const val BASE = "https://www.xfree.com"
        private const val CACHE_TTL_MS = 600_000L

        private val htmlCache = HashMap<String, Pair<Long, String>>()
        private val cacheMutex = Mutex()

        // Curated playlists shown on the site (id to name) — each has 80-200 posts.
        private val playlists = listOf(
            154L to "All Plump, All Yours",
            164L to "Backdoor Obsession",
            211L to "Best of April",
            144L to "Best of July",
            180L to "Best of Year",
            201L to "Breast Mania",
            135L to "Creampie Royalty!",
            203L to "Full Fist",
            163L to "Full Team Penetration",
            162L to "Golden Rituals",
            187L to "Gourmet Kinks",
            202L to "Hands of Lust",
            172L to "Hidden Kinks",
            210L to "Innocent Tease",
            204L to "Live Flirt",
            179L to "Made by Us!",
            20L to "Photo Gayllery",
        )
    }

    override fun catalogs(): List<HikariCatalog> = buildList {
        add(HikariCatalog("trending", "Trending Reels", HikariMediaType.MOVIE))
        add(HikariCatalog("all", "All Reels", HikariMediaType.MOVIE))
        add(HikariCatalog("gay", "Gay Reels", HikariMediaType.MOVIE))
        add(HikariCatalog("trans", "Trans Reels", HikariMediaType.MOVIE))
        for ((id, name) in playlists) {
            add(HikariCatalog("pl_$id", name, HikariMediaType.MOVIE, rawType = id.toString()))
        }
    }

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val path = when {
            catalog.id == "trending" -> "/"
            catalog.id in setOf("all", "gay", "trans") -> "/${catalog.id}"
            catalog.id.startsWith("pl_") -> {
                val id = catalog.rawType
                val name = playlists.firstOrNull { it.first.toString() == id }?.second ?: "Playlist"
                "/playlist/$id-${encode(name)}"
            }
            else -> "/"
        }
        val url = if (page <= 1) "$BASE$path" else "$BASE$path?page=$page"
        return parseWall(getCached(url) ?: return emptyList())
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        val url = if (page <= 1) "$BASE/search?q=${encode(q)}" else "$BASE/search?q=${encode(q)}&page=$page"
        return parseWall(getCached(url) ?: return emptyList())
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val page = getCached(videoUrl(media)) ?: return media
        val ogTitle = metaProperty(page, "og:title")?.let { unescapeEntities(it) }
        val title = ogTitle
            ?.replace(Regex("""\s+-\s+@.*$"""), "")   // strip " - @creator's Sex Reel On xfree.com …"
            ?.takeIf { it.isNotBlank() } ?: media.title
        val creator = ogTitle?.let { Regex("@(\\w+)").find(it)?.groupValues?.get(1) }
        val poster = metaProperty(page, "og:image")
        val description = metaProperty(page, "og:description")?.let { unescapeEntities(it) }
        val likes = description?.let { Regex("""(\d[\d,]*) likes""").find(it)?.groupValues?.get(1) }
        val duration = metaProperty(page, "og:video:duration")?.toIntOrNull()

        val overview = buildString {
            if (creator != null) {
                append("Uploader: @").append(creator)
                if (likes != null) append("  ·  ♥ ").append(likes).append(" likes")
                if (duration != null) append("  ·  ⏱ ").append(formatDuration(duration))
            } else if (description != null) {
                append(description)
            }
        }.trim()

        return media.copy(
            title = title,
            posterUrl = poster ?: media.posterUrl,
            overview = overview.ifBlank { null },
            genres = media.genres,
        )
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val page = getCached(videoUrl(media)) ?: return emptyList()
        val mp4 = metaProperty(page, "og:video")?.let { unescapeEntities(it) }
        if (mp4.isNullOrBlank()) return emptyList()
        return listOf(
            HikariStream(
                name = "MP4",
                url = mp4,
                headers = emptyMap(),
                isM3u8 = false,
            )
        )
    }

    // ------------------------------------------------------------------
    //  HTML helpers
    // ------------------------------------------------------------------

    private fun videoUrl(media: HikariMedia): String {
        val id = media.id.substringBefore('|')
        val slug = media.id.substringAfter('|', "")
        return if (slug.isNotBlank()) "$BASE/video?id=$id&title=$slug" else "$BASE/video?id=$id"
    }

    /** Parses a SSR wall: `<a href="/video?id=…&title=…" class="wall__item__media"><img … src="…">`. */
    private fun parseWall(html: String): List<HikariMedia> {
        val cardRe = Regex(
            """<a href="/video\?id=(\d+)&amp;title=([^"]+)" class="wall__item__media"[^>]*>\s*<img[^>]*src="([^"]+)""""
        )
        val out = LinkedHashMap<String, HikariMedia>()
        for (m in cardRe.findAll(html)) {
            val id = m.groupValues[1]
            val slug = m.groupValues[2]
            if (id.isBlank()) continue
            val title = slugifyTitle(slug)
            if (title.isBlank()) continue
            out[id] = HikariMedia(
                id = "$id|$slug",
                title = title,
                type = HikariMediaType.MOVIE,
                posterUrl = m.groupValues[3],
            )
        }
        return out.values.toList()
    }

    private fun slugifyTitle(slug: String): String {
        val words = slug.split('-').filter { it.isNotBlank() }
        if (words.isEmpty()) return ""
        return words.joinToString(" ") { w ->
            if (w.length <= 2) w else w.replaceFirstChar { it.uppercaseChar() }
        }.replaceFirstChar { it.uppercaseChar() }
    }

    private fun metaProperty(html: String, prop: String): String? =
        Regex("""<meta[^>]*property="$prop"[^>]*content="([^"]*)"""").find(html)?.groupValues?.get(1)

    private fun formatDuration(totalSec: Int): String {
        val m = totalSec / 60
        val s = totalSec % 60
        return if (m > 0) "%d:%02d".format(m, s) else "${s}s"
    }

    private fun unescapeEntities(s: String): String = s
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
        val html = HikariNet.getString(url) ?: return null
        cacheMutex.withLock {
            if (htmlCache.size > 40) htmlCache.clear()
            htmlCache[url] = now to html
        }
        return html
    }
}
