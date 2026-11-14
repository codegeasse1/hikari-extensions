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
 * MissAV (missav.ws) — free Japanese AV as Hikari extensions.
 *
 * Uses only public, unauthenticated pages:
 *  - browse/release/hot/genre pages are plain HTML with a video card grid,
 *    paginated with `?page=N` (`https://missav.ws/en/release`, `/today-hot`,
 *    `/uncensored-leak`, `/genres/<Name>` …),
 *  - a video page (`https://missav.ws/en/<id>`) embeds a Dean-Edwards-packed
 *    `eval(function(p,a,c,k,e,d)...)` block whose `source` is the signed
 *    `https://surrit.com/<uuid>/playlist.m3u8` LL-HLS manifest,
 *  - the CDN sits behind Cloudflare, so the stream is captured with a real
 *    WebView load of the video page (the site's own hls.js fires the request
 *    and the captured request headers — including the Cloudflare cookie — are
 *    forwarded to the player). If that ever fails, the packed m3u8 is still
 *    returned directly as a fallback.
 *
 * The homepage is built from many server-rendered rows (new releases, hot
 * today, uncensored leak + popular genre grids) so it fills the screen like
 * the Chaturbate extension.
 */
class MissavProvider : HikariProvider {

    override val id = "missav"
    override val name = "MissAV (JAV)"
    override val mainUrl = "https://missav.ws"
    override val description = "Free JAV — new releases, hot today, uncensored leaks and 100k+ genre videos."
    override val iconUrl: String? = null
    override val tvTypes = setOf(HikariMediaType.MOVIE)
    override val version = 4

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? = null

    companion object {
        private const val BASE = "https://missav.ws/en"
        private const val CDN = "https://fourhoi.com"
        private const val CACHE_TTL_MS = 600_000L
        private val htmlCache = HashMap<String, Pair<Long, String>>()
        private val cacheMutex = Mutex()
    }

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("release", "New Releases", HikariMediaType.MOVIE),
        HikariCatalog("today-hot", "Hot Today", HikariMediaType.MOVIE),
        HikariCatalog("uncensored", "Uncensored Leak", HikariMediaType.MOVIE),
        HikariCatalog("big-breasts", "Big Breasts", HikariMediaType.MOVIE),
        HikariCatalog("mature", "Mature Woman", HikariMediaType.MOVIE),
        HikariCatalog("creampie", "Creampie", HikariMediaType.MOVIE),
        HikariCatalog("wife", "Wife", HikariMediaType.MOVIE),
        HikariCatalog("pretty-girl", "Pretty Girl", HikariMediaType.MOVIE),
        HikariCatalog("oral", "Oral Sex", HikariMediaType.MOVIE),
        HikariCatalog("orgy", "Orgy", HikariMediaType.MOVIE),
        HikariCatalog("hd", "HD & 4K", HikariMediaType.MOVIE),
        HikariCatalog("exclusive", "Exclusive", HikariMediaType.MOVIE),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val path = catalogPath(catalog.id) ?: return emptyList()
        return parseCards(getCached(browseUrl(path, page)) ?: return emptyList())
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        val encoded = URLEncoder.encode(q, "UTF-8").replace("+", "%20")
        val url = if (page <= 1) "$BASE/search/$encoded" else "$BASE/search/$encoded?page=$page"
        return parseCards(getCached(url) ?: return emptyList())
    }

    // ------------------------------------------------------------------
    //  Meta + streams (from the video page)
    // ------------------------------------------------------------------

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val page = getCached("$BASE/${media.id}") ?: return media
        // og:title on a real video page is the full video title; on a dead/
        // removed video MissAV serves the generic site title instead.
        val ogTitle = metaProperty(page, "og:title")?.let { unescapeEntities(it) }
        val title = ogTitle?.takeIf { !it.startsWith("MissAV") } ?: media.title
        // og:image on real video pages is the LANDSCAPE cover (cover-n.jpg);
        // the poster/thumbnail (cover-t.jpg) stays as-is.
        val backdrop = metaProperty(page, "og:image")?.takeIf { it.startsWith("$CDN/") }
        val actor = metaProperty(page, "og:video:actor")
        val durationSec = metaProperty(page, "og:video:duration")?.toIntOrNull()
        val release = metaProperty(page, "og:video:release_date")
        val year = release?.take(4)?.toIntOrNull()
        val overview = buildString {
            if (!actor.isNullOrBlank()) append("Starring: ").append(actor)
            if (durationSec != null) {
                if (isNotEmpty()) append("\n")
                append("Runtime: ").append(formatDuration(durationSec))
            }
            if (!release.isNullOrBlank()) {
                if (isNotEmpty()) append(" · ")
                append(release)
            }
        }.trim()
        return media.copy(
            title = title,
            posterUrl = media.posterUrl,
            year = year ?: media.year,
            overview = overview.ifBlank { null },
            backdropUrl = backdrop ?: media.backdropUrl,
        )
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val pageUrl = "$BASE/${media.id}"

        // Primary: real WebView load — the site's own hls.js requests the CDN
        // manifest, passes the Cloudflare challenge, and we capture the request
        // URL + headers (including the cookie) for the player. The CDN
        // (surrit.com) answers plain clients with a Cloudflare 403 challenge,
        // so ONLY the captured request (which carries the session cookie) can
        // actually play — the packed-blob URL below is a last resort for when
        // a plain fetch gets through. Timeout stays under the app's 45s
        // per-provider stream budget so the fallback still gets a chance.
        val captured = HikariNet.resolveWithWebView(
            pageUrl,
            capture = Regex("https://surrit\\.com/[^\"'\\s]+\\.m3u8(\\?[^\"'\\s]*)?", RegexOption.IGNORE_CASE),
            additional = listOf(
                Regex("https://[^\"'\\s]+\\.m3u8(\\?[^\"'\\s]*)?", RegexOption.IGNORE_CASE),
                Regex("https://[^\"'\\s]+/master\\.m3u8(\\?[^\"'\\s]*)?", RegexOption.IGNORE_CASE),
            ),
            timeoutMs = 40_000,
        )
        // Prefer the site's own manifest, then any other m3u8 the player fired.
        val hit = captured.firstOrNull { it.url.contains("surrit.com", ignoreCase = true) }
            ?: captured.firstOrNull { it.url.contains(".m3u8", ignoreCase = true) }
        hit?.let {
            return listOf(
                HikariStream(
                    name = "HLS",
                    url = it.url,
                    headers = it.headers,
                    isM3u8 = true,
                )
            )
        }

        // Fallback: unpack the embedded manifest URL directly.
        val page = getCached(pageUrl) ?: return emptyList()
        val m3u8 = decodePackedStream(page) ?: return emptyList()
        return listOf(
            HikariStream(
                name = "HLS",
                url = m3u8,
                headers = mapOf(
                    "Referer" to "https://missav.ws/",
                    "User-Agent" to HikariNet.browserHeaders.getValue("User-Agent"),
                ),
                isM3u8 = true,
            )
        )
    }

    // ------------------------------------------------------------------
    //  HTML helpers
    // ------------------------------------------------------------------

    private fun catalogPath(id: String): String? = when (id) {
        "release" -> "/release"
        "today-hot" -> "/today-hot"
        "uncensored" -> "/uncensored-leak"
        "big-breasts" -> "/genres/Big%20Breasts"
        "mature" -> "/genres/Mature%20Woman"
        "creampie" -> "/genres/Creampie"
        "wife" -> "/genres/Wife"
        "pretty-girl" -> "/genres/Pretty%20Girl"
        "oral" -> "/genres/Oral%20Sex"
        "orgy" -> "/genres/Orgy"
        "hd" -> "/genres/Hd"
        "exclusive" -> "/genres/Exclusive"
        else -> null
    }

    private fun browseUrl(path: String, page: Int): String =
        if (page <= 1) "$BASE$path" else "$BASE$path?page=$page"

    /** Fetches an HTML page once per CACHE_TTL_MS (home builds many rows). */
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

    /** Extracts the video-card grid from a browse/search page. */
    private fun parseCards(html: String): List<HikariMedia> {
        // Card blocks: <div class="thumbnail group"> … </div></div>. MissAV
        // now serves result-card links behind CDN-group prefixes
        // (https://missav.ws/dm14/en/<id>) alongside the plain /en/<id> form,
        // so the link accepts an optional <group>/ segment. Posters are taken
        // from the card's own lazy-loaded <img data-src="…cover-t.jpg"> instead
        // of being reconstructed from the CDN root.
        val cardRe = Regex(
            """class="thumbnail group"[\s\S]*?</div>\s*</div>"""
        )
        val posterRe = Regex("""class="lozad w-full"\s*data-src="([^"]*)"""")
        val linkRe = Regex(
            """class="text-secondary group-hover:text-primary"\s*href="https://missav\.ws/(?:[a-z0-9]+/)?en/([^"?#]+)"[^>]*>\s*([\s\S]*?)\s*</a>"""
        )
        val out = LinkedHashMap<String, HikariMedia>()
        for (card in cardRe.findAll(html)) {
            val block = card.value
            val link = linkRe.find(block) ?: continue
            val id = link.groupValues[1]
            if (!Regex("^[a-z0-9-]+$").matches(id)) continue
            val title = unescapeEntities(link.groupValues[2]).trim()
            if (title.isBlank()) continue
            val poster = posterRe.find(block)?.groupValues?.get(1)
            out[id] = HikariMedia(
                id = id,
                title = title,
                type = HikariMediaType.MOVIE,
                posterUrl = poster ?: "$CDN/$id/cover-t.jpg",
            )
        }
        return out.values.toList()
    }

    private fun metaProperty(html: String, prop: String): String? =
        Regex("""<meta\s+property="$prop"\s+content="([^"]*)"""")
            .find(html)?.groupValues?.get(1)

    private fun unescapeEntities(s: String): String = s
        .replace("&quot;", "\"")
        .replace("&#039;", "'")
        .replace("&#39;", "'")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&nbsp;", " ")
        .replace(Regex("""&#(\d+);""")) { m ->
            m.groupValues[1].toIntOrNull()?.let { runCatching { it.toChar().toString() }.getOrNull() } ?: m.value
        }
        .replace(Regex("""\s+"""), " ")
        .trim()

    private fun formatDuration(totalSec: Int): String {
        val h = totalSec / 3600
        val m = (totalSec % 3600) / 60
        val s = totalSec % 60
        return if (h > 0) "%d:%02d:%02d".format(h, m, s)
        else "%d:%02d".format(m, s)
    }

    // ------------------------------------------------------------------
    //  Dean-Edwards packed eval decoder → m3u8
    // ------------------------------------------------------------------

    /**
     * The video page embeds `eval(function(p,a,c,k,e,d){...}('PAYLOAD',…,
     * 'k0|k1|…|kn'.split('|'),0,{}))`. The payload uses base-36 tokens for
     * each dictionary index; unpacking yields `source='https://…m3u8'` (the
     * master playlist) plus `source842`/`source1280` (single renditions).
     */
    private fun decodePackedStream(page: String): String? {
        val blobRe = Regex(
            """eval\(function\(p,a,c,k,e,d\)\{.*?,0,\{\}\)\)""",
            setOf(RegexOption.DOT_MATCHES_ALL)
        )
        for (blob in blobRe.findAll(page)) {
            val decoded = unpack(blob.value) ?: continue
            Regex("""\bsource='([^']+)'""").find(decoded)?.let { m ->
                val url = m.groupValues[1]
                if (url.contains(".m3u8")) return url
            }
        }
        return null
    }

    private fun unpack(blob: String): String? {
        val payloadM = Regex("""\}\('((?:[^'\\]|\\.)*)','""").find(blob)
            ?: Regex("""\}\('((?:[^'\\]|\\.)*)',""").find(blob)
            ?: return null
        val dictM = Regex("""'((?:[^'\\]|\\.)*)'\.split\('\|'\)""").find(blob) ?: return null
        var p = payloadM.groupValues[1].replace("\\'", "'")
        val dict = dictM.groupValues[1].replace("\\'", "'").split('|')
        for (i in dict.size - 1 downTo 0) {
            val token = i.toString(36)
            val v = dict[i]
            if (v.isNotEmpty()) {
                p = p.replace(Regex("\\b" + Regex.escape(token) + "\\b"), v)
            }
        }
        return p
    }
}
