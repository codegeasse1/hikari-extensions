package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import org.json.JSONObject
import java.net.URLEncoder

/**
 * Anime World India — Hindi/Tamil/Telugu anime & cartoons (WordPress).
 *
 * Faithful native port of KSHITIJ8473/raghav AnimeWorldProvider, verified
 * live Oct 2026:
 *  - Domain from the Firebase table (animeworldindia_url / animeworld_url /
 *    awi_url; canonical moved watchanimeworld.top -> watchanimeworld.one).
 *  - The homepage is JS-rendered (static HTML only holds ${item} templates),
 *    so site pages go through getStringSmart (rendered fetch), selecting
 *    li.post / article.post cards with a[href*=/series/] or /movies/.
 *  - 6 rows: Latest Series, Latest Movies, Franchises
 *    (a[href*=/category/franchise/]), Trending, Popular, All.
 *  - Search walks /page/1..3/?s=.
 *  - Detail: h1, .poster img, plot, a[href*=genre]; movies play inline;
 *    series use data-post + data-season tabs with the admin-ajax
 *    action_select_season endpoint; episode slugs carry (\d+)x(\d+).
 *  - Playback: zephyrix/zephyrflick iframes resolve via
 *    POST {player}/player/index.php?data={id}&do=getVideo (form hash+r) to
 *    videoSource/securedLink M3U8 (all audio renditions in one playlist);
 *    other iframes fall back to WebView capture.
 */
class AnimeWorldIndiaProvider : HikariProvider {

    override val id = "animeworldindia"
    override val name = "Anime World India"
    override val mainUrl = "https://watchanimeworld.one"
    override val description = "Hindi, Tamil, Telugu anime & cartoons from Anime World India."
    override val version = 2
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val ua =
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

    private fun pageHeaders(referer: String? = null): Map<String, String> {
        val h = mutableMapOf(
            "User-Agent" to ua,
            "Accept" to "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        )
        if (referer != null) h["Referer"] = referer
        return h
    }

    @Volatile
    private var base: String? = null

    private suspend fun domain(): String {
        base?.let { return it }
        var d = mainUrl
        try {
            val text = HikariNet.getString("https://cloudstreampluginhelper-default-rtdb.firebaseio.com/.json")
            if (text != null) {
                val o = JSONObject(text)
                val url = o.optString("animeworldindia_url").ifBlank { o.optString("animeworld_url") }
                    .ifBlank { o.optString("awi_url") }
                if (url.startsWith("http")) d = url.trimEnd('/')
            }
        } catch (_: Exception) {
        }
        base = d
        return d
    }

    private suspend fun siteGet(url: String, referer: String? = null): String? =
        HikariNet.getStringSmart(url, pageHeaders(referer))

    private fun strip(s: String): String =
        s.replace(Regex("<[^>]+>"), " ").replace(Regex("\\s+"), " ").trim()

    private fun unesc(s: String): String = s.replace("&amp;", "&").replace("&#39;", "'")
        .replace("&#039;", "'").replace("&quot;", "\"").replace("&lt;", "<")
        .replace("&gt;", ">").replace("&nbsp;", " ").trim()

    private fun posterOf(block: String): String? {
        // skip unrendered ${item.image} template cards
        if (block.contains("\${item.")) return null
        val img = Regex(
            """<img[^>]*>""",
            RegexOption.IGNORE_CASE,
        ).find(block)?.value ?: return null
        var src = Regex("""data-src="([^"]+)"""", RegexOption.IGNORE_CASE)
            .find(img)?.groupValues?.get(1)
            ?: Regex("""src="([^"]+)"""", RegexOption.IGNORE_CASE)
                .find(img)?.groupValues?.get(1).orEmpty()
        if (src.isBlank() || src.contains("\${")) return null
        if (src.startsWith("//")) src = "https:$src"
        return src.takeIf { it.startsWith("http") }
    }

    private data class Card(val title: String, val url: String, val poster: String?, val isMovie: Boolean)

    private fun cards(html: String): List<Card> {
        val out = ArrayList<Card>()
        val seen = HashSet<String>()
        val blocks = Regex(
            """<(?:li|article)[^>]*class="[^"]*post[^"]*"[^>]*>(.*?)</(?:li|article)>""",
            setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
        ).findAll(html)
        for (b in blocks) {
            val body = b.groupValues[1]
            if (body.contains("\${item.")) continue
            val link = Regex(
                """<a[^>]*href="([^"]*(?:/series/|/movies/)[^"]*)"""",
                RegexOption.IGNORE_CASE,
            ).find(body)?.groupValues?.get(1)?.trim() ?: continue
            if (link.endsWith("/series/") || link.endsWith("/movies/")) continue
            if (!seen.add(link)) continue
            val title = Regex(
                """<h[23][^>]*>(.*?)</h[23]>""",
                setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
            ).find(body)?.groupValues?.get(1)?.let { unesc(strip(it)) }
                ?.takeIf { it.isNotBlank() && !it.contains("\${") } ?: continue
            out.add(Card(title, link, posterOf(body), link.contains("/movies/")))
        }
        return out
    }

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("latest", "Latest Series", HikariMediaType.SERIES),
        HikariCatalog("movies", "Latest Movies", HikariMediaType.MOVIE),
        HikariCatalog("franchise", "Franchises", HikariMediaType.SERIES),
        HikariCatalog("trending", "Trending Anime", HikariMediaType.SERIES),
        HikariCatalog("popular", "Popular Series", HikariMediaType.SERIES),
        HikariCatalog("all", "All", HikariMediaType.SERIES),
    )

    private fun toMedia(c: Card): HikariMedia =
        HikariMedia(
            id = c.url, title = c.title,
            type = if (c.isMovie) HikariMediaType.MOVIE else HikariMediaType.SERIES,
            posterUrl = c.poster,
        )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val d = domain()
        if (catalog.id == "franchise") {
            val html = siteGet(d, "$d/") ?: return emptyList()
            val out = ArrayList<HikariMedia>()
            val seen = HashSet<String>()
            for (m in Regex(
                """<a[^>]*href="([^"]*/category/franchise/[^"]*)"[^>]*>(.*?)</a>""",
                setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
            ).findAll(html)) {
                val href = m.groupValues[1].trim()
                if (!href.startsWith("http") || !seen.add(href)) continue
                val inner = m.groupValues[2]
                if (inner.contains("\${")) continue
                val title = unesc(strip(inner)).takeIf { it.isNotBlank() } ?: continue
                val img = Regex("""<img[^>]+(?:data-src|src)="([^"]+)"""", RegexOption.IGNORE_CASE)
                    .find(inner)?.groupValues?.get(1)?.let {
                        var s = it
                        if (s.startsWith("//")) s = "https:$s"
                        s.takeIf { it.startsWith("http") }
                    }
                out.add(HikariMedia(id = href, title = title, type = HikariMediaType.SERIES, posterUrl = img))
            }
            return out
        }
        val p = page.coerceAtLeast(1)
        val url = if (p <= 1) "$d/" else "$d/page/$p/"
        val html = siteGet(url, "$d/") ?: return emptyList()
        val all = cards(html)
        if (all.isEmpty()) return emptyList()
        val pool = when (catalog.id) {
            "latest" -> all.filter { !it.isMovie }.take(20)
            "movies" -> all.filter { it.isMovie }.take(20)
            "trending" -> all.drop(20).take(20).ifEmpty { all.takeLast(20) }
            "popular" -> all.reversed().take(20)
            else -> all.take(30)
        }
        return pool.map { toMedia(it) }
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank()) return emptyList()
        val d = domain()
        val out = ArrayList<HikariMedia>()
        val seen = HashSet<String>()
        val enc = URLEncoder.encode(query.trim(), "UTF-8")
        for (p in 1..3) {
            try {
                val html = siteGet("$d/page/$p/?s=$enc", "$d/") ?: break
                val items = cards(html)
                if (items.isEmpty()) break
                for (c in items) {
                    if (seen.add(c.url)) out.add(toMedia(c))
                }
            } catch (_: Exception) {
                break
            }
        }
        return out
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        // franchise rows point at /category/ pages; keep them browsable as-is
        if (media.id.contains("/category/")) return media
        val d = domain()
        val html = siteGet(media.id, "$d/") ?: return media
        val title = Regex(
            """<h1[^>]*>(.*?)</h1>""",
            setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
        ).find(html)?.groupValues?.get(1)?.let { unesc(strip(it)) }
            ?.takeIf { it.isNotBlank() } ?: media.title
        fun AbsImg(re: Regex): String? {
            val m = re.find(html) ?: return null
            var src = m.groupValues[1]
            if (src.startsWith("//")) src = "https:$src"
            return src.takeIf { it.startsWith("http") }
        }
        val poster = AbsImg(
            Regex(
                """<(?:div|section)[^>]*class="[^"]*(?:poster|post-thumbnail)[^"]*"[^>]*>[\s\S]*?<img[^>]+(?:data-src|src)="([^"]+)"""",
                RegexOption.IGNORE_CASE,
            ),
        ) ?: media.posterUrl
        val backdrop = AbsImg(
            Regex(
                """<(?:div|section)[^>]*class="[^"]*(?:backdrop|bg)[^"]*"[^>]*>[\s\S]*?<img[^>]+(?:data-src|src)="([^"]+)"""",
                RegexOption.IGNORE_CASE,
            ),
        )
        val plot = Regex(
            """<(?:div|section)[^>]*class="[^"]*(?:description|synopsis|plot)[^"]*"[^>]*>(.*?)</(?:div|section)>""",
            setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
        ).find(html)?.groupValues?.get(1)?.let { unesc(strip(it)) }?.takeIf { it.length > 10 }
            ?: media.overview
        val genres = Regex("""<a[^>]*href="[^"]*genre[^"]*"[^>]*>([^<]+)</a>""", RegexOption.IGNORE_CASE)
            .findAll(html).map { unesc(it.groupValues[1].trim()) }.filter { it.isNotBlank() }.toList()
        val type = if (media.id.contains("/movies/")) HikariMediaType.MOVIE else media.type
        return media.copy(
            title = title, type = type, posterUrl = poster,
            backdropUrl = backdrop ?: poster, overview = plot,
            genres = genres.ifEmpty { media.genres },
        )
    }

    private fun epName(epUrl: String): String {
        val raw = epUrl.substringAfter("/episode/").trimEnd('/')
        val epNum = Regex("""(\d+)x(\d+)""").find(raw)?.groupValues?.get(2)?.toIntOrNull()
        val slug = raw.replace(Regex("""\d+x\d+"""), "").trim('-')
        val parts = slug.split("-").filter { it.isNotBlank() && it != "ep" && it != "episode" }
        var name = parts.joinToString(" ").replace("/", " ")
            .replace(Regex("\\s+"), " ").trim()
        if (name.isNotBlank()) {
            name = name.replaceFirstChar { if (it.isLowerCase()) it.titlecase() else it.toString() }
            return name
        }
        return if (epNum != null) "Episode $epNum" else slug.ifBlank { "Episode" }
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        if (media.id.contains("/category/") || media.id.contains("/movies/")) {
            return listOf(HikariEpisode(number = 1, id = media.id, name = media.title, image = media.posterUrl))
        }
        val d = domain()
        val html = siteGet(media.id, "$d/") ?: return null
        val postId = Regex("""data-post="([^"]+)"""").find(html)?.groupValues?.get(1).orEmpty()
        val seasons = Regex("""data-season="(\d+)"""").findAll(html)
            .mapNotNull { it.groupValues[1].toIntOrNull() }
            .distinct().sorted()
        val initial = Regex("""<a[^>]*href="([^"]*?/episode/[^"]*)"""", RegexOption.IGNORE_CASE)
            .findAll(html).map { it.groupValues[1] }.distinct().toList()
        val initialSeason = initial.firstOrNull()?.let {
            Regex("""(\d+)x(\d+)""").find(it)?.groupValues?.get(1)?.toIntOrNull()
        } ?: seasons.firstOrNull() ?: 1
        val full = (1..(seasons.maxOrNull() ?: initialSeason)).toList()
        val out = ArrayList<HikariEpisode>()
        coroutineScope {
            full.map { season ->
                async(Dispatchers.IO) {
                    if (season == initialSeason && initial.isNotEmpty()) initial
                    else seasonEps(d, postId, season)
                }
            }.forEach { def ->
                val links = try {
                    def.await()
                } catch (_: Exception) {
                    emptyList()
                }
                for (u in links) {
                    val m = Regex("""(\d+)x(\d+)""").find(u)
                    val sn = m?.groupValues?.get(1)?.toIntOrNull()
                        ?: (out.map { it.season }.maxOrNull() ?: 1)
                    val num = m?.groupValues?.get(2)?.toIntOrNull()
                        ?: (out.count { it.season == sn } + 1)
                    if (out.any { it.id == u }) continue
                    out.add(HikariEpisode(number = num, season = sn, id = u, name = epName(u), image = media.posterUrl))
                }
            }
        }
        if (out.isEmpty() && initial.isNotEmpty()) {
            for (u in initial) {
                val m = Regex("""(\d+)x(\d+)""").find(u)
                val sn = m?.groupValues?.get(1)?.toIntOrNull() ?: 1
                val num = m?.groupValues?.get(2)?.toIntOrNull() ?: (out.size + 1)
                out.add(HikariEpisode(number = num, season = sn, id = u, name = epName(u), image = media.posterUrl))
            }
        }
        out.sortBy { it.season * 100000 + it.number }
        return out.ifEmpty { null }
    }

    private suspend fun seasonEps(d: String, postId: String, season: Int): List<String> {
        if (postId.isBlank()) return emptyList()
        return try {
            val html = HikariNet.getString(
                "$d/wp-admin/admin-ajax.php?action=action_select_season&season=$season&post=$postId",
                pageHeaders("$d/") + mapOf("X-Requested-With" to "XMLHttpRequest"),
            ) ?: return emptyList()
            Regex("""<a[^>]*href="([^"]*?/episode/[^"]*)"""", RegexOption.IGNORE_CASE)
                .findAll(html).map { it.groupValues[1] }.distinct().toList()
        } catch (_: Exception) {
            emptyList()
        }
    }

    private val capture = Regex("""https?://[^"' \s]+\.m3u8[^"' \s]*|https?://[^"' \s]+\.mp4[^"' \s]*""")

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val d = domain()
        val pageUrl = episode?.id?.takeIf { it.startsWith("http") } ?: media.id
        val html = siteGet(pageUrl, "$d/") ?: return emptyList()
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        val iframes = Regex(
            """<iframe[^>]*(?:data-src|src)="([^"]+)"""",
            RegexOption.IGNORE_CASE,
        ).findAll(html).map { it.groupValues[1] }.filter { it.startsWith("http") }.distinct().toList()
        for (iframe in iframes) {
            if (iframe.contains("zephyrix") || iframe.contains("zephyrflick")) {
                try {
                    val videoId = Regex("""/video/([a-f0-9]+)""").find(iframe)?.groupValues?.get(1)
                        ?: continue
                    val playerBase = Regex("""(https?://[^/]+)""").find(iframe)?.groupValues?.get(1)
                        ?: continue
                    val body = "hash=${URLEncoder.encode(videoId, "UTF-8")}&r=${URLEncoder.encode("$d/", "UTF-8")}"
                    val resp = HikariNet.postString(
                        "$playerBase/player/index.php?data=$videoId&do=getVideo",
                        body,
                        mapOf(
                            "X-Requested-With" to "XMLHttpRequest",
                            "Origin" to playerBase,
                            "Referer" to "$playerBase/video/$videoId",
                            "Accept" to "application/json, text/javascript, */*; q=0.01",
                            "User-Agent" to ua,
                        ),
                        "application/x-www-form-urlencoded; charset=UTF-8",
                    ) ?: continue
                    val jo = JSONObject(resp)
                    var streamUrl = jo.optString("videoSource").ifBlank { jo.optString("securedLink") }
                    if (streamUrl.isBlank()) continue
                    if (!streamUrl.startsWith("http")) streamUrl = playerBase + streamUrl
                    if (!seen.add(streamUrl)) continue
                    out.add(
                        HikariStream(
                            name = "[AWI]", url = streamUrl,
                            headers = mapOf(
                                "Referer" to "$playerBase/",
                                "Accept" to "*/*",
                                "User-Agent" to ua,
                            ),
                            isM3u8 = true,
                        ),
                    )
                } catch (_: Exception) {
                }
            } else {
                try {
                    for (r in HikariNet.resolveWithWebView(iframe, capture, timeoutMs = 45_000)) {
                        if (!seen.add(r.url)) continue
                        out.add(
                            HikariStream(
                                name = "[AWI]", url = r.url, headers = r.headers,
                                isM3u8 = r.url.contains("m3u8"),
                            ),
                        )
                    }
                } catch (_: Exception) {
                }
            }
        }
        return out
    }
}
