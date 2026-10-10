package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import com.hikari.ext.HikariSubtitle
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import org.json.JSONObject

/**
 * 9anime (9anime.org.lv) — WordPress anime grid + base64 mirror embeds.
 *
 * Faithful native port of KSHITIJ8473/raghav NineAnime, verified live Oct 2026:
 *  - 8 catalog rows: Latest (home article.bs episode cards, resolved to their
 *    /anime/ detail pages), All Anime (/anime/?status=&type=&order=update) +
 *    6 genre rows (/genres/<slug>/, cards link /anime/ directly). Pagination
 *    /page/N/ (home, genres) and /anime/page/N/ (list).
 *  - Cards: article.bs > a[href] + title attr + img src + .typez (TV/Movie) +
 *    .sb Sub/Dub; genre/list cards carry .status Completed.
 *  - Detail /anime/<slug>/: h1.entry-title, .thumb img, .entry-content plot,
 *    .genxed tags, .info-content status/type, .eplister li (a href, .epl-num,
 *    .epl-title, .epl-date), newest-first so reversed. Non-/anime/ URLs
 *    (episode cards) resolve to their /anime/ page first, like the source.
 *  - Playback: select.mirror options are base64 iframes (HD-1 = gogo
 *    newplayer wrapper one level deeper, Omega = vidmoly, Moon = byse);
 *    plyr.php#base64 decodes inline, megaplay.buzz runs the native enc flow
 *    (NaMegaPlay), vidmoly matches file: m3u8, byse + unknown hosts fall back
 *    to WebView capture.
 */
class NineAnimeProvider : HikariProvider {

    override val id = "nineanime"
    override val name = "9anime"
    override val mainUrl = "https://9anime.org.lv"
    override val description = "Anime from 9anime — latest, all anime, genres, sub & dub mirrors."
    override val version = 2
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private fun pageHeaders(referer: String? = null): Map<String, String> {
        val h = mutableMapOf(
            "User-Agent" to NaNet.UA,
            "Accept" to "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        )
        if (referer != null) h["Referer"] = referer
        return h
    }

    @Volatile
    private var base: String? = null

    private suspend fun domain(): String {
        base?.let { return it }
        val remote = try {
            HikariNet.getString("https://cloudstreampluginhelper-default-rtdb.firebaseio.com/.json")
                ?.let { JSONObject(it).optString("nineanime") }
                ?.takeIf { it.startsWith("http") }?.trimEnd('/')
        } catch (_: Exception) {
            null
        }
        // the table currently holds no nineanime key, so this stays on mainUrl
        // until the maintainer publishes one; any value is liveness-checked
        if (remote != null) {
            try {
                val probe = HikariNet.getString("$remote/", pageHeaders())
                if (probe != null && probe.contains("<article")) {
                    base = remote
                    return remote
                }
            } catch (_: Exception) {
            }
        }
        base = mainUrl
        return mainUrl
    }

    private val capture = Regex("""https?://[^"' \s]+\.m3u8[^"' \s]*|https?://[^"' \s]+\.mp4[^"' \s]*""")

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("latest", "Latest Releases", HikariMediaType.SERIES),
        HikariCatalog("all", "All Anime", HikariMediaType.SERIES),
        HikariCatalog("action", "Action", HikariMediaType.SERIES),
        HikariCatalog("adventure", "Adventure", HikariMediaType.SERIES),
        HikariCatalog("fantasy", "Fantasy", HikariMediaType.SERIES),
        HikariCatalog("comedy", "Comedy", HikariMediaType.SERIES),
        HikariCatalog("drama", "Drama", HikariMediaType.SERIES),
        HikariCatalog("isekai", "Isekai", HikariMediaType.SERIES),
    )

    private fun pageUrl(catalog: HikariCatalog, page: Int, d: String): String {
        val p = page.coerceAtLeast(1)
        return when (catalog.id) {
            "latest" -> if (p <= 1) "$d/" else "$d/page/$p/"
            "all" -> if (p <= 1) "$d/anime/?status=&type=&order=update"
            else "$d/anime/page/$p/?status=&type=&order=update"
            else -> if (p <= 1) "$d/genres/${catalog.id}/"
            else "$d/genres/${catalog.id}/page/$p/"
        }
    }

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val d = domain()
        val html = HikariNet.getStringSmart(pageUrl(catalog, page, d), pageHeaders("$d/"))
            ?: return emptyList()
        val cards = parseArticleCards(html, d)
        if (cards.isEmpty()) return emptyList()
        if (catalog.id != "latest") return cards
        // home cards point at episode pages; resolve each to its anime page
        return coroutineScope {
            cards.map { card ->
                async(Dispatchers.IO) { resolveEpisodeCard(card, d) ?: card }
            }.awaitAll().distinctBy { it.id }
        }
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank()) return emptyList()
        if (page > 1) return emptyList()
        val d = domain()
        val q = query.trim().replace(" ", "+")
        val html = HikariNet.getStringSmart("$d/?s=$q", pageHeaders("$d/")) ?: return emptyList()
        val cards = parseArticleCards(html, d)
        if (cards.isEmpty()) return emptyList()
        return coroutineScope {
            cards.map { card ->
                async(Dispatchers.IO) {
                    if (card.id.contains("/anime/")) card
                    else resolveEpisodeCard(card, d) ?: card
                }
            }.awaitAll().distinctBy { it.id }
        }
    }

    private data class Card(val url: String, val title: String, val poster: String?, val typez: String)

    private fun parseArticleCards(html: String, d: String): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        val seen = HashSet<String>()
        val articles = Regex(
            """<article[^>]*>(.*?)</article>""",
            setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
        ).findAll(html)
        for (art in articles) {
            val body = art.groupValues[1]
            if (!body.contains("class=\"bsx\"") && !body.contains("bsx")) continue
            val a = Regex(
                """<a[^>]*href="([^"]+)"[^>]*>""",
                RegexOption.IGNORE_CASE,
            ).find(body) ?: continue
            var href = a.groupValues[1].trim()
            if (!href.startsWith("http")) href = d.trimEnd('/') + "/" + href.trimStart('/')
            if (!seen.add(href)) continue
            val title = Regex("""title="([^"]+)"""", RegexOption.IGNORE_CASE)
                .find(a.value)?.groupValues?.get(1)?.let { NaNet.unesc(it) }
                ?.takeIf { it.isNotBlank() }
                ?: Regex(
                    """<h2[^>]*>(.*?)</h2>""",
                    setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
                ).find(body)?.groupValues?.get(1)?.let { NaNet.unesc(NaNet.strip(it)) }
                ?.takeIf { it.isNotBlank() } ?: continue
            val poster = Regex(
                """<img[^>]+(?:data-src|src)="([^"]+)"""",
                RegexOption.IGNORE_CASE,
            ).find(body)?.groupValues?.get(1)?.takeIf { it.startsWith("http") }
            val typez = Regex(
                """class="typez[^"]*"[^>]*>([^<]*)<""",
                RegexOption.IGNORE_CASE,
            ).find(body)?.groupValues?.get(1)?.trim().orEmpty()
            val type = if (typez.contains("Movie", true)) HikariMediaType.MOVIE else HikariMediaType.SERIES
            out.add(HikariMedia(id = href, title = title, type = type, posterUrl = poster))
            if (out.size >= 40) break
        }
        return out
    }

    private suspend fun resolveEpisodeCard(card: HikariMedia, d: String): HikariMedia? {
        if (card.id.contains("/anime/")) return card
        return try {
            val html = HikariNet.getStringSmart(card.id, pageHeaders("$d/")) ?: return null
            // first /anime/ link whose path is a bare slug
            val link = Regex("""<a[^>]*href="([^"]*?/anime/[^"]*)"[^>]*>(.*?)</a>""")
                .findAll(html).firstOrNull { m ->
                    val path = m.groupValues[1].substringAfter("/anime/").trim('/')
                    path.isNotEmpty() && !path.contains('/') && !path.contains('?') &&
                        !path.contains("page", ignoreCase = true)
                } ?: return null
            var href = link.groupValues[1]
            if (!href.startsWith("http")) href = d.trimEnd('/') + "/" + href.trimStart('/')
            val linkTitle = NaNet.unesc(NaNet.strip(link.groupValues[2])).takeIf { it.isNotBlank() }
            val cleanTitle = linkTitle
                ?: card.title.replace(Regex("""(?i)\s*episode\s*\d+\s*$"""), "").trim()
                    .ifBlank { card.title }
            card.copy(id = href, title = cleanTitle)
        } catch (_: Exception) {
            null
        }
    }

    private suspend fun animeUrl(url: String, d: String): String {
        if (url.contains("/anime/")) return url
        return try {
            val html = HikariNet.getStringSmart(url, pageHeaders("$d/")) ?: return url
            Regex("""<a[^>]*href="([^"]*?/anime/[^"]*)"[^>]*>""")
                .findAll(html).firstOrNull { m ->
                    val path = m.groupValues[1].substringAfter("/anime/").trim('/')
                    path.isNotEmpty() && !path.contains('/') && !path.contains('?') &&
                        !path.contains("page", ignoreCase = true)
                }?.let { m ->
                    var href = m.groupValues[1]
                    if (!href.startsWith("http")) href = d.trimEnd('/') + "/" + href.trimStart('/')
                    href
                } ?: url
        } catch (_: Exception) {
            url
        }
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val d = domain()
        val url = animeUrl(media.id, d)
        val html = HikariNet.getStringSmart(url, pageHeaders("$d/")) ?: return media
        val title = Regex(
            """<h1[^>]*class="[^"]*entry-title[^"]*"[^>]*>(.*?)</h1>""",
            setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
        ).find(html)?.groupValues?.get(1)?.let { NaNet.unesc(NaNet.strip(it)) }
            ?: Regex(
                """<h1[^>]*>(.*?)</h1>""",
                setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
            ).find(html)?.groupValues?.get(1)?.let { NaNet.unesc(NaNet.strip(it)) }
            ?.takeIf { it.isNotBlank() } ?: media.title
        val poster = Regex(
            """<div[^>]*class="[^"]*thumb[^"]*"[^>]*>[\s\S]*?<img[^>]+(?:data-src|src)="([^"]+)"""",
            RegexOption.IGNORE_CASE,
        ).find(html)?.groupValues?.get(1)
            ?: Regex(
                """<div[^>]*class="[^"]*poster[^"]*"[^>]*>[\s\S]*?<img[^>]+(?:data-src|src)="([^"]+)"""",
                RegexOption.IGNORE_CASE,
            ).find(html)?.groupValues?.get(1)
            ?: Regex(
                """<meta[^>]+property="og:image"[^>]+content="([^"]+)"""",
                RegexOption.IGNORE_CASE,
            ).find(html)?.groupValues?.get(1)
        val plot = Regex(
            """<div[^>]*class="[^"]*entry-content[^"]*"[^>]*>(.*?)</div>""",
            setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
        ).find(html)?.groupValues?.get(1)?.let { NaNet.unesc(NaNet.strip(it)) }
            ?.takeIf { it.length > 10 }
            ?: Regex(
                """<meta[^>]+property="og:description"[^>]+content="([^"]+)"""",
                RegexOption.IGNORE_CASE,
            ).find(html)?.groupValues?.get(1)?.let { NaNet.unesc(it) }
        val tags = Regex(
            """<div[^>]*class="[^"]*genxed[^"]*"[^>]*>(.*?)</div>""",
            setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
        ).find(html)?.groupValues?.get(1)?.let { block ->
            Regex("""<a[^>]*>([^<]+)</a>""").findAll(block)
                .map { NaNet.unesc(it.groupValues[1].trim()) }
                .filter { it.isNotBlank() }.toList()
        }.orEmpty()
        val info = Regex(
            """class="info-content"[^>]*>(.*?)<""",
            setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
        ).findAll(html).map { NaNet.strip(it.groupValues[1]) }.joinToString(" ")
        val type = when {
            info.contains("Movie", true) -> HikariMediaType.MOVIE
            else -> HikariMediaType.SERIES
        }
        val posterHttp = poster?.takeIf { it.startsWith("http") } ?: media.posterUrl
        return media.copy(
            id = url, title = title, type = type, posterUrl = posterHttp,
            backdropUrl = posterHttp, overview = plot ?: media.overview,
            genres = tags.ifEmpty { media.genres },
        )
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val d = domain()
        val url = animeUrl(media.id, d)
        val html = HikariNet.getStringSmart(url, pageHeaders("$d/")) ?: return null
        val listBlock = Regex(
            """<[^>]*class="[^"]*eplister[^"]*"[^>]*>(.*?)</ul>""",
            setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
        ).find(html)?.groupValues?.get(1)
        if (listBlock != null) {
            val out = ArrayList<HikariEpisode>()
            for (m in Regex(
                """<li[^>]*>(.*?)</li>""",
                setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
            ).findAll(listBlock)) {
                val li = m.groupValues[1]
                val href = Regex("""<a[^>]*href="([^"]+)"""", RegexOption.IGNORE_CASE)
                    .find(li)?.groupValues?.get(1)?.trim() ?: continue
                if (!href.startsWith("http")) continue
                val num = Regex(
                    """<div[^>]*class="[^"]*epl-num[^"]*"[^>]*>([^<]*)<""",
                    RegexOption.IGNORE_CASE,
                ).find(li)?.groupValues?.get(1)?.trim()?.toIntOrNull()
                    ?: Regex("""\d+""").find(
                        Regex(
                            """<div[^>]*class="[^"]*epl-num[^"]*"[^>]*>([^<]*)<""",
                            RegexOption.IGNORE_CASE,
                        ).find(li)?.groupValues?.get(1).orEmpty(),
                    )?.value?.toIntOrNull()
                    ?: Regex("""episode[^\d]*(\d+)""", RegexOption.IGNORE_CASE)
                        .find(href)?.groupValues?.get(1)?.toIntOrNull()
                    ?: continue
                val epTitle = Regex(
                    """<div[^>]*class="[^"]*epl-title[^"]*"[^>]*>([^<]*)<""",
                    RegexOption.IGNORE_CASE,
                ).find(li)?.groupValues?.get(1)?.let { NaNet.unesc(it.trim()) }
                    ?.takeIf { it.isNotBlank() } ?: "Episode $num"
                out.add(HikariEpisode(number = num, id = href, name = epTitle, image = media.posterUrl))
            }
            // listed newest first; oldest first expected
            if (out.isNotEmpty()) return out.reversed()
        }
        if (html.contains("select class=\"mirror\"") || html.contains("mirror")) {
            return listOf(HikariEpisode(number = 1, id = url, name = media.title, image = media.posterUrl))
        }
        return null
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val d = domain()
        val pageUrl = episode?.id?.takeIf { it.startsWith("http") } ?: animeUrl(media.id, d)
        val html = HikariNet.getStringSmart(pageUrl, pageHeaders("$d/")) ?: return emptyList()
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        fun emit(url: String, name: String, headers: Map<String, String>, isM3u8: Boolean, subs: List<HikariSubtitle> = emptyList()) {
            if (url.isBlank() || !seen.add(url)) return
            out.add(HikariStream(name = name, url = url, headers = headers, subtitles = subs, isM3u8 = isM3u8))
        }

        val mirror = Regex(
            """<select[^>]*class="[^"]*mirror[^"]*"[^>]*>(.*?)</select>""",
            setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
        ).find(html)?.groupValues?.get(1)
        val embeds = ArrayList<String>()
        if (mirror != null) {
            for (opt in Regex("""<option[^>]*value="([^"]*)"""", RegexOption.IGNORE_CASE).findAll(mirror)) {
                val v = opt.groupValues[1]
                if (v.isBlank() || v == "...") continue
                val dec = NaNet.b64d(v) ?: continue
                val src = Regex("""<iframe[^>]*src="([^"]+)"""", RegexOption.IGNORE_CASE)
                    .find(dec)?.groupValues?.get(1) ?: continue
                if (src.startsWith("http")) embeds.add(src)
            }
        }
        for (m in Regex("""<iframe[^>]*src="([^"]+)"""", RegexOption.IGNORE_CASE).findAll(html)) {
            if (m.groupValues[1].startsWith("http")) embeds.add(m.groupValues[1])
        }

        for (embed in embeds.distinct()) {
            var targets = listOf(embed)
            if (embed.contains("gogoanime.me.uk/newplayer.php")) {
                try {
                    val ph = HikariNet.getString(embed, pageHeaders(pageUrl)) ?: ""
                    val inner = Regex("""<iframe[^>]*src="([^"]+)"""", RegexOption.IGNORE_CASE)
                        .findAll(ph).map { it.groupValues[1] }
                        .filter { it.startsWith("http") }.toList()
                    if (inner.isNotEmpty()) targets = inner
                } catch (_: Exception) {
                }
            }
            for (target in targets) {
                // plyr.php#base64 inline file (KiwiK in the source)
                if (target.contains("plyr.php#")) {
                    try {
                        val b64 = target.substringAfter("#").substringBefore("#")
                        val dec = NaNet.b64d(b64).orEmpty()
                        val file = Regex("""https?://[^"' \s]+\.(?:m3u8|mp4)[^"' \s]*""")
                            .find(dec)?.value
                        if (file != null) {
                            emit(
                                file, "[9anime] KiwiK",
                                mapOf("Referer" to "https://gogoanime.me.uk/"),
                                file.contains("m3u8"),
                            )
                            continue
                        }
                    } catch (_: Exception) {
                    }
                }
                if (target.contains("megaplay.buzz")) {
                    try {
                        val r = NaMegaPlay.resolve(target, pageUrl)
                        if (r != null) {
                            val subs = r.subs.map { (label, f) -> HikariSubtitle(label, f) }
                            emit(
                                r.url, "[9anime] MegaPlay",
                                mapOf(
                                    "User-Agent" to NaNet.UA,
                                    "Referer" to target.substringBefore("/stream/") + "/",
                                ),
                                true, subs,
                            )
                            continue
                        }
                    } catch (_: Exception) {
                    }
                }
                if (target.contains("vidmoly.biz")) {
                    try {
                        val vh = HikariNet.getString(target, pageHeaders(pageUrl))
                        if (vh != null) {
                            val m3u8 = Regex("""file\s*:\s*['"](https?://[^'"]+?\.m3u8[^'"]*)['"]""")
                                .find(vh)?.groupValues?.get(1)
                                ?: Regex("""https?://[^'"\s]+?\.m3u8[^'"\s]*""").find(vh)?.value
                            if (m3u8 != null) {
                                emit(
                                    m3u8, "[9anime] Vidmoly",
                                    mapOf(
                                        "User-Agent" to NaNet.UA,
                                        "Referer" to "https://vidmoly.biz/",
                                        "Origin" to "https://vidmoly.biz",
                                    ),
                                    true,
                                )
                                continue
                            }
                        }
                    } catch (_: Exception) {
                    }
                }
                // byse moon player + anything unknown: capture what the page plays
                val timeout = if (target.contains("bysesayeveum.com")) 90_000L else 60_000L
                val script = if (target.contains("bysesayeveum.com")) {
                    """document.querySelector('button,[role="button"],.vjs-big-play-button,.jw-icon-display,.vds-play-button,[onclick]')?.click();"""
                } else null
                try {
                    for (r in HikariNet.resolveWithWebView(target, capture, timeoutMs = timeout, script = script)) {
                        emit(r.url, "[9anime]", r.headers, r.url.contains("m3u8"))
                    }
                } catch (_: Exception) {
                }
            }
            if (out.isNotEmpty()) return out
        }
        return out
    }
}
