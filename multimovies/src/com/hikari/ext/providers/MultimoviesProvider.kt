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
import kotlinx.coroutines.coroutineScope
import org.json.JSONObject
import java.util.Collections

/**
 * Multimovies (multimovies.garden) — movies + TV + anime with 10 catalog rows.
 *
 * Faithful native port of KSHITIJ8473/raghav Multimovies (CloudStream MainAPI),
 * verified live Oct 2026:
 *  - Firebase-hosted domain table (key "multimovies") with a reply-content
 *    check (/assets/js/player.js): the table currently points at
 *    multimovies.motorcycles, which serves an unrelated WordPress site, so the
 *    check rejects it and the provider stays on multimovies.garden.
 *  - Cards: article.poster-card + button.card-save[data-save-title] JSON
 *    {titleId,title,poster,type,url,year} (HTML-entity escaped).
 *  - Catalogs/search: /catalog?...(&page=N), /search?q=, hasNext via a[rel=next].
 *  - Detail: h1 + #cinejoyOverview + ld+json + .cinejoy-detail-genres +
 *    .cinejoy-meta-pill pills; series add seasons (.cinejoy-season-pill-dropdown
 *    select), ep_range pages, and .cinejoy-ep-card#ep-item-S-E cards.
 *  - Playback: `const watchConfig = {...}` initialServers dispatched per host —
 *    modiplay hub (Cineverse proxy + apiV1 cipher), iqsmart gd mirror
 *    (file list -> mirror helper -> apiV1/Filemoon/xvidstyle), filesforever,
 *    Vidout (github hls index + subs), Vidsync, Bingr fleet, Filmu
 *    singularity, Vidbolt (movy/curx + Quasar/Saffron/Callisto scrapers),
 *    else a best-effort m3u8 sweep.
 *  - Byse/Filemoon embeds run the full challenge -> ECDSA attest -> PoW ->
 *    AES-GCM playback flow (MmFilemoon + MmCrypto, no WebView needed).
 *  - Site pages go through HikariNet.getStringSmart (the Hikari replacement
 *    for the source's Cloudflare-dialog mmGet); APIs use MmNet's raw client
 *    with status codes + cookie jar.
 */
class MultimoviesProvider : HikariProvider {

    override val id = "multimovies"
    override val name = "Multimovies"
    override val mainUrl = MmApi.DEFAULT_HOST
    override val description = "Movies, TV shows and anime from Multimovies — latest, trending, Bollywood, Hollywood, South Indian, OTT and anime rows."
    override val version = 2
    override val tvTypes = setOf(HikariMediaType.MOVIE, HikariMediaType.SERIES)

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("latest-movies", "Latest Movies", HikariMediaType.MOVIE, rawType = "/catalog?type=movie&sort=latest"),
        HikariCatalog("latest-tv", "Latest TV Shows", HikariMediaType.SERIES, rawType = "/catalog?type=tv&sort=latest"),
        HikariCatalog("trending", "Trending", HikariMediaType.MOVIE, rawType = "/catalog?sort=rating"),
        HikariCatalog("bollywood", "Bollywood", HikariMediaType.MOVIE, rawType = "/catalog?genre=bollywood-movies&sort=latest"),
        HikariCatalog("hollywood", "Hollywood", HikariMediaType.MOVIE, rawType = "/catalog?genre=hollywood&sort=latest"),
        HikariCatalog("south", "South Indian", HikariMediaType.MOVIE, rawType = "/catalog?genre=south-indian&sort=latest"),
        HikariCatalog("netflix", "Netflix", HikariMediaType.SERIES, rawType = "/catalog?genre=netflix&sort=latest"),
        HikariCatalog("hotstar", "Jio Hotstar", HikariMediaType.SERIES, rawType = "/catalog?genre=disney-hotstar&sort=latest"),
        HikariCatalog("anime-series", "Anime Series", HikariMediaType.SERIES, rawType = "/catalog?genre=anime-series&sort=latest"),
        HikariCatalog("anime-movies", "Anime Movies", HikariMediaType.MOVIE, rawType = "/catalog?genre=anime-movies&sort=latest"),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        MmApi.refreshDomain()
        val p = page.coerceAtLeast(1)
        val path = catalog.rawType.ifBlank { catalog.id }
        val url = if (p <= 1) "${MmApi.host()}$path" else "${MmApi.host()}$path&page=$p"
        val html = MmNet.siteGet(url) ?: return emptyList()
        return MmApi.parseCards(html).mapNotNull { it.toMedia() }
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        if (page > 1) return emptyList()
        MmApi.refreshDomain()
        val html = MmNet.siteGet("${MmApi.host()}/search?q=${MmNet.urlEncode(q)}") ?: return emptyList()
        return MmApi.parseCards(html).mapNotNull { it.toMedia() }
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        MmApi.refreshDomain()
        val html = MmNet.siteGet(media.id) ?: return media
        val meta = MmApi.parseDetail(html, media.id)
        return media.copy(
            title = meta.title.ifBlank { media.title },
            posterUrl = meta.poster ?: media.posterUrl,
            backdropUrl = meta.poster ?: media.backdropUrl,
            overview = meta.plot.ifBlank { media.overview },
            genres = meta.genres.ifEmpty { media.genres },
            year = meta.year ?: media.year,
            type = meta.type,
        )
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        MmApi.refreshDomain()
        if (!MmApi.isSeries(media)) return null
        val seriesBase = media.id.substringBefore('?')
        val first = MmNet.siteGet(media.id) ?: return null
        val current = MmApi.watchSeason(first) ?: MmApi.parseSeasons(first).firstOrNull() ?: 1
        val episodes = ArrayList<HikariEpisode>()
        episodes.addAll(MmApi.parseEpisodes(first, seriesBase))

        for (range in MmApi.parseEpisodeRanges(first)) {
            try {
                val doc = MmNet.siteGet("$seriesBase?season=$current&ep_range=$range") ?: continue
                episodes.addAll(MmApi.parseEpisodes(doc, seriesBase))
            } catch (_: Exception) {
            }
        }

        val remaining = MmApi.parseSeasons(first).filter { it != current }
        if (remaining.isNotEmpty()) {
            coroutineScope {
                remaining.map { season ->
                    async(Dispatchers.IO) {
                        try {
                            val seasonDoc = MmNet.siteGet("$seriesBase?season=$season") ?: return@async emptyList()
                            val eps = MmApi.parseEpisodes(seasonDoc, seriesBase).toMutableList()
                            for (range in MmApi.parseEpisodeRanges(seasonDoc)) {
                                try {
                                    val r = MmNet.siteGet("$seriesBase?season=$season&ep_range=$range") ?: continue
                                    eps.addAll(MmApi.parseEpisodes(r, seriesBase))
                                } catch (_: Exception) {
                                }
                            }
                            eps
                        } catch (_: Exception) {
                            emptyList()
                        }
                    }
                }.forEach { episodes.addAll(it.await()) }
            }
        }
        return episodes.sortedWith(compareBy({ it.season }, { it.number })).ifEmpty { null }
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        MmApi.refreshDomain()
        val data = episode?.id ?: media.id
        val html = MmNet.siteGet(data) ?: return emptyList()
        val servers = MmApi.parseWatchConfig(html)
        if (servers.isEmpty()) return emptyList()

        val ids = MmApi.extractIds(servers)
        val isTv = data.contains("/series/")
        val season = Regex("/season/(\\d+)/episode/").find(data)?.groupValues?.get(1)?.toIntOrNull()
        val ep = Regex("/season/\\d+/episode/(\\d+)").find(data)?.groupValues?.get(1)?.toIntOrNull()
        val title = MmApi.parseH1(html)
        val year = MmApi.parseYear(html)

        val streams = Collections.synchronizedList(ArrayList<HikariStream>())
        val subs = Collections.synchronizedList(ArrayList<HikariSubtitle>())
        val seenLinks = Collections.synchronizedSet(HashSet<String>())
        val seenSubs = Collections.synchronizedSet(HashSet<String>())
        val linkCb: (HikariStream) -> Unit = { s -> if (seenLinks.add(s.url)) streams.add(s) }
        val subCb: (HikariSubtitle) -> Unit = { s -> if (seenSubs.add(s.url)) subs.add(s) }

        coroutineScope {
            servers.map { server ->
                async(Dispatchers.IO) {
                    try {
                        MmApi.resolveServer(server, ids, isTv, season, ep, title, year, subCb, linkCb)
                    } catch (_: Exception) {
                        false
                    }
                }
            }.forEach { it.await() }
        }
        val allSubs = subs.toList()
        return streams.map { it.copy(subtitles = it.subtitles + allSubs) }.distinctBy { it.url }
    }
}

// ---------------------------------------------------------------------------

internal data class MmCard(
    val url: String,
    val title: String,
    val poster: String?,
    val year: Int?,
    val isTv: Boolean,
    val isAnime: Boolean,
)

internal data class MmDetail(
    val title: String,
    val plot: String,
    val poster: String?,
    val genres: List<String>,
    val year: Int?,
    val type: HikariMediaType,
)

internal data class MmWatchServer(val id: String, val name: String, val url: String)
internal data class MmTitleIds(val tmdbId: String?, val imdbId: String?)

internal object MmApi {
    const val DEFAULT_HOST = "https://multimovies.garden"
    private const val DOMAIN_URL = "https://cloudstreampluginhelper-default-rtdb.firebaseio.com/.json"

    @Volatile private var base = DEFAULT_HOST
    @Volatile private var lastDomain = 0L
    @Volatile private var validatedDomain: String? = null

    fun host(): String = base

    // the firebase list can lag behind migrations, only switch when the reply
    // really looks like the current app shell
    suspend fun refreshDomain() {
        val now = System.currentTimeMillis()
        if (now - lastDomain < 5 * 60 * 1000L) return
        lastDomain = now
        try {
            val txt = HikariNet.getString(DOMAIN_URL) ?: return
            val obj = JSONObject(txt)
            val remote = obj.optString("multimovies")
                .ifBlank { obj.optString("multimovies_url") }
                .ifBlank { obj.optString("multimovies_domain") }
                .trim().removeSuffix("/")
            if (remote.isBlank() || remote == base || remote == validatedDomain) return
            val ok = try {
                val probe = HikariNet.getString("$remote/") ?: ""
                probe.contains("/assets/js/player.js")
            } catch (_: Exception) {
                false
            }
            validatedDomain = remote
            if (ok) base = remote
        } catch (_: Exception) {
            validatedDomain = null
        }
    }

    private val articleRe = Regex(
        """<article[^>]*>(.*?)</article>""",
        setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
    )
    private val saveRe = Regex("""data-save-title="([^"]+)"""")
    private val imgRe = Regex("""<img[^>]+src="([^"]+)"""", RegexOption.IGNORE_CASE)

    fun parseCards(html: String): List<MmCard> {
        val out = ArrayList<MmCard>()
        val seen = HashSet<String>()
        for (m in articleRe.findAll(html)) {
            if (!m.value.contains("poster-card")) continue
            val block = m.groupValues[1]
            val raw = saveRe.find(block)?.groupValues?.get(1) ?: continue
            val data = try {
                JSONObject(raw.replace("&quot;", "\"").replace("&amp;", "&"))
            } catch (_: Exception) {
                continue
            }
            val url = data.optString("url")
            val title = data.optString("title")
            if (url.isBlank() || title.isBlank() || !seen.add(url)) continue
            val absUrl = MmNet.abs(base, url)
            val isTv = data.optString("type") == "tv" || url.contains("/series/")
            val isAnime = url.contains("anime", ignoreCase = true)
            val poster = imgRe.find(block)?.groupValues?.get(1)?.let { MmNet.abs(base, it) }
                ?: data.optString("poster").takeIf { it.isNotBlank() }
            out.add(
                MmCard(
                    absUrl, title, poster,
                    data.optInt("year", 0).takeIf { it > 0 },
                    isTv, isAnime,
                )
            )
        }
        return out
    }

    private fun MmCard.toMediaType(): HikariMediaType = when {
        isTv && isAnime -> HikariMediaType.SERIES
        isTv -> HikariMediaType.SERIES
        isAnime -> HikariMediaType.MOVIE
        else -> HikariMediaType.MOVIE
    }

    fun MmCard.toMedia(): HikariMedia? {
        if (url.isBlank() || title.isBlank()) return null
        return HikariMedia(
            id = url,
            title = title,
            type = toMediaType(),
            posterUrl = poster,
            year = year,
            rawType = if (isTv) "tv" else "movie",
        )
    }

    fun isSeries(media: HikariMedia): Boolean =
        media.rawType == "tv" || media.id.contains("/series/")

    fun parseH1(html: String): String =
        Regex("""<h1[^>]*>(.*?)</h1>""", setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE))
            .find(html)?.groupValues?.get(1)?.replace(Regex("<[^>]+>"), "")?.trim().orEmpty()

    fun parseOverview(html: String): String =
        Regex("""<([a-zA-Z0-9]+)[^>]*id="cinejoyOverview"[^>]*>(.*?)</\1>""", setOf(RegexOption.DOT_MATCHES_ALL))
            .find(html)?.groupValues?.get(2)?.replace(Regex("<[^>]+>"), "")?.trim().orEmpty()

    private fun parseLd(html: String): JSONObject? {
        val raw = Regex(
            """<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>""",
            setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
        ).find(html)?.groupValues?.get(1) ?: return null
        return try {
            JSONObject(raw)
        } catch (_: Exception) {
            null
        }
    }

    fun parseYear(html: String): String? =
        Regex("""\b((?:19|20)\d{2})\b""").find(pillText(html))?.groupValues?.get(1)

    private fun pillText(html: String): String =
        Regex("""class="[^"]*cinejoy-meta-pill[^"]*"[^>]*>([^<]*)""")
            .findAll(html).map { it.groupValues[1] }.joinToString(" ")

    private fun parseGenres(html: String, ld: JSONObject?): List<String> {
        val fromDetail = Regex(
            """<[^>]*cinejoy-detail-genres[^>]*>(.*?)</[^>]+>""",
            setOf(RegexOption.DOT_MATCHES_ALL),
        ).find(html)?.groupValues?.get(1)
            ?.replace(Regex("<[^>]+>"), "")
            ?.split("•")?.map { it.trim() }?.filter { it.isNotBlank() }
        if (!fromDetail.isNullOrEmpty()) return fromDetail
        val arr = ld?.optJSONArray("genre") ?: return emptyList()
        return (0 until arr.length()).map { arr.optString(it) }.filter { it.isNotBlank() }
    }

    fun parseDetail(html: String, url: String): MmDetail {
        val ld = parseLd(html)
        val title = parseH1(html).ifBlank { ld?.optString("name").orEmpty() }
        val plot = parseOverview(html).ifBlank { ld?.optString("description").orEmpty() }
        val image = ld?.optString("image")?.takeIf { it.isNotBlank() }
            ?: Regex("""<meta[^>]+property="og:image"[^>]+content="([^"]+)"""").find(html)?.groupValues?.get(1)
        val genres = parseGenres(html, ld)
        val year = Regex("""\b((?:19|20)\d{2})\b""").find(pillText(html))?.groupValues?.get(1)?.toIntOrNull()
        val series = url.contains("/series/")
        val anime = url.contains("anime", ignoreCase = true) || genres.any { it.contains("anime", true) }
        return MmDetail(
            title, plot, image, genres, year,
            if (series || anime) HikariMediaType.SERIES else HikariMediaType.MOVIE,
        )
    }

    fun parseSeasons(html: String): List<Int> {
        // the range select shares a similar class, only the pill dropdown lists real seasons
        val idx = html.indexOf("cinejoy-season-pill-dropdown")
        if (idx < 0) return emptyList()
        val sel = Regex("""<select[^>]*>(.*?)</select>""", setOf(RegexOption.DOT_MATCHES_ALL))
            .find(html, idx)?.groupValues?.get(1) ?: return emptyList()
        return Regex("""value="(\d+)"""").findAll(sel)
            .mapNotNull { it.groupValues[1].toIntOrNull() }.sorted().toList()
    }

    // seasons past 24 episodes come as ep_range pages, the selected range is
    // the one already rendered on the page
    fun parseEpisodeRanges(html: String): List<Int> {
        val sel = Regex(
            """<select[^>]*episode-range-select[^>]*>(.*?)</select>""",
            setOf(RegexOption.DOT_MATCHES_ALL),
        ).find(html)?.groupValues?.get(1) ?: return emptyList()
        return Regex("""<option(?![^>]*selected)[^>]*value="(\d+)"[^>]*>""")
            .findAll(sel).mapNotNull { it.groupValues[1].toIntOrNull() }.toList()
    }

    fun watchSeason(html: String): Int? = try {
        JSONObject(extractBracedObject(html, "const watchConfig = ")).optInt("season").takeIf { it > 0 }
    } catch (_: Exception) {
        null
    }

    fun parseEpisodes(html: String, seriesBase: String): List<HikariEpisode> {
        val out = ArrayList<HikariEpisode>()
        val marks = Regex("""id="ep-item-(\d+)-(\d+)"""").findAll(html).toList()
        for ((i, m) in marks.withIndex()) {
            val season = m.groupValues[1].toIntOrNull() ?: continue
            val episode = m.groupValues[2].toIntOrNull() ?: continue
            val end = marks.getOrNull(i + 1)?.range?.first ?: html.length
            val block = html.substring(m.range.first, end)
            val href = Regex("""<a[^>]*cinejoy-ep-thumb-link[^>]*href="([^"]+)"""")
                .find(block)?.groupValues?.get(1) ?: continue
            val name = Regex("""cinejoy-ep-name[^>]*>([^<]+)""")
                .find(block)?.groupValues?.get(1)?.trim().orEmpty()
            val still = Regex("""<img[^>]+src="([^"]+)"""").find(block)?.groupValues?.get(1).orEmpty()
            out.add(
                HikariEpisode(
                    number = episode,
                    id = MmNet.abs(seriesBase, href),
                    name = name.ifBlank { "Episode $episode" },
                    image = still.ifBlank { null },
                    season = season,
                )
            )
        }
        return out.sortedWith(compareBy({ it.season }, { it.number }))
    }

    fun parseWatchConfig(html: String): List<MmWatchServer> {
        val root = try {
            JSONObject(extractBracedObject(html, "const watchConfig = "))
        } catch (_: Exception) {
            return emptyList()
        }
        val servers = root.optJSONArray("initialServers") ?: return emptyList()
        val out = ArrayList<MmWatchServer>()
        for (i in 0 until servers.length()) {
            val s = servers.optJSONObject(i) ?: continue
            val url = s.optString("url")
            if (url.isBlank()) continue
            out.add(MmWatchServer(s.optString("id"), s.optString("name"), url))
        }
        return out
    }

    // line ending agnostic json block reader, the site mixes crlf and lf
    fun extractBracedObject(html: String, marker: String): String {
        val start = html.indexOf(marker)
        if (start < 0) return ""
        val braceStart = html.indexOf('{', start)
        if (braceStart < 0) return ""
        var depth = 0
        var inString = false
        var escaped = false
        for (i in braceStart until html.length) {
            val ch = html[i]
            if (escaped) {
                escaped = false
                continue
            }
            when {
                ch == '\\' && inString -> escaped = true
                ch == '"' -> inString = !inString
                ch == '{' && !inString -> depth++
                ch == '}' && !inString -> {
                    depth--
                    if (depth == 0) return html.substring(braceStart, i + 1)
                }
            }
        }
        return ""
    }

    fun extractIds(servers: List<MmWatchServer>): MmTitleIds {
        var tmdb: String? = null
        var imdb: String? = null
        for (server in servers) {
            if (tmdb == null) {
                tmdb = Regex("""(?:movie\?id=|watch/movie/|watch/tv/|embed/tmdb/tv\?id=|/tv/)(\d+)""")
                    .find(server.url)?.groupValues?.get(1)
            }
            if (imdb == null) {
                imdb = Regex("""(tt\d{6,})""").find(server.url)?.groupValues?.get(1)
            }
        }
        return MmTitleIds(tmdb, imdb)
    }

    suspend fun resolveServer(
        server: MmWatchServer,
        ids: MmTitleIds,
        isTv: Boolean,
        season: Int?,
        episode: Int?,
        title: String,
        year: String?,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ): Boolean {
        val host = MmNet.hostOf(server.url)
        return when {
            host.contains("modiplay.xyz") ->
                MmCineverse.resolve(server.url, server.name, callback)

            host.contains("iqsmartgames.com") ->
                MmIqsmart.resolve(server.url, server.name, callback)

            host.contains("filesforever.link") ->
                MmIqsmart.resolveFilesforever(server.url, server.name, callback)

            host.contains("vidout.pages.dev") -> {
                val tmdb = ids.tmdbId ?: return false
                MmVidout.resolve(tmdb, isTv, season, episode, server.name, subtitleCallback, callback)
            }

            host.contains("vidsync.pro") -> {
                val tmdb = ids.tmdbId
                val url = if (tmdb != null) server.url.replace("{tmdbId}", tmdb) else server.url
                MmVidsync.resolve(url, server.name, callback)
            }

            host.contains("bingr.one") -> {
                val tmdb = ids.tmdbId ?: return false
                MmBingr.resolve(isTv, tmdb, title, year, season, episode, server.name, subtitleCallback, callback)
            }

            host.contains("filmu.in") -> {
                val tmdb = ids.tmdbId ?: return false
                MmFilmu.resolve(isTv, tmdb, season, episode, server.name, subtitleCallback, callback)
            }

            host.contains("vidbolt.xyz") -> {
                val tmdb = ids.tmdbId ?: return false
                MmVidbolt.resolve(isTv, tmdb, ids.imdbId, title, year, season, episode, server.name, callback)
            }

            else -> {
                val html = MmNet.get(server.url, referer = "https://multimovies.garden/")
                if (html != null) {
                    val urls = Regex("https?://[^\"'\\s\\\\]+\\.m3u8[^\"'\\s\\\\]*")
                        .findAll(html)
                        .map { MmNet.deEsc(it.groupValues.first()) }
                        .toSet()
                    for (u in urls) {
                        callback(HikariStream(name = server.name, url = u, isM3u8 = true))
                    }
                    urls.isNotEmpty()
                } else false
            }
        }
    }
}
