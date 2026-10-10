package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import java.util.Collections

/**
 * TheMoviesFlix — movies & series download posts with direct-file resolvers.
 *
 * Faithful native port of KSHITIJ8473/raghav TheMoviesFlix (CloudStream MainAPI),
 * verified live Oct 2026 (site now lives at themoviesflix.forum, which is also
 * what the Firebase table holds):
 *  - 11 catalog rows (homepage + category pages, /page/N/ pagination).
 *  - Cards: article > a#featured-thumbnail[href][title] > img[data-src|src].
 *  - Detail: h2.mfx-main-title, og:image, div.mfx-plot-box, info-box lis,
 *    imdb block, div.mfx-download-group (h3 quality label + drive links).
 *  - Drive pages (mobilejsr.best, nexdrive-theme WP) hide every link in
 *    base64 `const encoded = "..."` blobs (slash-escaped); decoded HTML holds
 *    the M-Cloud / M-Drive / FilePress / GoFile / 1fichier / VikingFile /
 *    MegaUp buttons. Series drive pages cover episode ranges ("= E25 To E28"
 *    in the title) instead of per-episode h4s.
 *  - Playback: FastDl/fastdl.rest stubs (var reurl -> G-Direct), M-Cloud
 *    (double-atob button -> vcloud pipeline incl. vcloud.beer), M-Drive
 *    (urlMap -> one.vegadrive.me provider flow), classic VCloud/VegaDrive
 *    hubs, FilePress JSON API (+DotFlix extract), GoFile guest accounts;
 *    anything else falls back to WebView media capture. Zip/batch packs are
 *    skipped — no player opens an archive.
 */
class TheMoviesFlixProvider : HikariProvider {

    override val id = "themoviesflix"
    override val name = "TheMoviesFlix"
    override val mainUrl = "https://themoviesflix.forum"
    override val description = "Movies & series from TheMoviesFlix — 480p to 4K direct links with Hindi-dubbed, dual-audio and OTT rows."
    override val version = 2
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val rows = listOf(
        "" to "Latest Movies & Series",
        "category/web-series" to "Web Series",
        "category/hindi-dubbed-movies" to "Hindi Dubbed Movies",
        "category/english" to "English Movies",
        "category/bollywood" to "Bollywood Movies",
        "category/dual-audio-movies" to "Dual Audio Movies",
        "category/netflix" to "Netflix",
        "category/amazon-prime-video" to "Amazon Prime Video",
        "category/jiohotstar" to "JioHotstar",
        "category/disney-plus-hotstar" to "Disney+ Hotstar",
        "category/2160p" to "4K UHD",
    )

    override fun catalogs(): List<HikariCatalog> = rows.map { (path, label) ->
        val t = if (path == "category/web-series") HikariMediaType.SERIES else HikariMediaType.MOVIE
        HikariCatalog(path.ifBlank { "latest" }, label, t, rawType = path)
    }

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val domain = TmfNet.domain()
        val p = page.coerceAtLeast(1)
        val path = catalog.rawType.ifBlank { catalog.id }.let { if (it == "latest") "" else it }
        val url = if (path.isBlank()) {
            if (p > 1) "$domain/page/$p/" else "$domain/"
        } else {
            if (p > 1) "$domain/$path/page/$p/" else "$domain/$path/"
        }
        val html = TmfNet.fetchPage(url) ?: return emptyList()
        return with(TmfDetail) { parseCards(html).mapNotNull { it.toMedia() } }
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank() || page > 1) return emptyList()
        val domain = TmfNet.domain()
        val html = TmfNet.fetchPage("$domain/?s=${TmfNet.urlEncode(q)}") ?: return emptyList()
        return with(TmfDetail) { parseCards(html).mapNotNull { it.toMedia() } }
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val html = TmfNet.fetchPage(media.id) ?: return media
        val d = TmfDetail.parse(html, media.id) ?: return mediareturn media.copy(
            title = d.title.ifBlank { media.title },
            type = if (d.isSeries) HikariMediaType.SERIES else HikariMediaType.MOVIE,
            posterUrl = d.poster?.takeIf { it.isNotBlank() } ?: media.posterUrl,
            backdropUrl = d.poster?.takeIf { it.isNotBlank() } ?: media.backdropUrl,
            year = d.year ?: media.year,
            overview = d.plot?.takeIf { it.isNotBlank() } ?: media.overview,
            genres = d.genres.ifEmpty { media.genres },
            rawType = if (d.isSeries) "tv" else "movie",
        )
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        if (media.rawType != "tv" && !media.id.contains("/season/", true)) {
            // confirm from the detail page when the list row did not say tv
            val probe = TmfNet.fetchPage(media.id) ?: return null
            val d = TmfDetail.parse(probe, media.id) ?: return null
            if (!d.isSeries) return null
            return episodesOf(d)
        }
        val html = TmfNet.fetchPage(media.id) ?: return null
        val d = TmfDetail.parse(html, media.id) ?: return null
        if (!d.isSeries) return null
        return episodesOf(d)
    }

    private suspend fun episodesOf(d: TmfDetail.Detail): List<HikariEpisode>? {
        val seasonGroups = mutableMapOf<Int, MutableList<TmfDetail.Group>>()
        for (group in d.groups) {
            val seasonNum = TmfDetail.seasonNumber(group.label) ?: 1
            seasonGroups.getOrPut(seasonNum) { mutableListOf() }.add(group)
        }
        val episodes = Collections.synchronizedList(ArrayList<HikariEpisode>())
        coroutineScope {
            seasonGroups.map { (seasonNum, groups) ->
                async(Dispatchers.IO) {
                    val allDriveUrls = groups.joinToString("|") { it.redirectUrl }
                    val nums = try {
                        groups.map { group ->
                            async(Dispatchers.IO) {
                                val page = TmfNet.fetchDrivePage(group.redirectUrl) ?: return@async emptySet()
                                val set = page.episodes.keys.toMutableSet()
                                TmfNet.rangeOf(page.title)?.forEach { set.add(it) }
                                set
                            }
                        }.awaitAll().flatten().toSortedSet()
                    } catch (_: Exception) {
                        sortedSetOf()
                    }
                    if (nums.isEmpty()) {
                        episodes.add(
                            HikariEpisode(
                                number = 1,
                                id = "$allDriveUrls|$seasonNum|1",
                                name = groups.first().label,
                                season = seasonNum,
                            )
                        )
                    } else {
                        for (epNum in nums) {
                            episodes.add(
                                HikariEpisode(
                                    number = epNum,
                                    id = "$allDriveUrls|$seasonNum|$epNum",
                                    name = "Episode $epNum",
                                    season = seasonNum,
                                )
                            )
                        }
                    }
                }
            }.awaitAll()
        }
        return episodes.sortedWith(compareBy({ it.season }, { it.number })).ifEmpty { null }
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val out = Collections.synchronizedList(ArrayList<HikariStream>())
        if (episode != null) {
            val parts = episode.id.split("|")
            if (parts.size < 3) return emptyList()
            val epNum = parts.last().toIntOrNull() ?: return emptyList()
            val driveUrls = parts.dropLast(2).filter { it.isNotBlank() }
            if (driveUrls.isEmpty()) return emptyList()
            coroutineScope {
                driveUrls.map { driveUrl ->
                    async(Dispatchers.IO) {
                        try {
                            val page = TmfNet.fetchDrivePage(driveUrl) ?: return@async
                            val hrefs = page.episodes[epNum]
                                ?: if (TmfNet.rangeOf(page.title)?.contains(epNum) == true) page.links
                                else emptyList()
                            if (hrefs.isEmpty()) return@async
                            TmfSources.emitAll(hrefs, page.quality, page.info, "https://nexdrive.fit/") {
                                out.add(it)
                            }
                        } catch (_: Exception) {
                        }
                    }
                }.awaitAll()
            }
        } else {
            val html = TmfNet.fetchPage(media.id) ?: return emptyList()
            val d = TmfDetail.parse(html, media.id) ?: return emptyList()
            if (d.isSeries) return emptyList()
            val driveUrls = d.groups.map { it.redirectUrl }.filter { it.isNotBlank() }.distinct()
            if (driveUrls.isEmpty()) return emptyList()
            coroutineScope {
                driveUrls.map { driveUrl ->
                    async(Dispatchers.IO) {
                        try {
                            val page = TmfNet.fetchDrivePage(driveUrl) ?: return@async
                            if (page.links.isEmpty()) return@async
                            TmfSources.emitAll(page.links, page.quality, page.info, "https://nexdrive.fit/") {
                                out.add(it)
                            }
                        } catch (_: Exception) {
                        }
                    }
                }.awaitAll()
            }
        }
        return out.distinctBy { it.url }
    }
}

// ---------------------------------------------------------------------------

internal data class TmfCard(
    val url: String,
    val title: String,
    val poster: String?,
    val isSeries: Boolean,
)

internal object TmfDetail {

    data class Group(val label: String, val redirectUrl: String)
    data class Detail(
        val title: String,
        val poster: String?,
        val plot: String?,
        val year: Int?,
        val genres: List<String>,
        val isSeries: Boolean,
        val groups: List<Group>,
    )

    private val articleRe = Regex(
        """<article[^>]*>(.*?)</article>""",
        setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
    )
    private val anchorIdRe = Regex("""<a[^>]*id="featured-thumbnail"[^>]*>""", RegexOption.IGNORE_CASE)
    private val hrefRe = Regex("""href="([^"]+)"""", RegexOption.IGNORE_CASE)
    private val titleAttrRe = Regex("""title="([^"]+)"""", RegexOption.IGNORE_CASE)
    private val imgRe = Regex("""<img[^>]*>""", RegexOption.IGNORE_CASE)
    private val imgSrcRe = Regex("""(?:data-src|src)="([^"]+)"""", RegexOption.IGNORE_CASE)
    private val imgAltRe = Regex("""alt="([^"]*)"""", RegexOption.IGNORE_CASE)

    fun cleanTitle(raw: String): String {
        var t = raw.replace(Regex("^Download\\s*", RegexOption.IGNORE_CASE), "")
        listOf(
            " 480p", " 720p", " 1080p", " 2160p", " {Hindi", " {English",
            " (Hindi", " (English", " Hindi Dubbed", " Dual Audio",
            " [480p", " [720p", " [1080p", " Web Dl", " WEB-DL",
            " BluRay", " Blu-Ray", " Full Movie", " Complete",
        ).forEach { t = t.substringBefore(it) }
        return t.trim().trimEnd('(', '-', ':', '{').trim()
    }

    private fun looksSeries(text: String): Boolean =
        text.contains("Season", true) || text.contains("Series", true) ||
            text.contains("TV Show", true) ||
            Regex("""\bS\d{1,2}\b""", RegexOption.IGNORE_CASE).containsMatchIn(text)

    fun parseCards(html: String): List<TmfCard> {
        val out = ArrayList<TmfCard>()
        val seen = HashSet<String>()
        for (m in articleRe.findAll(html)) {
            if (!m.value.contains("featured-thumbnail")) continue
            val block = m.groupValues[1]
            val aTag = anchorIdRe.find(block)?.value ?: continue
            val href = hrefRe.find(aTag)?.groupValues?.get(1)?.trim().orEmpty()
            if (href.isBlank() || !seen.add(href)) continue
            val img = imgRe.find(block)?.value.orEmpty()
            val titleRaw = titleAttrRe.find(aTag)?.groupValues?.get(1)?.trim().orEmpty()
                .ifBlank { imgAltRe.find(img)?.groupValues?.get(1)?.trim().orEmpty() }
                .ifBlank { return@forEach continue }
            val poster = imgSrcRe.find(img)?.groupValues?.get(1)?.trim().orEmpty().ifBlank { null }
            out.add(TmfCard(href, cleanTitle(titleRaw), poster, looksSeries(titleRaw)))
        }
        return out
    }

    fun TmfCard.toMedia(): HikariMedia? {
        if (url.isBlank() || title.isBlank()) return null
        return HikariMedia(
            id = url,
            title = title,
            type = if (isSeries) HikariMediaType.SERIES else HikariMediaType.MOVIE,
            posterUrl = poster,
            rawType = if (isSeries) "tv" else "movie",
        )
    }

    fun TmfCard.toMediaPub(): HikariMedia? = with(TmfDetail) { toMedia() }

    private val packRe = Regex("""(?i)\b(zip|rar|7z|batch)\b""")

    private val groupStartRe = Regex("""<div[^>]*mfx-download-group[^>]*>""", RegexOption.IGNORE_CASE)
    private val h3Re = Regex("""<h3[^>]*>(.*?)</h3>""", setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE))
    private val groupAnchorRe = Regex(
        """<a[^>]+href="([^"]+)"[^>]*>([\s\S]{0,300}?)</a>""",
        setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
    )
    private val endMarkerRe = Regex(
        """sharedaddy|id="comments"|class="comments|post-navigation|</article>""",
        RegexOption.IGNORE_CASE,
    )

    fun parseGroups(html: String): List<Group> {
        val starts = groupStartRe.findAll(html).toList()
        if (starts.isEmpty()) return emptyList()
        val out = ArrayList<Group>()
        for ((i, s) in starts.withIndex()) {
            var end = starts.getOrNull(i + 1)?.range?.first ?: html.length
            val marker = endMarkerRe.find(html, s.range.first)?.range?.first
            if (marker != null && marker < end) end = marker
            val block = html.substring(s.range.first, end)
            val label = h3Re.find(block)?.groupValues?.get(1)
                ?.replace(Regex("<[^>]+>"), " ")?.replace(Regex("\\s+"), " ")?.trim().orEmpty()
            if (packRe.containsMatchIn(label)) continue
            for (a in groupAnchorRe.findAll(block)) {
                val href = a.groupValues[1].trim()
                val text = a.groupValues[2].replace(Regex("<[^>]+>"), " ").replace(Regex("\\s+"), " ").trim()
                when {
                    !href.startsWith("http") -> continue
                    text.contains("Batch", true) || text.contains("Zip", true) -> continue
                    packRe.containsMatchIn(text) -> continue
                }
                out.add(Group(label, href))
            }
        }
        return out.distinctBy { it.redirectUrl }
    }

    fun seasonNumber(text: String): Int? =
        Regex("""Season\s*(\d+)""", RegexOption.IGNORE_CASE).find(text)?.groupValues?.get(1)?.toIntOrNull()
            ?: Regex("""\bS(\d+)\b""", RegexOption.IGNORE_CASE).find(text)?.groupValues?.get(1)?.toIntOrNull()

    private fun stripTags(s: String): String =
        s.replace(Regex("<[^>]+>"), " ").replace(Regex("\\s+"), " ").trim()

    fun parse(html: String, url: String): Detail? {
        val titleRaw = Regex(
            """<h2[^>]*mfx-main-title[^>]*>(.*?)</h2>""",
            setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
        ).find(html)?.groupValues?.get(1)?.let { stripTags(it) }
            ?: Regex(
                """<h1[^>]*entry-title[^>]*>(.*?)</h1>""",
                setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
            ).find(html)?.groupValues?.get(1)?.let { stripTags(it) }
            ?: Regex("""<title>([^<]*)""", RegexOption.IGNORE_CASE)
                .find(html)?.groupValues?.get(1)?.substringBefore(" - TheMoviesflix")?.trim()
            ?: return null
        if (titleRaw.isBlank()) return null

        val pageTitle = Regex("""<title>([^<]*)""", RegexOption.IGNORE_CASE)
            .find(html)?.groupValues?.get(1).orEmpty()
        val poster = Regex(
            """<meta[^>]+property="og:image"[^>]+content="([^"]+)"""",
            RegexOption.IGNORE_CASE,
        ).find(html)?.groupValues?.get(1)
        val plot = Regex(
            """<div[^>]*mfx-plot-box[^>]*>(.*?)</div>""",
            setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
        ).find(html)?.groupValues?.get(1)?.let { stripTags(it) }
            ?: Regex(
                """<meta[^>]+property="og:description"[^>]+content="([^"]+)"""",
                RegexOption.IGNORE_CASE,
            ).find(html)?.groupValues?.get(1)

        fun infoField(name: String): String? {
            val li = Regex(
                """<li[^>]*>([^<]*?$name[^<]*)</li>""",
                setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
            ).find(html)?.groupValues?.get(1) ?: return null
            return li.replace(Regex("$name:\\s*", RegexOption.IGNORE_CASE), "").trim().ifBlank { null }
        }
        val year = infoField("Release Year")?.let { Regex("""(\d{4})""").find(it)?.groupValues?.get(1)?.toIntOrNull() }
            ?: Regex("""\((\d{4})\)""").find(titleRaw)?.groupValues?.get(1)?.toIntOrNull()
            ?: Regex("""(\d{4})""").find(titleRaw)?.groupValues?.get(1)?.toIntOrNull()
        val genres = infoField("Genres")?.split(",")?.map { it.trim() }?.filter { it.isNotBlank() } ?: emptyList()

        val groups = parseGroups(html)
        val allText = titleRaw + " " + pageTitle + " " + url + " " + groups.joinToString(" ") { it.label }
        val isSeries = looksSeries(allText) || allText.contains("Episode", true)

        return Detail(cleanTitle(titleRaw), poster, plot, year, genres, isSeries, groups)
    }
}
