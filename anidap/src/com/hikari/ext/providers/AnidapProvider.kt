package com.hikari.ext.providers

import android.util.Base64
import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import com.hikari.ext.HikariSubtitle
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import org.json.JSONArray
import org.json.JSONObject
import java.net.URLEncoder

/**
 * Anidap (anidap.lol) — anime via REST + chad + GraphQL endpoints.
 *
 * Faithful native port of KSHITIJ8473/raghav Anidap (CloudStream MainAPI),
 * verified live Oct 2026:
 *  - Firebase-hosted domain table (refreshDomain) with https://anidap.lol fallback.
 *  - Trending: GET {site}/api/anime/trending
 *    -> {success,data:{data:{currentPage,hasNextPage,results:[{id,title{
 *       romaji,english,userPreferred},image,description,status,cover,rating,
 *       releaseDate,genres,totalEpisodes,duration,type}]}}} (single page).
 *  - Recent: GET https://graphql.anidap.lol/api/recent?page=
 *    -> {page,hasNextPage,results:[{id(slug),anilistId,titleRomaji,
 *       titleEnglish,coverImage:{extraLarge},episodes,format,genres,isSub,isDub}]}
 *  - Plus AniList GraphQL rows (popular, top rated, movies, upcoming).
 *  - Search: GET {site}/api/anime/search?q= -> {results:[...]}.
 *  - Detail: GET {site}/api/anime/{anilistId|slug} -> {success,data:{id(slug),
 *    titles{en},titleEnglish,titleRomaji,coverImage{extraLarge},bannerImage,
 *    description,seasonYear,duration,genres[{name}],format,episodeCount}}.
 *  - Episodes: GET https://chad.anidap.lol/rest/api/episodes?id={slug}
 *    (+ ep1 servers fallback for sub/dub flags).
 *  - Servers: GET .../servers?id={slug}&epNum={n} -> {subProviders,dubProviders:
 *    [{id,tip}]} (adp first, like the web client).
 *  - Sources: GET .../sources?id=&epNum=&type=&providerId= -> {sources:[{url,
 *    quality,type}],tracks,headers}; HLS validated master->variant->segment,
 *    provider URL transforms (AnidapUrl port), mp4/dash probed.
 */
class AnidapProvider : HikariProvider {

    override val id = "anidap"
    override val name = "Anidap"
    override val mainUrl = "https://anidap.lol"
    override val description = "Anime from anidap.lol — trending, recently added, popular, top rated, movies and upcoming, sub & dub."
    override val version = 2
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("trending", "Trending", HikariMediaType.SERIES),
        HikariCatalog("recent", "Recently Added", HikariMediaType.SERIES),
        HikariCatalog("popular", "Most Popular", HikariMediaType.SERIES),
        HikariCatalog("top-rated", "Top Rated", HikariMediaType.SERIES),
        HikariCatalog("movies", "Anime Movies", HikariMediaType.MOVIE),
        HikariCatalog("upcoming", "Upcoming", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        AdApi.refreshDomain()
        val p = page.coerceAtLeast(1)
        return when (catalog.id) {
            "trending" -> if (p > 1) emptyList() else AdApi.trending().mapNotNull { it.toMedia() }
            "recent" -> AdApi.recent(p).mapNotNull { it.toMedia() }
            "popular" -> AdApi.anilistRow(AdApi.AL_POPULAR, p).mapNotNull { it.toMedia() }
            "top-rated" -> AdApi.anilistRow(AdApi.AL_TOP, p).mapNotNull { it.toMedia() }
            "movies" -> AdApi.anilistRow(AdApi.AL_MOVIES, p).mapNotNull { it.toMedia() }
            "upcoming" -> AdApi.anilistRow(AdApi.AL_UPCOMING, p).mapNotNull { it.toMedia() }
            else -> emptyList()
        }
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.length < 2 || page > 1) return emptyList()
        AdApi.refreshDomain()
        return AdApi.search(q).mapNotNull { it.toMedia() }
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        AdApi.refreshDomain()
        val key = AdApi.seriesKey(media.id) ?: return media
        val d = AdApi.detail(key) ?: return media
        val type = when (d.format.uppercase()) {
            "MOVIE" -> HikariMediaType.MOVIE
            else -> HikariMediaType.SERIES
        }
        return media.copy(
            id = "anidap:${d.anilistId.takeIf { it.isNotBlank() } ?: key}",
            title = d.title,
            type = type,
            posterUrl = d.poster ?: media.posterUrl,
            backdropUrl = d.banner ?: media.backdropUrl,
            overview = d.plot ?: media.overview,
            genres = d.genres.takeIf { it.isNotEmpty() } ?: media.genres,
            year = d.year ?: media.year,
        )
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        AdApi.refreshDomain()
        val key = AdApi.seriesKey(media.id) ?: return null
        val d = AdApi.detail(key)
        val slug = d?.slug?.takeIf { it.isNotBlank() } ?: key
        if (d != null) AdApi.rememberSlug(d.anilistId, slug)
        val eps = AdApi.chadEpisodes(slug)
        var ep1Sub: Boolean? = null
        var ep1Dub: Boolean? = null
        if (eps == null || eps.any { it.hasSub == null || it.hasDub == null }) {
            val s = AdApi.serversForEpisode(slug, "1")
            ep1Sub = (s["sub"]?.size ?: 0) > 0
            ep1Dub = (s["dub"]?.size ?: 0) > 0
        }
        val seriesId = d?.anilistId?.takeIf { it.isNotBlank() } ?: key
        fun list(dub: Boolean): List<HikariEpisode> {
            if (eps != null) {
                return eps.filter { e ->
                    if (dub) e.hasDub ?: (ep1Dub ?: false) else e.hasSub ?: (ep1Sub ?: true)
                }.map { e ->
                    val filler = if (e.isFiller) " (Filler)" else ""
                    HikariEpisode(number = e.number, id = "$seriesId|${if (dub) "dub" else "sub"}|${e.number}", name = (e.name?.let { "$it$filler" } ?: "Episode ${e.number}$filler"))
                }
            }
            val total = d?.totalEps ?: 0
            if (total <= 0) return emptyList()
            val any = if (dub) ep1Dub ?: false else ep1Sub ?: true
            if (!any) return emptyList()
            return (1..minOf(total, 3000)).map { n ->
                HikariEpisode(number = n, id = "$seriesId|${if (dub) "dub" else "sub"}|$n", name = "Episode $n")
            }
        }
        val sub = list(false)
        val dub = list(true)
        if (sub.isEmpty() && dub.isEmpty()) return null
        return sub + dub
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        AdApi.refreshDomain()
        val ref = episode?.id ?: return emptyList()
        val parts = ref.split("|")
        if (parts.size != 3) return emptyList()
        val seriesId = parts[0]
        val type = if (parts[1] == "dub") "dub" else "sub"
        val epNum = parts[2]
        val slug = AdApi.slugFor(seriesId) ?: AdApi.detail(seriesId)?.slug?.also { AdApi.rememberSlug(seriesId, it) } ?: seriesId
        val providers = AdApi.serversForEpisode(slug, epNum)[type] ?: emptyList()
        if (providers.isEmpty()) return emptyList()
        val out = java.util.Collections.synchronizedList(ArrayList<HikariStream>())
        val subSeen = java.util.Collections.synchronizedSet(HashSet<String>())
        coroutineScope {
            providers.map { prov ->
                async {
                    val payload = AdApi.fetchSources(slug, epNum, type, prov.id) ?: return@async
                    for ((url, label) in payload.tracks) {
                        if (!url.startsWith("http") || !subSeen.add(url)) continue
                    }
                    val subs = payload.tracks.filter { it.first.startsWith("http") }.distinctBy { it.first }
                        .map { HikariSubtitle(it.second.ifBlank { "Subtitle" }, it.first) }
                    val tip = AdApi.normalizeTip(prov.tip)
                    val hard = tip?.contains("Hard", true) == true
                    val labelBase = "Anidap ${prov.id}" + if (hard) " (Hardsub)" else ""
                    for (src in payload.sources) {
                        val srcUrl = src.url
                        val proxyUrl = AdApi.transform(srcUrl, prov.id, payload.referer)
                        when {
                            AdApi.looksLikeDash(srcUrl, src.type) -> {
                                val bytes = AdApi.fetchBytes(srcUrl, payload.headers, 1023)
                                if (bytes != null && bytes.size > 512) {
                                    out.add(HikariStream(name = labelBase, url = srcUrl, headers = payload.headers, subtitles = subs))
                                }
                            }
                            AdApi.looksLikeHls(srcUrl, src.type) -> {
                                var variants = AdApi.validateHls(srcUrl, payload.headers)
                                var headers = payload.headers
                                if (variants == null && proxyUrl != srcUrl) {
                                    val proxied = AdApi.validateHls(proxyUrl, AdApi.proxyHeaders())
                                    if (proxied != null) {
                                        variants = proxied
                                        headers = AdApi.proxyHeaders()
                                    }
                                }
                                if (variants == null) continue
                                for (v in variants) {
                                    out.add(HikariStream(name = labelBase, url = v.url, headers = headers, subtitles = subs, isM3u8 = true))
                                }
                            }
                            AdApi.looksLikeMp4(srcUrl, src.type) -> {
                                val bytes = AdApi.fetchBytes(srcUrl, payload.headers, 2047)
                                if (AdApi.isVideoBytes(bytes)) {
                                    out.add(HikariStream(name = labelBase, url = srcUrl, headers = payload.headers, subtitles = subs))
                                } else if (proxyUrl != srcUrl) {
                                    val b2 = AdApi.fetchBytes(proxyUrl, AdApi.proxyHeaders(), 2047)
                                    if (AdApi.isVideoBytes(b2)) {
                                        out.add(HikariStream(name = labelBase, url = proxyUrl, headers = AdApi.proxyHeaders(), subtitles = subs))
                                    }
                                }
                            }
                            else -> {
                                val captured = HikariNet.resolveWithWebView(
                                    srcUrl, AdApi.MEDIA_CAPTURE,
                                    timeoutMs = 45_000,
                                )
                                if (captured.isNotEmpty()) {
                                    for (r in captured) {
                                        out.add(HikariStream(name = labelBase, url = r.url, headers = r.headers.ifEmpty { payload.headers }, subtitles = subs, isM3u8 = r.url.contains("m3u8") || r.url.contains("master.txt")))
                                    }
                                } else {
                                    val bytes = AdApi.fetchBytes(srcUrl, payload.headers, 2047)
                                    if (AdApi.isVideoBytes(bytes)) {
                                        out.add(HikariStream(name = labelBase, url = srcUrl, headers = payload.headers, subtitles = subs))
                                    }
                                }
                            }
                        }
                    }
                }
            }.awaitAll()
        }
        return out.distinctBy { it.url }
    }

    private fun AdItem.toMedia(): HikariMedia? {
        if (anilistId.isBlank() || title.isBlank()) return null
        val type = if (format.equals("MOVIE", true)) HikariMediaType.MOVIE else HikariMediaType.SERIES
        return HikariMedia(id = "anidap:$anilistId", title = title, type = type, posterUrl = poster, backdropUrl = banner, overview = plot, genres = genres, year = year)
    }
}

// ---------------------------------------------------------------------------

private data class AdItem(
    val anilistId: String,
    val slug: String?,
    val title: String,
    val poster: String?,
    val banner: String?,
    val plot: String?,
    val genres: List<String>,
    val year: Int?,
    val format: String,
)

private data class AdDetail(
    val slug: String,
    val anilistId: String,
    val title: String,
    val poster: String?,
    val banner: String?,
    val plot: String?,
    val genres: List<String>,
    val year: Int?,
    val format: String,
    val totalEps: Int,
)

private data class AdEp(val number: Int, val name: String?, val thumb: String?, val desc: String?, val isFiller: Boolean, val hasSub: Boolean?, val hasDub: Boolean?)
private data class AdProvider(val id: String, val tip: String?)
private data class AdSource(val url: String, val quality: String?, val type: String?)
private data class AdPayload(val sources: List<AdSource>, val tracks: List<Pair<String, String>>, val headers: Map<String, String>, val referer: String?)
private data class AdVariant(val quality: Int?, val url: String)

private object AdApi {
    const val DEFAULT_SITE = "https://anidap.lol"
    const val CHAD = "https://chad.anidap.lol/rest/api"
    const val GRAPHQL = "https://graphql.anidap.lol"
    const val ANILIST = "https://graphql.anilist.co"
    const val UA = "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36"
    private const val DOMAIN_URL = "https://cloudstreampluginhelper-default-rtdb.firebaseio.com/.json"

    @Volatile private var site = DEFAULT_SITE
    @Volatile private var lastDomainFetch = 0L
    private val slugCache = java.util.concurrent.ConcurrentHashMap<String, String>()
    private val serversCache = java.util.concurrent.ConcurrentHashMap<String, Pair<Long, Map<String, List<AdProvider>>>>()

    val MEDIA_CAPTURE = Regex("""https?://[^"' \s]+\.m3u8[^"' \s]*|https?://[^"' \s]*master\.txt[^"' \s]*|https?://[^"' \s]+\.mp4[^"' \s]*""")

    suspend fun refreshDomain() {
        val now = System.currentTimeMillis()
        if (now - lastDomainFetch < 5 * 60 * 1000L) return
        lastDomainFetch = now
        try {
            val txt = HikariNet.getString(DOMAIN_URL) ?: return
            val o = JSONObject(txt)
            val v = o.optString("anidap").ifBlank { o.optString("anidap_url") }.trim().removeSuffix("/")
            if (v.startsWith("http")) site = v
        } catch (t: Throwable) { }
    }

    fun seriesKey(mediaId: String): String? {
        val tail = mediaId.substringAfterLast("/").substringAfterLast(":").substringBefore("|").substringBefore("?")
        return tail.takeIf { it.isNotBlank() }
    }

    fun rememberSlug(seriesId: String, slug: String) {
        if (seriesId.isNotBlank() && slug.isNotBlank()) slugCache[seriesId] = slug
    }

    fun slugFor(seriesId: String): String? = slugCache[seriesId]

    private fun baseHeaders(): Map<String, String> = mapOf("Referer" to "$site/home", "Accept" to "application/json", "User-Agent" to UA)

    fun proxyHeaders(): Map<String, String> = mapOf(
        "User-Agent" to UA,
        "Accept" to "*/*",
        "Accept-Language" to "en-US,en;q=0.9",
        "Origin" to site,
        "Referer" to "$site/",
        "Sec-Fetch-Dest" to "empty",
        "Sec-Fetch-Mode" to "cors",
        "Sec-Fetch-Site" to "cross-site",
    )

    fun normalizeTip(tip: String?): String? {
        if (tip.isNullOrBlank()) return null
        return tip.replace("Hard sub", "Hardsub").replace("Soft sub", "Soft Sub")
    }

    // -- catalogs -----------------------------------------------------------
    suspend fun trending(): List<AdItem> {
        val body = HikariNet.getStringSmart("$site/api/anime/trending", baseHeaders()) ?: return emptyList()
        return try {
            var root = JSONObject(body)
            // unwrap {success,data:{...}} layers until results appears
            var guard = 0
            while (!root.has("results") && guard++ < 4) {
                val d = root.optJSONObject("data") ?: break
                root = d
            }
            val arr = root.optJSONArray("results") ?: return emptyList()
            buildList {
                for (i in 0 until arr.length()) {
                    val n = arr.optJSONObject(i) ?: continue
                    val id = n.optString("id").takeIf { it.isNotBlank() && it != "null" } ?: continue
                    val t = n.optJSONObject("title")
                    val title = t?.optString("userPreferred")?.takeIf { it.isNotBlank() }
                        ?: t?.optString("english")?.takeIf { it.isNotBlank() }
                        ?: t?.optString("romaji")?.takeIf { it.isNotBlank() }
                        ?: n.optString("titleEnglish").takeIf { it.isNotBlank() }
                        ?: continue
                    add(AdItem(
                        anilistId = id,
                        slug = n.optString("slug").takeIf { it.isNotBlank() },
                        title = title,
                        poster = n.optString("image").takeIf { it.startsWith("http") },
                        banner = n.optString("cover").takeIf { it.startsWith("http") },
                        plot = n.optString("description").takeIf { it.isNotBlank() },
                        genres = n.optJSONArray("genres")?.let { g -> List(g.length()) { g.optString(it) } } ?: emptyList(),
                        year = n.optInt("releaseDate", 0).takeIf { it > 0 },
                        format = n.optString("type"),
                    ))
                }
            }
        } catch (t: Throwable) { emptyList() }
    }

    suspend fun recent(page: Int): List<AdItem> {
        val body = HikariNet.getStringSmart("$GRAPHQL/api/recent?page=$page", baseHeaders()) ?: return emptyList()
        return try {
            val arr = JSONObject(body).optJSONArray("results") ?: return emptyList()
            buildList {
                for (i in 0 until arr.length()) {
                    val n = arr.optJSONObject(i) ?: continue
                    if (n.optBoolean("isAdult", false)) continue
                    val aid = n.optString("anilistId").takeIf { it.isNotBlank() && it != "null" } ?: continue
                    val title = n.optString("titleEnglish").takeIf { it.isNotBlank() }
                        ?: n.optString("titleRomaji").takeIf { it.isNotBlank() } ?: continue
                    val c = n.optJSONObject("coverImage")
                    add(AdItem(
                        anilistId = aid,
                        slug = n.optString("id").takeIf { it.isNotBlank() },
                        title = title,
                        poster = c?.optString("extraLarge")?.takeIf { it.startsWith("http") } ?: c?.optString("large")?.takeIf { it.startsWith("http") },
                        banner = null, plot = null,
                        genres = n.optJSONArray("genres")?.let { g -> List(g.length()) { g.optString(it) } } ?: emptyList(),
                        year = null,
                        format = n.optString("format"),
                    ))
                }
            }
        } catch (t: Throwable) { emptyList() }
    }

    suspend fun search(q: String): List<AdItem> {
        val body = HikariNet.getStringSmart("$site/api/anime/search?q=${URLEncoder.encode(q, "UTF-8")}", baseHeaders()) ?: return emptyList()
        return try {
            val o = JSONObject(body)
            val arr = o.optJSONArray("results") ?: o.optJSONObject("data")?.optJSONArray("results") ?: return emptyList()
            buildList {
                for (i in 0 until arr.length()) {
                    val n = arr.optJSONObject(i) ?: continue
                    val aid = n.optString("anilistId").takeIf { it.isNotBlank() && it != "null" }
                        ?: n.optString("id").takeIf { it.isNotBlank() } ?: continue
                    val t = n.optJSONObject("title")
                    val title = t?.optString("userPreferred")?.takeIf { it.isNotBlank() }
                        ?: t?.optString("english")?.takeIf { it.isNotBlank() }
                        ?: t?.optString("romaji")?.takeIf { it.isNotBlank() }
                        ?: n.optString("titleEnglish").takeIf { it.isNotBlank() }
                        ?: n.optString("titleRomaji").takeIf { it.isNotBlank() } ?: continue
                    val c = n.optJSONObject("coverImage")
                    add(AdItem(
                        anilistId = aid,
                        slug = n.optString("slug").takeIf { it.isNotBlank() } ?: n.optString("id").takeIf { it.contains("-") },
                        title = title,
                        poster = c?.optString("extraLarge")?.takeIf { it.startsWith("http") }
                            ?: n.optString("image").takeIf { it.startsWith("http") },
                        banner = null, plot = null, genres = emptyList(), year = null, format = n.optString("format"),
                    ))
                }
            }
        } catch (t: Throwable) { emptyList() }
    }

    const val AL_POPULAR = "POPULARITY_DESC|"
    const val AL_TOP = "SCORE_DESC|"
    const val AL_MOVIES = "POPULARITY_DESC|MOVIE"
    const val AL_UPCOMING = "POPULARITY_DESC|UPCOMING"

    suspend fun anilistRow(kind: String, page: Int): List<AdItem> {
        val parts = kind.split("|")
        val sort = parts[0]
        val extra = parts.getOrNull(1) ?: ""
        val format = if (extra == "MOVIE") ", format: MOVIE" else ""
        val status = if (extra == "UPCOMING") ", status: NOT_YET_RELEASED" else ""
        val query = """query (${'$'}page: Int, ${'$'}perPage: Int) { Page(page: ${'$'}page, perPage: ${'$'}perPage) { media(type: ANIME, sort: $sort$format$status) { id title { romaji english } coverImage { large extraLarge } format status episodes seasonYear genres } } }"""
        val body = JSONObject().put("query", query)
            .put("variables", JSONObject().put("page", page).put("perPage", 24)).toString()
        val resp = HikariNet.postString(ANILIST, body, mapOf("Content-Type" to "application/json", "User-Agent" to UA, "Referer" to "https://anilist.co/")) ?: return emptyList()
        return try {
            val arr = JSONObject(resp).optJSONObject("data")?.optJSONObject("Page")?.optJSONArray("media") ?: return emptyList()
            buildList {
                for (i in 0 until arr.length()) {
                    val m = arr.optJSONObject(i) ?: continue
                    val aid = m.optInt("id", 0).takeIf { it != 0 }?.toString() ?: continue
                    val t = m.optJSONObject("title")
                    val title = t?.optString("english")?.takeIf { it.isNotBlank() } ?: t?.optString("romaji")?.takeIf { it.isNotBlank() } ?: continue
                    val c = m.optJSONObject("coverImage")
                    add(AdItem(
                        anilistId = aid, slug = null, title = title,
                        poster = c?.optString("extraLarge")?.takeIf { it.startsWith("http") } ?: c?.optString("large")?.takeIf { it.startsWith("http") },
                        banner = null, plot = null,
                        genres = m.optJSONArray("genres")?.let { g -> List(g.length()) { g.optString(it) } } ?: emptyList(),
                        year = m.optInt("seasonYear", 0).takeIf { it > 0 },
                        format = m.optString("format"),
                    ))
                }
            }
        } catch (t: Throwable) { emptyList() }
    }

    // -- detail / episodes --------------------------------------------------
    suspend fun detail(key: String): AdDetail? {
        val body = HikariNet.getStringSmart("$site/api/anime/$key", baseHeaders()) ?: return null
        return try {
            val root = JSONObject(body)
            val d = root.optJSONObject("data")?.takeIf { it.length() > 0 } ?: root
            val titles = d.optJSONObject("titles")
            val title = titles?.optString("en")?.takeIf { it.isNotBlank() }
                ?: d.optString("titleEnglish").takeIf { it.isNotBlank() }
                ?: d.optString("titleRomaji").takeIf { it.isNotBlank() }
                ?: titles?.optString("ja")?.takeIf { it.isNotBlank() } ?: return null
            val cover = d.optJSONObject("coverImage")
            val genres = ArrayList<String>()
            d.optJSONArray("genres")?.let { g ->
                for (i in 0 until g.length()) {
                    val e = g.opt(i)
                    val name = if (e is JSONObject) e.optString("name") else e.toString()
                    if (name.isNotBlank()) genres.add(name)
                }
            }
            AdDetail(
                slug = d.optString("id").takeIf { it.isNotBlank() } ?: key,
                anilistId = d.optString("anilistId").takeIf { it.isNotBlank() && it != "null" } ?: key,
                title = title,
                poster = cover?.optString("extraLarge")?.takeIf { it.startsWith("http") } ?: cover?.optString("large")?.takeIf { it.startsWith("http") },
                banner = d.optString("bannerImage").takeIf { it.startsWith("http") },
                plot = d.optString("description").takeIf { it.isNotBlank() },
                genres = genres,
                year = d.optInt("seasonYear", 0).takeIf { it > 0 },
                format = d.optString("format").ifBlank { d.optString("type") },
                totalEps = d.optInt("episodeCount", d.optInt("episodes", 0)),
            )
        } catch (t: Throwable) { null }
    }

    suspend fun chadEpisodes(slug: String): List<AdEp>? {
        val body = HikariNet.getStringSmart("$CHAD/episodes?id=${URLEncoder.encode(slug, "UTF-8")}", mapOf("Referer" to "$site/", "Accept" to "application/json", "User-Agent" to UA)) ?: return null
        return try {
            val t = body.trimStart()
            val arr = if (t.startsWith("[")) JSONArray(t) else JSONObject(t).let { o -> o.optJSONArray("episodes") ?: o.optJSONArray("data") ?: o.optJSONArray("results") ?: return null }
            buildList {
                for (i in 0 until arr.length()) {
                    val e = arr.optJSONObject(i) ?: continue
                    val num = e.optInt("number", -1)
                    if (num < 1) continue
                    val titles = e.optJSONObject("titles")
                    add(AdEp(
                        number = num,
                        name = titles?.optString("en")?.takeIf { it.isNotBlank() } ?: titles?.optString("x-jat")?.takeIf { it.isNotBlank() } ?: titles?.optString("ja")?.takeIf { it.isNotBlank() },
                        thumb = e.optString("img").takeIf { it.isNotBlank() },
                        desc = e.optString("description").takeIf { it.isNotBlank() },
                        isFiller = e.optBoolean("isFiller", false),
                        hasSub = if (e.has("hasSub")) e.optBoolean("hasSub") else null,
                        hasDub = if (e.has("hasDub")) e.optBoolean("hasDub") else null,
                    ))
                }
            }
        } catch (t: Throwable) { null }
    }

    suspend fun serversForEpisode(slug: String, epNum: String): Map<String, List<AdProvider>> {
        val key = "$slug|$epNum"
        serversCache[key]?.let { (ts, v) -> if (System.currentTimeMillis() - ts < 10 * 60_000L) return v }
        val out = try {
            val body = HikariNet.getStringSmart("$CHAD/servers?id=${URLEncoder.encode(slug, "UTF-8")}&epNum=$epNum", mapOf("Referer" to "$site/", "Accept" to "application/json", "User-Agent" to UA)) ?: return emptyMap()
            val root = JSONObject(body)
            fun parse(field: String): List<AdProvider> {
                val arr = root.optJSONArray(field) ?: return emptyList()
                return buildList {
                    for (i in 0 until arr.length()) {
                        val n = arr.opt(i) ?: continue
                        if (n is JSONObject) {
                            val id = n.optString("id")
                            if (id.isNotBlank()) add(AdProvider(id, n.optString("tip").takeIf { it.isNotBlank() }))
                        } else if (n.toString().isNotBlank()) add(AdProvider(n.toString(), null))
                    }
                }
            }
            fun withAdp(list: List<AdProvider>): List<AdProvider> =
                if (list.any { it.id == "adp" }) list else listOf(AdProvider("adp", null)) + list
            mapOf("sub" to withAdp(parse("subProviders")), "dub" to withAdp(parse("dubProviders")))
        } catch (t: Throwable) { emptyMap() }
        serversCache[key] = System.currentTimeMillis() to out
        return out
    }

    suspend fun fetchSources(slug: String, epNum: String, type: String, providerId: String): AdPayload? {
        return try {
            val body = HikariNet.getStringSmart("$CHAD/sources?id=${URLEncoder.encode(slug, "UTF-8")}&epNum=$epNum&type=$type&providerId=$providerId", mapOf("Referer" to "$site/", "Accept" to "application/json", "User-Agent" to UA)) ?: return null
            val root = JSONObject(body)
            val node = if (root.optJSONObject("data")?.has("sources") == true) root.getJSONObject("data") else root
            val srcArr = node.optJSONArray("sources") ?: return null
            val sources = buildList {
                for (i in 0 until srcArr.length()) {
                    val s = srcArr.optJSONObject(i) ?: continue
                    val u = s.optString("url")
                    if (u.isBlank()) continue
                    add(AdSource(u, s.optString("quality").takeIf { it.isNotBlank() }, s.optString("type").takeIf { it.isNotBlank() }))
                }
            }
            if (sources.isEmpty()) return null
            val tracksNode = node.optJSONArray("tracks") ?: node.optJSONArray("subtitles")
            val tracks = ArrayList<Pair<String, String>>()
            tracksNode?.let { t ->
                for (i in 0 until t.length()) {
                    val e = t.optJSONObject(i) ?: continue
                    val u = e.optString("url").replace("https:///", "https://").replace("http:///", "http://")
                    if (u.isBlank()) continue
                    val label = e.optString("label").ifBlank { e.optString("lang").ifBlank { "Subtitle" } }
                    if (label.equals("thumbnails", true)) continue
                    tracks.add(u to label)
                }
            }
            val headers = HashMap<String, String>()
            node.optJSONObject("headers")?.let { h ->
                val keys = h.keys()
                while (keys.hasNext()) {
                    val k = keys.next()
                    val v = h.optString(k)
                    if (v.isNotBlank()) headers[k] = v
                }
            }
            val referer = headers["Referer"] ?: headers["referer"]
            AdPayload(sources, tracks, headers, referer)
        } catch (t: Throwable) { null }
    }

    // -- url utils (AnidapUrl port; okhttp -> HikariNet) --------------------
    private const val UWU_XOR_KEY = "10b06cdc1ca48c9fb0b94af97cc040cf"
    private const val SHIRO_HOST = "https://hls.dramavideo.se"
    private val UWU_HOSTS = listOf("https://cdnx.aniwatchtv.site")
    private val UWU_PROXY_KEYS = mapOf(
        "sora" to "https://krussdomi.com",
        "yuki" to "https://megaplay.buzz",
        "uwu" to "https://kwik.cx/",
        "kiwi" to "https://anidb.app/",
        "miku" to "https://allanime.uns.bio",
    )
    private val FALLBACK_EXCLUDE = setOf("vee", "neko")
    private val stripDomainRe = Regex("^https?://[^/]+")
    private fun stripDomain(url: String): String = url.replace(stripDomainRe, "")

    private fun uwuToken(url: String, key: String): String {
        val kb = UWU_XOR_KEY.toByteArray(Charsets.US_ASCII)
        val data = url.toByteArray(Charsets.UTF_8) + byteArrayOf(0) + key.toByteArray(Charsets.UTF_8)
        val out = ByteArray(data.size) { i -> (data[i].toInt() xor kb[i % kb.size].toInt()).toByte() }
        return Base64.encodeToString(out, Base64.URL_SAFE or Base64.NO_WRAP).trimEnd('=')
    }

    private fun uwuProxy(url: String, key: String, hostIndex: Int = 0): String {
        val h = UWU_HOSTS[hostIndex % UWU_HOSTS.size]
        return "$h/uwu/${uwuToken(url, key)}"
    }

    private fun shiroProxy(url: String): String {
        val sb = StringBuilder()
        for (b in url.toByteArray(Charsets.UTF_8)) sb.append(String.format("%02x", (b.toInt() xor 137) and 0xFF))
        return "$SHIRO_HOST/media/$sb&origin=https://kem.clvd.xyz/"
    }

    private fun beepRewrite(url: String): String = when {
        url.startsWith("https://bd.24stream.xyz/media") || url.startsWith("https://bd.aniwatchtv.site/media") -> url
        url.startsWith("/") -> "https://bd.aniwatchtv.site/media" + url.replace("/r2", "")
        else -> "https://bd.aniwatchtv.site/media" + stripDomain(url).replace("/r2", "")
    }

    fun transform(url: String, providerId: String, referer: String?): String {
        val p = providerId.lowercase()
        var r = url
        if (r.contains("https://vivibebe.site/public/stream/")) {
            r = r.replace("https://vivibebe.site/public/stream/", "https://hawk.aniwatchtv.site/media/")
        }
        if (r.startsWith("https://playeng.animeapps.top/r2/")) {
            r = "https://bd.aniwatchtv.site/media" + stripDomain(r).replace("/r2", "")
        }
        when (p) {
            "shiro" -> r = shiroProxy(r)
            "sora", "yuki", "uwu", "kiwi", "miku" -> r = uwuProxy(r, UWU_PROXY_KEYS[p] ?: return r)
            "mochi" -> r = r.replace("https://tools.fast4speed.rsvp", "https://mp4.24stream.xyz/storage")
            "beep" -> r = beepRewrite(r)
        }
        if (r == url && !referer.isNullOrBlank() && p !in FALLBACK_EXCLUDE) return uwuProxy(url, referer)
        if (r.startsWith("http://")) r = "https://" + r.removePrefix("http://")
        return r
    }

    suspend fun fetchBytes(url: String, headers: Map<String, String>, rangeEnd: Int = 2047): ByteArray? {
        return try {
            HikariNet.getBytes(url, headers + ("Range" to "bytes=0-$rangeEnd"))
        } catch (t: Throwable) { null }
    }

    private fun indexOf(haystack: ByteArray, needle: ByteArray, from: Int = 0): Int {
        outer@ for (i in from..haystack.size - needle.size) {
            for (j in needle.indices) {
                if (haystack[i + j] != needle[j]) continue@outer
            }
            return i
        }
        return -1
    }

    fun isVideoBytes(d: ByteArray?): Boolean {
        if (d == null || d.isEmpty()) return false
        if (d[0] == 0x47.toByte()) return true
        if (d.size > 8 && d[0] == 0x89.toByte() && d[1] == 0x50.toByte()) {
            val iend = indexOf(d, "IEND".toByteArray())
            if (iend >= 0) {
                for (i in iend + 4 until minOf(d.size, iend + 4 + 64)) {
                    if (d[i] == 0x47.toByte()) return true
                }
            }
            return false
        }
        if (indexOf(d, "ftyp".toByteArray()) in 0..64) return true
        return false
    }

    fun resolveUrl(base: String, ref: String): String {
        if (ref.startsWith("http")) return ref
        val schemeIdx = base.indexOf("://")
        if (schemeIdx < 0) return ref
        val h = base.substring(schemeIdx + 3).substringBefore("/")
        val origin = base.substring(0, schemeIdx + 3) + h
        return if (ref.startsWith("/")) origin + ref else base.substringBeforeLast("/") + "/" + ref
    }

    fun parseMaster(body: String, masterUrl: String): List<AdVariant> {
        val out = ArrayList<AdVariant>()
        var pendingHeight: Int? = null
        for (raw in body.lines()) {
            val line = raw.trim()
            if (line.startsWith("#EXT-X-STREAM-INF")) {
                pendingHeight = Regex("RESOLUTION=(\\d+)x(\\d+)").find(line)?.groupValues?.get(2)?.toIntOrNull()
            } else if (line.isNotEmpty() && !line.startsWith("#")) {
                out.add(AdVariant(pendingHeight, resolveUrl(masterUrl, line)))
                pendingHeight = null
            }
        }
        return out
    }

    private fun firstSegment(body: String, playlistUrl: String): String? {
        for (raw in body.lines()) {
            val line = raw.trim()
            if (line.isNotEmpty() && !line.startsWith("#")) return resolveUrl(playlistUrl, line)
        }
        return null
    }

    fun looksLikeHls(url: String, declaredType: String?): Boolean {
        val t = (declaredType ?: "").lowercase()
        val path = url.substringBefore("?")
        return t.contains("mpegurl") || t.contains("m3u8") || path.endsWith(".m3u8") ||
            path.endsWith(".txt") || url.contains(".m3u8") || url.contains("index.txt")
    }

    fun looksLikeDash(url: String, declaredType: String?): Boolean {
        val t = (declaredType ?: "").lowercase()
        return t.contains("dash") || url.substringBefore("?").endsWith(".mpd") || url.contains(".mpd")
    }

    fun looksLikeMp4(url: String, declaredType: String?): Boolean {
        val t = (declaredType ?: "").lowercase()
        val path = url.substringBefore("?")
        return t.contains("mp4") || t.contains("webm") || t.contains("matroska") ||
            path.endsWith(".mp4") || path.endsWith(".webm") || path.endsWith(".mkv")
    }

    suspend fun validateHls(masterUrl: String, headers: Map<String, String>): List<AdVariant>? {
        val master = try { HikariNet.getString(masterUrl, headers) } catch (t: Throwable) { null } ?: return null
        if (!master.contains("#EXTM3U")) return null
        val variants = parseMaster(master, masterUrl)
        val probeTarget = variants.maxByOrNull { it.quality ?: 0 }?.url ?: masterUrl
        val playlist = try { HikariNet.getString(probeTarget, headers) } catch (t: Throwable) { null } ?: return null
        if (!playlist.contains("#EXTM3U")) return null
        val seg = firstSegment(playlist, probeTarget) ?: return null
        val bytes = fetchBytes(seg, headers) ?: return null
        if (!isVideoBytes(bytes)) return null
        return if (variants.isEmpty()) listOf(AdVariant(null, masterUrl)) else variants
    }
}
