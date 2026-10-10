package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import com.hikari.ext.HikariSubtitle
import java.net.URLDecoder
import java.net.URLEncoder
import java.security.MessageDigest
import org.json.JSONArray
import org.json.JSONObject

/**
 * NetNaija (netnaija.film) — Nollywood / Hollywood / anime movies & series.
 *
 * Faithful native port of KSHITIJ8473/raghav NetNaija: the authenticated BFF
 * at h5-api.aoneroom.com/wefeed-h5api-bff drives everything (home sections,
 * trending, channel filters, rankings, detail, play, captions). Token
 * bootstrap uses the X-Client-Token "<ts>,<md5(reversed ts)>" scheme; the
 * JWT the server hands back through the x-user *response header* is not
 * visible through the HikariNet SDK (status+body only), so requests fly with
 * the client token — the BFF serves all list/detail/play calls that way
 * (verified live: /subject/trending returns 200 + real catalog JSON with no
 * auth at all). Rendered search-result pages (__NUXT_DATA__) back up search.
 */
class NetNaijaProvider : HikariProvider {

    override val id = "netnaija"
    override val name = "NetNaija"
    override var mainUrl = "https://netnaija.film"
    override val description = "Nollywood, Hollywood & anime movies and series from NetNaija."
    override val version = 2
    override val tvTypes = setOf(HikariMediaType.MOVIE, HikariMediaType.SERIES)

    private companion object {
        const val BFF = "https://h5-api.aoneroom.com/wefeed-h5api-bff"
        const val UA =
            "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36"
        const val MOVIES_CHANNEL = 1
        const val SERIES_CHANNEL = 2
        const val ANIME_CHANNEL = 1006
        const val RANK_TRENDING_ANIME = "62133389738001440"
        const val RANK_TOP100_ANIME = "1513079728666723416"
        const val FB_RTDB = "https://cloudstreampluginhelper-default-rtdb.firebaseio.com/.json"
    }

    private val baseHeaders = mapOf(
        "User-Agent" to UA,
        "Accept" to "application/json",
        "Origin" to "https://netnaija.film",
        "Referer" to "https://netnaija.film/",
        "X-Request-Lang" to "en",
        "X-Client-Info" to "{\"timezone\":\"Asia/Kolkata\"}",
    )
    private val playHeaders = mapOf(
        "Referer" to "https://netnaija.film/",
        "Origin" to "https://netnaija.film",
        "User-Agent" to UA,
    )

    // /home cache (one response holds every curated section).
    @Volatile private var homeCache: Map<String, JSONArray> = emptyMap()
    @Volatile private var homeCacheTime = 0L
    @Volatile private var dynamicRows: List<Pair<String, String>> = emptyList()
    private val homeLock = Any()

    private suspend fun refreshDomain() {
        try {
            val body = HikariNet.getString(FB_RTDB) ?: return
            val o = JSONObject(body)
            val d = o.optString("netnaija")
                .ifBlank { o.optString("netnaija_url") }
                .ifBlank { o.optString("netnaija_domain") }
            if (d.isNotBlank()) mainUrl = d.trimEnd('/')
        } catch (_: Exception) {
        }
    }

    private fun clientToken(): String {
        val ts = System.currentTimeMillis() / 1000
        val rev = ts.toString().reversed()
        val md5 = MessageDigest.getInstance("MD5").digest(rev.toByteArray())
            .joinToString("") { "%02x".format(it) }
        return "$ts,$md5"
    }

    private fun headers(extra: Map<String, String> = emptyMap()): Map<String, String> {
        val h = baseHeaders.toMutableMap()
        h["X-Client-Token"] = clientToken()
        h.putAll(extra)
        return h
    }

    private fun cleanTitle(t: String): String =
        t.replace(Regex("[^\\p{L}\\p{N} &+\\[\\]().'-]"), "").trim()

    // ------------------------------------------------------------------ rows

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("trending", "Trending Now", HikariMediaType.MOVIE),
        HikariCatalog("filter:$MOVIES_CHANNEL:Latest", "Latest Movies", HikariMediaType.MOVIE),
        HikariCatalog("filter:$SERIES_CHANNEL:Latest", "Latest Series", HikariMediaType.SERIES),
        HikariCatalog("filter:$ANIME_CHANNEL:Hottest", "Anime", HikariMediaType.SERIES),
        HikariCatalog("filter:$ANIME_CHANNEL:Latest", "Latest Anime", HikariMediaType.SERIES),
        HikariCatalog("rank:$RANK_TRENDING_ANIME", "Trending Anime", HikariMediaType.SERIES),
        HikariCatalog("rank:$RANK_TOP100_ANIME", "Top 100 Anime", HikariMediaType.SERIES),
    )

    private suspend fun homeSections(): Map<String, JSONArray> {
        synchronized(homeLock) {
            if (homeCache.isNotEmpty() && System.currentTimeMillis() - homeCacheTime < 10 * 60_000L)
                return homeCache
        }
        return try {
            val root = HikariNet.getJson("$BFF/home", headers()) ?: return homeCache
            val ops = root.optJSONObject("data")?.optJSONArray("operatingList")
                ?: return homeCache
            val byTitle = LinkedHashMap<String, JSONArray>()
            for (i in 0 until ops.length()) {
                val s = ops.optJSONObject(i) ?: continue
                val title = s.optString("title").ifBlank { continue }
                val subjects = s.optJSONArray("subjects") ?: continue
                val ok = JSONArray()
                for (j in 0 until subjects.length()) {
                    val o = subjects.optJSONObject(j) ?: continue
                    if (o.optString("title").isBlank() || o.optString("detailPath").isBlank()) continue
                    ok.put(o)
                }
                if (ok.length() > 0) byTitle[title] = ok
            }
            if (byTitle.isNotEmpty()) {
                synchronized(homeLock) {
                    homeCache = byTitle
                    homeCacheTime = System.currentTimeMillis()
                    val seen = HashSet<String>()
                    dynamicRows = byTitle.keys.mapNotNull { raw ->
                        val d = cleanTitle(raw).ifBlank { raw }
                        if (seen.add(d.lowercase())) "home:$raw" to d else null
                    }
                }
            }
            byTitle
        } catch (_: Exception) {
            homeCache
        }
    }

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        refreshDomain()
        // Prime curated sections so the dynamic rows below also resolve.
        try {
            homeSections()
        } catch (_: Exception) {
        }
        val p = page.coerceAtLeast(1)
        val key = catalog.id
        return try {
            when {
                key == "trending" -> {
                    val root = HikariNet.getJson("$BFF/subject/trending?page=$p&perPage=24", headers())
                        ?: return emptyList()
                    val data = root.optJSONObject("data") ?: return emptyList()
                    subjectsToMedia(data.optJSONArray("subjectList")) to
                        (data.optJSONObject("pager")?.optBoolean("hasMore") == true)
                }
                key.startsWith("filter:") -> {
                    val parts = key.split(":")
                    val channel = parts.getOrNull(1)?.toIntOrNull() ?: return emptyList()
                    val sort = parts.getOrNull(2) ?: "Hottest"
                    val body = JSONObject()
                        .put("page", p).put("perPage", 24)
                        .put("channelId", channel).put("sort", sort)
                    val txt = HikariNet.postString(
                        "$BFF/subject/filter", body.toString(),
                        headers(mapOf("Content-Type" to "application/json")),
                    ) ?: return emptyList()
                    val data = JSONObject(txt).optJSONObject("data") ?: return emptyList()
                    subjectsToMedia(data.optJSONArray("items")) to
                        (data.optJSONObject("pager")?.optBoolean("hasMore") == true)
                }
                key.startsWith("rank:") -> {
                    val listId = key.removePrefix("rank:")
                    val root = HikariNet.getJson(
                        "$BFF/ranking-list/content?id=$listId&page=$p&perPage=24", headers(),
                    ) ?: return emptyList()
                    val data = root.optJSONObject("data") ?: return emptyList()
                    subjectsToMedia(data.optJSONArray("subjectList")) to
                        (data.optJSONObject("pager")?.optBoolean("hasMore") == true)
                }
                key.startsWith("home:") -> {
                    if (p > 1) return emptyList()
                    val arr = homeSections()[key.removePrefix("home:")] ?: return emptyList()
                    subjectsToMedia(arr) to false
                }
                else -> return emptyList()
            }.first
        } catch (_: Exception) {
            emptyList()
        }
    }

    /** Curated site sections as extra rows (Hikari calls catalogs() once, so
     *  these ride getCatalog via "home:" keys primed above). */
    suspend fun extraCatalogs(): List<HikariCatalog> {
        homeSections()
        return dynamicRows.map { (k, name) ->
            HikariCatalog(k, name, HikariMediaType.SERIES)
        }
    }

    private fun subjectsToMedia(arr: JSONArray?): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        if (arr == null) return out
        for (i in 0 until arr.length()) {
            subjectToMedia(arr.optJSONObject(i))?.let { out.add(it) }
        }
        return out
    }

    private fun subjectToMedia(o: JSONObject?): HikariMedia? {
        if (o == null) return null
        val title = o.optString("title").ifBlank { return null }
        val path = o.optString("detailPath").ifBlank { return null }
        val poster = o.optJSONObject("cover")?.optString("url")?.takeIf { it.startsWith("http") }
        val movie = o.optInt("subjectType", 1) == 1
        return HikariMedia(
            id = "$mainUrl/movieDetail/$path",
            title = title,
            type = if (movie) HikariMediaType.MOVIE else HikariMediaType.SERIES,
            posterUrl = poster,
            year = o.optString("releaseDate").take(4).toIntOrNull(),
        )
    }

    // ----------------------------------------------------------------- search

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank()) return emptyList()
        refreshDomain()
        apiSearch(query.trim())?.let { if (it.isNotEmpty()) return it }
        return try {
            val html = HikariNet.getString(
                "$mainUrl/search-result?keyword=${URLEncoder.encode(query.trim(), "UTF-8")}",
                mapOf("User-Agent" to UA),
            ) ?: return emptyList()
            parseNuxtSearch(html)
        } catch (_: Exception) {
            emptyList()
        }
    }

    private suspend fun apiSearch(query: String): List<HikariMedia>? {
        return try {
            val body = JSONObject().put("keyword", query).put("page", 1).put("perPage", 20)
            val txt = HikariNet.postString("$BFF/subject/search", body.toString(), headers())
                ?: return null
            subjectsToMedia(JSONObject(txt).optJSONObject("data")?.optJSONArray("items"))
        } catch (_: Exception) {
            null
        }
    }

    // Nuxt flattens page state into one array of indexes; resolve level by level.
    private fun parseNuxtSearch(html: String): List<HikariMedia> {
        val m = Regex(
            """<script[^>]*id="__NUXT_DATA__"[^>]*>(.*?)</script>""",
            RegexOption.DOT_MATCHES_ALL,
        ).find(html) ?: return emptyList()
        val root = try {
            JSONArray(m.groupValues[1])
        } catch (_: Exception) {
            return emptyList()
        }
        for (i in 0 until root.length()) {
            val node = root.optJSONObject(i) ?: continue
            if (node.has("items") && node.has("pager") && node.opt("items") is Number) {
                val resolved = resolveNuxt(root, node.getInt("items"), 0) as? List<*> ?: continue
                val out = ArrayList<HikariMedia>()
                for (e in resolved) {
                    if (e is Map<*, *>) {
                        @Suppress("UNCHECKED_CAST")
                        val o = JSONObject(e as Map<String, Any?>)
                        subjectToMedia(o)?.let { out.add(it) }
                    }
                }
                return out
            }
        }
        return emptyList()
    }

    private fun resolveNuxt(arr: JSONArray, idx: Int, depth: Int): Any? {
        if (idx < 0 || idx >= arr.length() || depth > 8) return null
        return when (val n = arr.get(idx)) {
            is JSONObject -> {
                val out = LinkedHashMap<String, Any?>()
                for (k in n.keys()) out[k] = nuxtVal(arr, n.get(k), depth)
                out
            }
            is JSONArray -> (0 until n.length()).map { nuxtVal(arr, n.get(it), depth) }
            else -> n
        }
    }

    private fun nuxtVal(arr: JSONArray, v: Any?, depth: Int): Any? =
        if (v is Number) resolveNuxt(arr, v.toInt(), depth + 1) else v

    // ------------------------------------------------------------ episodes

    private fun detailPathOf(media: HikariMedia): String? =
        media.id.substringAfterLast("/").substringBefore("?").takeIf { it.isNotBlank() }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val path = detailPathOf(media) ?: return media
        refreshDomain()
        return try {
            val root = HikariNet.getJson("$BFF/detail?detailPath=$path", headers()) ?: return media
            val s = root.optJSONObject("data")?.optJSONObject("subject") ?: return media
            val title = s.optString("title").ifBlank { media.title }
            val poster = s.optJSONObject("cover")?.optString("url")?.takeIf { it.startsWith("http") }
                ?: media.posterUrl
            val genres = s.optString("genre").split(",").map { it.trim() }.filter { it.isNotEmpty() }
            media.copy(
                title = title,
                posterUrl = poster,
                backdropUrl = poster,
                overview = s.optString("description").ifBlank { null } ?: media.overview,
                year = s.optString("releaseDate").take(4).toIntOrNull() ?: media.year,
                genres = genres.ifEmpty { media.genres },
            )
        } catch (_: Exception) {
            media
        }
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val path = detailPathOf(media) ?: return null
        return try {
            val root = HikariNet.getJson("$BFF/detail?detailPath=$path", headers()) ?: return null
            val data = root.optJSONObject("data") ?: return null
            val s = data.optJSONObject("subject") ?: return null
            val movie = s.optInt("subjectType", 1) == 1
            val dubs = s.optJSONArray("dubs") ?: JSONArray().also {
                it.put(
                    JSONObject()
                        .put("subjectId", s.optString("subjectId"))
                        .put("detailPath", path)
                        .put("lanName", "Original Audio")
                        .put("lanCode", "en")
                        .put("type", 0)
                        .put("original", true),
                )
            }
            if (movie) {
                val id = "nn:" + URLEncoder.encode(
                    JSONObject().put("dubs", dubs).put("season", 0).put("episode", 0).toString(),
                    "UTF-8",
                )
                return listOf(
                    HikariEpisode(number = 1, id = id, name = s.optString("title"), image = media.posterUrl),
                )
            }
            val seasons = data.optJSONObject("resource")?.optJSONArray("seasons") ?: return null
            val out = ArrayList<HikariEpisode>()
            for (i in 0 until seasons.length()) {
                val sn = seasons.optJSONObject(i) ?: continue
                val seasonNum = sn.optInt("se", -1)
                val maxEp = sn.optInt("maxEp", 0)
                if (seasonNum <= 0 || maxEp <= 0) continue
                for (ep in 1..maxEp) {
                    val id = "nn:" + URLEncoder.encode(
                        JSONObject().put("dubs", dubs).put("season", seasonNum).put("episode", ep).toString(),
                        "UTF-8",
                    )
                    out.add(HikariEpisode(number = ep, season = seasonNum, id = id, name = "Episode $ep", image = media.posterUrl))
                }
            }
            out.sortWith(compareBy({ it.season }, { it.number }))
            out.ifEmpty { null }
        } catch (_: Exception) {
            null
        }
    }

    // -------------------------------------------------------------- streams

    private fun sourceLabel(dub: JSONObject): String? {
        val lan = dub.optString("lanName").ifBlank { return null }
        if (dub.optBoolean("original")) return "Original"
        val base = lan.substringBefore(" dub").substringBefore(" sub").trim()
        if (base.isEmpty()) return null
        val pretty = when (base.lowercase()) {
            "ptbr" -> "PT-BR"
            "esla" -> "ES-LA"
            else -> base.replaceFirstChar { it.uppercase() }
        }
        return if (dub.optInt("type", 0) == 1) "$pretty Hardsub" else "$pretty Dub"
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val ref = episode?.id ?: return emptyList()
        if (!ref.startsWith("nn:")) return emptyList()
        val epData = try {
            JSONObject(URLDecoder.decode(ref.removePrefix("nn:"), "UTF-8"))
        } catch (_: Exception) {
            return emptyList()
        }
        val season = epData.optInt("season")
        val ep = epData.optInt("episode")
        val dubs = epData.optJSONArray("dubs") ?: return emptyList()
        val out = ArrayList<HikariStream>()
        val subs = ArrayList<HikariSubtitle>()
        val seen = HashSet<String>()
        for (i in 0 until dubs.length()) {
            val dub = dubs.optJSONObject(i) ?: continue
            val dubId = dub.optString("subjectId").ifBlank { continue }
            val dubPath = dub.optString("detailPath").ifBlank { continue }
            val label = sourceLabel(dub) ?: continue
            val playUrl = "$BFF/subject/play?subjectId=$dubId&se=$season&ep=$ep&detailPath=$dubPath"
            val resp = HikariNet.fetch(
                playUrl,
                headers(
                    mapOf(
                        "X-Source" to "webNetnaijaSite",
                        "Referer" to "$mainUrl/videoPlayPage/$dubPath",
                    ),
                ),
            ) ?: continue
            val data = try {
                JSONObject(resp.body ?: continue).optJSONObject("data")
            } catch (_: Exception) {
                continue
            } ?: continue
            val streams = data.optJSONArray("streams")
            var firstStreamId: String? = null
            if (streams != null) {
                for (j in 0 until streams.length()) {
                    val st = streams.optJSONObject(j) ?: continue
                    val url = st.optString("url").ifBlank { continue }
                    if (st.optBoolean("vipLocked")) continue
                    val res = st.optString("resolutions").ifBlank { continue }
                    if (firstStreamId == null) firstStreamId = st.optString("id").ifBlank { null }
                    val q = res.toIntOrNull()
                    val qLabel = when (q) {
                        360 -> "360p"; 480 -> "480p"; 720 -> "720p"; 1080 -> "1080p"
                        else -> "${res}p"
                    }
                    val mb = st.optString("size").toLongOrNull()?.let { if (it > 0) " (${it / 1_048_576}MB)" else "" } ?: ""
                    if (!seen.add(url)) continue
                    out.add(
                        HikariStream(
                            name = "NetNaija $label $qLabel$mb",
                            url = url, headers = playHeaders,
                        ),
                    )
                }
            }
            val dash = data.optJSONArray("dash")
            if (dash != null) {
                for (j in 0 until dash.length()) {
                    val url = dash.optJSONObject(j)?.optString("url")?.ifBlank { null } ?: continue
                    if (!seen.add(url)) continue
                    out.add(HikariStream(name = "NetNaija $label DASH", url = url, headers = playHeaders, isMpd = true))
                }
            }
            val hls = data.optJSONArray("hls")
            if (hls != null) {
                for (j in 0 until hls.length()) {
                    val o = hls.optJSONObject(j) ?: continue
                    val url = o.optString("url").ifBlank { continue }
                    val res = o.optString("resolutions", "0")
                    if (!seen.add(url)) continue
                    out.add(HikariStream(name = "NetNaija $label HLS ${res}p", url = url, headers = playHeaders, isM3u8 = true))
                }
            }
            if (firstStreamId != null && (dub.optBoolean("original") || subs.isEmpty())) {
                fetchCaptions(firstStreamId, dubId, dubPath, subs)
            }
        }
        if (subs.isNotEmpty() && out.isNotEmpty()) return out.map { it.copy(subtitles = it.subtitles + subs) }
        return out
    }

    private suspend fun fetchCaptions(streamId: String, subjectId: String, detailPath: String, subs: MutableList<HikariSubtitle>) {
        try {
            val root = HikariNet.getJson(
                "$BFF/subject/caption?format=MP4&id=$streamId&subjectId=$subjectId&detailPath=$detailPath",
                headers(mapOf("X-Source" to "webNetnaijaSite")),
            ) ?: return
            val caps = root.optJSONObject("data")?.optJSONArray("captions") ?: return
            for (i in 0 until caps.length()) {
                val c = caps.optJSONObject(i) ?: continue
                val url = c.optString("url").ifBlank { continue }
                val lang = c.optString("lanName").ifBlank { c.optString("lan", "Unknown") }
                subs.add(HikariSubtitle(lang, url))
            }
        } catch (_: Exception) {
        }
    }
}
