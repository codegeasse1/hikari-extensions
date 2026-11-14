package com.hikari.ext.providers

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
 * Re:ANIME (reanime.to) — anime catalog + FlixCloud servers.
 *
 * Faithful native port of KSHITIJ8473/raghav ReAnime:
 *  - Firebase-hosted domain table (refreshDomain) with https://reanime.to fallback.
 *  - Search:  GET {api}/api/v1/search?q=&limit=&offset=  -> {results:[{anime_id,
 *    anilist_id, title:{english,romaji,native}, cover_image:{extra_large},
 *    season_year, format, status, genres, average_score, subbed, dubbed}]}
 *  - Sorted rows: same endpoint + &sort=popularity_desc|score_desc|trending_desc.
 *  - Home rows: GET {api}/api/v1/home/{latest-aired,new-on-site,upcoming}
 *    -> {data, has_more, next_cursor} with CURSOR pagination.
 *  - Detail: GET {api}/api/v1/anime/{anime_id} (full object).
 *  - Episodes: GET {api}/api/v1/anime/{anime_id}/episodes?limit=2000.
 *  - Servers: GET {api}/api/flix/{anilistId}/{ep} (or /api/flix/0/{ep}?tmdb=&season=)
 *    -> {success, servers:[{serverName, dataLink, dataType}]}.
 *  - Playback: full FlixResolver (wasm-obfuscation + PBKDF2 + AES/CBC +
 *    XOR playlist) + localhost FlixProxy, exactly like the CloudStream source.
 */
class ReAnimeProvider : HikariProvider {

    override val id = "reanime"
    override val name = "Re:ANIME"
    override val mainUrl = "https://reanime.to"
    override val description = "Anime from ReAnime — trending, popular, top rated, latest episodes, recently added and upcoming, sub & dub via FlixCloud."
    override val version = 2
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    companion object {
        private const val FALLBACK = "https://reanime.to"
        private const val FIREBASE_URL = "https://cloudstreampluginhelper-default-rtdb.firebaseio.com/.json"
        private const val DOMAIN_KEY = "reanime"
        private const val CACHE_TTL_MS = 5 * 60 * 1000L
        private const val UA = "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36"
        private const val PAGE_SIZE = 24

        @Volatile private var domain = FALLBACK
        @Volatile private var lastDomainLoad = 0L
        private val cursorLock = Any()
        private val nextCursors = HashMap<String, String?>()

        private fun apiHeaders(): Map<String, String> = mapOf(
            "User-Agent" to UA,
            "Accept" to "application/json",
            "Referer" to "$domain/",
        )

        suspend fun refreshDomain() {
            val now = System.currentTimeMillis()
            if (now - lastDomainLoad < CACHE_TTL_MS) return
            lastDomainLoad = now
            try {
                val body = HikariNet.getString(FIREBASE_URL) ?: return
                val o = JSONObject(body)
                val v = o.optString(DOMAIN_KEY).ifBlank { o.optString("${DOMAIN_KEY}_url") }
                    .ifBlank { o.optString("${DOMAIN_KEY}_domain") }.trim().removeSuffix("/")
                if (v.startsWith("http")) domain = v
            } catch (t: Throwable) { }
        }

        fun base(): String = domain
    }

    // ------------------------------------------------------------------
    // Catalogs (6 rows)
    // ------------------------------------------------------------------

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("trending", "Trending Now", HikariMediaType.SERIES),
        HikariCatalog("popular", "Most Popular", HikariMediaType.SERIES),
        HikariCatalog("top-rated", "Top Rated", HikariMediaType.SERIES),
        HikariCatalog("latest-aired", "Latest Episodes", HikariMediaType.SERIES),
        HikariCatalog("new-on-site", "Recently Added", HikariMediaType.SERIES),
        HikariCatalog("upcoming", "Upcoming", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        refreshDomain()
        val p = page.coerceAtLeast(1)
        return when (catalog.id) {
            "trending" -> parseResults(get("${base()}/api/v1/search?limit=$PAGE_SIZE&offset=${(p - 1) * PAGE_SIZE}&sort=trending_desc"))
            "popular" -> parseResults(get("${base()}/api/v1/search?limit=$PAGE_SIZE&offset=${(p - 1) * PAGE_SIZE}&sort=popularity_desc"))
            "top-rated" -> parseResults(get("${base()}/api/v1/search?limit=$PAGE_SIZE&offset=${(p - 1) * PAGE_SIZE}&sort=score_desc"))
            "latest-aired", "new-on-site", "upcoming" -> homeSection(catalog.id, p)
            else -> emptyList()
        }
    }

    private suspend fun homeSection(section: String, page: Int): List<HikariMedia> {
        val key = "$section|$page"
        val cursor = synchronized(cursorLock) { if (page <= 1) null else nextCursors[key] }
        // Page > 1 without a stored cursor means the previous page had no more data.
        if (page > 1 && cursor == null && synchronized(cursorLock) { !nextCursors.containsKey(key) }) return emptyList()
        val sep = if (cursor == null) "?" else "&cursor=${enc(cursor)}"
        val body = get("${base()}/api/v1/home/$section?limit=$PAGE_SIZE$sep") ?: return emptyList()
        return try {
            val o = JSONObject(body)
            val arr = o.optJSONArray("data") ?: return emptyList()
            o.optString("next_cursor").takeIf { it.isNotBlank() }?.let { nc ->
                synchronized(cursorLock) { nextCursors["$section|${page + 1}"] = nc }
            }
            if (page > 1) synchronized(cursorLock) { nextCursors.remove(key) }
            parseItems(arr)
        } catch (t: Throwable) { emptyList() }
    }

    // ------------------------------------------------------------------
    // Search
    // ------------------------------------------------------------------

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        refreshDomain()
        val q = query.trim()
        if (q.isEmpty()) return emptyList()
        val p = page.coerceAtLeast(1)
        val body = get("${base()}/api/v1/search?q=${enc(q)}&limit=$PAGE_SIZE&offset=${(p - 1) * PAGE_SIZE}")
            ?: return emptyList()
        return parseResults(body)
    }

    // ------------------------------------------------------------------
    // Meta + episodes
    // ------------------------------------------------------------------

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        refreshDomain()
        val slug = media.id.substringAfterLast("/").substringBefore("?").ifBlank { return media }
        val body = get("${base()}/api/v1/anime/${enc(slug)}") ?: return media
        return try {
            val d = JSONObject(body)
            val t = d.optJSONObject("title")
            val title = t?.optString("english")?.takeIf { it.isNotBlank() }
                ?: t?.optString("romaji")?.takeIf { it.isNotBlank() }
                ?: t?.optString("native")?.takeIf { it.isNotBlank() } ?: media.title
            val c = d.optJSONObject("cover_image")
            val poster = c?.optString("extra_large")?.takeIf { it.startsWith("http") }
                ?: c?.optString("large")?.takeIf { it.startsWith("http") } ?: media.posterUrl
            val banner = d.optString("banner_image").takeIf { it.startsWith("http") } ?: poster
            val overview = d.optString("description").takeIf { it.isNotBlank() }
                ?.let { stripHtml(it) } ?: media.overview
            val genres = d.optJSONArray("genres")?.let { arr ->
                buildList { for (i in 0 until arr.length()) arr.optString(i).takeIf { it.isNotBlank() }?.let { add(it) } }
            } ?: media.genres
            val year = d.optInt("season_year", 0).takeIf { it > 0 } ?: media.year
            media.copy(title = title, posterUrl = poster, backdropUrl = banner, overview = overview, genres = genres, year = year)
        } catch (t: Throwable) { media }
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        refreshDomain()
        val slug = media.id.substringAfterLast("/").substringBefore("?").ifBlank { return null }
        var anilistId = 0
        var tmdbId = 0
        var season = 1
        try {
            val detail = get("${base()}/api/v1/anime/${enc(slug)}")
            if (detail != null) {
                val d = JSONObject(detail)
                anilistId = d.optInt("anilist_id", 0)
                tmdbId = d.optInt("themoviedb_id", 0)
                season = d.optJSONObject("external_seasons")?.optInt("tmdb", 1)?.takeIf { it > 0 } ?: 1
            }
        } catch (t: Throwable) { }
        val body = get("${base()}/api/v1/anime/${enc(slug)}/episodes?limit=2000") ?: return null
        return try {
            val arr = JSONObject(body).optJSONArray("data") ?: return null
            val sub = ArrayList<HikariEpisode>()
            val dub = ArrayList<HikariEpisode>()
            var sawFlags = false
            for (i in 0 until arr.length()) {
                val e = arr.optJSONObject(i) ?: continue
                val num = e.optInt("episode_number", 0).takeIf { it > 0 } ?: continue
                if (e.has("subbed") || e.has("dubbed")) sawFlags = true
                val isSub = e.optBoolean("subbed", false)
                val isDub = e.optBoolean("dubbed", false)
                val rawTitle = e.optString("title").takeIf { it.isNotBlank() && !it.startsWith("Episode ") }
                val thumb = e.optString("thumbnail").takeIf { it.startsWith("http") } ?: media.posterUrl
                val ref = "$slug|$num|$anilistId|$tmdbId|$season"
                if (isSub || !sawFlags) sub.add(HikariEpisode(number = num, id = "$ref|sub", name = rawTitle ?: "Episode $num", image = thumb))
                if (isDub || !sawFlags) dub.add(HikariEpisode(number = num, id = "$ref|dub", name = "${rawTitle ?: "Episode $num"} (Dub)", image = thumb))
            }
            if (sub.isEmpty() && dub.isEmpty()) return null
            (sub.sortedBy { it.number } + dub.sortedBy { it.number })
        } catch (t: Throwable) { null }
    }

    // ------------------------------------------------------------------
    // Streams (FlixCloud via resolver + local proxy)
    // ------------------------------------------------------------------

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        refreshDomain()
        val ref = episode?.id ?: return emptyList()
        val parts = ref.split("|")
        if (parts.size < 6) return emptyList()
        val slug = parts[0]
        val ep = parts[1].toIntOrNull()?.takeIf { it > 0 } ?: return emptyList()
        val anilistId = parts[2].toIntOrNull() ?: 0
        val tmdbId = parts[3].toIntOrNull() ?: 0
        val season = parts[4].toIntOrNull() ?: 1
        val lang = parts[5].ifBlank { "sub" }

        val flixUrl = when {
            anilistId > 0 -> "${base()}/api/flix/$anilistId/$ep"
            tmdbId > 0 -> "${base()}/api/flix/0/$ep?tmdb=$tmdbId&season=$season"
            else -> return emptyList()
        }
        val body = get(flixUrl, mapOf("Referer" to "${base()}/watch/")) ?: return emptyList()
        val servers = try {
            val o = JSONObject(body)
            if (o.optBoolean("success", false) != true) return emptyList()
            o.optJSONArray("servers") ?: return emptyList()
        } catch (t: Throwable) { return emptyList() }

        data class Embed(val url: String, val server: String, val kind: String)
        val embeds = ArrayList<Embed>()
        for (i in 0 until servers.length()) {
            val s = servers.optJSONObject(i) ?: continue
            val link = s.optString("dataLink").takeIf { it.startsWith("http") } ?: continue
            embeds.add(Embed(link, s.optString("serverName").ifBlank { "HD" }, s.optString("dataType")))
        }
        if (embeds.isEmpty()) return emptyList()
        // Prefer servers tagged for the requested audio, then try the rest.
        val ordered = embeds.sortedBy { if (it.kind.equals(lang, true)) 0 else 1 }
            .distinctBy { it.url.substringBefore("?") + it.server }

        val out = ArrayList<HikariStream>()
        val seenSubs = HashSet<String>()
        val seenUrls = HashSet<String>()
        coroutineScope {
            ordered.map { (embedUrl, serverName) ->
                async {
                    val res = ReFlixResolver.resolve(embedUrl, "${base()}/") ?: return@async null
                    val proxyMaster = ReFlixProxy.registerMaster(res.m3u8, res.masterContent, res.pkKey)
                        ?: return@async null
                    Triple(res, proxyMaster, serverName)
                }
            }.awaitAll().forEach { triple ->
                if (triple == null) return@forEach
                val (res, proxyMaster, serverName) = triple
                val hasEnglishAudio = res.masterContent.contains("LANGUAGE=\"en\"") ||
                    res.masterContent.contains("NAME=\"English\"")
                val hasOtherAudio = Regex("TYPE=AUDIO[^\\n]*LANGUAGE=\"(?!en)[^\"]*\"")
                    .containsMatchIn(res.masterContent) ||
                    (res.masterContent.contains("TYPE=AUDIO") && !hasEnglishAudio)
                val wantDub = lang == "dub"
                val useLang = when {
                    wantDub && hasEnglishAudio -> "dub"
                    !wantDub && (hasOtherAudio || !hasEnglishAudio) -> "sub"
                    wantDub && !hasEnglishAudio -> "sub"
                    else -> "dub"
                }
                val suffix = if (serverName.isNotBlank() && !serverName.equals("HD-1", true)) " ($serverName)" else ""
                val label = if (useLang == "dub") "Re:ANIME Dub$suffix" else "Re:ANIME Sub$suffix"
                val url = "$proxyMaster?lang=$useLang"
                val newStreams = ArrayList<HikariStream>()
                val newSubs = ArrayList<HikariSubtitle>()
                synchronized(seenUrls) {
                    if (seenUrls.add(url)) {
                        val height = Regex("RESOLUTION=\\d+x(\\d+)").findAll(res.masterContent)
                            .mapNotNull { it.groupValues[1].toIntOrNull() }.maxOrNull()
                        newStreams.add(
                            HikariStream(
                                name = if (height != null && height >= 500) "$label ${height}p" else label,
                                url = url,
                                headers = mapOf("Referer" to "https://flixcloud.cc/"),
                                isM3u8 = true,
                            )
                        )
                    }
                    for (s in res.subtitles) {
                        if (!seenSubs.add(s.url)) continue
                        val ext = s.format?.uppercase()
                        newSubs.add(HikariSubtitle(if (ext != null) "${s.language ?: "Subtitle"} ($ext)" else (s.language ?: "Subtitle"), s.url))
                    }
                }
                synchronized(out) { out.addAll(newStreams.map { it.copy(subtitles = it.subtitles + newSubs) }) }
            }
        }
        return out
    }

    // ------------------------------------------------------------------
    // JSON helpers
    // ------------------------------------------------------------------

    private suspend fun get(url: String, extra: Map<String, String> = emptyMap()): String? {
        val h = apiHeaders() + extra
        // Plain client first; fall back to the rendered/WebView path on WAF blocks.
        return HikariNet.getString(url, h) ?: HikariNet.getStringSmart(url, h)
    }

    private fun parseResults(body: String?): List<HikariMedia> {
        if (body == null) return emptyList()
        return try {
            parseItems(JSONObject(body).optJSONArray("results") ?: return emptyList())
        } catch (t: Throwable) { emptyList() }
    }

    private fun parseItems(arr: JSONArray): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        for (i in 0 until arr.length()) {
            val e = arr.optJSONObject(i) ?: continue
            val slug = e.optString("anime_id")
            if (slug.isBlank()) continue
            val t = e.optJSONObject("title")
            val title = t?.optString("english")?.takeIf { it.isNotBlank() }
                ?: t?.optString("romaji")?.takeIf { it.isNotBlank() }
                ?: t?.optString("native")?.takeIf { it.isNotBlank() }
                ?: t?.optString("user_preferred")?.takeIf { it.isNotBlank() } ?: continue
            val c = e.optJSONObject("cover_image")
            val poster = c?.optString("extra_large")?.takeIf { it.startsWith("http") }
                ?: c?.optString("large")?.takeIf { it.startsWith("http") }
            val year = e.optInt("season_year", 0).takeIf { it > 0 }
            val genres = e.optJSONArray("genres")?.let { g ->
                buildList { for (j in 0 until g.length()) g.optString(j).takeIf { it.isNotBlank() }?.let { add(it) } }
            } ?: emptyList()
            val isMovie = e.optString("format").equals("MOVIE", true)
            out.add(
                HikariMedia(
                    id = "${base()}/anime/$slug",
                    title = title,
                    type = if (isMovie) HikariMediaType.MOVIE else HikariMediaType.SERIES,
                    posterUrl = poster,
                    year = year,
                    genres = genres,
                )
            )
        }
        return out
    }

    private fun stripHtml(s: String): String = s
        .replace(Regex("<br\\s*/?>"), "\n")
        .replace(Regex("<[^>]+>"), "")
        .replace("&amp;", "&").replace("&quot;", "\"").replace("&#039;", "'").replace("&lt;", "<").replace("&gt;", ">")
        .trim()

    private fun enc(s: String): String = URLEncoder.encode(s, "UTF-8")
}
