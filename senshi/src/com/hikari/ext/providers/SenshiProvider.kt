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
import kotlinx.coroutines.delay
import org.json.JSONArray
import org.json.JSONObject

/**
 * Senshi (senshi.to) — anime catalog + vidcloud/anidap CDN streams.
 *
 * Faithful native port: episode-embeds/latest, anime/recently-added,
 * anime/trending/{day,week}, anime/upcoming, anime/filter catalogs; anime +
 * episodes detail; episode-embeds per episode; direct CDN masters
 * (EM3U8v1-decrypted via [SnCrypt]) through the localhost [SnProxy].
 * (The CloudStream source's optional __oct JS-bridge "open" path needs a
 * scriptable WebView and is not portable; the direct embed URLs the site's
 * own watch page plays are used instead.)
 */
class SenshiProvider : HikariProvider {

    override val id = "senshi"
    override val name = "Senshi"
    override val mainUrl = "https://senshi.to"
    override val description = "Anime from Senshi — latest episodes, trending day & week, recently added, upcoming and full A–Z, sub & dub CDN streams."
    override val version = 2
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    companion object {
        private const val FALLBACK = "https://senshi.to"
        private const val FIREBASE_URL = "https://cloudstreampluginhelper-default-rtdb.firebaseio.com/.json"
        private const val CACHE_TTL_MS = 5 * 60 * 1000L
        private const val UA =
            "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/154.0.0.0 Mobile Safari/537.36"

        @Volatile private var domain = FALLBACK
        @Volatile private var lastDomainLoad = 0L

        fun base(): String = domain

        suspend fun refreshDomain() {
            val now = System.currentTimeMillis()
            if (now - lastDomainLoad < CACHE_TTL_MS) return
            lastDomainLoad = now
            try {
                val body = HikariNet.getString(FIREBASE_URL) ?: return
                val o = JSONObject(body)
                val v = o.optString("senshi").ifBlank { o.optString("senshi_url") }
                    .ifBlank { o.optString("senshi_domain") }.trim().removeSuffix("/")
                if (v.startsWith("http")) domain = v
            } catch (t: Throwable) { }
        }

        fun apiHeaders(): Map<String, String> = mapOf(
            "User-Agent" to UA,
            "Accept" to "application/json, text/plain, */*",
            "Referer" to "${domain}/",
        )

        fun postHeaders(): Map<String, String> = mapOf(
            "User-Agent" to UA,
            "Accept" to "application/json, text/plain, */*",
            "Content-Type" to "application/json",
            "Origin" to domain,
            "Referer" to "$domain/browse",
        )

        fun cdnHeaders(): Map<String, String> = mapOf(
            "User-Agent" to UA,
            "Accept" to "*/*",
            "Accept-Language" to "en-US,en;q=0.9",
            "Origin" to domain,
            "Referer" to "$domain/",
            "sec-fetch-dest" to "empty",
            "sec-fetch-mode" to "cors",
            "sec-fetch-site" to "cross-site",
        )

        fun streamHeaders(): Map<String, String> = mapOf(
            "User-Agent" to UA,
            "Accept" to "*/*",
            "Origin" to domain,
            "Referer" to "$domain/",
        )
    }

    // ------------------------------------------------------------------
    // Catalogs (6 rows)
    // ------------------------------------------------------------------

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("latest", "Latest Episodes", HikariMediaType.SERIES),
        HikariCatalog("recent", "Recently Added", HikariMediaType.SERIES),
        HikariCatalog("trending-day", "Trending Today", HikariMediaType.SERIES),
        HikariCatalog("trending-week", "Trending This Week", HikariMediaType.SERIES),
        HikariCatalog("upcoming", "Upcoming", HikariMediaType.SERIES),
        HikariCatalog("all", "All Anime", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        refreshDomain()
        val p = page.coerceAtLeast(1)
        return when (catalog.id) {
            "latest" -> {
                if (p > 1) return emptyList()
                val text = get("${base()}/episode-embeds/latest") ?: return emptyList()
                try {
                    val arr = JSONArray(text)
                    val seen = HashSet<Int>()
                    buildList {
                        for (i in 0 until arr.length()) {
                            val anime = arr.optJSONObject(i)?.optJSONObject("anime") ?: continue
                            val id = anime.optInt("id", 0)
                            if (id == 0 || !seen.add(id)) continue
                            parseAnime(anime)?.let { add(it) }
                        }
                    }
                } catch (t: Throwable) { emptyList() }
            }
            "recent" -> {
                if (p > 1) return emptyList()
                parseAnimeList(get("${base()}/anime/recently-added"))
            }
            "trending-day" -> {
                if (p > 1) return emptyList()
                parseAnimeList(get("${base()}/anime/trending/day"))
            }
            "trending-week" -> {
                if (p > 1) return emptyList()
                parseAnimeList(get("${base()}/anime/trending/week"))
            }
            "upcoming" -> {
                if (p > 1) return emptyList()
                parseAnimeList(get("${base()}/anime/upcoming"))
            }
            "all" -> {
                val resp = postFilter(searchTerm = null, page = p, limit = 40, sortBy = "recent") ?: return emptyList()
                resp.first
            }
            else -> emptyList()
        }
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank()) return emptyList()
        refreshDomain()
        if (page > 1) return emptyList()
        return postFilter(searchTerm = query.trim(), page = 1, limit = 30, sortBy = null)?.first ?: emptyList()
    }

    // ------------------------------------------------------------------
    // Meta + episodes
    // ------------------------------------------------------------------

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        refreshDomain()
        val publicId = media.id.substringBefore("?").substringAfterLast("/").ifBlank { return media }
        val anime = get("${base()}/anime/$publicId")?.let {
            runCatching { parseAnimeFull(JSONObject(it)) }.getOrNull()
        } ?: return media
        return media.copy(
            title = anime.title,
            posterUrl = anime.poster ?: media.posterUrl,
            overview = anime.overview ?: media.overview,
            genres = if (anime.genres.isNotEmpty()) anime.genres else media.genres,
            year = anime.year ?: media.year,
        )
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        refreshDomain()
        val publicId = media.id.substringBefore("?").substringAfterLast("/").ifBlank { return null }
        val animeText = get("${base()}/anime/$publicId") ?: return null
        val anime = runCatching { JSONObject(animeText) }.getOrNull() ?: return null
        val malId = anime.optInt("id", 0).takeIf { it > 0 } ?: return null

        val epsText = get("${base()}/episodes/$malId") ?: return null
        val episodes = try {
            val arr = JSONArray(epsText)
            buildList {
                for (i in 0 until arr.length()) {
                    val e = arr.optJSONObject(i) ?: continue
                    val num = e.optInt("ep_id", 0).takeIf { it > 0 } ?: continue
                    add(Triple(num, e.optString("ep_title").takeIf { it.isNotBlank() } ?: "Episode $num", e))
                }
            }.sortedBy { it.first }
        } catch (t: Throwable) { return null }
        if (episodes.isEmpty()) return null

        var hasSub = anime.optInt("sub_count", 0) > 0
        var hasDub = anime.optInt("dub_count", 0) > 0
        if (!hasSub && !hasDub) {
            hasSub = true
            probeEmbeds(malId, episodes.first().first)?.let { statuses ->
                hasSub = statuses.any { it.second == "sub" }
                hasDub = statuses.any { it.second == "dub" }
                if (!hasSub && !hasDub) hasSub = true
            }
        }
        val sub = if (hasSub) episodes.map { (num, title, e) -> toEpisode(malId, num, title, e, "sub", media) } else emptyList()
        val dubCount = anime.optInt("dub_count", 0)
        val dub = if (hasDub) buildDubEpisodes(malId, episodes, dubCount, media) else emptyList()
        if (sub.isEmpty() && dub.isEmpty()) return null
        return sub + dub
    }

    private data class EpRow(val num: Int, val title: String, val obj: JSONObject)

    private suspend fun buildDubEpisodes(malId: Int, episodes: List<Triple<Int, String, JSONObject>>, dubCount: Int, media: HikariMedia): List<HikariEpisode> {
        val rows = episodes.map { EpRow(it.first, it.second, it.third) }
        val base = if (dubCount in 1..rows.size) rows.take(dubCount) else rows
        val trailing = if (dubCount in 1..rows.size) rows.drop(dubCount).takeLast(6) else emptyList()
        val extra = if (trailing.isEmpty()) emptyList() else coroutineScope {
            trailing.map { ep ->
                async {
                    val hasDub = probeEmbeds(malId, ep.num)?.any { it.second == "dub" } ?: false
                    if (hasDub) ep else null
                }
            }.awaitAll().filterNotNull()
        }
        return (base + extra).sortedBy { it.num }.map { toEpisode(malId, it.num, it.title, it.obj, "dub", media) }
    }

    private fun toEpisode(malId: Int, num: Int, baseTitle: String, e: JSONObject, type: String, media: HikariMedia): HikariEpisode {
        val title = when {
            e.optBoolean("ep_filler", false) -> "$baseTitle (Filler)"
            e.optBoolean("ep_recap", false) -> "$baseTitle (Recap)"
            else -> baseTitle
        }
        val thumb = e.optString("ep_thumbnail").takeIf { it.startsWith("http") } ?: media.posterUrl
        return HikariEpisode(
            number = num,
            id = "$malId|$num|$type",
            name = if (type == "dub") "$title (Dub)" else title,
            image = thumb,
        )
    }

    /** Returns list of (url, sub|dub) embed entries. */
    private suspend fun probeEmbeds(malId: Int, epId: Int): List<Pair<String, String>>? {
        val text = get("${base()}/episode-embeds/$malId/$epId") ?: return null
        return try {
            val arr = JSONArray(text)
            buildList {
                for (i in 0 until arr.length()) {
                    val e = arr.optJSONObject(i) ?: continue
                    val url = e.optString("url").takeIf { it.startsWith("http") } ?: continue
                    val st = e.optString("status").lowercase()
                    val kind = when {
                        st == "dub" -> "dub"
                        st == "sub" || st == "hardsub" -> "sub"
                        else -> continue
                    }
                    add(url to kind)
                }
            }
        } catch (t: Throwable) { null }
    }

    // ------------------------------------------------------------------
    // Streams: direct CDN masters + proxy
    // ------------------------------------------------------------------

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        refreshDomain()
        val parts = (episode?.id ?: return emptyList()).split("|")
        if (parts.size < 3) return emptyList()
        val malId = parts[0].toIntOrNull()?.takeIf { it > 0 } ?: return emptyList()
        val ep = parts[1].toIntOrNull()?.takeIf { it > 0 } ?: return emptyList()
        val wantDub = parts[2] == "dub"
        val modeLabel = if (wantDub) "Dub" else "Sub"

        val embeds = probeEmbeds(malId, ep) ?: return emptyList()
        if (embeds.isEmpty()) return emptyList()
        val matching = embeds.filter { (_, kind) -> if (wantDub) kind == "dub" else kind == "sub" }.ifEmpty { embeds }
        val masters = matching.map { it.first }.distinct()
        if (masters.isEmpty()) return emptyList()

        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        coroutineScope {
            masters.map { master ->
                async {
                    val found = ArrayList<HikariStream>()
                    if (emitStreamLinks(master, modeLabel, wantDub, found)) found else emptyList()
                }
            }.awaitAll().forEach { list ->
                synchronized(out) {
                    for (s in list) if (seen.add(s.url)) out.add(s)
                }
            }
        }
        return out
    }

    private suspend fun emitStreamLinks(master: String, modeLabel: String, wantDub: Boolean, out: MutableList<HikariStream>): Boolean {
        var masterText: String? = null
        for (attempt in 0..2) {
            if (attempt > 0) delay(1200L)
            masterText = try {
                HikariNet.getString(master, cdnHeaders())
                    ?: HikariNet.getStringSmart(master, cdnHeaders())
            } catch (t: Throwable) { null }
            if (masterText != null) break
        }
        var playlist = masterText
        if (playlist != null && SnCrypt.isEncrypted(playlist)) playlist = SnCrypt.decrypt(playlist)
        if (playlist != null && playlist.startsWith("#EXTM3U")) {
            val proxyBase = SnProxy.register(master, playlist, senshiUpstreamHeaders(master))
            if (proxyBase != null) {
                val mode = if (wantDub) "dub" else "sub"
                val variants = parseVariantQualities(playlist)
                if (variants.isEmpty()) {
                    out.add(HikariStream(name = "Senshi $modeLabel", url = "$proxyBase/m/$mode/0/master.m3u8", isM3u8 = true))
                } else {
                    for ((label, idx) in variants) {
                        out.add(HikariStream(name = "Senshi $modeLabel $label", url = "$proxyBase/m/$mode/$idx/master.m3u8", isM3u8 = true))
                    }
                }
                return true
            }
        }
        // Fallback: hand the CDN master straight to the player.
        if (masterText != null) {
            out.add(
                HikariStream(
                    name = "Senshi $modeLabel",
                    url = master,
                    headers = streamHeaders(),
                    isM3u8 = true,
                )
            )
            return true
        }
        return false
    }

    private fun parseVariantQualities(master: String): List<Pair<String, Int>> {
        val lines = master.split("\n").map { it.trim() }.filter { it.isNotEmpty() }
        val qualities = mutableListOf<Pair<String, Int>>()
        var pending = false
        var currentRes: String? = null
        var ordinal = 0
        for (line in lines) {
            if (line.startsWith("#EXT-X-STREAM-INF")) {
                pending = true
                currentRes = Regex("RESOLUTION=(\\d+)x(\\d+)").find(line)?.groupValues?.get(2)?.toIntOrNull()
                    ?.takeIf { it > 0 }?.let { "${it}p" }
            } else if (pending && !line.startsWith("#")) {
                currentRes?.let { qualities.add(it to ordinal) }
                ordinal++
                pending = false
            }
        }
        return qualities.sortedByDescending { it.first.dropLast(1).toIntOrNull() ?: 0 }
    }

    // ------------------------------------------------------------------
    // HTTP + parsing
    // ------------------------------------------------------------------

    private suspend fun get(url: String): String? =
        HikariNet.getString(url, apiHeaders()) ?: HikariNet.getStringSmart(url, apiHeaders())

    private suspend fun postFilter(searchTerm: String?, page: Int, limit: Int, sortBy: String?): Pair<List<HikariMedia>, Int>? {
        val body = JSONObject()
        if (searchTerm != null) body.put("searchTerm", searchTerm)
        body.put("page", page).put("limit", limit)
        if (sortBy != null) body.put("sortBy", sortBy)
        val text = HikariNet.postString("${base()}/anime/filter", body.toString(), postHeaders(), "application/json; charset=utf-8")
            ?: return null
        return try {
            val o = JSONObject(text)
            val arr = o.optJSONArray("data") ?: return null
            val list = buildList {
                for (i in 0 until arr.length()) parseAnime(arr.optJSONObject(i) ?: continue)?.let { add(it) }
            }
            Pair(list, o.optInt("total", 0))
        } catch (t: Throwable) { null }
    }

    private fun parseAnimeList(body: String?): List<HikariMedia> {
        if (body == null) return emptyList()
        return try {
            val arr = JSONArray(body)
            buildList {
                for (i in 0 until arr.length()) parseAnime(arr.optJSONObject(i) ?: continue)?.let { add(it) }
            }
        } catch (t: Throwable) { emptyList() }
    }

    private data class AnimeInfo(
        val title: String,
        val poster: String?,
        val overview: String?,
        val genres: List<String>,
        val year: Int?,
    )

    private fun parseAnime(o: JSONObject): HikariMedia? {
        val id = o.optString("public_id").ifBlank { return null }
        val title = o.optString("title").takeIf { it.isNotBlank() }
            ?: o.optString("title_english").takeIf { it.isNotBlank() } ?: return null
        val pic = o.optString("anime_picture")
        val poster = if (pic.startsWith("http")) pic else if (pic.isNotBlank() && pic.startsWith("/")) "${base()}$pic" else null
        return HikariMedia(
            id = "${base()}/anime/$id",
            title = title,
            type = if (o.optString("type").equals("MOVIE", true)) HikariMediaType.MOVIE else HikariMediaType.SERIES,
            posterUrl = poster,
            year = o.optInt("ani_year", 0).takeIf { it > 0 },
        )
    }

    private fun parseAnimeFull(o: JSONObject): AnimeInfo {
        val title = o.optString("title").takeIf { it.isNotBlank() }
            ?: o.optString("title_english").takeIf { it.isNotBlank() } ?: "Anime ${o.optInt("id", 0)}"
        val pic = o.optString("anime_picture")
        val poster = if (pic.startsWith("http")) pic else if (pic.isNotBlank() && pic.startsWith("/")) "${base()}$pic" else null
        val genres = o.optString("genres").split(",").map { it.trim() }.filter { it.isNotBlank() }
        return AnimeInfo(
            title = title,
            poster = poster,
            overview = o.optString("ani_description").takeIf { it.isNotBlank() },
            genres = genres,
            year = o.optInt("ani_year", 0).takeIf { it > 0 },
        )
    }

    private fun senshiUpstreamHeaders(master: String): Map<String, String> = cdnHeaders()
}
