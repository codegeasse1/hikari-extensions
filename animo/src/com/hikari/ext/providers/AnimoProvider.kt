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
 * Animo (4animo.xyz) — anime via api.kryzox.xyz + cdn.4animo.xyz embeds.
 * Ported from KSHITIJ8473/raghav Animo (CloudStream) to native Hikari.
 */
class AnimoProvider : HikariProvider {
    override val id = "animo"
    override val name = "Animo"
    override val mainUrl = "https://4animo.xyz"
    override val description = "Anime from 4animo.xyz — trending, recently updated, movies, TV, OVA/ONA, sub & dub, direct HLS."
    override val version = 2
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val apiUrl = "https://api.kryzox.xyz"
    private val cdnUrl = "https://cdn.4animo.xyz"
    private val ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36"

    @Volatile private var site = "https://4animo.xyz"
    @Volatile private var lastDomainFetch = 0L

    private suspend fun refreshDomain() {
        val now = System.currentTimeMillis()
        if (now - lastDomainFetch < 5 * 60 * 1000L) return
        lastDomainFetch = now
        try {
            val txt = HikariNet.getString("https://cloudstreampluginhelper-default-rtdb.firebaseio.com/.json") ?: return
            val v = JSONObject(txt).optString("animo").ifBlank { JSONObject(txt).optString("animo_url") }.trim().removeSuffix("/")
            if (v.startsWith("http")) site = v
        } catch (t: Throwable) { }
    }

    private fun apiHeaders(): Map<String, String> = mapOf(
        "User-Agent" to ua,
        "Accept" to "application/json, text/plain, */*",
        "Accept-Language" to "en-US,en;q=0.5",
        "Referer" to "$site/",
        "Origin" to site
    )

    private val rows = listOf(
        "trending" to "Trending",
        "recently-updated" to "Recently Updated",
        "recently-added" to "Recently Added",
        "top" to "Top Rated",
        "movie" to "Movies",
        "tv" to "TV Series",
        "ova" to "OVA",
        "ona" to "ONA",
        "special" to "Specials",
        "completed" to "Completed"
    )

    override fun catalogs(): List<HikariCatalog> =
        rows.map { (k, label) ->
            val t = if (k == "movie") HikariMediaType.MOVIE else HikariMediaType.SERIES
            HikariCatalog(k, label, t, rawType = k)
        }

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        refreshDomain()
        val key = catalog.rawType.ifBlank { catalog.id }
        val url = "$apiUrl/anime/$key?page=$page&limit=20"
        val txt = HikariNet.getStringSmart(url, apiHeaders()) ?: return emptyList()
        return parseAnimeList(txt).mapNotNull { it.toMedia() }
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank()) return emptyList()
        if (page > 1) return emptyList()
        refreshDomain()
        val enc = URLEncoder.encode(query.trim(), "UTF-8")
        val txt = HikariNet.getStringSmart("$apiUrl/anime/search?keyword=$enc&page=1&limit=20", apiHeaders()) ?: return emptyList()
        return parseAnimeList(txt).mapNotNull { it.toMedia() }
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val animeId = media.id.substringAfterLast("/").substringBefore("?").toIntOrNull() ?: return media
        refreshDomain()
        val txt = HikariNet.getStringSmart("$apiUrl/anime/$animeId", apiHeaders()) ?: return media
        val obj = runCatching { JSONObject(txt) }.getOrNull() ?: return media
        val titles = obj.optJSONObject("titles")
        val title = titles?.optString("english")?.takeIf { it.isNotBlank() }
            ?: titles?.optString("romaji")?.takeIf { it.isNotBlank() } ?: media.title
        val images = obj.optJSONObject("images")
        val poster = images?.optString("poster")?.takeIf { it.isNotBlank() } ?: media.posterUrl
        val synopsis = obj.optString("synopsis").takeIf { it.isNotBlank() }
        val genres = obj.optJSONArray("genres")?.let { arr -> (0 until arr.length()).mapNotNull { arr.optString(it).takeIf { s -> s.isNotBlank() } } } ?: media.genres
        val air = obj.optJSONObject("air")?.optString("start")?.substringBefore("-")?.toIntOrNull()
        val type = when (obj.optString("type").uppercase()) {
            "MOVIE" -> HikariMediaType.MOVIE
            else -> HikariMediaType.SERIES
        }
        return media.copy(title = title, type = type, posterUrl = poster, overview = synopsis ?: media.overview, genres = genres.ifEmpty { media.genres }, year = air ?: media.year)
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val animeId = media.id.substringAfterLast("/").substringBefore("?").toIntOrNull() ?: return null
        refreshDomain()
        val txt = HikariNet.getStringSmart("$apiUrl/anime/$animeId/episodes", apiHeaders()) ?: return null
        val arr = runCatching { JSONObject(txt).optJSONArray("data") } .getOrNull() ?: return null
        val out = ArrayList<HikariEpisode>()
        for (i in 0 until arr.length()) {
            val ep = arr.optJSONObject(i) ?: continue
            val num = ep.optInt("number", -1).takeIf { it > 0 } ?: continue
            val epId = ep.optInt("id", -1).takeIf { it > 0 } ?: continue
            val titles = ep.optJSONObject("titles")
            val name = titles?.optString("en")?.takeIf { it.isNotBlank() }
                ?: titles?.optString("romaji")?.takeIf { it.isNotBlank() } ?: "Episode $num"
            val embedId = ep.optString("embed_id").takeIf { it.isNotBlank() } ?: ""
            val ani = ep.optString("ani").takeIf { it.isNotBlank() } ?: ""
            val thumb = ep.optString("thumbnail").takeIf { it.isNotBlank() }
            if (ep.optBoolean("sub", false)) {
                out.add(HikariEpisode(number = num, id = "$animeId|$epId|$embedId|$num|$ani|sub", name = name, image = thumb))
            }
            if (ep.optBoolean("dub", false)) {
                out.add(HikariEpisode(number = num, id = "$animeId|$epId|$embedId|$num|$ani|dub", name = "$name (Dub)", image = thumb))
            }
        }
        return out.ifEmpty { null }
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val raw = episode?.id ?: return emptyList()
        val p = raw.split("|")
        if (p.size != 6) return emptyList()
        val episodeId = p[1]
        val embedId = p[2]
        val ani = p[4]
        val type = p[5].ifBlank { "sub" }
        val q = "?k=1&autoPlay=1&skipIntro=1&skipOutro=1"
        val embeds = mutableListOf(
            "a-1" to "$cdnUrl/embed/a-1/$episodeId/$type$q",
            "s-1" to "$cdnUrl/embed/s-1/${embedId.ifBlank { episodeId }}/$type$q"
        )
        if (ani.isNotEmpty()) {
            embeds.add("hd-1" to "$cdnUrl/embed/hd-1/ani/$ani/$type$q")
            embeds.add("hd-2" to "$cdnUrl/embed/hd-2/ani/$ani/$type$q")
        }
        val seenSubs = HashSet<String>()
        return coroutineScope {
            embeds.map { (key, url) -> async { resolveSource(url, key, type, seenSubs) } }.awaitAll().flatten()
        }
    }

    private suspend fun resolveSource(embedUrl: String, key: String, type: String, seenSubs: HashSet<String>): List<HikariStream> {
        val pageHeaders = mapOf(
            "User-Agent" to ua,
            "Accept" to "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language" to "en-US,en;q=0.7"
        )
        val html = HikariNet.getString(embedUrl, pageHeaders) ?: return emptyList()
        val token = Regex("getSources\\?t=([A-Za-z0-9_.-]+)").find(html)?.groupValues?.get(1) ?: return emptyList()
        val reqHeaders = mapOf(
            "User-Agent" to ua, "Accept" to "*/*", "Referer" to embedUrl,
            "Sec-Fetch-Site" to "same-origin", "Sec-Fetch-Mode" to "cors", "Sec-Fetch-Dest" to "empty"
        )
        val srcTxt = HikariNet.getString("$cdnUrl/stream/getSources?t=$token", reqHeaders) ?: return emptyList()
        if (srcTxt.contains("invalid token")) return emptyList()
        val srcObj = runCatching { JSONObject(srcTxt) }.getOrNull() ?: return emptyList()
        val file = srcObj.optJSONArray("sources")?.optJSONObject(0)?.optString("file")?.takeIf { it.isNotBlank() } ?: return emptyList()
        val masterUrl = if (file.startsWith("http")) file else "$cdnUrl/${file.removePrefix("/")}"
        val master = HikariNet.getString(masterUrl, reqHeaders) ?: return emptyList()
        if (!master.trim().startsWith("#EXTM3U")) return emptyList()
        val playHeaders = mapOf("User-Agent" to ua, "Accept" to "*/*", "Referer" to embedUrl, "Origin" to cdnUrl)
        val subs = ArrayList<HikariSubtitle>()
        srcObj.optJSONArray("tracks")?.let { arr ->
            for (i in 0 until arr.length()) {
                val t = arr.optJSONObject(i) ?: continue
                val f = t.optString("file").takeIf { it.isNotBlank() } ?: continue
                val u = if (f.startsWith("http")) f else "$cdnUrl/${f.removePrefix("/")}"
                if (seenSubs.add(u)) subs.add(HikariSubtitle(t.optString("label").ifBlank { "English" }, u))
            }
        }
        return listOf(HikariStream(name = "Animo $key ($type)", url = masterUrl, headers = playHeaders, subtitles = subs, isM3u8 = true))
    }

    private fun parseAnimeList(text: String): List<JSONObject> {
        return try {
            val t = text.trim()
            if (t.startsWith("[")) {
                val arr = JSONArray(t)
                (0 until arr.length()).mapNotNull { arr.optJSONObject(it) }
            } else {
                val arr = JSONObject(t).optJSONArray("data") ?: return emptyList()
                (0 until arr.length()).mapNotNull { arr.optJSONObject(it) }
            }
        } catch (_: Exception) { emptyList() }
    }

    private fun JSONObject.toMedia(): HikariMedia? {
        val id = optInt("id", -1).takeIf { it > 0 } ?: return null
        val titles = optJSONObject("titles") ?: return null
        val title = titles.optString("english").takeIf { it.isNotBlank() }
            ?: titles.optString("romaji").takeIf { it.isNotBlank() } ?: return null
        val poster = optJSONObject("images")?.optString("poster")?.takeIf { it.isNotBlank() }
        return HikariMedia(id = "$mainUrl/anime/$id", title = title, type = HikariMediaType.SERIES, posterUrl = poster)
    }
}
