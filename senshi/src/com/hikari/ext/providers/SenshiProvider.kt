package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import org.json.JSONObject
import java.net.URLEncoder

/**
 * Senshi (senshi.to) — anime via JSON: latest embeds, filter search, episodes.
 * Ported from KSHITIJ8473/raghav Senshi to native Hikari.
 *
 *  GET /episode-embeds/latest            latest row
 *  POST /filter {searchTerm,page,limit}  search + catalog rows
 *  GET /episodes/{malId}                 episode list (ep_id)
 *  GET /episode-embeds/{malId}/{epId}    embeds (sub/dub flags) for playback
 * Embeds resolve via WebView m3u8 capture.
 */
class SenshiProvider : HikariProvider {

    override val id = "senshi"
    override val name = "Senshi"
    override val mainUrl = "https://senshi.to"
    override val description = "Anime from Senshi — latest episodes, filter browsing, sub & dub."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val apiHeaders: Map<String, String>
        get() = mapOf(
            "User-Agent" to "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36",
            "Accept" to "application/json, text/plain, */*",
            "Referer" to "$mainUrl/",
            "Origin" to mainUrl,
        )
    private val capture = Regex("""https?://[^"' \s]+\.m3u8[^"' \s]*|https?://[^"' \s]+\.mp4[^"' \s]*""")

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("latest", "Latest Episodes", HikariMediaType.SERIES),
        HikariCatalog("trending", "Trending", HikariMediaType.SERIES),
        HikariCatalog("top", "Top Rated", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val p = page.coerceAtLeast(1)
        if (catalog.id == "latest") {
            if (p > 1) return emptyList()
            val body = HikariNet.getString("$mainUrl/episode-embeds/latest", apiHeaders) ?: return emptyList()
            return try {
                val arr = if (body.trimStart().startsWith("[")) org.json.JSONArray(body) else return emptyList()
                val out = ArrayList<HikariMedia>()
                for (i in 0 until arr.length()) {
                    out.add(parseAnimeObj(arr.optJSONObject(i) ?: continue)?.let {
                        HikariMedia(id = it.first, title = it.second, type = HikariMediaType.SERIES, posterUrl = it.third)
                    } ?: continue)
                }
                out
            } catch (t: Throwable) { emptyList() }
        }
        val sort = if (catalog.id == "top") "score" else "trending"
        return postFilter("", p, sort)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank()) return emptyList()
        return postFilter(query.trim(), page.coerceAtLeast(1), "")
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val malId = media.id.substringAfter("senshi:", media.id).split("|")[0].ifBlank { return media }
        val html = HikariNet.getStringSmart("$mainUrl/anime/$malId", apiHeaders) ?: return media
        val title = Regex("""<meta[^>]+property="og:title"[^>]+content="([^"]+)"""").find(html)?.groupValues?.get(1) ?: media.title
        val poster = Regex("""<meta[^>]+property="og:image"[^>]+content="([^"]+)"""").find(html)?.groupValues?.get(1)?.takeIf { it.startsWith("http") } ?: media.posterUrl
        return media.copy(title = title, posterUrl = poster, backdropUrl = poster)
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val malId = media.id.substringAfter("senshi:", media.id).split("|")[0].toIntOrNull() ?: return null
        val body = HikariNet.getString("$mainUrl/episodes/$malId", apiHeaders) ?: return null
        return try {
            val arr = org.json.JSONArray(body)
            if (arr.length() == 0) return null
            val sorted = (0 until arr.length()).mapNotNull { arr.optJSONObject(it) }.filter { it.optInt("ep_id", 0) != 0 }.sortedBy { it.optInt("ep_id", 0) }
            if (sorted.isEmpty()) return null
            // Probe first episode for sub/dub availability.
            val probe = probeEmbeds(malId, sorted.first().optInt("ep_id", 0))
            val hasSub = probe?.any { it.isSub } ?: true
            val hasDub = probe?.any { it.isDub } ?: false
            val out = ArrayList<HikariEpisode>()
            for (e in sorted) {
                val epid = e.optInt("ep_id", 0)
                val n = e.optInt("number", e.optInt("episode", epid))
                val name = e.optString("title").takeIf { it.isNotBlank() } ?: "Episode $n"
                if (hasSub) out.add(HikariEpisode(number = n, id = "senshi:$malId|$epid|sub", name = name, image = media.posterUrl))
                if (hasDub) out.add(HikariEpisode(number = n, id = "senshi:$malId|$epid|dub", name = "$name (Dub)", image = media.posterUrl))
            }
            out.sortedBy { it.number }.ifEmpty { null }
        } catch (t: Throwable) { null }
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val ref = episode?.id ?: return emptyList()
        val parts = ref.substringAfter("senshi:").split("|")
        if (parts.size < 3) return emptyList()
        val malId = parts[0].toIntOrNull() ?: return emptyList()
        val epId = parts[1].toIntOrNull() ?: return emptyList()
        val wantDub = parts[2] == "dub"
        val embeds = probeEmbeds(malId, epId) ?: return emptyList()
        val matching = embeds.filter { if (wantDub) it.isDub else it.isSub }.ifEmpty { embeds }
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        for (e in matching.take(6)) {
            if (!e.url.startsWith("http")) continue
            for (r in HikariNet.resolveWithWebView(e.url, capture, timeoutMs = 45_000)) {
                if (!seen.add(r.url)) continue
                out.add(HikariStream(name = "Senshi ${if (wantDub) "Dub" else "Sub"}", url = r.url, headers = r.headers, isM3u8 = r.url.contains("m3u8")))
            }
            if (out.isNotEmpty()) return out
        }
        return out
    }

    private data class Embed(val url: String, val kind: String) {
        val isDub: Boolean get() = kind.contains("dub", true)
        val isSub: Boolean get() = !isDub
    }

    private suspend fun probeEmbeds(malId: Int, epId: Int): List<Embed>? {
        return try {
            val body = HikariNet.getString("$mainUrl/episode-embeds/$malId/$epId", apiHeaders) ?: return null
            val arr = if (body.trimStart().startsWith("[")) org.json.JSONArray(body) else return null
            buildList {
                for (i in 0 until arr.length()) {
                    val o = arr.optJSONObject(i) ?: continue
                    val url = o.optString("url").ifBlank { o.optString("embed") }.ifBlank { o.optString("src") }
                    if (!url.startsWith("http")) continue
                    add(Embed(url, o.optString("type").ifBlank { o.optString("audio") }))
                }
            }.ifEmpty { null }
        } catch (t: Throwable) { null }
    }

    private suspend fun postFilter(term: String, page: Int, sort: String): List<HikariMedia> {
        return try {
            val payload = JSONObject().put("searchTerm", term).put("page", page).put("limit", 30)
            if (sort.isNotBlank()) payload.put("sort", sort)
            val res = HikariNet.postJson("$mainUrl/filter", payload, apiHeaders) ?: return emptyList()
            val arr = res.optJSONArray("data") ?: return emptyList()
            val out = ArrayList<HikariMedia>()
            for (i in 0 until arr.length()) {
                parseAnimeObj(arr.optJSONObject(i) ?: continue)?.let { (id, title, poster) ->
                    out.add(HikariMedia(id = id, title = title, type = HikariMediaType.SERIES, posterUrl = poster))
                }
            }
            out
        } catch (t: Throwable) { emptyList() }
    }

    private fun parseAnimeObj(o: JSONObject): Triple<String, String, String?>? {
        val malId = o.optInt("malId", o.optInt("mal_id", o.optInt("id", 0)))
        if (malId == 0) return null
        val title = o.optString("title").ifBlank { o.optString("name") }.ifBlank { return null }
        val poster = o.optString("poster").ifBlank { o.optString("image") }.takeIf { it.startsWith("http") }
        return Triple("senshi:$malId", title, poster)
    }
}
