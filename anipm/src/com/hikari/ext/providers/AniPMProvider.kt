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
 * AniPM (ani.pm) — anime via JSON API + Settlar embeds.
 * Ported from KSHITIJ8473/raghav AniPM to native Hikari.
 *
 * Endpoints (GET JSON):
 *  /api/anime/search?q=                      search
 *  /api/anime/browse?sort=&page=&limit=30     home rows
 *  /api/anime/latest-episodes?page=           home row
 *  /api/anime/series/{id}?routes=e3          detail + episodes
 *  /api/anime/anipm-server/_packages?anilistId=  hardsub package map
 *  /api/anime/playback-bootstrap/settlar/{id}?ep=&lang=&backup=1
 *  /api/anime/settlar/session?selection=&provider=anipm&ep=&channel=&telemetry=0 -> {embedUrl}
 * Embed resolve: https://embed.settlar.io/api/embed/session?t={token}
 */
class AniPMProvider : HikariProvider {

    override val id = "anipm"
    override val name = "AniPM"
    override val mainUrl = "https://ani.pm"
    override val description = "Anime from ani.pm — latest episodes, trending, movies, sub & dub via Settlar."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val settlarEmbed = "https://embed.settlar.io"
    private val apiHeaders = mapOf(
        "User-Agent" to "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36",
        "Accept" to "application/json, text/plain, */*",
        "Referer" to "$mainUrl/",
    )
    private val m3u8Re = Regex("""https?://[^"' \s]+\.m3u8[^"' \s]*|https?://[^"' \s]+\.mp4[^"' \s]*""")
    private val genericTitle = Regex("""^Episode \d+(\.\d+)?$""")

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("latest", "Latest Episodes", HikariMediaType.SERIES),
        HikariCatalog("trending", "Trending", HikariMediaType.SERIES),
        HikariCatalog("top", "Top Rated", HikariMediaType.SERIES),
        HikariCatalog("movies", "Movies", HikariMediaType.MOVIE),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val p = page.coerceAtLeast(1)
        return when (catalog.id) {
            "latest" -> {
                val body = HikariNet.getString("$mainUrl/api/anime/latest-episodes?page=$p", apiHeaders) ?: return emptyList()
                parseLatest(body)
            }
            "movies" -> parseBrowse(fetchBrowse("popular", p, "movie"))
            "top" -> parseBrowse(fetchBrowse("score", p, null))
            else -> parseBrowse(fetchBrowse("trending", p, null))
        }
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank() || page > 1) return emptyList()
        val body = HikariNet.getString("$mainUrl/api/anime/search?q=${URLEncoder.encode(query.trim(), "UTF-8")}", apiHeaders)
            ?: return emptyList()
        return try {
            val o = JSONObject(body)
            val arr = o.optJSONArray("items") ?: o.optJSONArray("data") ?: o.optJSONArray("results") ?: return emptyList()
            parseTitles(arr)
        } catch (t: Throwable) { emptyList() }
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val seriesId = media.id.substringAfterLast("|").ifBlank { return media }
        val body = HikariNet.getString("$mainUrl/api/anime/series/$seriesId?routes=e3", apiHeaders) ?: return media
        return try {
            val o = JSONObject(body)
            val s = if (o.has("series")) o.getJSONObject("series") else o
            val title = s.optString("title").ifBlank { media.title }
            val poster = abs(s.optString("poster")).takeIf { it.startsWith("http") } ?: media.posterUrl
            val banner = abs(s.optString("banner")).takeIf { it.startsWith("http") }
            val desc = s.optString("description").takeIf { it.isNotBlank() } ?: s.optString("synopsis").takeIf { it.isNotBlank() } ?: media.overview
            val genres = s.optJSONArray("genres")?.let { arr -> buildList { for (i in 0 until arr.length()) arr.optString(i).takeIf { it.isNotBlank() }?.let { add(it) } } } ?: media.genres
            media.copy(title = title, posterUrl = poster, backdropUrl = banner ?: poster, overview = desc, genres = genres)
        } catch (t: Throwable) { media }
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val seriesId = media.id.substringAfterLast("|").ifBlank { return null }
        val body = HikariNet.getString("$mainUrl/api/anime/series/$seriesId?routes=e3", apiHeaders) ?: return null
        return try {
            val o = JSONObject(body)
            val s = if (o.has("series")) o.getJSONObject("series") else o
            val eps = s.optJSONArray("episodes") ?: return null
            val out = ArrayList<HikariEpisode>()
            for (i in 0 until eps.length()) {
                val e = eps.optJSONObject(i) ?: continue
                val num = e.optInt("number", e.optInt("episode", i + 1))
                var name = e.optString("title")
                if (name.isBlank() || genericTitle.matches(name)) name = "Episode $num"
                val thumb = abs(e.optString("thumbnail")).takeIf { it.startsWith("http") }
                out.add(HikariEpisode(number = num, id = "$seriesId|$num|sub", name = name, image = thumb))
                out.add(HikariEpisode(number = num, id = "$seriesId|$num|dub", name = "$name (Dub)", image = thumb))
            }
            out.ifEmpty { null }
        } catch (t: Throwable) { null }
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val ref = episode?.id ?: return emptyList()
        val parts = ref.split("|")
        if (parts.size < 3) return emptyList()
        val seriesId = parts[0].toIntOrNull() ?: return emptyList()
        val ep = parts[1].toIntOrNull() ?: return emptyList()
        val lang = parts.getOrNull(2)?.takeIf { it == "dub" } ?: "sub"
        val out = ArrayList<HikariStream>()
        // 1) bootstrap -> selection list
        val boot = HikariNet.getString("$mainUrl/api/anime/playback-bootstrap/settlar/$seriesId?ep=$ep&lang=$lang&backup=1", apiHeaders)
        val selections = ArrayList<String>()
        if (boot != null) {
            try {
                val o = JSONObject(boot)
                val arr = o.optJSONArray("selections") ?: o.optJSONArray("options") ?: o.optJSONArray("servers")
                if (arr != null) for (i in 0 until arr.length()) {
                    val s = arr.opt(i)?.toString()
                    if (!s.isNullOrBlank()) selections.add(s)
                }
            } catch (t: Throwable) { }
        }
        if (selections.isEmpty()) selections.add("default")
        for (sel in selections.take(4)) {
            try {
                val sess = HikariNet.getString(
                    "$mainUrl/api/anime/settlar/session?selection=${URLEncoder.encode(sel, "UTF-8")}&provider=anipm&ep=$ep&channel=$lang&telemetry=0",
                    apiHeaders
                ) ?: continue
                val embedUrl = JSONObject(sess).optString("embedUrl")
                if (embedUrl.isBlank()) continue
                val token = Regex("t=([^&]+)").find(embedUrl)?.groupValues?.get(1) ?: continue
                val resolved = HikariNet.getString("$settlarEmbed/api/embed/session?t=$token", apiHeaders + mapOf("Referer" to "$settlarEmbed/embed/v1"))
                    ?: continue
                val ro = JSONObject(resolved)
                val file = ro.optString("url").ifBlank { ro.optString("file") }.ifBlank { ro.optString("src") }
                val subs = ro.optJSONArray("subtitles") ?: ro.optJSONArray("tracks")
                val subList = ArrayList<com.hikari.ext.HikariSubtitle>()
                if (subs != null) for (i in 0 until subs.length()) {
                    val s = subs.optJSONObject(i) ?: continue
                    val u = s.optString("url").ifBlank { s.optString("file") }
                    if (u.startsWith("http")) subList.add(com.hikari.ext.HikariSubtitle(s.optString("lang", "English"), u))
                }
                if (file.startsWith("http") && out.none { it.url == file }) {
                    out.add(HikariStream(name = "AniPM ${lang.uppercase()} — Settlar", url = file, subtitles = subList, isM3u8 = file.contains("m3u8")))
                }
            } catch (t: Throwable) { }
        }
        return out
    }

    private suspend fun fetchBrowse(sort: String, page: Int, format: String?): String? {
        val sb = StringBuilder("$mainUrl/api/anime/browse?sort=$sort&page=$page&limit=30")
        if (format != null) sb.append("&format=$format")
        return HikariNet.getString(sb.toString(), apiHeaders)
    }

    private fun parseBrowse(body: String?): List<HikariMedia> {
        if (body == null) return emptyList()
        return try {
            val o = JSONObject(body)
            val arr = o.optJSONArray("items") ?: o.optJSONArray("data") ?: return emptyList()
            parseTitles(arr)
        } catch (t: Throwable) { emptyList() }
    }

    private fun parseLatest(body: String): List<HikariMedia> {
        return try {
            val o = JSONObject(body)
            val arr = o.optJSONArray("data") ?: o.optJSONArray("items") ?: return emptyList()
            val out = ArrayList<HikariMedia>()
            for (i in 0 until arr.length()) {
                val e = arr.optJSONObject(i) ?: continue
                val id = e.optString("seriesId").ifBlank { e.optString("id") }
                if (id.isBlank()) continue
                val title = e.optString("title").ifBlank { continue }
                out.add(HikariMedia(id = "$mainUrl|$id", title = title, type = HikariMediaType.SERIES, posterUrl = abs(e.optString("poster")).takeIf { it.startsWith("http") }))
            }
            out
        } catch (t: Throwable) { emptyList() }
    }

    private fun parseTitles(arr: org.json.JSONArray): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        for (i in 0 until arr.length()) {
            val e = arr.optJSONObject(i) ?: continue
            val id = e.optString("id").ifBlank { continue }
            val title = e.optString("title").ifBlank { continue }
            val poster = abs(e.optString("poster")).takeIf { it.startsWith("http") }
            val format = e.optString("format")
            out.add(HikariMedia(id = "$mainUrl|$id", title = title, type = if (format.equals("movie", true)) HikariMediaType.MOVIE else HikariMediaType.SERIES, posterUrl = poster))
        }
        return out
    }

    private fun abs(u: String): String {
        if (u.isBlank()) return u
        if (u.startsWith("http")) return u
        return if (u.startsWith("/")) "$mainUrl$u" else "$mainUrl/$u"
    }
}
