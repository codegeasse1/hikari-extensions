package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import com.hikari.ext.HikariSubtitle
import org.json.JSONArray
import org.json.JSONObject
import java.net.URLEncoder
import java.security.MessageDigest

/**
 * MovieBox (api3/4/4sg/5/6.aoneroom.com wefeed-mobile-bff) — movies & series,
 * same app-backend family as NetNaija (the allforu MovieBox + NetNaija plugins
 * share these subject-api endpoints):
 *  - list / search/v2 / get / season-info / play-info / get-stream-captions.
 *  - Requests carry x-client-token ("<ts>,<md5>" scheme, same as the working
 *    NetNaija port); calls are retried unauthenticated since the backend
 *    serves several of them that way too.
 * Endpoint verified live Oct 2026 (POST /subject-api/list answers 400 on
 * wrong content-type instead of 404 — the API is up).
 */
class MovieBoxProvider : HikariProvider {

    override val id = "moviebox"
    override val name = "MovieBox"
    override val mainUrl = "https://api3.aoneroom.com"
    override val description = "Movies & TV series — trending, latest, top rated, search with subtitles."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val UA =
        "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36"

    companion object {
        val HOSTS = listOf(
            "https://api3.aoneroom.com",
            "https://api4.aoneroom.com",
            "https://api4sg.aoneroom.com",
            "https://api5.aoneroom.com",
            "https://api6.aoneroom.com",
        )
        var host = HOSTS[0]
    }

    private fun clientToken(): String {
        return try {
            val ts = System.currentTimeMillis() / 1000
            val rev = ts.toString().reversed()
            val md5 = MessageDigest.getInstance("MD5").digest(rev.toByteArray())
                .joinToString("") { "%02x".format(it) }
            "$ts,$md5"
        } catch (t: Throwable) {
            "${System.currentTimeMillis() / 1000},"
        }
    }

    private fun headers(): Map<String, String> = mapOf(
        "User-Agent" to UA,
        "Accept" to "application/json",
        "Content-Type" to "application/json",
        "x-client-token" to clientToken(),
    )

    private suspend fun post(path: String, body: JSONObject): JSONObject? {
        for (h in HOSTS) {
            for (auth in listOf(true, false)) {
                try {
                    val hd = if (auth) headers() else headers() - "x-client-token"
                    val txt = HikariNet.postString("$h$path", body.toString(), hd, "application/json")
                    if (txt == null) continue
                    val o = runCatching { JSONObject(txt) }.getOrNull() ?: continue
                    if (o.optInt("code", 200) !in 200..299 && o.optBoolean("success", true).not()) continue
                    host = h
                    return o
                } catch (t: Throwable) { }
            }
        }
        return null
    }

    private suspend fun get(path: String): JSONObject? {
        for (h in HOSTS) {
            try {
                val o = HikariNet.getJson("$h$path", headers()) ?: continue
                host = h
                return o
            } catch (t: Throwable) { }
        }
        return null
    }

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("trendmov", "Trending Movies", HikariMediaType.MOVIE),
        HikariCatalog("latestmov", "Latest Movies", HikariMediaType.MOVIE),
        HikariCatalog("topmov", "Top Movies", HikariMediaType.MOVIE),
        HikariCatalog("trendtv", "Trending Series", HikariMediaType.SERIES),
        HikariCatalog("latesttv", "Latest Series", HikariMediaType.SERIES),
        HikariCatalog("anime", "Anime", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val p = page.coerceAtLeast(1)
        val body = JSONObject().put("page", p).put("perPage", 24)
        when (catalog.id) {
            "trendmov" -> body.put("channel", "movie").put("sort", "trending")
            "latestmov" -> body.put("channel", "movie").put("sort", "latest")
            "topmov" -> body.put("channel", "movie").put("sort", "rating")
            "trendtv" -> body.put("channel", "tv").put("sort", "trending")
            "latesttv" -> body.put("channel", "tv").put("sort", "latest")
            "anime" -> body.put("channel", "anime").put("sort", "trending")
        }
        val root = post("/wefeed-mobile-bff/subject-api/list", body)
            ?: get("/wefeed-mobile-bff/subject-api/list?page=$p&perPage=24")
            ?: return emptyList()
        return subjectsToMedia(extractList(root))
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        val p = page.coerceAtLeast(1)
        val root = post(
            "/wefeed-mobile-bff/subject-api/search/v2",
            JSONObject().put("keyword", q).put("page", p).put("perPage", 24),
        ) ?: get("/wefeed-mobile-bff/subject-api/search/v2?keyword=${URLEncoder.encode(q, "UTF-8")}&page=$p")
        if (root == null) return emptyList()
        return subjectsToMedia(extractList(root))
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val sid = media.id.substringAfter("mb:", "").substringBefore(":")
        if (sid.isBlank()) return media
        val root = get("/wefeed-mobile-bff/subject-api/get?subjectId=${URLEncoder.encode(sid, "UTF-8")}")
            ?: return media
        val d = root.optJSONObject("data") ?: root.optJSONObject("subject") ?: return media
        return media.copy(
            title = d.optString("title").ifBlank { d.optString("name") }.ifBlank { media.title },
            posterUrl = d.optString("poster").ifBlank { d.optString("cover") }.takeIf { it.startsWith("http") } ?: media.posterUrl,
            backdropUrl = d.optString("backdrop").takeIf { it.startsWith("http") } ?: media.backdropUrl,
            overview = d.optString("description").ifBlank { d.optString("overview") }.takeIf { it.isNotBlank() } ?: media.overview,
            year = d.optInt("year", 0).takeIf { it > 0 } ?: d.optString("releaseDate").take(4).toIntOrNull() ?: media.year,
        )
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val sid = media.id.substringAfter("mb:", "").substringBefore(":")
        if (sid.isBlank()) return null
        if (media.rawType == "movie") {
            return listOf(HikariEpisode(number = 1, id = "${media.id}#ep1", name = media.title, image = media.posterUrl))
        }
        val root = get("/wefeed-mobile-bff/subject-api/season-info?subjectId=${URLEncoder.encode(sid, "UTF-8")}")
            ?: return listOf(HikariEpisode(number = 1, id = "${media.id}#ep1", name = media.title, image = media.posterUrl))
        val out = ArrayList<HikariEpisode>()
        val seasons = root.optJSONObject("data")?.optJSONArray("seasons")
            ?: root.optJSONArray("seasons")
        if (seasons != null) {
            for (i in 0 until seasons.length()) {
                val s = seasons.optJSONObject(i) ?: continue
                val sn = s.optInt("season", s.optInt("seasonNumber", i + 1))
                val eps = s.optJSONArray("episodes") ?: s.optJSONArray("episodeList")
                if (eps != null) {
                    for (j in 0 until eps.length()) {
                        val e = eps.optJSONObject(j) ?: continue
                        val n = e.optInt("episode", e.optInt("episodeNumber", j + 1))
                        out.add(HikariEpisode(number = n, season = sn, id = "${media.id}#s${sn}e$n", name = e.optString("title").ifBlank { "S${sn} E$n" }))
                    }
                } else {
                    val count = s.optInt("episodeCount", s.optInt("totalEpisode", 0))
                    for (n in 1..count) out.add(HikariEpisode(number = n, season = sn, id = "${media.id}#s${sn}e$n", name = "S${sn} E$n"))
                }
            }
        }
        return out.ifEmpty { null }
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val ref = episode?.id ?: media.id
        val sid = ref.substringAfter("mb:", "").substringBefore(":")
        if (sid.isBlank()) return emptyList()
        val se = Regex("#s(\\d+)e(\\d+)").find(ref)
        val season = se?.groupValues?.get(1)?.toIntOrNull() ?: 1
        val ep = se?.groupValues?.get(2)?.toIntOrNull() ?: 1
        val root = get("/wefeed-mobile-bff/subject-api/play-info?subjectId=${URLEncoder.encode(sid, "UTF-8")}&season=$season&episode=$ep")
            ?: post("/wefeed-mobile-bff/subject-api/play-info", JSONObject().put("subjectId", sid).put("season", season).put("episode", ep))
            ?: return emptyList()
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        val data = root.optJSONObject("data") ?: root
        val playList = data.optJSONArray("playList") ?: data.optJSONArray("sources") ?: data.optJSONArray("streams")
        if (playList != null) {
            for (i in 0 until playList.length()) {
                val s = playList.optJSONObject(i) ?: continue
                val url = s.optString("url").ifBlank { s.optString("playUrl") }
                if (!url.startsWith("http") || !seen.add(url)) continue
                val q = s.optString("quality").ifBlank { s.optString("resolution") }
                out.add(
                    HikariStream(
                        name = "MovieBox" + (if (q.isNotBlank()) " $q" else ""),
                        url = url,
                        headers = mapOf("User-Agent" to UA, "Referer" to "https://api3.aoneroom.com/"),
                        isM3u8 = url.contains(".m3u8"),
                    )
                )
            }
        }
        // Captions attached to the first stream when present.
        try {
            val caps = get("/wefeed-mobile-bff/subject-api/get-stream-captions?subjectId=${URLEncoder.encode(sid, "UTF-8")}")
            val arr = caps?.optJSONObject("data")?.optJSONArray("captions") ?: caps?.optJSONArray("captions")
            if (arr != null && out.isNotEmpty()) {
                val subs = ArrayList<HikariSubtitle>()
                for (i in 0 until arr.length()) {
                    val c = arr.optJSONObject(i) ?: continue
                    val u = c.optString("url")
                    if (u.startsWith("http")) subs.add(HikariSubtitle(c.optString("lang").ifBlank { c.optString("language") }.ifBlank { "Subtitle" }, u))
                }
                if (subs.isNotEmpty()) return out.map { it.copy(subtitles = it.subtitles + subs) }
            }
        } catch (t: Throwable) { }
        return out
    }

    // ------------------------------------------------------------------
    // Helpers.
    // ------------------------------------------------------------------

    private fun extractList(root: JSONObject): JSONArray? {
        val data = root.optJSONObject("data") ?: return root.optJSONArray("list")
            ?: root.optJSONArray("subjects")
        return data.optJSONArray("list")
            ?: data.optJSONArray("subjects")
            ?: data.optJSONArray("subjectList")
            ?: data.optJSONArray("items")
    }

    private fun subjectsToMedia(arr: JSONArray?): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        if (arr == null) return out
        for (i in 0 until arr.length()) {
            val o = arr.optJSONObject(i) ?: continue
            val sid = o.optString("subjectId").ifBlank { o.optString("id") }
            val title = o.optString("title").ifBlank { o.optString("name") }.trim()
            if (sid.isBlank() || title.isBlank()) continue
            val kind = o.optString("type").lowercase()
            val isMovie = kind.contains("movie") || (!kind.contains("tv") && !kind.contains("series") && !kind.contains("anime") && o.optInt("episodeCount", 0) == 0 && kind.isNotBlank())
            val poster = o.optString("poster").ifBlank { o.optString("cover") }.takeIf { it.startsWith("http") }
            out.add(
                HikariMedia(
                    id = "mb:$sid:${if (isMovie) "movie" else "tv"}",
                    title = title,
                    type = if (isMovie) HikariMediaType.MOVIE else HikariMediaType.SERIES,
                    posterUrl = poster,
                    year = o.optInt("year", 0).takeIf { it > 0 },
                    rawType = if (isMovie) "movie" else "tv",
                )
            )
        }
        return out
    }
}
