package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import com.hikari.ext.HikariSubtitle
import android.util.Base64
import java.net.URLEncoder
import org.json.JSONArray
import org.json.JSONObject

/**
 * NetNaija-box (MovieBox app backend) — movies, series, anime + live sports.
 *
 * Native port of KSHITIJ8473/raghav NetNaija-box (single MainAPI NetNaijaApp,
 * hence a single extension): the mobile BFF on the api*.aoneroom.com host
 * pool drives 40 section rows (rankings, channel browse lists, live sports),
 * search/v2, subject get/season-info, and play-info streams (DASH first, mp4
 * mirrors, sign-cookie policy decode) with ext captions. Requests are signed
 * with the mobile HMAC scheme (see NnAppAuth); hosts rotate on failure.
 *
 * NOTE: the mobile gateway region-blocks some datacenter IPs (403
 * "Service not available in current region" seen from a probe); on normal
 * user networks the same calls succeed.
 */
class NetNaijaAppProvider : HikariProvider {

    override val id = "netnaijaapp"
    override val name = "NetNaija-box"
    override var mainUrl = "https://api3.aoneroom.com"
    override val description = "Movies, series, anime & live sports from the NetNaija-box app backend."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.MOVIE, HikariMediaType.SERIES)

    private companion object {
        val HOSTS = listOf(
            "https://api6.aoneroom.com",
            "https://api5.aoneroom.com",
            "https://api4.aoneroom.com",
            "https://api4sg.aoneroom.com",
            "https://api3.aoneroom.com",
        )
        const val SPORT_API = "https://h5-sport-api.aoneroom.com/wefeed-h5api-bff"
        const val SPORT_URL = "https://www.sportslive.wine/"
        const val WEB_UA =
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

        // "key" to "row title" — full sectionCatalog from the source plugin.
        val SECTIONS = listOf(
            "live|matches" to "⚽ Live Football & Sports",
            "r|0|9167640870324258216" to "Trending Movies",
            "r|0|5692654647815587592" to "In Cinema",
            "r|0|414907768299210008" to "Bollywood",
            "r|0|3859721901924910512" to "South Indian",
            "r|0|8019599703232971616" to "Hollywood",
            "r|0|1488104699998914056" to "New Release",
            "r|0|6027735606879570952" to "New Punjabi",
            "r|0|6144409817256968824" to "New Bengali",
            "r|5|719331337777440448" to "Top Series",
            "r|5|4903182713986896328" to "Indian Drama",
            "r|5|1255898847918934600" to "Reality TV",
            "r|5|1976033493293449744" to "Asian Drama",
            "r|5|3910636007619709856" to "Western TV",
            "r|5|5177200225164885656" to "Turkish Drama",
            "1|1" to "Movies",
            "1|2" to "Series",
            "1|1006" to "Anime",
            "2|2;country=Japan;genre=Animation" to "Anime (Series)",
            "1|1;country=India" to "Indian (Movies)",
            "1|2;country=India" to "Indian (Series)",
            "1|1;classify=Hindi dub;country=United States" to "USA (Movies)",
            "1|2;classify=Hindi dub;country=United States" to "USA (Series)",
            "1|1;country=Japan" to "Japan (Movies)",
            "1|2;country=Japan" to "Japan (Series)",
            "1|1;country=China" to "China (Movies)",
            "1|2;country=China" to "China (Series)",
            "1|1;country=Philippines" to "Philippines (Movies)",
            "1|2;country=Philippines" to "Philippines (Series)",
            "1|1;country=Thailand" to "Thailand (Movies)",
            "1|2;country=Thailand" to "Thailand (Series)",
            "1|1;country=Nigeria" to "Nollywood (Movies)",
            "1|2;country=Nigeria" to "Nollywood (Series)",
            "1|1;country=Korea" to "South Korean (Movies)",
            "1|2;country=Korea" to "South Korean (Series)",
            "1|1;classify=Hindi dub;genre=Action" to "Action (Movies)",
            "1|1;classify=Hindi dub;genre=Crime" to "Crime (Movies)",
            "1|1;classify=Hindi dub;genre=Comedy" to "Comedy (Movies)",
            "1|2;classify=Hindi dub;genre=Crime" to "Crime (Series)",
            "1|2;classify=Hindi dub;genre=Comedy" to "Comedy (Series)",
        )
    }

    override fun catalogs(): List<HikariCatalog> = SECTIONS.map { (k, name) ->
        val t = if (k.startsWith("1|2") || k.startsWith("2|") || k.startsWith("r|5")) HikariMediaType.SERIES else HikariMediaType.MOVIE
        HikariCatalog(k, name, t)
    }

    // ------------------------------------------------------------- plumbing

    private suspend fun signedGet(url: String, extra: Map<String, String> = emptyMap()): String? {
        for (host in HOSTS) {
            val u = if (url.startsWith("http")) url else host + url
            try {
                val r = HikariNet.fetch(u, NnAppAuth.headers("GET", u) + extra) ?: continue
                if (r.status == 401 || r.status == 441) continue
                if (r.status == 200 && r.body != null) {
                    mainUrl = host
                    return r.body
                }
            } catch (_: Exception) {
            }
        }
        return null
    }

    private suspend fun signedPost(url: String, body: String): String? {
        for (host in HOSTS) {
            val u = if (url.startsWith("http")) url else host + url
            try {
                val h = NnAppAuth.headers("POST", u, "application/json; charset=utf-8", body = body)
                val t = HikariNet.postString(u, body, h, "application/json; charset=utf-8")
                if (t != null) {
                    mainUrl = host
                    return t
                }
            } catch (_: Exception) {
            }
        }
        return null
    }

    private fun itemToMedia(o: JSONObject): HikariMedia? {
        val title = o.optString("title").ifBlank { return null }
        val sid = o.optString("subjectId").ifBlank { return null }
        val poster = o.optJSONObject("cover")?.optString("url")?.takeIf { it.startsWith("http") }
        val series = o.optInt("subjectType", 1) == 2
        return HikariMedia(
            id = sid, title = title,
            type = if (series) HikariMediaType.SERIES else HikariMediaType.MOVIE,
            posterUrl = poster,
        )
    }

    // -------------------------------------------------------------- catalogs

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val p = page.coerceAtLeast(1)
        val key = catalog.id
        return try {
            when {
                key.startsWith("live|") -> liveMatches()
                key.startsWith("r|") -> {
                    val parts = key.split("|")
                    val tab = parts.getOrNull(1) ?: "0"
                    val cat = parts.getOrNull(2) ?: return emptyList()
                    val txt = signedGet(
                        "$mainUrl/wefeed-mobile-bff/tab/ranking-list?tabId=$tab&categoryType=$cat&page=$p&perPage=20&restrictKid=1",
                    ) ?: return emptyList()
                    val items = JSONObject(txt).optJSONObject("data")?.optJSONArray("items")
                        ?: return emptyList()
                    (0 until items.length()).mapNotNull { itemToMedia(items.optJSONObject(it) ?: return@mapNotNull null) }
                }
                key.startsWith("1|") || key.startsWith("2|") -> {
                    val channel = key.substringBefore(";").split("|").getOrNull(1) ?: return emptyList()
                    val body = JSONObject().put("page", p).put("perPage", 20).put("channelId", channel)
                    for (f in key.substringAfter(";", "").split(";")) {
                        if (f.isBlank()) continue
                        val kv = f.split("=", limit = 2)
                        if (kv.size == 2) body.put(kv[0], kv[1])
                    }
                    val txt = signedPost("$mainUrl/wefeed-mobile-bff/subject-api/list", body.toString())
                        ?: return emptyList()
                    val arr = JSONObject(txt).optJSONObject("data")?.optJSONArray("subjects")
                        ?: return emptyList()
                    (0 until arr.length()).mapNotNull { itemToMedia(arr.optJSONObject(it) ?: return@mapNotNull null) }
                }
                else -> emptyList()
            }
        } catch (_: Exception) {
            emptyList()
        }
    }

    private suspend fun liveMatches(): List<HikariMedia> {
        return try {
            val h = mapOf(
                "User-Agent" to WEB_UA, "Accept" to "application/json",
                "Origin" to SPORT_URL.trimEnd('/'), "Referer" to SPORT_URL,
                "X-Device-Info" to "{}",
            )
            val txt = HikariNet.getString("$SPORT_API/live/match-list-v5", h) ?: return emptyList()
            val list = JSONObject(txt).optJSONObject("data")?.optJSONArray("list") ?: return emptyList()
            val out = ArrayList<HikariMedia>()
            for (i in 0 until list.length()) {
                val m = list.optJSONObject(i) ?: continue
                val id = m.optString("id").ifBlank { continue }
                val t1 = m.optJSONObject("team1")
                val t2 = m.optJSONObject("team2")
                val n1 = t1?.optString("name") ?: "Team 1"
                val n2 = t2?.optString("name") ?: "Team 2"
                val status = m.optString("status")
                if (status == "MatchEnded" || status == "2" || status == "FT") continue
                val playPath = m.optString("playPath").ifBlank { continue }
                if (!playPath.startsWith("http")) continue
                val league = m.optString("league")
                val title = (if (league.isNotBlank()) "$league: " else "") + "$n1 vs $n2"
                val poster = t1?.optString("avatar")?.takeIf { it.startsWith("http") }
                    ?: t2?.optString("avatar")?.takeIf { it.startsWith("http") }
                val payload = JSONObject()
                    .put("title", title).put("poster", poster ?: "")
                    .put("playPath", playPath)
                    .put("sources", m.optJSONArray("playSource") ?: JSONArray())
                val eid = "sport_match:" + URLEncoder.encode(payload.toString(), "UTF-8")
                out.add(HikariMedia(id = eid, title = title, type = HikariMediaType.SERIES, posterUrl = poster))
            }
            out
        } catch (_: Exception) {
            emptyList()
        }
    }

    // ---------------------------------------------------------------- search

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank()) return emptyList()
        return try {
            val p = page.coerceAtLeast(1)
            val body = JSONObject().put("page", p).put("perPage", 20)
                .put("keyword", query.trim()).put("restrictKid", 1).toString()
            val txt = signedPost("$mainUrl/wefeed-mobile-bff/subject-api/search/v2", body)
                ?: return emptyList()
            val groups = JSONObject(txt).optJSONObject("data")?.optJSONArray("results")
                ?: return emptyList()
            val out = ArrayList<HikariMedia>()
            for (i in 0 until groups.length()) {
                val subs = groups.optJSONObject(i)?.optJSONArray("subjects") ?: continue
                for (j in 0 until subs.length()) {
                    itemToMedia(subs.optJSONObject(j) ?: continue)?.let { out.add(it) }
                }
            }
            out
        } catch (_: Exception) {
            emptyList()
        }
    }

    // -------------------------------------------------------------- episodes

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        if (media.id.startsWith("sport_match:")) return media
        return try {
            val txt = signedGet("$mainUrl/wefeed-mobile-bff/subject-api/get?subjectId=${media.id}")
                ?: return media
            val d = JSONObject(txt).optJSONObject("data") ?: return media
            val title = d.optString("title").ifBlank { media.title }
            val poster = d.optJSONObject("cover")?.optString("url")?.takeIf { it.startsWith("http") }
                ?: media.posterUrl
            media.copy(title = title, posterUrl = poster, backdropUrl = poster,
                overview = d.optString("description").ifBlank { null } ?: media.overview)
        } catch (_: Exception) {
            media
        }
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        if (media.id.startsWith("sport_match:")) {
            return listOf(HikariEpisode(number = 1, id = media.id, name = media.title, image = media.posterUrl))
        }
        return try {
            val txt = signedGet("$mainUrl/wefeed-mobile-bff/subject-api/get?subjectId=${media.id}")
                ?: return null
            val d = JSONObject(txt).optJSONObject("data") ?: return null
            val detailPath = d.optString("detailPath")
            // Movie or single-video subject: one playable item.
            val subjectType = d.optInt("subjectType", 1)
            if (subjectType != 2) {
                val eid = "nb:" + URLEncoder.encode(
                    JSONObject().put("sid", media.id).put("se", 0).put("ep", 0)
                        .put("dp", detailPath).toString(), "UTF-8",
                )
                return listOf(HikariEpisode(number = 1, id = eid, name = d.optString("title"), image = media.posterUrl))
            }
            val seTxt = signedGet("$mainUrl/wefeed-mobile-bff/subject-api/season-info?subjectId=${media.id}")
            val out = ArrayList<HikariEpisode>()
            if (seTxt != null) {
                val sd = try {
                    JSONObject(seTxt).optJSONObject("data")
                } catch (_: Exception) {
                    null
                }
                val seasons = sd?.optJSONArray("seasons") ?: sd?.optJSONArray("list")
                if (seasons != null) {
                    for (i in 0 until seasons.length()) {
                        val s = seasons.optJSONObject(i) ?: continue
                        val sn = s.optInt("se", s.optInt("season", -1))
                        val maxEp = s.optInt("maxEp", s.optInt("episodeCount", s.optInt("epCount", 0)))
                        if (sn <= 0 || maxEp <= 0) continue
                        for (ep in 1..maxEp) {
                            val eid = "nb:" + URLEncoder.encode(
                                JSONObject().put("sid", media.id).put("se", sn).put("ep", ep)
                                    .put("dp", detailPath).toString(), "UTF-8",
                            )
                            out.add(HikariEpisode(number = ep, season = sn, id = eid, name = "Episode $ep", image = media.posterUrl))
                        }
                    }
                }
                // Flat episode list fallback.
                val eps = sd?.optJSONArray("episodes")
                if (out.isEmpty() && eps != null) {
                    for (i in 0 until eps.length()) {
                        val e = eps.optJSONObject(i) ?: continue
                        val sn = e.optInt("se", 1)
                        val num = e.optInt("ep", e.optInt("episode", i + 1))
                        val eid = "nb:" + URLEncoder.encode(
                            JSONObject().put("sid", media.id).put("se", sn).put("ep", num)
                                .put("dp", detailPath).toString(), "UTF-8",
                        )
                        out.add(HikariEpisode(number = num, season = sn, id = eid,
                            name = e.optString("name").ifBlank { "Episode $num" }, image = media.posterUrl))
                    }
                }
            }
            out.sortWith(compareBy({ it.season }, { it.number }))
            out.ifEmpty { null }
        } catch (_: Exception) {
            null
        }
    }

    // --------------------------------------------------------------- streams

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val ref = episode?.id ?: return emptyList()
        if (ref.startsWith("sport_match:")) return sportStreams(ref)
        if (!ref.startsWith("nb:")) return emptyList()
        return try {
            val o = JSONObject(java.net.URLDecoder.decode(ref.removePrefix("nb:"), "UTF-8"))
            val sid = o.optString("sid").ifBlank { return emptyList() }
            val se = o.optInt("se")
            val ep = o.optInt("ep")
            val txt = signedGet("$mainUrl/wefeed-mobile-bff/subject-api/play-info?subjectId=$sid&se=$se&ep=$ep")
                ?: return emptyList()
            val data = JSONObject(txt).optJSONObject("data") ?: return emptyList()
            val arr = data.optJSONArray("streams") ?: return emptyList()
            val out = ArrayList<HikariStream>()
            val subs = ArrayList<HikariSubtitle>()
            val seen = HashSet<String>()
            // DASH variants first so adaptive streams sort to the top.
            val sorted = (0 until arr.length()).map { arr.optJSONObject(it) }
                .sortedByDescending {
                    val u = it?.optString("url") ?: ""
                    val f = it?.optString("format") ?: ""
                    if (f.equals("DASH", true) || u.contains(".mpd")) 1 else 0
                }
            var firstStreamId: String? = null
            for (st in sorted) {
                st ?: continue
                var url = st.optString("url").ifBlank { continue }
                val format = st.optString("format")
                val res = st.optString("resolutions")
                val cookie = st.optString("signCookie").ifBlank { null }
                val streamId = st.optString("id").ifBlank { "$sid|$se|$ep" }
                if (firstStreamId == null) firstStreamId = streamId
                if (cookie != null) {
                    policyResource(cookie)?.let { url = it }
                }
                if (!url.startsWith("http") || !seen.add(url)) continue
                val mpd = format.equals("DASH", true) || url.contains(".mpd")
                val m3u8 = url.contains(".m3u8")
                val label = if (res.isNotBlank()) " $res" else ""
                val nm = "NetNaija-box" + (if (mpd) " DASH" else "") + label
                val hs = if (cookie != null) mapOf("Cookie" to cookie) else emptyMap()
                out.add(HikariStream(name = nm, url = url, headers = hs, isMpd = mpd, isM3u8 = m3u8))
            }
            if (firstStreamId != null) loadCaptions(sid, firstStreamId, subs)
            if (subs.isNotEmpty() && out.isNotEmpty()) return out.map { it.copy(subtitles = it.subtitles + subs) }
            out
        } catch (_: Exception) {
            emptyList()
        }
    }

    private fun sportStreams(ref: String): List<HikariStream> {
        return try {
            val o = JSONObject(java.net.URLDecoder.decode(ref.removePrefix("sport_match:"), "UTF-8"))
            val out = ArrayList<HikariStream>()
            val seen = HashSet<String>()
            val paths = ArrayList<String>()
            o.optString("playPath").takeIf { it.startsWith("http") }?.let { paths.add(it) }
            val srcs = o.optJSONArray("sources")
            if (srcs != null) for (i in 0 until srcs.length()) {
                srcs.optJSONObject(i)?.optString("path")?.takeIf { it.startsWith("http") }?.let { paths.add(it) }
            }
            for (u in paths) {
                if (!seen.add(u)) continue
                out.add(HikariStream(name = "Live Sports", url = u, isM3u8 = u.contains("m3u8"), isMpd = u.contains("mpd")))
            }
            out
        } catch (_: Exception) {
            emptyList()
        }
    }

    /** Sign cookies embed the manifest path; decoding one yields the real URL. */
    private fun policyResource(cookie: String): String? {
        try {
            Regex("Edge-Cache-Cookie=urlprefix=([^:;\\s]+)").find(cookie)?.let { m ->
                var s = m.groupValues[1].replace('_', '/').replace('-', '+')
                s += "=".repeat((4 - (s.length % 4)) % 4)
                val prefix = String(Base64.decode(s, Base64.URL_SAFE)).trimEnd('/')
                if (prefix.startsWith("http")) return "$prefix/index.mpd"
            }
            val pol = Regex("CloudFront-Policy=([^;]+)").find(cookie)?.groupValues?.get(1)
                ?: return null
            for (raw in listOf(
                pol.replace('-', '+').replace('~', '/').replace('_', '='),
                pol.replace('-', '+').replace('_', '/'),
            )) {
                try {
                    var s = raw
                    s += "=".repeat((4 - (s.length % 4)) % 4)
                    val json = String(Base64.decode(s, Base64.URL_SAFE))
                    val res = JSONObject(json).optJSONArray("Statement")
                        ?.optJSONObject(0)?.optString("Resource")
                    if (!res.isNullOrBlank()) return res
                } catch (_: Exception) {
                }
            }
        } catch (_: Exception) {
        }
        return null
    }

    private suspend fun loadCaptions(sid: String, streamId: String, subs: MutableList<HikariSubtitle>) {
        val seen = HashSet<String>()
        for (u in listOf(
            "$mainUrl/wefeed-mobile-bff/subject-api/get-stream-captions?subjectId=$sid&streamId=$streamId",
            "$mainUrl/wefeed-mobile-bff/subject-api/get-ext-captions?subjectId=$sid&resourceId=$streamId&episode=0",
        )) {
            try {
                val txt = signedGet(u) ?: continue
                val arr = JSONObject(txt).optJSONObject("data")?.optJSONArray("extCaptions") ?: continue
                for (i in 0 until arr.length()) {
                    val c = arr.optJSONObject(i) ?: continue
                    val url = c.optString("url").ifBlank { continue }
                    if (!seen.add(url)) continue
                    subs.add(HikariSubtitle(c.optString("lanName").ifBlank { c.optString("lan", "Unknown") }, url))
                }
            } catch (_: Exception) {
            }
        }
    }
}
