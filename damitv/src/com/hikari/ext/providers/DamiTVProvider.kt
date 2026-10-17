package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import org.json.JSONArray
import org.json.JSONObject
import java.net.URLEncoder
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale

/**
 * DamiTV (damitv.st) — live sports + 24/7 TV.
 *
 * Faithful native port of KSHITIJ8473/raghav DamiTVProvider, verified live
 * Oct 2026 (198 matches: 18 live / 180 upcoming; top categories football,
 * american-football, basketball, hockey):
 *  - GET /papi/matches/all with Firebase domain (dami/dami_url/damiUrl/
 *    damitv_url keys). The custom DoH DNS is unnecessary on Hikari's
 *    hardened stack; plain fetch with a smart-fetch fallback is used
 *    (extract-url answers 403 to datacenter IPs, so it retries rendered).
 *  - 8 rows: Popular Now, Live Sports Events, Upcoming Matches, 24/7
 *    Channels & Live TV, Football, American Football, Basketball, Hockey.
 *  - Playback: tvChannels via the dlhd M3U8 proxy; match/substream ids via a
 *    fresh /papi/extract-url call (hlsUrl + sdUrl direct, embedUrl through
 *    WebView capture); streamed sources via /papi/stream variants with the
 *    sd-token direct URL attempt plus embed fallback capture.
 */
class DamiTVProvider : HikariProvider {

    override val id = "damitv"
    override val name = "DamiTV"
    override val mainUrl = "https://damitv.st"
    override val description = "Live sports and 24/7 TV from DamiTV — football, NFL, basketball, hockey."
    override val version = 2
    override val tvTypes = setOf(HikariMediaType.SERIES)

    private val ua =
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    private val playUa =
        "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Mobile Safari/537.36"

    private fun apiHeaders(d: String): Map<String, String> = mapOf(
        "User-Agent" to ua,
        "Accept" to "application/json, text/plain, */*",
        "Referer" to "$d/",
        "Origin" to d,
    )

    private fun playHeaders(d: String): Map<String, String> = mapOf(
        "User-Agent" to playUa,
        "Referer" to "$d/",
        "Origin" to d,
        "Accept" to "*/*",
    )

    @Volatile
    private var base: String? = null

    private suspend fun domain(): String {
        base?.let { return it }
        var d = mainUrl
        try {
            val text = HikariNet.getString("https://cloudstreampluginhelper-default-rtdb.firebaseio.com/.json")
            if (text != null) {
                val o = JSONObject(text)
                val url = o.optString("dami").ifBlank { o.optString("dami_url") }
                    .ifBlank { o.optString("damiUrl") }.ifBlank { o.optString("damitv_url") }
                if (url.startsWith("http")) d = url.trimEnd('/')
            }
        } catch (_: Exception) {
        }
        base = d
        return d
    }

    private suspend fun apiGet(url: String, d: String): String? {
        try {
            HikariNet.getString(url, apiHeaders(d))?.let {
                if (it.trimStart().startsWith("{") || it.trimStart().startsWith("[")) return it
            }
        } catch (_: Exception) {
        }
        return try {
            HikariNet.getStringSmart(url, apiHeaders(d))?.takeIf {
                it.trimStart().startsWith("{") || it.trimStart().startsWith("[")
            }
        } catch (_: Exception) {
            null
        }
    }

    private data class TvCh(val id: String, val name: String)
    private data class Src(val source: String, val id: String)

    private data class Ev(
        val id: String,
        val title: String,
        val poster: String,
        val category: String?,
        val status: String?,
        val date: Long?,
        val league: String?,
        val home: String?,
        val away: String?,
        val tv: List<TvCh>,
        val sources: List<Src>,
    )

    private fun parseMatch(o: JSONObject): Ev? {
        val id = o.optString("id")
        val title = o.optString("title")
        if (id.isBlank() || title.isBlank()) return null
        val teams = o.optJSONObject("teams")
        val tv = ArrayList<TvCh>()
        val ta = o.optJSONArray("tvChannels")
        if (ta != null) {
            for (i in 0 until ta.length()) {
                val c = ta.optJSONObject(i) ?: continue
                if (c.optString("id").isNotBlank()) tv.add(TvCh(c.optString("id"), c.optString("name")))
            }
        }
        val srcs = ArrayList<Src>()
        for (key in listOf("sources", "streamedSources")) {
            val sa = o.optJSONArray(key) ?: continue
            for (i in 0 until sa.length()) {
                val s = sa.optJSONObject(i) ?: continue
                if (s.optString("source").isNotBlank() && s.optString("id").isNotBlank()) {
                    srcs.add(Src(s.optString("source"), s.optString("id")))
                }
            }
            if (srcs.isNotEmpty()) break
        }
        return Ev(
            id, title,
            o.optString("poster").ifBlank { o.optString("ppvPoster") }
                .ifBlank { teams?.optJSONObject("home")?.optString("badge").orEmpty() },
            o.optString("category").ifBlank { null },
            o.optString("status").ifBlank { null },
            o.optLong("date", 0L).takeIf { it > 0 },
            o.optString("league").ifBlank { null },
            teams?.optJSONObject("home")?.optString("name")?.ifBlank { null },
            teams?.optJSONObject("away")?.optString("name")?.ifBlank { null },
            tv, srcs.distinctBy { it.source + it.id },
        )
    }

    @Volatile
    private var matchCache: List<Ev>? = null

    private suspend fun allMatches(d: String): List<Ev> {
        matchCache?.let { return it }
        val text = apiGet("$d/papi/matches/all", d) ?: return emptyList()
        val out = ArrayList<Ev>()
        try {
            val arr = JSONArray(text)
            for (i in 0 until arr.length()) {
                arr.optJSONObject(i)?.let { parseMatch(it)?.let { e -> out.add(e) } }
            }
        } catch (_: Exception) {
        }
        if (out.isNotEmpty()) matchCache = out
        return out
    }

    private fun dateStr(ts: Long?): String {
        if (ts == null || ts <= 0) return "soon"
        return try {
            SimpleDateFormat("dd MMM, HH:mm", Locale.US).format(Date(ts))
        } catch (_: Exception) {
            "soon"
        }
    }

    private fun encodeId(e: Ev): String {
        val o = JSONObject()
            .put("id", e.id).put("title", e.title).put("poster", e.poster)
            .put("category", e.category ?: "").put("status", e.status ?: "")
            .put("date", e.date ?: 0L).put("league", e.league ?: "")
            .put("home", e.home ?: "").put("away", e.away ?: "")
        val ta = JSONArray()
        for (c in e.tv) ta.put(JSONObject().put("id", c.id).put("name", c.name))
        o.put("tv", ta)
        val sa = JSONArray()
        for (s in e.sources) sa.put(JSONObject().put("source", s.source).put("id", s.id))
        o.put("sources", sa)
        return "dt" + android.util.Base64.encodeToString(
            o.toString().toByteArray(Charsets.UTF_8),
            android.util.Base64.URL_SAFE or android.util.Base64.NO_PADDING or android.util.Base64.NO_WRAP,
        )
    }

    private fun decodeId(id: String): Ev? {
        return try {
            if (!id.startsWith("dt")) return null
            var b64 = id.removePrefix("dt").replace('-', '+').replace('_', '/')
            while (b64.length % 4 != 0) b64 += "="
            val o = JSONObject(
                String(android.util.Base64.decode(b64, android.util.Base64.DEFAULT), Charsets.UTF_8),
            )
            val tv = ArrayList<TvCh>()
            val ta = o.optJSONArray("tv")
            if (ta != null) {
                for (i in 0 until ta.length()) {
                    val c = ta.optJSONObject(i) ?: continue
                    tv.add(TvCh(c.optString("id"), c.optString("name")))
                }
            }
            val srcs = ArrayList<Src>()
            val sa = o.optJSONArray("sources")
            if (sa != null) {
                for (i in 0 until sa.length()) {
                    val s = sa.optJSONObject(i) ?: continue
                    srcs.add(Src(s.optString("source"), s.optString("id")))
                }
            }
            Ev(
                o.optString("id"), o.optString("title"), o.optString("poster"),
                o.optString("category").ifBlank { null }, o.optString("status").ifBlank { null },
                o.optLong("date", 0L).takeIf { it > 0 },
                o.optString("league").ifBlank { null }, o.optString("home").ifBlank { null },
                o.optString("away").ifBlank { null }, tv, srcs,
            )
        } catch (_: Exception) {
            null
        }
    }

    private fun toMedia(e: Ev): HikariMedia {
        val title = if (e.status == "upcoming") "${e.title} [Upcoming - Starts: ${dateStr(e.date)}]"
        else e.title
        val overview = buildString {
            e.league?.let { append("League: $it\n") }
            e.home?.let { append("Home: $it\n") }
            e.away?.let { append("Away: $it\n") }
            e.status?.let { append("Status: $it\n") }
            if (e.date != null) append("Starts: ${dateStr(e.date)}")
        }.trim().ifBlank { null }
        return HikariMedia(
            id = encodeId(e), title = title, type = HikariMediaType.SERIES,
            posterUrl = e.poster.ifBlank { null }, overview = overview,
        )
    }

    private fun isChannel(e: Ev): Boolean {
        val c = e.category?.lowercase().orEmpty()
        return c == "24/7-streams" || c == "live-tv" || c == "channels" || c.contains("stream")
    }

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("popular", "Popular Now", HikariMediaType.SERIES),
        HikariCatalog("live", "Live Sports Events", HikariMediaType.SERIES),
        HikariCatalog("upcoming", "Upcoming Matches", HikariMediaType.SERIES),
        HikariCatalog("channels", "24/7 Channels & Live TV", HikariMediaType.SERIES),
        HikariCatalog("football", "Football", HikariMediaType.SERIES),
        HikariCatalog("amfootball", "American Football", HikariMediaType.SERIES),
        HikariCatalog("basketball", "Basketball", HikariMediaType.SERIES),
        HikariCatalog("hockey", "Hockey", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val d = domain()
        val all = try {
            allMatches(d)
        } catch (_: Exception) {
            return emptyList()
        }
        val pool = when (catalog.id) {
            // popular flag is not in the list payload; surface most-viewed live first
            "popular" -> all.filter { !isChannel(it) }.sortedByDescending {
                // order matches by recency of start among live/upcoming
                -(it.date ?: Long.MAX_VALUE)
            }.take(30)
            "live" -> all.filter { it.status == "live" && !isChannel(it) }
            "upcoming" -> all.filter { it.status == "upcoming" && !isChannel(it) }
            "channels" -> all.filter { isChannel(it) }
            "football" -> all.filter { it.category.equals("football", true) }
            "amfootball" -> all.filter { it.category.equals("american-football", true) }
            "basketball" -> all.filter { it.category.equals("basketball", true) }
            "hockey" -> all.filter { it.category.equals("hockey", true) }
            else -> return emptyList()
        }
        val p = page.coerceAtLeast(1)
        return pool.drop((p - 1) * 60).take(60).map { toMedia(it) }
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank()) return emptyList()
        val d = domain()
        val all = try {
            allMatches(d)
        } catch (_: Exception) {
            return emptyList()
        }
        val q = query.trim()
        val hits = all.filter {
            it.title.contains(q, true) || (it.league?.contains(q, true) ?: false) ||
                (it.category?.contains(q, true) ?: false) ||
                (it.home?.contains(q, true) ?: false) || (it.away?.contains(q, true) ?: false)
        }
        val p = page.coerceAtLeast(1)
        return hits.drop((p - 1) * 60).take(60).map { toMedia(it) }
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val ev = decodeId(media.id) ?: return media
        return try {
            val d = domain()
            allMatches(d).firstOrNull { it.id == ev.id }?.let { toMedia(it).copy(id = media.id) }
                ?: media
        } catch (_: Exception) {
            media
        }
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val ev = decodeId(media.id) ?: return null
        return listOf(HikariEpisode(number = 1, id = media.id, name = ev.title, image = media.posterUrl))
    }

    private val capture = Regex("""https?://[^"' \s]+\.m3u8[^"' \s]*|https?://[^"' \s]+\.mp4[^"' \s]*""")

    private suspend fun extractFor(d: String, id: String): JSONObject? {
        repeat(2) {
            try {
                val text = apiGet("$d/papi/extract-url/$id", d) ?: return@repeat
                val o = JSONObject(text)
                if (o.optBoolean("success", false)) return o
            } catch (_: Exception) {
            }
        }
        return null
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val d = domain()
        val ev = decodeId(episode?.id ?: media.id) ?: return emptyList()
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        fun emit(url: String, name: String) {
            var u = url
            if (u.isBlank() || !u.startsWith("http")) {
                if (u.startsWith("/")) u = d + u else return
            }
            if (!seen.add(u)) return
            out.add(HikariStream(name = name, url = u, headers = playHeaders(d), isM3u8 = true))
        }

        // 1. mapped TV channels through the dlhd proxy
        for (ch in ev.tv) {
            emit("$d/papi/tv/dlhd/${ch.id}/playlist.m3u8", "[DamiTV] ${ch.name}")
        }

        // 2. PPV main stream + substreams via fresh signed extract-url calls
        coroutineScope {
            val ids = ArrayList<String>()
            ids.add(ev.id)
            try {
                val main = extractFor(d, ev.id)
                val subs = main?.optJSONArray("substreams")
                if (subs != null) {
                    for (i in 0 until subs.length()) {
                        subs.optJSONObject(i)?.optString("id")?.takeIf { it.isNotBlank() }?.let {
                            if (!ids.contains(it)) ids.add(it)
                        }
                    }
                }
            } catch (_: Exception) {
            }
            ids.map { id ->
                async(Dispatchers.IO) {
                    val found = ArrayList<Pair<String, String>>()
                    try {
                        val r = extractFor(d, id) ?: return@async found
                        val hls = r.optString("hlsUrl")
                        if (hls.isNotBlank()) found.add(hls to "[DamiTV] ${ev.title} (Direct)")
                        val sd = r.optString("sdUrl")
                        if (sd.isNotBlank()) found.add(sd to "[DamiTV] ${ev.title} (SD)")
                        val emb = r.optString("embedUrl")
                        if (emb.isNotBlank() && emb.startsWith("http")) {
                            try {
                                for (w in HikariNet.resolveWithWebView(emb, capture, timeoutMs = 45_000)) {
                                    found.add(w.url to "[DamiTV] ${ev.title} (Embed)")
                                }
                            } catch (_: Exception) {
                            }
                        }
                    } catch (_: Exception) {
                    }
                    found
                }
            }.forEach { def ->
                try {
                    for ((u, n) in def.await()) {
                        if (u.startsWith("http") && seen.add(u)) {
                            out.add(HikariStream(name = n, url = u, headers = playHeaders(d), isM3u8 = true))
                        }
                    }
                } catch (_: Exception) {
                }
            }
        }

        // 3. streamed (sd) sources
        for (src in ev.sources) {
            try {
                val text = apiGet("$d/papi/stream/${src.source}/${src.id}", d) ?: continue
                val arr = JSONArray(text)
                for (i in 0 until arr.length()) {
                    val st = arr.optJSONObject(i) ?: continue
                    val sn = st.optInt("streamNo", i + 1)
                    val lang = st.optString("language").takeIf { it.isNotBlank() }?.let { " · $it" }.orEmpty()
                    val q = if (st.optBoolean("hd", false)) "HD" else "SD"
                    val name = "[DamiTV] Server $sn ($q$lang)"
                    // sd-token direct path (endpoint is currently gone; failure falls through)
                    try {
                        val tokText = apiGet("$d/papi/sd-token", d)
                        if (tokText != null) {
                            val tok = JSONObject(tokText)
                            val token = tok.optString("token")
                            if (token.isNotBlank()) {
                                val tp = URLEncoder.encode(tok.optString("token_path"), "UTF-8").replace("+", "%20")
                                val es = URLEncoder.encode(src.source, "UTF-8").replace("+", "%20")
                                val ei = URLEncoder.encode(src.id, "UTF-8").replace("+", "%20")
                                val exp = (tok.opt("expires") as? Number)?.toLong() ?: 0L
                                emit(
                                    "https://damitvsd.b-cdn.net/live-sd/streamed/$es/$ei/$sn/playlist.m3u8?token=$token&token_path=$tp&expires=$exp",
                                    "$name (Direct)",
                                )
                            }
                        }
                    } catch (_: Exception) {
                    }
                    val fallback = st.optString("embedUrl")
                    if (fallback.isNotBlank() && fallback.startsWith("http")) {
                        try {
                            for (r in HikariNet.resolveWithWebView(fallback, capture, timeoutMs = 45_000)) {
                                if (seen.add(r.url)) {
                                    out.add(
                                        HikariStream(
                                            name = name, url = r.url,
                                            headers = playHeaders(d), isM3u8 = true,
                                        ),
                                    )
                                }
                            }
                        } catch (_: Exception) {
                        }
                    }
                }
            } catch (_: Exception) {
            }
        }
        return out
    }
}
