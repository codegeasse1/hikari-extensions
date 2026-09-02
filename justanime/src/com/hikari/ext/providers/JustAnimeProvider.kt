package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import com.hikari.ext.HikariSubtitle
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.json.JSONArray
import org.json.JSONObject
import java.net.URLEncoder

/**
 * JustAnime (justanime.to) — anime streaming with a clean JSON API behind
 * `core.justanime.to/api`. All calls need `Origin`/`Referer` headers for
 * justanime.to (the API 403s "Origin unknown" without them).
 *
 *  - Home rows come from `/home` (trending, popular, latestEpisode,
 *    latestCompleted, airing, upcoming, favourite — the app also keys by
 *    these names),
 *  - detail = `/anime/{id}`, episodes = `/anime/{id}/episodes?page=N`,
 *  - search = `/search?query=…&type=video&page=…&limit=…` → `results`,
 *  - playback: `/watch/{animeId}/episode/{n}/anineko/sub/hd1` (server in
 *    {anineko, animegg, megaplay}, lang in {sub, dub, hsub}) returns
 *    `{slug, server, lang, sources[], headers, subtitles}` — or, on some
 *    servers, an object keyed by audioType with `sub`/`dub` entries carrying
 *    `sources` + `headers` + `tracks`. Sources are always played through the
 *    site's own proxies (the CDNs hotlink-gate segments): each server's proxy
 *    (neko/gg/momo/as/lok.justanime.to) plus the generic open fallbacks, so a
 *    dead server proxy (e.g. neko) can't break playback.
 */
class JustAnimeProvider : HikariProvider {

    override val id = "justanime"
    override val name = "JustAnime"
    override val mainUrl = "https://justanime.to"
    override val description = "Anime from justanime.to — trending, popular, airing and upcoming rows, full episode lists, sub & dub, direct HLS via the site's own proxies."
    override val version = 3
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    companion object {
        private const val API = "https://core.justanime.to/api"
        private const val CACHE_TTL_MS = 600_000L

        private val apiHeaders = mapOf(
            "User-Agent" to HikariNet.browserHeaders["User-Agent"].orEmpty(),
            "Accept" to "application/json, text/plain, */*",
            "Referer" to "https://justanime.to/",
            "Origin" to "https://justanime.to",
        )

        private val jsonCache = HashMap<String, Pair<Long, String>>()
        private val cacheMutex = Mutex()

        // API `rawType` section key → home row.
        private val homeRows = listOf(
            "trending" to "Trending",
            "popular" to "Popular",
            "latestEpisode" to "Latest Episodes",
            "latestCompleted" to "Recently Completed",
            "airing" to "Airing Now",
            "upcoming" to "Upcoming",
            "favourite" to "Favourites",
        )

        // (server key, its own proxy base — the site's player routes every
        // stream through one of these. Some die (neko for anineko has been
        // down for a while), which is why the site also keeps generic
        // fallback proxies that relay any CDN URL.)
        private val serverProxies = mapOf(
            "anineko" to "https://neko.justanime.to/m3u8-proxy?url=",
            "animegg" to "https://gg.justanime.to/proxy?url=",
            "megaplay" to "https://momo.justanime.to/proxy?url=",
            "anisnatch" to "https://as.justanime.to/m3u8-proxy?url=",
            "animelok" to "https://lok.justanime.to/proxy?url=",
        )

        // Generic proxies verified reachable and open (momo/as relay any URL;
        // gg has a CDN allowlist). Listed before the per-server hosts so
        // playback survives a dead server proxy.
        private val fallbackProxies = listOf(
            "https://momo.justanime.to/proxy?url=",
            "https://as.justanime.to/m3u8-proxy?url=",
            "https://gg.justanime.to/proxy?url=",
        )

        // Headers for the request to the proxy hosts themselves.
        private val justHeaders = mapOf(
            "Referer" to "https://justanime.to/",
            "Origin" to "https://justanime.to",
        )

        private val langs = listOf("sub", "dub", "hsub")
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        // Movie-style titles have no episode list.
        if (media.type == HikariMediaType.MOVIE) return null
        val id = media.id.trim()
        if (id.isBlank() || !id.all { it.isDigit() }) return null
        val episodes = ArrayList<HikariEpisode>()
        var page = 1
        while (page <= 15) {
            val json = getJsonCached("$API/anime/$id/episodes?page=$page") ?: break
            val arr = json.optJSONArray("episodes") ?: break
            if (arr.length() == 0) break
            for (i in 0 until arr.length()) {
                val e = arr.optJSONObject(i) ?: continue
                val number = e.optInt("number", 0)
                if (number <= 0) continue
                episodes.add(
                    HikariEpisode(
                        number = number,
                        id = "$id|$number",
                        name = e.optString("title").takeIf { it.isNotBlank() && it != "null" } ?: "Episode $number",
                        image = e.optString("image").takeIf { it.isNotBlank() },
                    )
                )
            }
            if (!json.optBoolean("hasNextPage", false)) break
            page++
        }
        return episodes
    }

    override fun catalogs(): List<HikariCatalog> = homeRows.map { (key, label) ->
        HikariCatalog(key, label, HikariMediaType.SERIES, rawType = key)
    }

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        if (page > 1) return emptyList()
        val key = catalog.rawType.ifBlank { catalog.id }
        val json = getJsonCached("$API/home") ?: return emptyList()
        val arr = json.optJSONArray(key) ?: return emptyList()
        val out = ArrayList<HikariMedia>()
        for (i in 0 until arr.length()) {
            arr.optJSONObject(i)?.toMedia()?.let { out.add(it) }
        }
        return out
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        val apiPage = (page - 1).coerceAtLeast(0)
        val json = getJsonCached(
            "$API/search?query=${encode(q)}&type=video&page=$apiPage&limit=20",
            maxAgeMs = 30_000L,
        ) ?: return emptyList()
        val arr = json.optJSONArray("results") ?: return emptyList()
        val out = ArrayList<HikariMedia>()
        for (i in 0 until arr.length()) {
            arr.optJSONObject(i)?.toMedia()?.let { out.add(it) }
        }
        return out
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val json = getJsonCached("$API/anime/${media.id}") ?: return media
        val d = json.optJSONObject("data") ?: json
        val title = d.title()
            ?.takeIf { it.isNotBlank() && it != "null" } ?: media.title
        val poster = d.coverImagePoster()
            ?.takeIf { it.startsWith("http") } ?: media.posterUrl
        val plot = d.optString("description")
            .replace(Regex("<[^>]+>"), " ")
            .replace(Regex("\\s+"), " ")
            .trim()
            .takeIf { it.isNotBlank() }
        val genres = d.optJSONArray("genres")?.let { arr ->
            buildList {
                for (i in 0 until arr.length()) {
                    arr.optString(i).takeIf { it.isNotBlank() }?.let { add(it) }
                }
            }
        } ?: emptyList()
        val year = d.optInt("seasonYear", 0).takeIf { it > 0 } ?: d.optInt("year", 0).takeIf { it > 0 }
        val type = mediaTypeOf(d.optString("format").ifBlank { d.optString("type") })
        return media.copy(
            title = title,
            posterUrl = poster,
            type = type,
            year = year ?: media.year,
            overview = plot,
            genres = if (genres.isNotEmpty()) genres else media.genres,
        )
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val animeId = media.id.trim()
        val epNum = episode?.id?.substringAfter('|', "")?.trim()
            ?.takeIf { it.isNotBlank() }
            ?: if (media.type == HikariMediaType.MOVIE) "1" else return emptyList()
        if (animeId.isBlank() || epNum.isBlank()) return emptyList()

        val streams = ArrayList<HikariStream>()
        for (lang in langs) {
            if (streams.size >= 12) break
            for (server in serverProxies.keys) {
                val result = fetchWatch("$API/watch/$animeId/episode/$epNum/$server/$lang/hd1", server)
                    ?: fetchWatch("$API/watch/$animeId/episode/$epNum/$server/$lang", server)
                if (result.isNullOrEmpty()) continue
                streams.addAll(result)
                break // first server that has this language wins
            }
        }
        return streams.distinctBy { it.name + it.url }
    }

    // ------------------------------------------------------------------
    //  Parsing helpers
    // ------------------------------------------------------------------

    /** Parses a `/watch/…` response into playable streams (both response shapes). */
    private suspend fun fetchWatch(url: String, server: String): List<HikariStream>? {
        val json = getJsonCached(url, maxAgeMs = 30_000L) ?: return null
        // Shape B: keyed by audioType ("sub"/"dub"/"hsub").
        for (lang in langs) {
            json.optJSONObject(lang)?.let { return parseWatchResult(lang, it, server) }
        }
        // Shape A: flat {slug, server, lang, sources, headers, subtitles}.
        if (json.has("sources") || json.has("headers")) return parseWatchResult("sub", json, server)
        return null
    }

    private fun parseWatchResult(lang: String, body: JSONObject, server: String): List<HikariStream>? {
        val sources = body.optJSONArray("sources") ?: return null
        if (sources.length() == 0) return null
        val headers = buildMap {
            put("User-Agent", HikariNet.browserHeaders["User-Agent"].orEmpty())
            body.optJSONObject("headers")?.let { h ->
                h.optString("Referer").takeIf { it.isNotBlank() }?.let { put("Referer", it) }
                h.optString("Origin").takeIf { it.isNotBlank() }?.let { put("Origin", it) }
            }
        }
        val headerJson = JSONObject(headers).toString()
        val subtitles = parseSubtitles(body.opt("tracks")) ?: parseSubtitles(body.opt("subtitles"))

        // Every source is exposed through each proxy in the pool — the site
        // never serves the CDN directly (it hotlink-gates segments, which is
        // what makes a bare URL play a few seconds and then die). The generic
        // open proxies come first so a dead server proxy doesn't break
        // playback; the bare CDN is listed last as "Direct" as a last resort.
        val pool = LinkedHashSet<String>()
        fallbackProxies.forEach { pool.add(it) }
        serverProxies[server]?.let { pool.add(it) }

        val out = ArrayList<HikariStream>()
        for (i in 0 until sources.length()) {
            val s = sources.optJSONObject(i) ?: continue
            val rawUrl = s.optString("url").trim()
            if (rawUrl.isBlank()) continue
            val isM3u8 = s.optBoolean("isM3U8", rawUrl.contains(".m3u8", true))
            val quality = s.optString("quality").takeIf { it.isNotBlank() } ?: "Auto"
            val base = "$lang · $quality"
            var n = 0
            for (proxy in pool) {
                n++
                out.add(
                    HikariStream(
                        name = if (n == 1) "$base · Proxy" else "$base · Proxy $n",
                        url = proxy + encode(rawUrl) + "&headers=" + encode(headerJson),
                        headers = justHeaders,
                        subtitles = subtitles,
                        isM3u8 = isM3u8,
                    )
                )
            }
            out.add(
                HikariStream(
                    name = "$base · Direct",
                    url = rawUrl,
                    headers = headers,
                    subtitles = subtitles,
                    isM3u8 = isM3u8,
                )
            )
        }
        return out
    }

    private fun parseSubtitles(raw: Any?): List<HikariSubtitle> {
        if (raw == null || raw == JSONObject.NULL) return emptyList()
        val arr: JSONArray = when (raw) {
            is JSONArray -> raw
            is JSONObject -> JSONArray().put(raw)
            is String -> runCatching { JSONArray(raw) }.getOrNull() ?: return emptyList()
            else -> return emptyList()
        }
        val out = ArrayList<HikariSubtitle>()
        for (i in 0 until arr.length()) {
            val t = arr.optJSONObject(i) ?: continue
            val url = t.optString("url").takeIf { it.isNotBlank() }
                ?: t.optString("file").takeIf { it.isNotBlank() }
            if (url.isNullOrBlank()) continue
            val lang = t.optString("label").takeIf { it.isNotBlank() }
                ?: t.optString("lang").takeIf { it.isNotBlank() }
                ?: t.optString("language").takeIf { it.isNotBlank() }
                ?: "Subtitle"
            out.add(HikariSubtitle(lang, url))
        }
        return out
    }

    private fun JSONObject.toMedia(): HikariMedia? {
        val id = optString("id").takeIf { it.isNotBlank() && it != "null" } ?: return null
        val title = title()?.takeIf { it.isNotBlank() && it != "null" } ?: return null
        val poster = optString("cover").takeIf { it.startsWith("http") }
            ?: optString("bannerImage").takeIf { it.startsWith("http") }
        val type = mediaTypeOf(optString("type").ifBlank { optString("format") })
        val year = optInt("year", 0).takeIf { it > 0 }
            ?: optInt("seasonYear", 0).takeIf { it > 0 }
        return HikariMedia(
            id = id,
            title = title,
            type = type,
            posterUrl = poster,
            year = year,
        )
    }

    private fun JSONObject.title(): String? {
        val t = optJSONObject("title") ?: return optString("title").takeIf { it.isNotBlank() && it != "null" }
        return t.optString("english").takeIf { it.isNotBlank() && it != "null" }
            ?: t.optString("romaji").takeIf { it.isNotBlank() && it != "null" }
            ?: t.optString("native").takeIf { it.isNotBlank() && it != "null" }
    }

    private fun JSONObject.coverImagePoster(): String? {
        val c = optJSONObject("coverImage") ?: return null
        return c.optString("extraLarge").takeIf { it.isNotBlank() }
            ?: c.optString("large").takeIf { it.isNotBlank() }
            ?: optString("bannerImage").takeIf { it.isNotBlank() }
    }

    private fun mediaTypeOf(type: String): HikariMediaType =
        if (type.equals("MOVIE", true)) HikariMediaType.MOVIE else HikariMediaType.SERIES

    private fun encode(s: String): String =
        URLEncoder.encode(s, "UTF-8").replace("+", "%20")

    /** Fetches a JSON API response once per TTL (searches/streams use a short TTL). */
    private suspend fun getJsonCached(url: String, maxAgeMs: Long = CACHE_TTL_MS): JSONObject? {
        val now = System.currentTimeMillis()
        cacheMutex.withLock {
            jsonCache[url]?.let { (t, body) ->
                if (now - t < maxAgeMs) {
                    return runCatching { JSONObject(body) }.getOrNull()
                }
            }
        }
        val body = HikariNet.getString(url, apiHeaders) ?: return null
        val parsed = runCatching { JSONObject(body) }.getOrNull() ?: return null
        cacheMutex.withLock {
            if (jsonCache.size > 60) jsonCache.clear()
            jsonCache[url] = now to body
        }
        return parsed
    }
}
