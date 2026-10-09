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
 * RaghavAnime — mega bundle aggregator (native, no bridge).
 * Ported from KSHITIJ8473/raghav RaghavAnime (CloudStream MainAPI bundle with
 * RaghavAniChan / Anikoto / AniSuge / AniWaves / Anidap / Animo / GoTaku /
 * Miruro / ReAnime / Senshi / Xanime / 2DHive / embeds) to ONE native Hikari
 * provider with per-source catalog rows.
 *
 * Search fans out to the plain-JSON sub-sources (AniChan, GoTaku, ReAnime,
 * Senshi) and merges with source-prefixed ids (ra|<source>|<id>); episodes
 * and streams route back to the owning source's API + WebView fallback.
 * (Xanime's AES-GCM GraphQL and the site-scrape sources stay reachable
 * through their own dedicated extensions in this repo.)
 */
class RaghavAnimeProvider : HikariProvider {

    override val id = "raghavanime"
    override val name = "RaghavAnime"
    override val mainUrl = "https://anichan.to"
    override val description = "Mega anime bundle — AniChan, GoTaku, ReAnime & Senshi in one: trending, latest, movies, sub & dub."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36"
    private fun headers(referer: String) = mapOf(
        "User-Agent" to ua,
        "Accept" to "application/json, text/plain, */*",
        "Referer" to referer,
    )
    private val pageHeaders = mapOf(
        "User-Agent" to ua,
        "Accept" to "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    )
    private val capture = Regex("""https?://[^"' \s]+\.m3u8[^"' \s]*|https?://[^"' \s]+\.mp4[^"' \s]*""")

    private val anichan = "https://anichan.to"
    private val gotaku = "https://gotaku.to"
    private val reanime = "https://reanime.to"
    private val senshi = "https://senshi.to"

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("anichan-trending", "AniChan Trending", HikariMediaType.SERIES),
        HikariCatalog("gotaku-trending", "GoTaku Trending", HikariMediaType.SERIES),
        HikariCatalog("reanime-latest", "ReAnime Latest", HikariMediaType.SERIES),
        HikariCatalog("senshi-latest", "Senshi Latest", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val p = page.coerceAtLeast(1)
        return when (catalog.id) {
            "gotaku-trending" -> {
                val body = HikariNet.getString("$gotaku/api/v1/titles?sort=trending&page=$p&limit=28", headers(gotaku)) ?: return emptyList()
                parseGenericArray(body, "gotaku")
            }
            "reanime-latest" -> {
                val body = HikariNet.getString("$reanime/api/v1/home/latest-aired?limit=40&offset=${(p - 1) * 40}", headers(reanime)) ?: return emptyList()
                parseGenericArray(body, "reanime")
            }
            "senshi-latest" -> {
                if (p > 1) return emptyList()
                val body = HikariNet.getString("$senshi/episode-embeds/latest", headers(senshi)) ?: return emptyList()
                parseGenericArray(body, "senshi")
            }
            else -> {
                if (p > 1) return emptyList()
                val body = HikariNet.getString("$anichan/api/catalog/trending?page=1", headers(anichan)) ?: return emptyList()
                parseGenericArray(body, "anichan")
            }
        }
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank() || page > 1) return emptyList()
        val q = URLEncoder.encode(query.trim(), "UTF-8")
        val out = ArrayList<HikariMedia>()
        try {
            HikariNet.getString("$anichan/api/suggest?q=$q", headers(anichan))?.let { out.addAll(parseGenericArray(it, "anichan")) }
        } catch (t: Throwable) { }
        try {
            HikariNet.getString("$gotaku/api/v1/titles?q=$q&limit=15", headers(gotaku))?.let { out.addAll(parseGenericArray(it, "gotaku")) }
        } catch (t: Throwable) { }
        try {
            HikariNet.getString("$reanime/api/v1/search?q=$q&limit=15&offset=0", headers(reanime))?.let { out.addAll(parseGenericArray(it, "reanime")) }
        } catch (t: Throwable) { }
        try {
            HikariNet.postJson("$senshi/filter", JSONObject().put("searchTerm", query.trim()).put("page", 1).put("limit", 15), headers(senshi))?.let { o ->
                o.optJSONArray("data")?.let { arr ->
                    for (i in 0 until arr.length()) {
                        val e = arr.optJSONObject(i) ?: continue
                        val id = e.optInt("malId", e.optInt("id", 0))
                        if (id == 0) continue
                        val title = e.optString("title").ifBlank { continue }
                        out.add(HikariMedia(id = "ra|senshi|$id", title = "$title [Senshi]", type = HikariMediaType.SERIES, posterUrl = e.optString("poster").takeIf { it.startsWith("http") }))
                    }
                }
            }
        } catch (t: Throwable) { }
        return out.distinctBy { it.id }.take(60)
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val (src, sid) = splitRef(media.id) ?: return media
        val clean = media.title.replace(Regex(""" \[(AniChan|GoTaku|ReAnime|Senshi)]$"""), "")
        return media.copy(title = clean.ifBlank { media.title })
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val (src, sid) = splitRef(media.id) ?: return null
        return when (src) {
            "gotaku" -> gotakuEpisodes(sid, media)
            "reanime" -> reanimeEpisodes(sid, media)
            "senshi" -> senshiEpisodes(sid, media)
            else -> anichanEpisodes(sid, media)
        }
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val ref = episode?.id ?: return emptyList()
        val parts = ref.split("|")
        if (parts.size < 4 || parts[0] != "raep") return emptyList()
        val src = parts[1]
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        val watchUrls = when (src) {
            "gotaku" -> listOf("$gotaku/titles/${parts[2]}")
            "reanime" -> listOf("$reanime/anime/${parts[2]}")
            "senshi" -> listOf("$senshi/anime/${parts[2]}")
            else -> listOf("$anichan/watch/${parts[2]}/${parts.getOrNull(4) ?: "1"}", "$anichan/anime/${parts[2]}")
        }
        for (w in watchUrls) {
            val html = HikariNet.getStringSmart(w, pageHeaders) ?: continue
            for (m in Regex("""<iframe[^>]+src="([^"]+)"""").findAll(html)) {
                val s = m.groupValues[1]
                if (!s.startsWith("http")) continue
                for (r in HikariNet.resolveWithWebView(s, capture, timeoutMs = 45_000)) {
                    if (!seen.add(r.url)) continue
                    out.add(HikariStream(name = "RaghavAnime ${src.uppercase()}", url = r.url, headers = r.headers, isM3u8 = r.url.contains("m3u8")))
                }
                if (out.isNotEmpty()) return out
            }
            for (m in capture.findAll(html)) {
                if (!seen.add(m.value)) continue
                out.add(HikariStream(name = "RaghavAnime ${src.uppercase()}", url = m.value, isM3u8 = m.value.contains("m3u8")))
            }
            if (out.isNotEmpty()) return out
        }
        // GoTaku/ReAnime/Senshi JSON stream paths as second chance.
        if (src == "gotaku" && parts.size >= 6) {
            try {
                val emb = HikariNet.getString("$gotaku/api/v1/episodes/${parts[3]}/embed?type=${parts[5]}", headers(gotaku))
                val url = emb?.let { JSONObject(it).let { o -> (if (o.has("data")) o.getJSONObject("data") else o).optString("url") } }
                if (!url.isNullOrBlank()) {
                    val full = if (url.startsWith("http")) url else "$gotaku$url"
                    for (r in HikariNet.resolveWithWebView(full, capture, timeoutMs = 45_000)) {
                        if (!seen.add(r.url)) continue
                        out.add(HikariStream(name = "RaghavAnime GOTAKU", url = r.url, headers = r.headers, isM3u8 = r.url.contains("m3u8")))
                    }
                }
            } catch (t: Throwable) { }
        }
        return out
    }

    // ---- per-source episodes ----

    private suspend fun anichanEpisodes(sid: String, media: HikariMedia): List<HikariEpisode>? {
        var total = 0
        var dub = false
        try {
            HikariNet.getString("$anichan/api/watch/episodes?anilistId=$sid", headers(anichan))?.let {
                val o = JSONObject(it)
                total = o.optInt("episodes", o.optInt("count", 0))
                dub = o.optBoolean("dub", o.optBoolean("hasDub", false))
            }
        } catch (t: Throwable) { }
        if (total <= 0) total = 12
        val out = ArrayList<HikariEpisode>()
        for (n in 1..total.coerceAtMost(3000)) out.add(HikariEpisode(number = n, id = "raep|anichan|$sid|sub|$n", name = "Episode $n", image = media.posterUrl))
        if (dub) for (n in 1..total.coerceAtMost(3000)) out.add(HikariEpisode(number = n, id = "raep|anichan|$sid|dub|$n", name = "Episode $n (Dub)", image = media.posterUrl))
        return out
    }

    private suspend fun gotakuEpisodes(sid: String, media: HikariMedia): List<HikariEpisode>? {
        return try {
            val body = HikariNet.getString("$gotaku/api/v1/titles/$sid/episodes", headers(gotaku)) ?: return null
            val arr = JSONObject(body).optJSONArray("data") ?: return null
            val out = ArrayList<HikariEpisode>()
            for (i in 0 until arr.length()) {
                val e = arr.optJSONObject(i) ?: continue
                val epid = e.optString("id").ifBlank { continue }
                val num = e.optInt("number", 0).takeIf { it > 0 } ?: (i + 1)
                out.add(HikariEpisode(number = num, id = "raep|gotaku|$sid|$epid|hard_sub|$num", name = "Episode $num", image = media.posterUrl))
                out.add(HikariEpisode(number = num, id = "raep|gotaku|$sid|$epid|dub|$num", name = "Episode $num (Dub)", image = media.posterUrl))
            }
            out.sortedBy { it.number }.ifEmpty { null }
        } catch (t: Throwable) { null }
    }

    private suspend fun reanimeEpisodes(slug: String, media: HikariMedia): List<HikariEpisode>? {
        return try {
            val body = HikariNet.getString("$reanime/api/v1/anime/$slug/episodes?limit=2000", headers(reanime)) ?: return null
            val arr = JSONObject(body).optJSONArray("data") ?: return null
            val out = ArrayList<HikariEpisode>()
            for (i in 0 until arr.length()) {
                val e = arr.optJSONObject(i) ?: continue
                val num = e.optInt("episodeNumber", 0).takeIf { it > 0 } ?: continue
                out.add(HikariEpisode(number = num, id = "raep|reanime|$slug|sub|$num", name = "Episode $num", image = media.posterUrl))
                out.add(HikariEpisode(number = num, id = "raep|reanime|$slug|dub|$num", name = "Episode $num (Dub)", image = media.posterUrl))
            }
            out.sortedBy { it.number }.ifEmpty { null }
        } catch (t: Throwable) { null }
    }

    private suspend fun senshiEpisodes(malId: String, media: HikariMedia): List<HikariEpisode>? {
        return try {
            val body = HikariNet.getString("$senshi/episodes/$malId", headers(senshi)) ?: return null
            val arr = org.json.JSONArray(body)
            val sorted = (0 until arr.length()).mapNotNull { arr.optJSONObject(it) }.sortedBy { it.optInt("ep_id", 0) }
            if (sorted.isEmpty()) return null
            val out = ArrayList<HikariEpisode>()
            for (e in sorted) {
                val epid = e.optInt("ep_id", 0)
                if (epid == 0) continue
                val n = e.optInt("number", e.optInt("episode", epid))
                out.add(HikariEpisode(number = n, id = "raep|senshi|$malId|$epid|sub", name = e.optString("title").takeIf { it.isNotBlank() } ?: "Episode $n", image = media.posterUrl))
            }
            out.sortedBy { it.number }.ifEmpty { null }
        } catch (t: Throwable) { null }
    }

    private fun parseGenericArray(body: String, src: String): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        try {
            val arr = if (body.trimStart().startsWith("[")) org.json.JSONArray(body)
            else {
                val o = JSONObject(body)
                o.optJSONArray("data") ?: o.optJSONArray("results") ?: o.optJSONArray("items") ?: return emptyList()
            }
            for (i in 0 until arr.length()) {
                val e = arr.optJSONObject(i) ?: continue
                val rawId = when (src) {
                    "senshi" -> e.optInt("malId", e.optInt("id", 0)).takeIf { it != 0 }?.toString().orEmpty()
                    "gotaku" -> e.optString("id")
                    "reanime" -> e.optString("slug").ifBlank { e.optString("id") }
                    else -> e.optString("anilistId").ifBlank { e.optString("id") }
                }.ifBlank { continue }
                val title = e.optString("title_english").ifBlank { e.optString("title") }.ifBlank { e.optString("name") }.ifBlank { continue }
                val poster = e.optString("cover").ifBlank { e.optString("poster") }.ifBlank { e.optString("image") }.takeIf { it.startsWith("http") }
                val label = when (src) {
                    "anichan" -> "AniChan"; "gotaku" -> "GoTaku"; "reanime" -> "ReAnime"; else -> "Senshi"
                }
                out.add(HikariMedia(id = "ra|$src|$rawId", title = "$title [$label]", type = HikariMediaType.SERIES, posterUrl = poster))
            }
        } catch (t: Throwable) { }
        return out
    }

    private fun splitRef(id: String): Pair<String, String>? {
        val parts = id.split("|")
        if (parts.size < 3 || parts[0] != "ra") return null
        return parts[1] to parts[2]
    }
}
