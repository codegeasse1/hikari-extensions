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
 * RaghavAnime Kitsu — Kitsu-ordered variant of the RaghavAnime mega bundle.
 * Ported from KSHITIJ8473/raghav RaghavAnimeKitsu to native Hikari.
 *
 * Same per-source rows as RaghavAnime (AniChan / GoTaku / ReAnime / Senshi)
 * plus a Kitsu.io search source (kitsu.io/api/edge/anime?filter[text]=) so
 * results follow Kitsu popularity ordering. Ids are rk|<source>|<id>;
 * episodes/streams route to the owning source exactly like RaghavAnime.
 */
class RaghavAnimeKitsuProvider : HikariProvider {

    override val id = "raghavanimekitsu"
    override val name = "RaghavAnime Kitsu"
    override val mainUrl = "https://kitsu.io"
    override val description = "Mega anime bundle, Kitsu-ordered — AniChan, GoTaku, ReAnime, Senshi + Kitsu search."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36"
    private fun headers(referer: String) = mapOf(
        "User-Agent" to ua,
        "Accept" to "application/json, text/plain, */*",
        "Referer" to referer,
    )
    private val kitsuHeaders = mapOf(
        "User-Agent" to ua,
        "Accept" to "application/vnd.api+json",
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
        HikariCatalog("kitsu-trending", "Kitsu Trending", HikariMediaType.SERIES),
        HikariCatalog("anichan-trending", "AniChan Trending", HikariMediaType.SERIES),
        HikariCatalog("gotaku-trending", "GoTaku Trending", HikariMediaType.SERIES),
        HikariCatalog("reanime-latest", "ReAnime Latest", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val p = page.coerceAtLeast(1)
        if (catalog.id == "kitsu-trending") return kitsuBrowse("trending", p)
        if (catalog.id == "gotaku-trending") {
            val body = HikariNet.getString("$gotaku/api/v1/titles?sort=trending&page=$p&limit=28", headers(gotaku)) ?: return emptyList()
            return parseGenericArray(body, "gotaku")
        }
        if (catalog.id == "reanime-latest") {
            val body = HikariNet.getString("$reanime/api/v1/home/latest-aired?limit=40&offset=${(p - 1) * 40}", headers(reanime)) ?: return emptyList()
            return parseGenericArray(body, "reanime")
        }
        if (p > 1) return emptyList()
        val body = HikariNet.getString("$anichan/api/catalog/trending?page=1", headers(anichan)) ?: return emptyList()
        return parseGenericArray(body, "anichan")
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank() || page > 1) return emptyList()
        val out = ArrayList<HikariMedia>()
        // Kitsu first (ordering bias), then sub-sources.
        try { out.addAll(kitsuSearch(query.trim())) } catch (t: Throwable) { }
        val q = URLEncoder.encode(query.trim(), "UTF-8")
        try { HikariNet.getString("$anichan/api/suggest?q=$q", headers(anichan))?.let { out.addAll(parseGenericArray(it, "anichan")) } } catch (t: Throwable) { }
        try { HikariNet.getString("$gotaku/api/v1/titles?q=$q&limit=10", headers(gotaku))?.let { out.addAll(parseGenericArray(it, "gotaku")) } } catch (t: Throwable) { }
        try { HikariNet.getString("$reanime/api/v1/search?q=$q&limit=10&offset=0", headers(reanime))?.let { out.addAll(parseGenericArray(it, "reanime")) } } catch (t: Throwable) { }
        return out.distinctBy { it.id }.take(60)
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val clean = media.title.replace(Regex(""" \[(Kitsu|AniChan|GoTaku|ReAnime|Senshi)]$"""), "")
        return media.copy(title = clean.ifBlank { media.title })
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val parts = media.id.split("|")
        if (parts.size < 3 || parts[0] != "rk") return null
        val src = parts[1]
        val sid = parts[2]
        return when (src) {
            "gotaku" -> simpleCount(sid, gotaku, media, "gotaku")
            "reanime" -> simpleCount(sid, reanime, media, "reanime")
            "senshi" -> senshiEpisodes(sid, media)
            "kitsu" -> kitsuEpisodes(sid, media)
            else -> anichanEpisodes(sid, media)
        }
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val ref = episode?.id ?: return emptyList()
        val parts = ref.split("|")
        if (parts.size < 4 || parts[0] != "rkep") return emptyList()
        val src = parts[1]
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        val watchUrls = when (src) {
            "gotaku" -> listOf("$gotaku/titles/${parts[2]}")
            "reanime" -> listOf("$reanime/anime/${parts[2]}")
            "senshi" -> listOf("$senshi/anime/${parts[2]}")
            else -> listOf("$anichan/watch/${parts[2]}/${parts.getOrNull(4) ?: "1"}")
        }
        for (w in watchUrls) {
            val html = HikariNet.getStringSmart(w, pageHeaders) ?: continue
            for (m in Regex("""<iframe[^>]+src="([^"]+)"""").findAll(html)) {
                val s = m.groupValues[1]
                if (!s.startsWith("http")) continue
                for (r in HikariNet.resolveWithWebView(s, capture, timeoutMs = 45_000)) {
                    if (!seen.add(r.url)) continue
                    out.add(HikariStream(name = "RaghavKitsu ${src.uppercase()}", url = r.url, headers = r.headers, isM3u8 = r.url.contains("m3u8")))
                }
                if (out.isNotEmpty()) return out
            }
            for (m in capture.findAll(html)) {
                if (!seen.add(m.value)) continue
                out.add(HikariStream(name = "RaghavKitsu ${src.uppercase()}", url = m.value, isM3u8 = m.value.contains("m3u8")))
            }
            if (out.isNotEmpty()) return out
        }
        return out
    }

    private suspend fun kitsuBrowse(kind: String, page: Int): List<HikariMedia> {
        val offset = (page - 1) * 20
        val sort = if (kind == "trending") "-userCount" else "-averageRating"
        val body = HikariNet.getString("https://kitsu.io/api/edge/anime?page[limit]=20&page[offset]=$offset&sort=$sort", kitsuHeaders) ?: return emptyList()
        return parseKitsu(body)
    }

    private suspend fun kitsuSearch(q: String): List<HikariMedia> {
        val body = HikariNet.getString("https://kitsu.io/api/edge/anime?filter[text]=${URLEncoder.encode(q, "UTF-8")}&page[limit]=10", kitsuHeaders) ?: return emptyList()
        return parseKitsu(body)
    }

    private fun parseKitsu(body: String): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        try {
            val arr = JSONObject(body).optJSONArray("data") ?: return emptyList()
            for (i in 0 until arr.length()) {
                val e = arr.optJSONObject(i) ?: continue
                val id = e.optString("id").ifBlank { continue }
                val attr = e.optJSONObject("attributes") ?: continue
                val title = attr.optJSONObject("titles")?.optString("en")?.takeIf { it.isNotBlank() }
                    ?: attr.optString("canonicalTitle").ifBlank { continue }
                val poster = attr.optJSONObject("posterImage")?.optString("large")?.takeIf { it.startsWith("http") }
                out.add(HikariMedia(id = "rk|kitsu|$id", title = "$title [Kitsu]", type = HikariMediaType.SERIES, posterUrl = poster))
            }
        } catch (t: Throwable) { }
        return out
    }

    private suspend fun kitsuEpisodes(kitsuId: String, media: HikariMedia): List<HikariEpisode>? {
        var count = 0
        try {
            val body = HikariNet.getString("https://kitsu.io/api/edge/anime/$kitsuId", kitsuHeaders)
            count = body?.let { JSONObject(it).optJSONObject("data")?.optJSONObject("attributes")?.optInt("episodeCount", 0) } ?: 0
        } catch (t: Throwable) { }
        if (count <= 0) count = 12
        return (1..count.coerceAtMost(2000)).map { n -> HikariEpisode(number = n, id = "rkep|kitsu|$kitsuId|sub|$n", name = "Episode $n", image = media.posterUrl) }
    }

    private suspend fun anichanEpisodes(sid: String, media: HikariMedia): List<HikariEpisode>? {
        var total = 12
        var dub = false
        try {
            HikariNet.getString("$anichan/api/watch/episodes?anilistId=$sid", headers(anichan))?.let {
                val o = JSONObject(it)
                total = o.optInt("episodes", o.optInt("count", 12))
                dub = o.optBoolean("dub", false)
            }
        } catch (t: Throwable) { }
        val out = ArrayList<HikariEpisode>()
        for (n in 1..total.coerceAtMost(3000)) out.add(HikariEpisode(number = n, id = "rkep|anichan|$sid|sub|$n", name = "Episode $n", image = media.posterUrl))
        if (dub) for (n in 1..total.coerceAtMost(3000)) out.add(HikariEpisode(number = n, id = "rkep|anichan|$sid|dub|$n", name = "Episode $n (Dub)", image = media.posterUrl))
        return out
    }

    private suspend fun simpleCount(sid: String, base: String, media: HikariMedia, src: String): List<HikariEpisode>? {
        // Episode counts need the detail endpoints; keep a bounded list via watch-info style fallback.
        var total = 12
        try {
            val u = if (src == "gotaku") "$base/api/v1/titles/$sid" else "$base/api/v1/anime/$sid"
            HikariNet.getString(u, headers(base))?.let {
                val o = JSONObject(it)
                val d = if (o.has("data")) o.getJSONObject("data") else o
                total = d.optInt("episodes", d.optInt("episodeCount", 12))
            }
        } catch (t: Throwable) { }
        if (total <= 0) total = 12
        return (1..total.coerceAtMost(2000)).flatMap { n ->
            listOf(
                HikariEpisode(number = n, id = "rkep|$src|$sid|sub|$n", name = "Episode $n", image = media.posterUrl),
                HikariEpisode(number = n, id = "rkep|$src|$sid|dub|$n", name = "Episode $n (Dub)", image = media.posterUrl),
            )
        }
    }

    private suspend fun senshiEpisodes(malId: String, media: HikariMedia): List<HikariEpisode>? {
        return try {
            val body = HikariNet.getString("$senshi/episodes/$malId", headers(senshi)) ?: return null
            val arr = org.json.JSONArray(body)
            (0 until arr.length()).mapNotNull { arr.optJSONObject(it) }.sortedBy { it.optInt("ep_id", 0) }
                .filter { it.optInt("ep_id", 0) != 0 }
                .map { e ->
                    val epid = e.optInt("ep_id", 0)
                    val n = e.optInt("number", epid)
                    HikariEpisode(number = n, id = "rkep|senshi|$malId|$epid|sub", name = e.optString("title").takeIf { it.isNotBlank() } ?: "Episode $n", image = media.posterUrl)
                }.ifEmpty { null }
        } catch (t: Throwable) { null }
    }

    private fun parseGenericArray(body: String, src: String): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        try {
            val arr = if (body.trimStart().startsWith("[")) org.json.JSONArray(body)
            else JSONObject(body).let { o -> o.optJSONArray("data") ?: o.optJSONArray("results") ?: o.optJSONArray("items") ?: return emptyList() }
            for (i in 0 until arr.length()) {
                val e = arr.optJSONObject(i) ?: continue
                val rawId = when (src) {
                    "gotaku" -> e.optString("id")
                    "reanime" -> e.optString("slug").ifBlank { e.optString("id") }
                    else -> e.optString("anilistId").ifBlank { e.optString("id") }
                }.ifBlank { continue }
                val title = e.optString("title_english").ifBlank { e.optString("title") }.ifBlank { continue }
                val poster = e.optString("cover").ifBlank { e.optString("poster") }.ifBlank { e.optString("image") }.takeIf { it.startsWith("http") }
                val label = when (src) { "gotaku" -> "GoTaku"; "reanime" -> "ReAnime"; else -> "AniChan" }
                out.add(HikariMedia(id = "rk|$src|$rawId", title = "$title [$label]", type = HikariMediaType.SERIES, posterUrl = poster))
            }
        } catch (t: Throwable) { }
        return out
    }
}
