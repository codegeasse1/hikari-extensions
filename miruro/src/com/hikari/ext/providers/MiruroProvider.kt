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
 * Miruro (miruro.com) — anime via AniList GraphQL + Miruro episode pipe.
 * Ported from KSHITIJ8473/raghav Miruro to native Hikari.
 *
 * Search/catalogs use https://graphql.anilist.co (POPULARITY_DESC search);
 * detail pages live at {site}/info/{anilistId}/{slug}; episodes come from
 * the site's episode pipe per provider (sub/ssub/dub lists, best-provider
 * pick with max episode count); embeds resolve via WebView capture.
 */
class MiruroProvider : HikariProvider {

    override val id = "miruro"
    override val name = "Miruro"
    override val mainUrl = "https://www.miruro.com"
    override val description = "Anime from Miruro — AniList catalog, multi-provider sub & dub."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val anilist = "https://graphql.anilist.co"
    private val jsonHeaders = mapOf(
        "User-Agent" to "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36",
        "Accept" to "application/json",
        "Content-Type" to "application/json",
        "Origin" to "https://anilist.co",
        "Referer" to "https://anilist.co/",
    )
    private val pageHeaders = mapOf(
        "User-Agent" to "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36",
        "Accept" to "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer" to "$mainUrl/",
    )
    private val capture = Regex("""https?://[^"' \s]+\.m3u8[^"' \s]*|https?://[^"' \s]+\.mp4[^"' \s]*""")

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("trending", "Trending", HikariMediaType.SERIES),
        HikariCatalog("popular", "All Time Popular", HikariMediaType.SERIES),
        HikariCatalog("top", "Top Rated", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val sort = when (catalog.id) {
            "popular" -> "POPULARITY_DESC"
            "top" -> "SCORE_DESC"
            else -> "TRENDING_DESC"
        }
        return anilistPage(null, page.coerceAtLeast(1), sort)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank()) return emptyList()
        return anilistPage(query.trim(), page.coerceAtLeast(1), "POPULARITY_DESC")
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val anilistId = media.id.substringAfter("miruro:", media.id).split("|")[0].filter { it.isDigit() }.ifBlank {
            Regex("""/info/(\d+)""").find(media.id)?.groupValues?.get(1).orEmpty()
        }
        if (anilistId.isBlank()) return media
        val html = HikariNet.getStringSmart("$mainUrl/info/$anilistId", pageHeaders)
        if (html != null) {
            val title = Regex("""<meta[^>]+property="og:title"[^>]+content="([^"]+)"""").find(html)?.groupValues?.get(1) ?: media.title
            val poster = Regex("""<meta[^>]+property="og:image"[^>]+content="([^"]+)"""").find(html)?.groupValues?.get(1)?.takeIf { it.startsWith("http") } ?: media.posterUrl
            return media.copy(title = title, posterUrl = poster, backdropUrl = poster)
        }
        return media
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val anilistId = media.id.substringAfter("miruro:", media.id).split("|")[0].filter { it.isDigit() }.ifBlank {
            Regex("""/info/(\d+)""").find(media.id)?.groupValues?.get(1).orEmpty()
        }.ifBlank { return null }
        // Episode pipe: try known endpoints, pick provider with most episodes.
        val bodies = listOf(
            "$mainUrl/api/episodes?anilistId=$anilistId",
            "$mainUrl/api/anime/$anilistId/episodes",
        ).mapNotNull { u -> try { HikariNet.getString(u, pageHeaders) } catch (t: Throwable) { null } }
        var bestSub: List<JSONObject> = emptyList()
        var bestDub: List<JSONObject> = emptyList()
        for (b in bodies) {
            try {
                val o = JSONObject(b)
                val providers = o.optJSONObject("providers") ?: continue
                for (key in providers.keys()) {
                    val p = providers.optJSONObject(key) ?: continue
                    val eps = p.optJSONObject("episodes") ?: continue
                    val sub = (eps.optJSONArray("sub") ?: eps.optJSONArray("ssub"))?.let { arr ->
                        buildList { for (i in 0 until arr.length()) arr.optJSONObject(i)?.let { add(it) } }
                    }.orEmpty()
                    val dub = eps.optJSONArray("dub")?.let { arr ->
                        buildList { for (i in 0 until arr.length()) arr.optJSONObject(i)?.let { add(it) } }
                    }.orEmpty()
                    if (sub.size > bestSub.size) bestSub = sub
                    if (dub.size > bestDub.size) bestDub = dub
                }
            } catch (t: Throwable) { }
        }
        if (bestSub.isEmpty() && bestDub.isEmpty()) {
            // Fallback: episode count from AniList, plain numbered list.
            val count = anilistEpisodeCount(anilistId)
            if (count <= 0) return null
            val out = ArrayList<HikariEpisode>()
            for (n in 1..count.coerceAtMost(2000)) {
                out.add(HikariEpisode(number = n, id = "miruro:$anilistId|sub|$n", name = "Episode $n", image = media.posterUrl))
                out.add(HikariEpisode(number = n, id = "miruro:$anilistId|dub|$n", name = "Episode $n (Dub)", image = media.posterUrl))
            }
            return out
        }
        val out = ArrayList<HikariEpisode>()
        for ((idx, e) in bestSub.withIndex()) {
            val n = e.optInt("number", e.optInt("episode", idx + 1))
            out.add(HikariEpisode(number = n, id = "miruro:$anilistId|sub|$n", name = e.optString("title").takeIf { it.isNotBlank() } ?: "Episode $n", image = media.posterUrl))
        }
        for ((idx, e) in bestDub.withIndex()) {
            val n = e.optInt("number", e.optInt("episode", idx + 1))
            out.add(HikariEpisode(number = n, id = "miruro:$anilistId|dub|$n", name = (e.optString("title").takeIf { it.isNotBlank() } ?: "Episode $n") + " (Dub)", image = media.posterUrl))
        }
        return out.sortedBy { it.number }
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val ref = episode?.id ?: return emptyList()
        val parts = ref.substringAfter("miruro:").split("|")
        if (parts.size < 3) return emptyList()
        val anilistId = parts[0]
        val lang = parts[1]
        val ep = parts[2]
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        for (w in listOf("$mainUrl/watch/$anilistId/$ep", "$mainUrl/info/$anilistId")) {
            val html = HikariNet.getStringSmart(w, pageHeaders) ?: continue
            for (m in Regex("""<iframe[^>]+src="([^"]+)"""").findAll(html)) {
                val src = m.groupValues[1]
                if (!src.startsWith("http")) continue
                for (r in HikariNet.resolveWithWebView(src, capture, timeoutMs = 60_000)) {
                    if (!seen.add(r.url)) continue
                    out.add(HikariStream(name = "Miruro ${lang.uppercase()}", url = r.url, headers = r.headers, isM3u8 = r.url.contains("m3u8")))
                }
                if (out.isNotEmpty()) return out
            }
            for (m in capture.findAll(html)) {
                if (!seen.add(m.value)) continue
                out.add(HikariStream(name = "Miruro ${lang.uppercase()}", url = m.value, isM3u8 = m.value.contains("m3u8")))
            }
            if (out.isNotEmpty()) return out
        }
        return out
    }

    private suspend fun anilistPage(search: String?, page: Int, sort: String): List<HikariMedia> {
        val query = """
            query (${"$"}search: String, ${"$"}page: Int, ${"$"}perPage: Int) {
              Page(page: ${"$"}page, perPage: ${"$"}perPage) {
                media(type: ANIME, search: ${"$"}search, sort: $sort) {
                  id title { romaji english } coverImage { large } averageScore genres format episodes
                }
              }
            }
        """.trimIndent()
        val vars = JSONObject().put("page", page).put("perPage", 30)
        if (search != null) vars.put("search", search)
        val body = JSONObject().put("query", query).put("variables", vars)
        return try {
            val res = HikariNet.postJson(anilist, body, jsonHeaders) ?: return emptyList()
            val arr = res.optJSONObject("data")?.optJSONObject("Page")?.optJSONArray("media") ?: return emptyList()
            val out = ArrayList<HikariMedia>()
            for (i in 0 until arr.length()) {
                val m = arr.optJSONObject(i) ?: continue
                val id = m.optInt("id", 0)
                if (id == 0) continue
                val t = m.optJSONObject("title")
                val title = t?.optString("english")?.takeIf { it.isNotBlank() } ?: t?.optString("romaji") ?: continue
                val poster = m.optJSONObject("coverImage")?.optString("large")?.takeIf { it.startsWith("http") }
                out.add(HikariMedia(id = "miruro:$id", title = title, type = if (m.optString("format").equals("MOVIE", true)) HikariMediaType.MOVIE else HikariMediaType.SERIES, posterUrl = poster))
            }
            out
        } catch (t: Throwable) { emptyList() }
    }

    private suspend fun anilistEpisodeCount(anilistId: String): Int {
        return try {
            val q = "query (${"$"}id: Int) { Media(id: ${"$"}id, type: ANIME) { episodes } }"
            val body = JSONObject().put("query", q).put("variables", JSONObject().put("id", anilistId.toIntOrNull() ?: return 0))
            HikariNet.postJson(anilist, body, jsonHeaders)?.optJSONObject("data")?.optJSONObject("Media")?.optInt("episodes", 0) ?: 0
        } catch (t: Throwable) { 0 }
    }
}
