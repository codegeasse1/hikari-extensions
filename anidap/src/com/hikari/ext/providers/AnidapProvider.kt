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
 * Anidap (anidap.lol) — anime via REST + chad + GraphQL endpoints.
 * Ported from KSHITIJ8473/raghav Anidap to native Hikari.
 *
 *  GET {site}/api/anime/trending            trending row
 *  GET {site}/api/anime/search?q=            search
 *  GET {site}/api/anime/{id}                 detail
 *  GET {graphql}/api/recent?page=            recently added row
 *  GET {chad}/rest/api/episodes?id={slug}    per-episode sub/dub flags
 *  GET {chad}/rest/api/servers?id={slug}&epNum={n}  providers per episode
 * Streams resolve from provider embeds via WebView m3u8 capture.
 */
class AnidapProvider : HikariProvider {

    override val id = "anidap"
    override val name = "Anidap"
    override val mainUrl = "https://anidap.lol"
    override val description = "Anime from anidap.lol — trending, recently added, sub & dub."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val chad = "https://chad.anidap.lol/rest/api"
    private val graphql = "https://graphql.anidap.lol"
    private val apiHeaders = mapOf(
        "User-Agent" to "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36",
        "Accept" to "application/json, text/plain, */*",
        "Referer" to "$mainUrl/",
    )
    private val capture = Regex("""https?://[^"' \s]+\.m3u8[^"' \s]*|https?://[^"' \s]*master\.txt[^"' \s]*|https?://[^"' \s]+\.mp4[^"' \s]*""")

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("trending", "Trending", HikariMediaType.SERIES),
        HikariCatalog("recent", "Recently Added", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val p = page.coerceAtLeast(1)
        if (catalog.id == "recent") {
            val body = HikariNet.getString("$graphql/api/recent?page=$p", apiHeaders) ?: return emptyList()
            return parseListBody(body)
        }
        if (p > 1) return emptyList()
        val body = HikariNet.getString("$mainUrl/api/anime/trending", apiHeaders) ?: return emptyList()
        return parseListBody(body)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank() || page > 1) return emptyList()
        val body = HikariNet.getString("$mainUrl/api/anime/search?q=${URLEncoder.encode(query.trim(), "UTF-8")}", apiHeaders)
            ?: return emptyList()
        return parseListBody(body)
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val slug = media.id.substringAfter("$mainUrl|").substringAfterLast("|").ifBlank { return media }
        val body = HikariNet.getString("$mainUrl/api/anime/$slug", apiHeaders) ?: return media
        return try {
            val o = JSONObject(body)
            val d = if (o.has("data")) o.getJSONObject("data") else o
            val title = d.optString("title").ifBlank { media.title }
            val poster = d.optString("poster").ifBlank { d.optString("image") }.takeIf { it.startsWith("http") } ?: media.posterUrl
            val desc = d.optString("description").takeIf { it.isNotBlank() } ?: media.overview
            media.copy(title = title, posterUrl = poster, backdropUrl = poster, overview = desc)
        } catch (t: Throwable) { media }
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val tail = media.id.substringAfter("$mainUrl|", "")
        if (tail.isBlank()) return null
        val parts = tail.split("|")
        val slug = parts.last()
        val anilistId = parts.firstOrNull().orEmpty()
        // Episode flags from chad.
        var total = 0
        var hasDub = true
        try {
            val body = HikariNet.getString("$chad/episodes?id=${URLEncoder.encode(slug, "UTF-8")}", apiHeaders)
            if (body != null) {
                val arr = if (body.trimStart().startsWith("[")) org.json.JSONArray(body)
                else JSONObject(body).optJSONArray("episodes") ?: JSONObject(body).optJSONArray("data")
                if (arr != null) {
                    total = arr.length()
                    var dubCount = 0
                    for (i in 0 until arr.length()) {
                        val e = arr.optJSONObject(i) ?: continue
                        if (e.optBoolean("hasDub", true)) dubCount++
                    }
                    hasDub = dubCount > 0
                }
            }
        } catch (t: Throwable) { }
        if (total <= 0) {
            try {
                val body = HikariNet.getString("$mainUrl/api/anime/$slug", apiHeaders)
                if (body != null) {
                    val o = JSONObject(body)
                    val d = if (o.has("data")) o.getJSONObject("data") else o
                    total = d.optInt("episodeCount", d.optInt("episodes", 0))
                }
            } catch (t: Throwable) { }
        }
        if (total <= 0) total = 12
        if (total > 3000) total = 3000
        val out = ArrayList<HikariEpisode>()
        for (n in 1..total) out.add(HikariEpisode(number = n, id = "$mainUrl|$anilistId|$slug|$n|sub", name = "Episode $n"))
        if (hasDub) for (n in 1..total) out.add(HikariEpisode(number = n, id = "$mainUrl|$anilistId|$slug|$n|dub", name = "Episode $n (Dub)"))
        return out
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val ref = episode?.id ?: return emptyList()
        val parts = ref.split("|")
        if (parts.size < 5) return emptyList()
        val slug = parts[2]
        val epNum = parts[3]
        val lang = parts[4]
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        // Provider list for this episode; each provider id hints an embed host.
        var providerIds = listOf("adp")
        try {
            val body = HikariNet.getString("$chad/servers?id=${URLEncoder.encode(slug, "UTF-8")}&epNum=$epNum", apiHeaders)
            if (body != null) {
                val o = JSONObject(body)
                val ids = LinkedHashSet<String>()
                ids.add("adp")
                for (key in listOf("sub", "dub", "ssub")) {
                    val arr = o.optJSONArray(key) ?: o.optJSONArray(if (lang == "dub") "dub" else "sub")
                    if (arr != null) for (i in 0 until arr.length()) {
                        val n = arr.opt(i) ?: continue
                        val s = if (n is JSONObject) n.optString("id") else n.toString()
                        if (s.isNotBlank()) ids.add(s)
                    }
                }
                // Also scan raw text for quoted provider ids.
                for (m in Regex(""""id"\s*:\s*"([a-zA-Z0-9_-]{2,30})"""").findAll(body)) ids.add(m.groupValues[1])
                providerIds = ids.toList()
            }
        } catch (t: Throwable) { }
        // Resolve via the site watch page + provider embeds with WebView capture.
        val watchUrls = listOf("$mainUrl/watch/$slug/$epNum", "$mainUrl/anime/$slug")
        for (w in watchUrls) {
            val html = HikariNet.getStringSmart(w, mapOf("User-Agent" to (apiHeaders["User-Agent"] ?: ""), "Referer" to "$mainUrl/")) ?: continue
            val embeds = Regex("""<iframe[^>]+src="([^"]+)"""").findAll(html).map { it.groupValues[1] }.filter { it.startsWith("http") }.toList()
            for (e in embeds) {
                val captured = HikariNet.resolveWithWebView(e, capture, timeoutMs = 45_000)
                for (r in captured) {
                    if (!seen.add(r.url)) continue
                    out.add(HikariStream(name = "Anidap ${lang.uppercase()}", url = r.url, headers = r.headers, isM3u8 = r.url.contains("m3u8") || r.url.contains("master.txt")))
                }
                if (out.isNotEmpty()) return out
            }
            for (m in capture.findAll(html)) {
                if (!seen.add(m.value)) continue
                out.add(HikariStream(name = "Anidap ${lang.uppercase()}", url = m.value, isM3u8 = m.value.contains("m3u8") || m.value.contains("master.txt")))
            }
            if (out.isNotEmpty()) return out
        }
        // Last resort: provider hint URLs are not directly playable; surface nothing rather than dead links.
        return out
    }

    private fun parseListBody(body: String): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        try {
            val arr = if (body.trimStart().startsWith("[")) org.json.JSONArray(body)
            else {
                val o = JSONObject(body)
                o.optJSONArray("data") ?: o.optJSONArray("results") ?: o.optJSONArray("items") ?: return emptyList()
            }
            for (i in 0 until arr.length()) {
                val o = arr.optJSONObject(i) ?: continue
                val id = o.optString("anilistId").ifBlank { o.optString("id") }.ifBlank { continue }
                val slug = o.optString("slug").ifBlank { o.optString("id") }
                val title = o.optString("title").ifBlank { o.optString("name") }.ifBlank { continue }
                val poster = o.optString("poster").ifBlank { o.optString("image") }.ifBlank { o.optString("cover") }.takeIf { it.startsWith("http") }
                out.add(HikariMedia(id = "$mainUrl|$id|$slug", title = title, type = HikariMediaType.SERIES, posterUrl = poster))
            }
        } catch (t: Throwable) { }
        return out
    }
}
