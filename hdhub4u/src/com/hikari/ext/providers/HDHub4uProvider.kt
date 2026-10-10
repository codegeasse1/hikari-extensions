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
 * HDHub4u (hdhub4u.glass) — Bollywood / Hollywood / South movies & web series.
 * The site shell is JS-rendered, so catalogs come from TMDB (same metadata the
 * plugin itself uses), verified pattern Oct 2026:
 *  - Rows: /trending/movie, /movie/popular, /movie/top_rated, /trending/tv,
 *    /tv/popular, /tv/top_rated (dual API keys, same as the JustPlay port).
 *  - TMDB id -> imdb_id via /movie|tv/<id>/external_ids, then the site's
 *    Typesense index (search.pingora.fyi) resolves the HDHub4u detail page,
 *    whose hubcloud.php / VidStack / PixelDrain / StreamTape links play.
 *  - Detail pages are fetched with the rendered-page fallback when needed.
 */
class HDHub4uProvider : HikariProvider {

    override val id = "hdhub4u"
    override val name = "HDHub4u"
    override val mainUrl = "https://hdhub4u.glass"
    override val description = "Bollywood, Hollywood & South movies and web series in HD — trending, popular, top rated."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val UA =
        "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36"

    companion object {
        const val TMDB = "https://api.themoviedb.org/3"
        const val TMDB_KEY = "1865f43a0549ca50d341dd9ab8b29f49"
        const val TMDB_KEY_ALT = "98ae14df2b8d8f8f8136499daf79f0e0"
        const val IMG = "https://image.tmdb.org/t/p/w500"
        const val TYPESENSE = "https://search.pingora.fyi/collections/post/documents/search?q="
        var base = "https://hdhub4u.glass"
    }

    private fun jh(): Map<String, String> = mapOf("User-Agent" to UA, "Accept" to "application/json")

    private fun tmdb(path: String, query: String): JSONObject? {
        val p = if (path.startsWith("/")) path else "/$path"
        HikariNet.getString("$TMDB$p?api_key=$TMDB_KEY&$query", jh())?.let {
            try { return JSONObject(it) } catch (t: Throwable) { }
        }
        HikariNet.getString("$TMDB$p?api_key=$TMDB_KEY_ALT&$query", jh())?.let {
            try { return JSONObject(it) } catch (t: Throwable) { }
        }
        return null
    }

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("trendmov", "Trending Movies", HikariMediaType.MOVIE),
        HikariCatalog("popmov", "Popular Movies", HikariMediaType.MOVIE),
        HikariCatalog("topmov", "Top Rated Movies", HikariMediaType.MOVIE),
        HikariCatalog("trendtv", "Trending Series", HikariMediaType.SERIES),
        HikariCatalog("poptv", "Popular Series", HikariMediaType.SERIES),
        HikariCatalog("toptv", "Top Rated Series", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val p = page.coerceAtLeast(1)
        val (path, movie) = when (catalog.id) {
            "trendmov" -> "/trending/movie/day?page=$p" to true
            "popmov" -> "/movie/popular?page=$p" to true
            "topmov" -> "/movie/top_rated?page=$p" to true
            "trendtv" -> "/trending/tv/day?page=$p" to false
            "poptv" -> "/tv/popular?page=$p" to false
            else -> "/tv/top_rated?page=$p" to false
        }
        val root = tmdb(path, "language=en-US") ?: return emptyList()
        return parseTmdb(root.optJSONArray("results"), movie)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank() || page > 1) return emptyList()
        val out = ArrayList<HikariMedia>()
        val seen = HashSet<String>()
        for ((path, movie) in listOf("/search/movie" to true, "/search/tv" to false)) {
            val root = tmdb(path, "query=${URLEncoder.encode(q, "UTF-8")}&page=1&include_adult=false&language=en-US") ?: continue
            for (m in parseTmdb(root.optJSONArray("results"), movie)) {
                if (seen.add(m.id)) out.add(m)
            }
            if (out.size >= 30) break
        }
        // Site index best-effort (may be Cloudflare-walled from some networks).
        try {
            val ts = HikariNet.getJson(TYPESENSE + URLEncoder.encode(q, "UTF-8"), jh())
            val hits = ts?.optJSONArray("hits")
            if (hits != null) {
                for (i in 0 until hits.length()) {
                    val d = hits.optJSONObject(i)?.optJSONObject("document") ?: continue
                    val url = docUrl(d) ?: continue
                    val title = d.optString("title").ifBlank { d.optString("name") }.trim()
                    if (title.isBlank() || !seen.add("ts:$url")) continue
                    out.add(HikariMedia(id = "hdhub4u-ts:$url", title = title, type = HikariMediaType.MOVIE))
                }
            }
        } catch (t: Throwable) { }
        return out
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        if (media.id.startsWith("hdhub4u-ts:")) {
            val html = HikariNet.getString(media.id.removePrefix("hdhub4u-ts:"), mapOf("User-Agent" to UA)) ?: return media
            val ogImage = Regex("<meta[^>]+property=\"og:image\"[^>]+content=\"([^\"]+)\"").find(html)?.groupValues?.get(1)
            return media.copy(posterUrl = ogImage?.takeIf { it.startsWith("http") } ?: media.posterUrl)
        }
        return media
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        if (media.type == HikariMediaType.MOVIE || media.id.startsWith("hdhub4u-ts:")) {
            return listOf(HikariEpisode(number = 1, id = media.id, name = media.title, image = media.posterUrl))
        }
        // TV: TMDB season map (1..N) — detail resolution happens per episode.
        val kind = if (media.rawType == "tv") "tv" else "movie"
        val tmdbId = media.id.substringAfter("tmdb:", "").substringBefore(":")
        if (kind == "tv" && tmdbId.isNotEmpty()) {
            val root = tmdb("/tv/$tmdbId", "language=en-US") ?: return null
            val seasons = root.optJSONArray("seasons")
            if (seasons != null) {
                val out = ArrayList<HikariEpisode>()
                for (i in 0 until seasons.length()) {
                    val s = seasons.optJSONObject(i) ?: continue
                    val sn = s.optInt("season_number", -1)
                    val count = s.optInt("episode_count", 0)
                    if (sn <= 0 || count <= 0) continue
                    for (ep in 1..count) {
                        out.add(HikariEpisode(number = ep, season = sn, id = "${media.id}#s${sn}e$ep", name = "S${sn} E$ep"))
                    }
                }
                if (out.isNotEmpty()) return out
            }
        }
        return null
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        val detail = resolveDetailUrl(media)
        if (detail != null) {
            val html = HikariNet.getString(detail, mapOf("User-Agent" to UA, "Referer" to "$base/"))
            if (html != null) {
                // HubCloud links.
                for (m in Regex("<a[^>]+href=\"([^\"]*hubcloud\\.php[^\"]*)\"").findAll(html)) {
                    val u = absUrl(m.groupValues[1], detail)
                    if (u.startsWith("http") && seen.add(u)) {
                        resolveHubLink(u, detail, out, seen)
                    }
                }
                for (m in Regex("https?://[^\\s\"'<>]*hubcloud[^\\s\"'<>]*").findAll(html)) {
                    val u = m.value
                    if (u.contains("/drive/") && seen.add(u)) resolveHubLink(u, detail, out, seen)
                }
                // Direct file hosts.
                for (m in Regex("https?://[^\\s\"'<>\\\\]+\\.(?:m3u8|mp4|mkv)[^\\s\"'<>\\\\]*").findAll(html)) {
                    val u = m.value
                    if (u.startsWith("http") && seen.add(u)) {
                        out.add(HikariStream(name = "HDHub4u", url = u, headers = mapOf("User-Agent" to UA, "Referer" to detail), isM3u8 = u.contains(".m3u8")))
                    }
                }
            }
        }
        if (out.isEmpty() && detail != null) {
            // Rendered-page fallback for JS-mounted players.
            try {
                val hits = HikariNet.resolveWithWebView(
                    detail,
                    Regex("https?://[^\"'\\s]+?\\.(?:m3u8|mp4)(?:[?#][^\"'\\s]*)?"),
                    timeoutMs = 45_000,
                )
                for (hit in hits) {
                    if (hit.url.isBlank() || !seen.add(hit.url)) continue
                    out.add(HikariStream(name = "HDHub4u", url = hit.url, headers = hit.headers, isM3u8 = hit.url.contains(".m3u8", true)))
                }
            } catch (t: Throwable) { }
        }
        return out
    }

    // ------------------------------------------------------------------
    // Helpers.
    // ------------------------------------------------------------------

    private fun parseTmdb(arr: org.json.JSONArray?, movie: Boolean): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        if (arr == null) return out
        for (i in 0 until arr.length()) {
            val o = arr.optJSONObject(i) ?: continue
            val id = o.optInt("id", 0)
            if (id == 0) continue
            val title = (if (movie) o.optString("title").ifBlank { o.optString("name") } else o.optString("name").ifBlank { o.optString("title") }).trim()
            if (title.isBlank()) continue
            val poster = o.optString("poster_path").takeIf { it.isNotBlank() }?.let { IMG + it }
            out.add(
                HikariMedia(
                    id = "tmdb:$id:${if (movie) "movie" else "tv"}",
                    title = title,
                    type = if (movie) HikariMediaType.MOVIE else HikariMediaType.SERIES,
                    posterUrl = poster,
                    year = o.optString("release_date").take(4).toIntOrNull()
                        ?: o.optString("first_air_date").take(4).toIntOrNull(),
                    overview = o.optString("overview").takeIf { it.isNotBlank() },
                    rawType = if (movie) "movie" else "tv",
                )
            )
        }
        return out
    }

    private suspend fun resolveDetailUrl(media: HikariMedia): String? {
        if (media.id.startsWith("hdhub4u-ts:")) return media.id.removePrefix("hdhub4u-ts:")
        if (media.id.startsWith("http")) return media.id
        // tmdb:<id>:<kind> -> imdb_id -> site index.
        val parts = media.id.split(":")
        if (parts.size < 3 || parts[0] != "tmdb") return null
        val kind = if (parts[2] == "tv") "tv" else "movie"
        val ext = tmdb("/$kind/${parts[1]}/external_ids", "") ?: return null
        val imdb = ext.optString("imdb_id").trim()
        if (imdb.isBlank()) return null
        return try {
            val ts = HikariNet.getJson(TYPESENSE + URLEncoder.encode(imdb, "UTF-8"), jh()) ?: return null
            val hits = ts.optJSONArray("hits") ?: return null
            for (i in 0 until hits.length()) {
                val d = hits.optJSONObject(i)?.optJSONObject("document") ?: continue
                val url = docUrl(d)
                if (url != null) return url
            }
            null
        } catch (t: Throwable) { null }
    }

    private fun docUrl(d: JSONObject): String? {
        for (k in listOf("url", "link", "detail_url", "permalink", "slug")) {
            val v = d.optString(k).trim()
            if (v.isBlank()) continue
            if (v.startsWith("http")) return v
            if (k == "slug" && v.length > 2) return "$base/$v/"
        }
        return null
    }

    private fun absUrl(href: String, page: String): String {
        if (href.startsWith("http")) return href
        return try {
            java.net.URL(java.net.URL(page), href).toString()
        } catch (t: Throwable) { href }
    }

    private suspend fun resolveHubLink(url: String, referer: String, out: MutableList<HikariStream>, seen: HashSet<String>) {
        // hubcloud.php landing -> HubCloud drive -> file host.
        var cur: String? = url
        for (h in 0 until 4) {
            val c = cur ?: break
            if (c.contains("/drive/") && c.contains("hubcloud")) {
                resolveHubDrive(c, referer, out, seen)
                return
            }
            val body = HikariNet.getString(c, mapOf("User-Agent" to UA, "Referer" to referer)) ?: return
            val drive = Regex("https?://[^\"'\\s<>]*hubcloud[^\"'\\s<>]*/drive/[^\"'\\s<>]+").find(body)?.value
            if (drive != null) {
                resolveHubDrive(drive, c, out, seen)
                return
            }
            val file = Regex("https?://[^\\s\"'<>\\\\]+\\.(?:m3u8|mp4|mkv)[^\\s\"'<>\\\\]*").find(body)?.value
            if (file != null && seen.add(file)) {
                out.add(HikariStream(name = "HDHub4u Hub", url = file, headers = mapOf("User-Agent" to UA, "Referer" to c), isM3u8 = file.contains(".m3u8")))
                return
            }
            cur = Regex("<a[^>]+href=\"(https?://[^\"]+)\"").findAll(body)
                .map { it.groupValues[1] }.firstOrNull { it.contains("hubcloud") || it.contains("hubdrive") || it.contains("pixeldrain") }
        }
    }

    private suspend fun resolveHubDrive(driveUrl: String, referer: String, out: MutableList<HikariStream>, seen: HashSet<String>) {
        try {
            val host = try { java.net.URL(driveUrl).host } catch (t: Throwable) { return }
            val page = HikariNet.getString(driveUrl, mapOf("User-Agent" to UA, "Referer" to referer)) ?: return
            val resolver = Regex("""var\s+url\s*=\s*'([^']+)'""").find(page)?.groupValues?.get(1) ?: return
            val body = HikariNet.getString(resolver, mapOf("User-Agent" to UA, "Referer" to "https://$host/")) ?: return
            Regex("""<a[^>]*href="(https?://[^"]+)"[^>]*id="fsl"""").find(body)?.let { m ->
                val fsl = m.groupValues[1]
                if (fsl.startsWith("http") && seen.add(fsl)) {
                    out.add(HikariStream(name = "HDHub4u HubCloud", url = fsl))
                }
            }
        } catch (t: Throwable) { }
    }
}
