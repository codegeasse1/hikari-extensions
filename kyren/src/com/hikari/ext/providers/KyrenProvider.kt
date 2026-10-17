package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import com.hikari.ext.HikariSubtitle
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import org.json.JSONObject
import java.net.URLEncoder

/**
 * Kyren (kyren.moe) — anime via /api/stream JSON API + MegaPlay embeds.
 *
 * Faithful native split-out of the bundle RaghavKyren source. Kyren has no
 * browse of its own, so catalogs + search + metadata ride the public AniList
 * GraphQL API (same queries the bundle uses) and episode titles/thumbnails
 * come from api.ani.zip mappings; streams come from Kyren's servers
 * (megaplay, megaplay-direct, tryembed).
 */
class KyrenProvider : HikariProvider {

    override val id = "kyren"
    override val name = "Kyren"
    override val mainUrl = "https://kyren.moe"
    override val description = "Anime from Kyren — trending, popular, top rated, airing and movies via AniList; sub & dub streams from Kyren servers."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    companion object {
        private const val BASE = "https://kyren.moe"
        private val UA = "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36"
        private val SERVERS = listOf("megaplay", "megaplay-direct", "tryembed")
        private val capture = Regex("https?://[^\"'\\s]+?\\.(?:m3u8|mp4)(?:[?#][^\"'\\s]*)?")
        private val m3u8Regex = Regex("https?://[^\\s\"'<>\\\\]+\\.m3u8[^\\s\"'<>\\\\]*")

        private fun apiHeaders(): Map<String, String> = mapOf(
            "User-Agent" to UA,
            "Accept" to "application/json",
            "Referer" to "$BASE/",
        )

        @Volatile private var aniZipCache = HashMap<Int, JSONObject>()
    }

    // ------------------------------------------------------------------
    // Catalogs (6 AniList rows)
    // ------------------------------------------------------------------

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("trending", "Trending Now", HikariMediaType.SERIES),
        HikariCatalog("popular", "Most Popular", HikariMediaType.SERIES),
        HikariCatalog("top-rated", "Top Rated", HikariMediaType.SERIES),
        HikariCatalog("airing", "Top Airing", HikariMediaType.SERIES),
        HikariCatalog("movies", "Movies", HikariMediaType.MOVIE),
        HikariCatalog("favorites", "Most Favorited", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val p = page.coerceAtLeast(1)
        val cards = when (catalog.id) {
            "trending" -> KyAnilist.list(listOf("TRENDING_DESC", "POPULARITY_DESC"), p)
            "popular" -> KyAnilist.list(listOf("POPULARITY_DESC"), p)
            "top-rated" -> KyAnilist.list(listOf("SCORE_DESC"), p, format = "TV")
            "airing" -> KyAnilist.list(listOf("START_DATE_DESC"), p, status = "RELEASING")
            "movies" -> KyAnilist.list(listOf("POPULARITY_DESC"), p, format = "MOVIE")
            "favorites" -> KyAnilist.list(listOf("FAVOURITES_DESC"), p)
            else -> return emptyList()
        }
        return cards.map { KyAnilist.HikariMediaFromCard("kyren", it) }
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isEmpty()) return emptyList()
        return KyAnilist.search(q, page.coerceAtLeast(1)).map { KyAnilist.HikariMediaFromCard("kyren", it) }
    }

    // ------------------------------------------------------------------
    // Meta + episodes
    // ------------------------------------------------------------------

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val id = media.id.substringAfter("kyren:").substringBefore("|").toIntOrNull() ?: return media
        val c = KyAnilist.info(id) ?: return media
        return media.copy(
            title = c.title,
            posterUrl = c.poster ?: media.posterUrl,
            backdropUrl = c.banner ?: c.poster ?: media.backdropUrl,
            overview = c.overview ?: media.overview,
            genres = if (c.genres.isNotEmpty()) c.genres else media.genres,
            year = c.year ?: media.year,
            type = if (c.isMovie) HikariMediaType.MOVIE else HikariMediaType.SERIES,
        )
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val id = media.id.substringAfter("kyren:").substringBefore("|").toIntOrNull() ?: return null
        val info = KyAnilist.info(id)
        val zip = aniZip(id)
        val indexes = LinkedHashSet<Int>()
        zip?.optJSONObject("episodes")?.keys()?.forEach { k -> k.toIntOrNull()?.takeIf { it > 0 }?.let { indexes.add(it) } }
        val total = info?.episodes
        if (total != null && total > 0) for (n in 1..minOf(total, 2000)) indexes.add(n)
        if (indexes.isEmpty()) indexes.add(1)
        val title = info?.title ?: media.title
        val out = ArrayList<HikariEpisode>()
        for (n in indexes.sorted()) {
            val epObj = zip?.optJSONObject("episodes")?.optJSONObject(n.toString())
            val epTitle = epObj?.optJSONObject("title")?.optString("en")?.takeIf { it.isNotBlank() }
            val img = epObj?.optString("image")?.takeIf { it.startsWith("http") } ?: media.posterUrl
            val ref = "$id|$n|${URLEncoder.encode(title, "UTF-8")}"
            out.add(HikariEpisode(number = n, id = "$ref|sub", name = epTitle ?: "Episode $n", image = img))
            out.add(HikariEpisode(number = n, id = "$ref|dub", name = "${epTitle ?: "Episode $n"} (Dub)", image = img))
        }
        return out
    }

    // ------------------------------------------------------------------
    // Streams
    // ------------------------------------------------------------------

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val parts = (episode?.id ?: return emptyList()).split("|")
        if (parts.size < 4) return emptyList()
        val anilistId = parts[0].toIntOrNull()?.takeIf { it > 0 } ?: return emptyList()
        val ep = parts[1].toIntOrNull()?.takeIf { it > 0 } ?: return emptyList()
        val title = runCatching { java.net.URLDecoder.decode(parts[2], "UTF-8") }.getOrNull().orEmpty()
        val lang = if (parts[3] == "dub") "dub" else "sub"

        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        coroutineScope {
            SERVERS.map { server ->
                async {
                    val links = ArrayList<HikariStream>()
                    val subs = ArrayList<HikariSubtitle>()
                    try {
                        val url = "$BASE/api/stream/$anilistId/$ep?lang=$lang&title=${URLEncoder.encode(title, "UTF-8")}&server=$server"
                        val text = HikariNet.getString(url, apiHeaders()) ?: return@async null
                        val root = runCatching { JSONObject(text) }.getOrNull() ?: return@async null
                        if (root.optBoolean("ok", false) != true) return@async null
                        val sources = root.optJSONArray("sources") ?: return@async null
                        for (i in 0 until sources.length()) {
                            val s = sources.optJSONObject(i) ?: continue
                            val sourceUrl = s.optString("url")
                            if (sourceUrl.isBlank()) continue
                            val provider = s.optString("provider").ifBlank { server }
                            val langLabel = if (s.optBoolean("isDub", false) || s.optString("language") == "dub") "Dub" else "Sub"
                            val q = when (s.optString("quality").lowercase()) {
                                "1080p" -> " 1080p"
                                "720p" -> " 720p"
                                "480p" -> " 480p"
                                "360p" -> " 360p"
                                else -> ""
                            }
                            val label = "Kyren $provider $langLabel$q".trim()
                            when (s.optString("type")) {
                                "hls" -> synchronized(seen) {
                                    if (seen.add(sourceUrl)) {
                                        links.add(
                                            HikariStream(
                                                name = label,
                                                url = sourceUrl,
                                                headers = mapOf("User-Agent" to UA, "Referer" to "$BASE/"),
                                                isM3u8 = true,
                                            )
                                        )
                                    }
                                }
                                else -> resolveEmbedUrl(sourceUrl, label, subs, links)
                            }
                        }
                        root.optJSONArray("subtitles")?.let { arr ->
                            for (i in 0 until arr.length()) {
                                val su = arr.optJSONObject(i) ?: continue
                                val u = su.optString("url")
                                if (u.isBlank()) continue
                                synchronized(seen) {
                                    if (seen.add("sub:$u")) subs.add(HikariSubtitle(su.optString("label").ifBlank { su.optString("lang").ifBlank { "English" } }, u))
                                }
                            }
                        }
                    } catch (t: Throwable) { }
                    Pair(links, subs)
                }
            }.awaitAll().forEach { result ->
                if (result == null) return@forEach
                val (links, subs) = result
                synchronized(out) { out.addAll(links.map { it.copy(subtitles = it.subtitles + subs) }) }
            }
        }
        return out
    }

    private suspend fun resolveEmbedUrl(embedUrl: String, label: String, subs: MutableList<HikariSubtitle>, links: MutableList<HikariStream>) {
        val host = runCatching { java.net.URL(embedUrl).host.lowercase() }.getOrNull().orEmpty()
        if (host.endsWith("megaplay.buzz") || host.endsWith("vidwish.live") || host.endsWith("vidtube.site") || host.contains("megaplay-")) {
            val stream = KyMegaPlay.resolveStream(embedUrl, "$BASE/") ?: return
            KyMegaPlay.emitLinks("Kyren", label, stream.m3u8, "https://$host/", stream.subtitles, subs::add, links::add)
            return
        }
        try {
            val hits = HikariNet.resolveWithWebView(embedUrl, capture, timeoutMs = 30_000)
            for (h in hits) {
                if (h.url.isBlank()) continue
                links.add(HikariStream(name = label, url = h.url, headers = h.headers, isM3u8 = h.url.contains(".m3u8", ignoreCase = true)))
            }
            if (hits.isNotEmpty()) return
        } catch (t: Throwable) { }
        val html = HikariNet.getString(embedUrl, mapOf("User-Agent" to UA, "Referer" to "$BASE/")) ?: return
        val m3u8 = m3u8Regex.find(html)?.value
            ?: KyJsPacker.parseAndUnpack(html)?.let { m3u8Regex.find(it)?.value } ?: return
        links.add(HikariStream(name = label, url = m3u8, headers = mapOf("User-Agent" to UA, "Referer" to "https://$host/"), isM3u8 = true))
    }

    private suspend fun aniZip(anilistId: Int): JSONObject? {
        synchronized(aniZipCache) { aniZipCache[anilistId]?.let { return it } }
        return try {
            val o = HikariNet.getJson("https://api.ani.zip/mappings?anilist_id=$anilistId")
            if (o != null) synchronized(aniZipCache) {
                if (aniZipCache.size > 40) aniZipCache.clear()
                aniZipCache[anilistId] = o
                o
            } else null
        } catch (t: Throwable) { null }
    }
}
