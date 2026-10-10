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
import org.json.JSONArray
import org.json.JSONObject
import java.net.URLEncoder

/**
 * AniKage (anikage.cc) — anime via /api/media/anime JSON API + proxy streams.
 *
 * Faithful native split-out of the bundle RaghavAniKage source:
 * browse/detail/episodes/servers/sources endpoints, PUBLIC_PROXY_URL stream
 * proxy, MegaPlay/Flix/inline/packed embed routing, sub+dup episode tracks.
 */
private data class AkServer(val id: String, val providerId: String, val subTypes: List<String>)

class AniKageProvider : HikariProvider {

    override val id = "anikage"
    override val name = "AniKage"
    override val mainUrl = "https://anikage.cc"
    override val description = "Anime from AniKage — popular, top airing, movies and genre rows, sub & dub proxy streams plus embeds."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    companion object {
        private const val FALLBACK = "https://anikage.cc"
        private const val FIREBASE_URL = "https://cloudstreampluginhelper-default-rtdb.firebaseio.com/.json"
        private const val CACHE_TTL_MS = 5 * 60 * 1000L
        private const val PAGE_SIZE = 24
        private val UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

        @Volatile private var domain = FALLBACK
        @Volatile private var lastDomainLoad = 0L
        @Volatile private var cachedProxy: String? = null
        @Volatile private var cachedProxyAt = 0L

        private fun apiHeaders(): Map<String, String> = mapOf(
            "Accept" to "application/json",
            "User-Agent" to UA,
        )

        suspend fun refreshDomain() {
            val now = System.currentTimeMillis()
            if (now - lastDomainLoad < CACHE_TTL_MS) return
            lastDomainLoad = now
            try {
                val body = HikariNet.getString(FIREBASE_URL) ?: return
                val o = JSONObject(body)
                val v = o.optString("anikage").ifBlank { o.optString("anikage_url") }
                    .ifBlank { o.optString("anikage_domain") }.trim().removeSuffix("/")
                if (v.startsWith("http")) domain = v
            } catch (t: Throwable) { }
        }

        fun base(): String = domain
        private fun api(): String = "${domain}/api/media/anime"

        private suspend fun firebaseKey(vararg keys: String): String? {
            return try {
                val o = JSONObject(HikariNet.getString(FIREBASE_URL) ?: return null)
                keys.firstNotNullOfOrNull { k -> o.optString(k).takeIf { it.isNotBlank() } }?.removeSuffix("/")
            } catch (t: Throwable) { null }
        }

        suspend fun proxyBase(): String {
            if (System.currentTimeMillis() - cachedProxyAt < 10 * 60 * 1000L) {
                cachedProxy?.let { return it }
            }
            var parsed: String? = null
            try {
                val html = HikariNet.getString(domain, mapOf("User-Agent" to UA))
                parsed = html?.let { Regex("\"PUBLIC_PROXY_URL\"\\s*:\\s*\"([^\"]+)\"").find(it)?.groupValues?.get(1) }
            } catch (t: Throwable) { }
            cachedProxyAt = System.currentTimeMillis()
            if (parsed != null) cachedProxy = parsed
            return cachedProxy
                ?: firebaseKey("anikageproxy", "akageproxy")
                ?: "https://og.bakayaro.live"
        }

        fun proxyHeaders(): Map<String, String> =
            mapOf("Referer" to "${domain}/", "Origin" to domain)

        fun buildProxyUrl(path: String, proxy: String, type: String = "stream"): String = when {
            path.startsWith("http://") || path.startsWith("https://") -> path
            path.startsWith("/m3u8/") || path.startsWith("/stream/") || path.startsWith("/hls/") -> "$proxy$path"
            path.startsWith("m3u8/") || path.startsWith("stream/") || path.startsWith("hls/") -> "$proxy/$path"
            else -> "$proxy/$type/$path"
        }
    }

    // ------------------------------------------------------------------
    // Catalogs (6 rows, native browse filters — all verified live)
    // ------------------------------------------------------------------

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("popular", "Popular", HikariMediaType.SERIES),
        HikariCatalog("airing", "Top Airing", HikariMediaType.SERIES),
        HikariCatalog("movies", "Movies", HikariMediaType.MOVIE),
        HikariCatalog("action", "Action", HikariMediaType.SERIES),
        HikariCatalog("romance", "Romance", HikariMediaType.SERIES),
        HikariCatalog("fantasy", "Fantasy", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        refreshDomain()
        val p = page.coerceAtLeast(1)
        val extra = when (catalog.id) {
            "airing" -> "&status=RELEASING"
            "movies" -> "&format=MOVIE"
            "action" -> "&genres=Action"
            "romance" -> "&genres=Romance"
            "fantasy" -> "&genres=Fantasy"
            else -> ""
        }
        val body = HikariNet.getString("${api()}/browse?sort=popularity&page=$p&limit=$PAGE_SIZE&adult=true$extra", apiHeaders())
            ?: return emptyList()
        return try {
            parseResults(JSONObject(body).optJSONArray("data") ?: return emptyList())
        } catch (t: Throwable) { emptyList() }
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        refreshDomain()
        val q = query.trim()
        if (q.isEmpty()) return emptyList()
        val p = page.coerceAtLeast(1)
        val body = HikariNet.getString("${api()}/browse?q=${enc(q)}&sort=popularity&page=$p&limit=25&adult=true", apiHeaders())
            ?: return emptyList()
        return try {
            parseResults(JSONObject(body).optJSONArray("data") ?: return emptyList())
        } catch (t: Throwable) { emptyList() }
    }

    // ------------------------------------------------------------------
    // Meta + episodes
    // ------------------------------------------------------------------

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        refreshDomain()
        val slug = media.id.substringAfterLast("/").substringBefore("?").ifBlank { return media }
        val body = HikariNet.getString("${api()}/${enc(slug)}", apiHeaders()) ?: return media
        return try {
            val d = JSONObject(body).optJSONObject("anime") ?: return media
            val t = d.optJSONObject("title")
            val title = t?.optString("english")?.takeIf { it.isNotBlank() }
                ?: t?.optString("romaji")?.takeIf { it.isNotBlank() }
                ?: t?.optString("native")?.takeIf { it.isNotBlank() } ?: media.title
            val c = d.optJSONObject("coverImage")
            val poster = c?.optString("extraLarge")?.takeIf { it.startsWith("http") }
                ?: c?.optString("large")?.takeIf { it.startsWith("http") } ?: media.posterUrl
            val banner = d.optString("bannerImage").takeIf { it.startsWith("http") } ?: poster
            val overview = d.optString("description").takeIf { it.isNotBlank() }
                ?.replace(Regex("<[^>]+>"), "")?.trim()?.takeIf { it.isNotBlank() } ?: media.overview
            val genres = d.optJSONArray("genres")?.let { arr ->
                buildList { for (i in 0 until arr.length()) arr.optString(i).takeIf { it.isNotBlank() }?.let { add(it) } }
            } ?: media.genres
            val year = d.optInt("seasonYear", 0).takeIf { it > 0 } ?: media.year
            media.copy(title = title, posterUrl = poster, backdropUrl = banner, overview = overview, genres = genres, year = year)
        } catch (t: Throwable) { media }
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        refreshDomain()
        val slug = media.id.substringAfterLast("/").substringBefore("?").ifBlank { return null }
        val body = HikariNet.getString("${api()}/${enc(slug)}/episodes", apiHeaders()) ?: return null
        return try {
            val arr = JSONArray(body)
            if (arr.length() == 0) return null
            val sub = ArrayList<HikariEpisode>()
            val dub = ArrayList<HikariEpisode>()
            for (i in 0 until arr.length()) {
                val e = arr.optJSONObject(i) ?: continue
                val num = e.optInt("number", 0).takeIf { it > 0 } ?: continue
                val img = e.optString("image").takeIf { it.startsWith("http") } ?: media.posterUrl
                val desc = e.optString("description").takeIf { it.isNotBlank() }
                sub.add(HikariEpisode(number = num, id = "$slug|$num|sub", name = e.optString("title").takeIf { it.isNotBlank() } ?: "Episode $num", image = img))
                dub.add(HikariEpisode(number = num, id = "$slug|$num|dub", name = "${e.optString("title").takeIf { it.isNotBlank() } ?: "Episode $num"} (Dub)", image = img))
                if (desc != null) { /* description carried on episode name only; app shows name */ }
            }
            if (sub.isEmpty()) return null
            sub.sortedBy { it.number } + dub.sortedBy { it.number }
        } catch (t: Throwable) { null }
    }

    // ------------------------------------------------------------------
    // Streams
    // ------------------------------------------------------------------

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        refreshDomain()
        val parts = (episode?.id ?: return emptyList()).split("|")
        if (parts.size < 3) return emptyList()
        val slug = parts[0]
        val epNum = parts[1]
        val lang = if (parts[2] == "dub") "dub" else "sub"
        val proxy = proxyBase()

        val serversBody = HikariNet.getString("${api()}/${enc(slug)}/episodes/${enc(epNum)}/servers", apiHeaders())
            ?: return emptyList()
        val servers = try {
            val arr = JSONObject(serversBody).optJSONArray("servers") ?: return emptyList()
            buildList {
                for (i in 0 until arr.length()) {
                    val s = arr.optJSONObject(i) ?: continue
                    val id = s.optString("id")
                    if (id.isBlank()) continue
                    val st = s.optJSONArray("subTypes")?.let { a -> buildList { for (j in 0 until a.length()) add(a.optString(j)) } } ?: emptyList()
                    add(AkServer(id, s.optString("providerId").ifBlank { id }, st))
                }
            }
        } catch (t: Throwable) { return emptyList() }
        if (servers.isEmpty()) return emptyList()

        val out = ArrayList<HikariStream>()
        val seenSubs = HashSet<String>()
        val seenLinks = HashSet<String>()
        coroutineScope {
            servers.map { server ->
                async {
                    val links = ArrayList<HikariStream>()
                    val subs = ArrayList<HikariSubtitle>()
                    try {
                        if (server.subTypes.isNotEmpty() && !server.subTypes.contains(lang)) return@async null
                        val url = "${api()}/${enc(slug)}/episodes/${enc(epNum)}/sources?provider=${enc(server.providerId)}&lang=$lang&server=${enc(server.id)}"
                        val text = HikariNet.getString(url, apiHeaders()) ?: return@async null
                        var root = runCatching { JSONObject(text) }.getOrNull() ?: return@async null
                        if (root.optBoolean("stale", false)) {
                            HikariNet.getString("$url&_=${System.currentTimeMillis()}", apiHeaders())?.let {
                                runCatching { JSONObject(it) }.getOrNull()?.let { fresh -> root = fresh }
                            }
                        }
                        val subType = if (lang == "sub") {
                            if (root.optJSONArray("subtitles")?.length() ?: 0 > 0) "Softsub" else "Hardsub"
                        } else "Dub"
                        val baseName = "AniKage ${server.id.replaceFirstChar { it.uppercase() }} $subType".trim()

                        val subArr = root.optJSONArray("subtitles")
                        if (subArr != null) {
                            for (i in 0 until subArr.length()) {
                                val s = subArr.optJSONObject(i) ?: continue
                                val file = s.optString("file")
                                if (file.isBlank()) continue
                                val subUrl = buildProxyUrl(file, proxy, "stream")
                                synchronized(seenSubs) {
                                    if (seenSubs.add(subUrl)) subs.add(HikariSubtitle(s.optString("label").ifBlank { lang }, subUrl))
                                }
                            }
                        }

                        val usedEmbeds = LinkedHashSet<String>()
                        val srcArr = root.optJSONArray("sources")
                        if (srcArr != null) {
                            for (i in 0 until srcArr.length()) {
                                val src = srcArr.optJSONObject(i) ?: continue
                                val rawUrl = src.optString("url")
                                val embedUrl = src.optString("embedUrl").takeIf { it.isNotBlank() }
                                if (embedUrl != null && usedEmbeds.add(embedUrl)) {
                                    val embedLabel = "AniKage ${src.optString("server").takeIf { it.isNotBlank() } ?: server.id} $subType"
                                    AkEmbeds.resolveEmbed(embedUrl, "${base()}/", embedLabel, "AniKage", lang, subs::add, links::add)
                                }
                                if (rawUrl.isNotBlank()) {
                                    val isM3u8 = src.optBoolean("isM3U8", false)
                                    val videoUrl = buildProxyUrl(rawUrl, proxy, if (isM3u8) "m3u8" else "stream")
                                    val qualityClean = src.optString("quality").trim()
                                        .replace(Regex("^dub\\s+", RegexOption.IGNORE_CASE), "").takeIf { it.isNotBlank() }
                                    val serverTag = src.optString("server").takeIf { it.isNotBlank() && it != server.id }
                                        ?.replaceFirstChar { it.uppercase() }
                                    val nm = listOfNotNull(baseName, serverTag, qualityClean?.replaceFirstChar { it.uppercase() }).joinToString(" ")
                                    synchronized(seenLinks) {
                                        if (seenLinks.add(videoUrl)) {
                                            links.add(HikariStream(name = nm, url = videoUrl, headers = proxyHeaders(), isM3u8 = isM3u8))
                                        }
                                    }
                                }
                            }
                        }
                        val embArr = root.optJSONArray("embeds")
                        if (embArr != null) {
                            for (i in 0 until embArr.length()) {
                                val em = embArr.optJSONObject(i) ?: continue
                                val embedUrl = em.optString("url").takeIf { it.isNotBlank() } ?: continue
                                if (!usedEmbeds.add(embedUrl)) continue
                                val embedLabel = "AniKage ${em.optString("server").takeIf { it.isNotBlank() } ?: server.id} $subType"
                                AkEmbeds.resolveEmbed(embedUrl, "${base()}/", embedLabel, "AniKage", lang, subs::add, links::add)
                            }
                        }
                    } catch (t: Throwable) { }
                    Pair(links, subs)
                }
            }.awaitAll().forEach { result ->
                if (result == null) return@forEach
                val (links, subs) = result
                val attach = synchronized(seenSubs) { subs.filter { true } }
                synchronized(out) {
                    out.addAll(links.map { it.copy(subtitles = it.subtitles + attach) })
                }
            }
        }
        return out
    }

    // ------------------------------------------------------------------

    private fun parseResults(arr: JSONArray): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        for (i in 0 until arr.length()) {
            val e = arr.optJSONObject(i) ?: continue
            val slug = e.optString("slug")
            if (slug.isBlank()) continue
            val t = e.optJSONObject("title")
            val title = t?.optString("english")?.takeIf { it.isNotBlank() }
                ?: t?.optString("romaji")?.takeIf { it.isNotBlank() }
                ?: t?.optString("native")?.takeIf { it.isNotBlank() } ?: continue
            val c = e.optJSONObject("coverImage")
            val poster = c?.optString("extraLarge")?.takeIf { it.startsWith("http") }
                ?: c?.optString("large")?.takeIf { it.startsWith("http") }
            val year = e.optInt("year", 0).takeIf { it > 0 }
            val genres = e.optJSONArray("genres")?.let { g ->
                buildList { for (j in 0 until g.length()) g.optString(j).takeIf { it.isNotBlank() }?.let { add(it) } }
            } ?: emptyList()
            val isMovie = e.optString("format").equals("MOVIE", true)
            out.add(
                HikariMedia(
                    id = "${base()}/anime/$slug",
                    title = title,
                    type = if (isMovie) HikariMediaType.MOVIE else HikariMediaType.SERIES,
                    posterUrl = poster,
                    year = year,
                    genres = genres,
                )
            )
        }
        return out
    }

    private fun enc(s: String): String = URLEncoder.encode(s, "UTF-8")
}
