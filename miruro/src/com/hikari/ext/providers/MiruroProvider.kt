package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import com.hikari.ext.HikariSubtitle
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import org.json.JSONArray
import org.json.JSONObject
import java.util.concurrent.ConcurrentHashMap

/**
 * Miruro — anime via AniList metadata + the miruro secure-pipe episode/source
 * API (16 providers: kiwi/pewe/bonk/bee/ally/hop/moo/nun/bun/twin/cog/
 * mega/nova/wave/zen/flux).
 *
 * Faithful native port: AniList catalogs/search/detail, pipe episodes with
 * best-provider sub/ssub/dub assembly, pipe sources (hls/mp4 direct +
 * megaplay/vidwish/webview embeds), pipe subtitles.
 */
class MiruroProvider : HikariProvider {

    override val id = "miruro"
    override val name = "Miruro"
    override val mainUrl = "https://www.miruro.to"
    override val description = "Anime from Miruro — trending, popular, recent, top rated and movies via 16 pooled providers, sub & dub."
    override val version = 2
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    companion object {
        private val PROVIDER_ORDER = listOf(
            "kiwi", "pewe", "bonk", "bee", "ally", "hop",
            "moo", "nun", "bun", "twin", "cog",
            "mega", "nova", "wave", "zen", "flux",
        )
        private val PROVIDER_NAMES = mapOf(
            "kiwi" to "AnimePahe", "pewe" to "AniDB", "bonk" to "AnimeDao",
            "bee" to "AniKoto", "ally" to "AllManga", "hop" to "KickAssAnime",
            "moo" to "AnimeGG", "nun" to "Nun", "bun" to "Bun",
            "twin" to "Twin", "cog" to "Cog", "mega" to "MegaAnime",
            "nova" to "Nova", "wave" to "Wave", "zen" to "Zen", "flux" to "Flux",
        )
        private const val UA =
            "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36"
    }

    // ------------------------------------------------------------------
    // Catalogs (6 AniList rows)
    // ------------------------------------------------------------------

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("trending", "Trending", HikariMediaType.SERIES),
        HikariCatalog("popular", "Popular", HikariMediaType.SERIES),
        HikariCatalog("recent", "Recently Updated", HikariMediaType.SERIES),
        HikariCatalog("top-rated", "Top Rated", HikariMediaType.SERIES),
        HikariCatalog("movies", "Movies", HikariMediaType.MOVIE),
        HikariCatalog("favorites", "Most Favorited", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        MrPipe.refreshDomain()
        val p = page.coerceAtLeast(1)
        val cards = when (catalog.id) {
            "trending" -> MrAnilist.list(listOf("TRENDING_DESC"), p)
            "popular" -> MrAnilist.list(listOf("POPULARITY_DESC"), p)
            "recent" -> MrAnilist.list(listOf("START_DATE_DESC"), p, status = "RELEASING")
            "top-rated" -> MrAnilist.list(listOf("SCORE_DESC"), p, format = "TV")
            "movies" -> MrAnilist.list(listOf("POPULARITY_DESC"), p, format = "MOVIE")
            "favorites" -> MrAnilist.list(listOf("FAVOURITES_DESC"), p)
            else -> return emptyList()
        }
        return cards.map { MrAnilist.HikariMediaFromCard("miruro", it) }
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isEmpty()) return emptyList()
        MrPipe.refreshDomain()
        return MrAnilist.search(q, page.coerceAtLeast(1)).map { MrAnilist.HikariMediaFromCard("miruro", it) }
    }

    // ------------------------------------------------------------------
    // Meta + episodes (pipe-backed)
    // ------------------------------------------------------------------

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val id = media.id.substringAfter("miruro:").substringBefore("|").toIntOrNull() ?: return media
        MrPipe.refreshDomain()
        val c = MrAnilist.info(id) ?: return media
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

    private data class EpRef(val number: Int, val title: String?, val image: String?, val entries: List<String>)

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val anilistId = media.id.substringAfter("miruro:").substringBefore("|").toIntOrNull() ?: return null
        MrPipe.refreshDomain()
        val body = try {
            MrPipe.pipeRequest("episodes", mapOf("anilistId" to anilistId))
        } catch (t: Throwable) { return null }
        val providers = try {
            JSONObject(body).optJSONObject("providers")
        } catch (t: Throwable) { return null } ?: return null

        fun epsOf(prov: String, vararg cats: String): List<JSONObject> {
            val out = ArrayList<JSONObject>()
            val catsObj = providers.optJSONObject(prov)?.optJSONObject("episodes") ?: return out
            for (c in cats) {
                val arr = catsObj.optJSONArray(c) ?: continue
                for (i in 0 until arr.length()) arr.optJSONObject(i)?.let { out.add(it) }
            }
            return out
        }
        fun numOf(e: JSONObject): Int? = e.optInt("number", 0).takeIf { it > 0 }
        fun idOf(e: JSONObject): String? = e.optString("id").takeIf { it.isNotBlank() }

        // Best sub provider (sub preferred, ssub fallback), ordered then rest.
        val orderedProvNames = PROVIDER_ORDER.filter { providers.has(it) } +
            providers.keys().asSequence().filter { it !in PROVIDER_ORDER }.toList()
        var bestSub: String? = null
        var bestSubCount = 0
        for (pn in orderedProvNames) {
            val count = maxOf(epsOf(pn, "sub").size, epsOf(pn, "ssub").size)
            if (count > bestSubCount) { bestSubCount = count; bestSub = pn }
        }
        val subOut = ArrayList<HikariEpisode>()
        if (bestSub != null && bestSubCount > 0) {
            val base = epsOf(bestSub, "sub").ifEmpty { epsOf(bestSub, "ssub") }
            for (ep in base) {
                val num = numOf(ep) ?: continue
                val parts = mutableListOf("sub", anilistId.toString())
                for (pn in orderedProvNames) {
                    val match = epsOf(pn, "sub").firstOrNull { numOf(it) == num }
                        ?: epsOf(pn, "ssub").firstOrNull { numOf(it) == num } ?: continue
                    val mid = idOf(match) ?: continue
                    val cat = if (epsOf(pn, "sub").any { numOf(it) == num && idOf(it) == mid }) "sub" else "ssub"
                    parts.add("$pn:$mid:$cat")
                }
                if (parts.size > 2) {
                    subOut.add(
                        HikariEpisode(
                            number = num,
                            id = parts.joinToString("|"),
                            name = ep.optString("title").takeIf { it.isNotBlank() } ?: "Episode $num",
                            image = ep.optString("image").takeIf { it.startsWith("http") } ?: media.posterUrl,
                        )
                    )
                }
            }
        }
        var bestDub: String? = null
        var bestDubCount = 0
        for (pn in orderedProvNames) {
            val count = epsOf(pn, "dub").size
            if (count > bestDubCount) { bestDubCount = count; bestDub = pn }
        }
        val dubOut = ArrayList<HikariEpisode>()
        if (bestDub != null && bestDubCount > 0) {
            for (ep in epsOf(bestDub, "dub")) {
                val num = numOf(ep) ?: continue
                val parts = mutableListOf("dub", anilistId.toString())
                for (pn in orderedProvNames) {
                    val match = epsOf(pn, "dub").firstOrNull { numOf(it) == num } ?: continue
                    val mid = idOf(match) ?: continue
                    parts.add("$pn:$mid:dub")
                }
                if (parts.size > 2) {
                    dubOut.add(
                        HikariEpisode(
                            number = num,
                            id = parts.joinToString("|"),
                            name = "${ep.optString("title").takeIf { it.isNotBlank() } ?: "Episode $num"} (Dub)",
                            image = ep.optString("image").takeIf { it.startsWith("http") } ?: media.posterUrl,
                        )
                    )
                }
            }
        }
        if (subOut.isEmpty() && dubOut.isEmpty()) return null
        return subOut.sortedBy { it.number } + dubOut.sortedBy { it.number }
    }

    // ------------------------------------------------------------------
    // Streams
    // ------------------------------------------------------------------

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        MrPipe.refreshDomain()
        val parts = (episode?.id ?: return emptyList()).split("|")
        if (parts.size < 3) return emptyList()
        val dubOrSub = parts[0]
        val anilistId = parts[1].toIntOrNull()
        val entries = parts.drop(2).mapNotNull { entry ->
            val cp = entry.split(":")
            if (cp.size < 3) {
                if (cp.size == 2) Triple(cp[0], cp[1], dubOrSub) else null
            } else {
                val provider = cp[0]
                val category = cp.last()
                val episodeId = cp.drop(1).dropLast(1).joinToString(":")
                if (provider.isEmpty() || episodeId.isEmpty() || category.isEmpty()) null
                else Triple(provider, episodeId, category)
            }
        }
        if (entries.isEmpty()) return emptyList()
        val seenUrls = ConcurrentHashMap.newKeySet<String>()
        val out = ArrayList<HikariStream>()
        val seenOut = HashSet<String>()
        coroutineScope {
            entries.map { (provider, episodeId, category) ->
                async {
                    processProvider(provider, episodeId, category, anilistId, seenUrls)
                }
            }.awaitAll().forEach { list ->
                synchronized(out) {
                    for (s in list) if (seenOut.add(s.url)) out.add(s)
                }
            }
        }
        return out
    }

    private suspend fun processProvider(
        provider: String,
        episodeId: String,
        category: String,
        anilistId: Int?,
        seenUrls: MutableSet<String>,
    ): List<HikariStream> {
        val out = ArrayList<HikariStream>()
        val displayName = PROVIDER_NAMES[provider] ?: provider
        try {
            val query = mutableMapOf<String, Any?>("episodeId" to episodeId, "provider" to provider, "category" to category)
            if (anilistId != null) query["anilistId"] = anilistId
            val body = MrPipe.pipeRequest("sources", query)
            val root = JSONObject(body)
            val streams = root.optJSONArray("streams") ?: return out
            for (i in 0 until streams.length()) {
                val s = streams.optJSONObject(i) ?: continue
                val url = s.optString("url").ifBlank { continue }
                if (!seenUrls.add(url)) continue
                val referer = s.optString("referer").takeIf { it.isNotBlank() } ?: "${MrPipe.workingDomain()}/"
                when (s.optString("type")) {
                    "hls" -> {
                        val qualityLabel = s.optString("quality").takeIf { it.isNotBlank() } ?: "Auto"
                        val fansub = s.optString("fansub").takeIf { it.isNotBlank() }?.let { " [$it]" } ?: ""
                        out.add(
                            HikariStream(
                                name = "Miruro $displayName$fansub - $qualityLabel",
                                url = url,
                                headers = mapOf("Referer" to referer, "User-Agent" to UA),
                                isM3u8 = true,
                            )
                        )
                    }
                    "mp4" -> {
                        val qualityLabel = s.optString("quality").takeIf { it.isNotBlank() } ?: "SD"
                        out.add(
                            HikariStream(
                                name = "Miruro $displayName (MP4) - $qualityLabel",
                                url = url,
                                headers = mapOf("Referer" to referer, "User-Agent" to UA),
                            )
                        )
                    }
                    else -> {
                        val subs = ArrayList<HikariSubtitle>()
                        val links = ArrayList<HikariStream>()
                        val low = url.lowercase()
                        if (low.contains("megaplay.buzz") || (low.contains("megaplay") && !low.contains("vidwish"))) {
                            MrExtractors.megaplay(url, referer, "https://megaplay.buzz", "Miruro MegaPlay", subs::add, links::add)
                        } else if (low.contains("vidwish.live") || low.contains("vidwish")) {
                            MrExtractors.megaplay(url, referer, "https://vidwish.live", "Miruro VidWish", subs::add, links::add)
                        } else {
                            MrExtractors.webview(url, referer, "Miruro $displayName", links::add)
                        }
                        for (l in links) {
                            if (!seenUrls.add(l.url)) continue
                            out.add(l.copy(subtitles = l.subtitles + subs))
                        }
                    }
                }
            }
            root.optJSONArray("subtitles")?.let { arr ->
                val subs = buildList {
                    for (i in 0 until arr.length()) {
                        val su = arr.optJSONObject(i) ?: continue
                        val u = su.optString("url").ifBlank { continue }
                        add(HikariSubtitle(su.optString("lang").ifBlank { "English" }, u))
                    }
                }
                if (subs.isNotEmpty()) {
                    for (i in out.indices) out[i] = out[i].copy(subtitles = out[i].subtitles + subs)
                }
            }
        } catch (t: Throwable) { }
        return out
    }
}
