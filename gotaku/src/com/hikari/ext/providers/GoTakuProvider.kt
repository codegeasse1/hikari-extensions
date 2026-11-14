package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream

/**
 * GoTaku (gotaku.to) — anime via sealed /api/v1 JSON + tokenized CDN proxy.
 *
 * Faithful native port: fetchTitles/fetchTitleDetail/fetchEpisodes/fetchEmbed
 * (AES-GCM-sealed bodies), embed page manifest handshake, GCM playlist +
 * CTR-masked segments through the localhost GtProxy.
 */
class GoTakuProvider : HikariProvider {

    override val id = "gotaku"
    override val name = "GoTaku"
    override val mainUrl = "https://gotaku.to"
    override val description = "Anime from GoTaku — trending, recently updated, sub/dub tracks, movies and ONA, direct tokenized streams."
    override val version = 2
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("trending_day", "Trending Today", HikariMediaType.SERIES),
        HikariCatalog("trending_week", "Trending This Week", HikariMediaType.SERIES),
        HikariCatalog("latest", "Recently Updated", HikariMediaType.SERIES),
        HikariCatalog("track:sub", "Latest Sub", HikariMediaType.SERIES),
        HikariCatalog("track:dub", "Latest Dub", HikariMediaType.SERIES),
        HikariCatalog("type:MOVIE", "Anime Movies", HikariMediaType.MOVIE),
        HikariCatalog("type:ONA", "ONA", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        GtApi.refreshDomain()
        val params = mutableMapOf("sort" to "latest", "limit" to "28", "page" to page.coerceAtLeast(1).toString())
        when {
            catalog.id == "trending_day" || catalog.id == "trending_week" -> params["sort"] = catalog.id
            catalog.id.startsWith("track:") -> params["track"] = catalog.id.removePrefix("track:")
            catalog.id.startsWith("type:") -> params["type"] = catalog.id.removePrefix("type:")
            else -> if (catalog.id != "latest") return emptyList()
        }
        return GtApi.fetchTitles(params).first.mapNotNull { it.toMedia() }
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank()) return emptyList()
        GtApi.refreshDomain()
        if (page > 1) return emptyList()
        return GtApi.fetchTitles(mapOf("q" to query.trim(), "limit" to "28")).first.mapNotNull { it.toMedia() }
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        GtApi.refreshDomain()
        val titleId = media.id.substringAfter("title/").substringBefore("?").takeIf { it.isNotBlank() } ?: return media
        val detail = GtApi.fetchTitleDetail(titleId) ?: return media
        return media.copy(
            title = detail.name ?: media.title,
            posterUrl = detail.poster ?: media.posterUrl,
            backdropUrl = detail.backdrop ?: media.backdropUrl,
            overview = detail.synopsis ?: media.overview,
            genres = if (detail.genres.isNotEmpty()) detail.genres else media.genres,
            year = detail.year ?: media.year,
        )
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        GtApi.refreshDomain()
        val titleId = media.id.substringAfter("title/").substringBefore("?").takeIf { it.isNotBlank() } ?: return null
        val episodes = GtApi.fetchEpisodes(titleId)
        if (episodes.isEmpty()) return null
        val sub = ArrayList<HikariEpisode>()
        val dub = ArrayList<HikariEpisode>()
        for (entry in episodes) {
            val id = entry.id ?: continue
            val num = entry.number ?: continue
            val name = episodeDisplayName(entry)
            if (entry.sub == true) sub.add(HikariEpisode(number = num, id = "$id|hard_sub", name = name, image = entry.thumbnail ?: media.posterUrl))
            if (entry.dub == true) dub.add(HikariEpisode(number = num, id = "$id|dub", name = "$name (Dub)", image = entry.thumbnail ?: media.posterUrl))
        }
        if (sub.isEmpty() && dub.isEmpty()) return null
        return sub.sortedBy { it.number } + dub.sortedBy { it.number }
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        GtApi.refreshDomain()
        val ref = episode?.id ?: return emptyList()
        val episodeId = ref.substringBefore("|").ifBlank { return emptyList() }
        val track = ref.substringAfter("|", "hard_sub").ifBlank { "hard_sub" }
        val trackLabel = if (track == "dub") "Dub" else "Hardsub"

        val embedUrl = GtApi.fetchEmbed(episodeId, track) ?: return emptyList()
        val stream = resolveStream(embedUrl) ?: return emptyList()
        val qualities = parseQualities(stream.masterPlaylist)
        if (qualities.isEmpty()) {
            return listOf(HikariStream(name = "GoTaku $trackLabel", url = "${stream.proxyUrl}/m/0/master.m3u8", isM3u8 = true))
        }
        return qualities.map { (_, _, index) ->
            val label = "GoTaku $trackLabel"
            HikariStream(name = "$label ${variantLabel(stream.masterPlaylist, index)}", url = "${stream.proxyUrl}/m/$index/master.m3u8", isM3u8 = true)
        }
    }

    private class ResolvedStream(val proxyUrl: String, val masterPlaylist: String)

    private suspend fun resolveStream(embedUrl: String): ResolvedStream? {
        return try {
            val site = GtApi.site()
            val path = if (embedUrl.startsWith("http")) embedUrl else site + embedUrl
            val html = HikariNet.getString(path, GtApi.browserHeaders + mapOf("Referer" to "$site/"))
                ?: HikariNet.getStringSmart(path, GtApi.browserHeaders + mapOf("Referer" to "$site/"))
                ?: return null
            val base = Regex("data-manifest-base=\"([^\"]+)\"").find(html)?.groupValues?.get(1) ?: return null
            val stamp = Regex("data-manifest-stamp=\"([^\"]+)\"").find(html)?.groupValues?.get(1) ?: return null

            var master: String? = null
            var manifest: GtApi.ManifestInfo? = null
            for (attempt in 0 until 3) {
                manifest = GtApi.fetchManifest(base, stamp) ?: continue
                val key = GtCrypto.playlistKey(manifest.keySeed, manifest.token)
                val masterBytes = HikariNet.getBytes(
                    manifest.source,
                    GtApi.browserHeaders + mapOf("Referer" to "$site/", "Origin" to site),
                ) ?: continue
                master = GtCrypto.decryptPlaylist(key, masterBytes)
                if (master != null && master.startsWith("#EXTM3U")) break
                master = null
            }
            if (master == null || manifest == null) return null
            val proxyBase = GtProxy.register(
                manifestBase = base,
                stamp = manifest.stamp,
                token = manifest.token,
                expiresAt = manifest.expiresAt,
                keySeed = manifest.keySeed,
                segmentBytes = manifest.segmentBytes,
                masterUrl = manifest.source,
            ) ?: return null
            ResolvedStream(proxyBase, master)
        } catch (t: Throwable) {
            null
        }
    }

    private fun parseQualities(master: String): List<Triple<String, Int, Int>> {
        val lines = master.split("\n").map { it.trim() }.filter { it.isNotEmpty() }
        val out = mutableListOf<Triple<String, Int, Int>>()
        var index = -1
        for (line in lines) {
            if (!line.startsWith("#EXT-X-STREAM-INF")) continue
            val height = Regex("RESOLUTION=(\\d+)x(\\d+)").find(line)?.groupValues?.get(2)?.toIntOrNull() ?: continue
            index++
            val label = when {
                height >= 2160 -> "2160p"
                height >= 1080 -> "1080p"
                height >= 720 -> "720p"
                height >= 480 -> "480p"
                height >= 360 -> "360p"
                else -> "${height}p"
            }
            out.add(Triple(label, height, index))
        }
        return out
    }

    private fun variantLabel(master: String, index: Int): String {
        return parseQualities(master).firstOrNull { it.third == index }?.first ?: ""
    }

    private fun episodeDisplayName(entry: GtApi.EpisodeEntry): String {
        val base = entry.name?.takeIf { it.isNotBlank() }
            ?: return "Episode ${entry.number ?: entry.label ?: ""}".trim()
        val label = entry.label ?: return base
        return if (base != label && !label.matches(Regex("\\d+"))) "$base ($label)" else base
    }

    private fun GtApi.TitleEntry.toMedia(): HikariMedia? {
        val name = this.name ?: return null
        val id = this.id ?: return null
        return HikariMedia(
            id = "${GtApi.site()}/title/$id",
            title = name,
            type = if (this.format == "MOVIE") HikariMediaType.MOVIE else HikariMediaType.SERIES,
            posterUrl = this.poster,
            year = this.year,
            overview = this.synopsis,
            genres = this.genres,
            backdropUrl = this.backdrop,
        )
    }
}
