package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream

/**
 * M3U Player — live TV from curated public IPTV playlists (iptv-org,
 * verified live Oct 2026). Each playlist is a home row; entries expose
 * tvg-name / tvg-logo / group-title:
 *  - animation, kids, sports, news, movies, documentary.
 * Streams play directly (HLS/MP4 URLs as listed upstream).
 */
class M3UPlayerProvider : HikariProvider {

    override val id = "m3uplayer"
    override val name = "M3U Player"
    override val mainUrl = "https://iptv-org.github.io"
    override val description = "Live TV by category — animation, kids, sports, news, movies, documentary."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    companion object {
        const val BASE = "https://iptv-org.github.io/iptv/categories"
        const val ROW_CAP = 200
    }

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("animation", "Animation", HikariMediaType.SERIES),
        HikariCatalog("kids", "Kids", HikariMediaType.SERIES),
        HikariCatalog("sports", "Sports", HikariMediaType.SERIES),
        HikariCatalog("news", "News", HikariMediaType.SERIES),
        HikariCatalog("movies", "Movies", HikariMediaType.MOVIE),
        HikariCatalog("documentary", "Documentary", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val txt = HikariNet.getString("$BASE/${catalog.id}.m3u", mapOf("User-Agent" to "Hikari")) ?: return emptyList()
        val all = parseM3u(txt)
        if (page <= 1) return all.take(ROW_CAP)
        return all.drop(ROW_CAP * page).take(ROW_CAP)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim().lowercase()
        if (q.isBlank()) return emptyList()
        val out = ArrayList<HikariMedia>()
        for (c in catalogs()) {
            if (out.size >= 60) break
            val txt = HikariNet.getString("$BASE/${c.id}.m3u", mapOf("User-Agent" to "Hikari")) ?: continue
            out.addAll(parseM3u(txt).filter { it.title.lowercase().contains(q) }.take(20))
        }
        return out
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        return listOf(HikariEpisode(number = 1, id = media.id, name = "${media.title} · Live", image = media.posterUrl))
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val url = (episode?.id ?: media.id).substringAfter("m3u:")
        if (!url.startsWith("http")) return emptyList()
        return listOf(
            HikariStream(
                name = media.title,
                url = url,
                isM3u8 = url.contains(".m3u8"),
            )
        )
    }

    // ------------------------------------------------------------------
    // M3U parsing (pure function of playlist text).
    // ------------------------------------------------------------------

    fun parseM3u(txt: String): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        val seen = HashSet<String>()
        val lines = txt.lineSequence().map { it.trim() }.toList()
        var i = 0
        while (i < lines.size) {
            val line = lines[i]
            if (line.startsWith("#EXTINF")) {
                val attrs = line.substringAfter("#EXTINF:").substringAfter(",", "")
                val name = line.substringAfterLast(",").trim()
                    .ifBlank { Regex("tvg-name=\"([^\"]+)\"").find(attrs)?.groupValues?.get(1)?.trim() }
                    .ifBlank { null } ?: run { i++; continue }
                val logo = Regex("tvg-logo=\"([^\"]+)\"").find(attrs)?.groupValues?.get(1)?.takeIf { it.startsWith("http") }
                var j = i + 1
                while (j < lines.size && (lines[j].isBlank() || lines[j].startsWith("#"))) j++
                val url = if (j < lines.size) lines[j] else ""
                i = j + 1
                if (!url.startsWith("http") || !seen.add(url)) continue
                out.add(
                    HikariMedia(
                        id = "m3u:$url",
                        title = name,
                        type = HikariMediaType.SERIES,
                        posterUrl = logo,
                    )
                )
            } else {
                i++
            }
        }
        return out
    }
}
