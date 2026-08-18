package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import org.json.JSONObject

/**
 * Chaturbate (chaturbate.com) — live cam rooms as Hikari extensions.
 *
 * Uses only public, unauthenticated endpoints:
 *  - room list:  GET https://chaturbate.com/api/ts/roomlist/room-list/?limit=&offset=
 *  - stream URL: the room page (https://chaturbate.com/<username>/) embeds a
 *    `window.initialRoomDossier` object whose `hls_source` is a signed LL-HLS
 *    playlist for the live broadcast.
 *
 * The room-list API ignores search/filter params, so search scans a few pages
 * of the live list and matches locally (username, subject, tags).
 */
class ChaturbateProvider : HikariProvider {

    override val id = "chaturbate"
    override val name = "Chaturbate (Hikari)"
    override val mainUrl = "https://chaturbate.com"
    override val description = "Live cam rooms — public broadcasts play in the built-in player."
    override val iconUrl: String? = null
    override val tvTypes = setOf(HikariMediaType.MOVIE)

    override fun catalogs() = listOf(
        HikariCatalog("live", "Live Rooms", HikariMediaType.MOVIE),
    )

    // ------------------------------------------------------------------
    //  Room list
    // ------------------------------------------------------------------

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        if (catalog.id != "live") return emptyList()
        return rooms(24, (page - 1) * 24)
    }

    private suspend fun rooms(limit: Int, offset: Int): List<HikariMedia> {
        val json = HikariNet.getJson(
            "https://chaturbate.com/api/ts/roomlist/room-list/?limit=$limit&offset=$offset"
        ) ?: return emptyList()
        val arr = json.optJSONArray("rooms") ?: return emptyList()
        val out = mutableListOf<HikariMedia>()
        for (i in 0 until arr.length()) {
            roomToMedia(arr.optJSONObject(i))?.let { out += it }
        }
        return out
    }

    private fun roomToMedia(r: JSONObject): HikariMedia? {
        val username = r.optString("username").ifBlank { return null }
        val subject = stripHtml(r.optString("room_subject").ifBlank { r.optString("subject") })
        val viewers = r.optInt("num_users", 0)
        val age = r.optInt("display_age", 0)
        val gender = r.optString("gender").ifBlank { "" }
        val location = r.optString("location").ifBlank { "" }
        val tags = r.optJSONArray("tags")?.let { a ->
            (0 until a.length()).mapNotNull { a.optString(it).ifBlank { null } }
        } ?: emptyList()

        val overview = buildString {
            if (subject.isNotBlank()) append(subject)
            val bits = mutableListOf<String>()
            if (viewers > 0) bits += "$viewers viewers"
            if (age > 0) bits += "$age"
            if (gender.isNotBlank()) bits += gender
            if (location.isNotBlank()) bits += location
            if (bits.isNotEmpty()) {
                if (isNotEmpty()) append(" · ")
                append(bits.joinToString(" · "))
            }
        }.trim()

        return HikariMedia(
            id = username,
            title = username,
            type = HikariMediaType.MOVIE,
            posterUrl = r.optString("img").ifBlank { null },
            overview = overview.ifBlank { null },
            genres = tags,
        )
    }

    // ------------------------------------------------------------------
    //  Search (local filter — the API ignores search params)
    // ------------------------------------------------------------------

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim().lowercase()
        if (q.isBlank()) return emptyList()
        val seen = LinkedHashMap<String, HikariMedia>()
        for (offset in intArrayOf(0, 48, 96)) {
            val list = rooms(48, offset)
            if (list.isEmpty()) break
            for (m in list) {
                if (matches(m, q)) seen[m.id] = m
            }
        }
        return seen.values.take(48)
    }

    private fun matches(m: HikariMedia, q: String): Boolean {
        if (m.id.contains(q, ignoreCase = true)) return true
        if (m.title.contains(q, ignoreCase = true)) return true
        if ((m.overview ?: "").contains(q, ignoreCase = true)) return true
        return m.genres.any { it.contains(q, ignoreCase = true) }
    }

    // ------------------------------------------------------------------
    //  Meta + streams (from the room page's embedded dossier)
    // ------------------------------------------------------------------

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val page = HikariNet.getString("https://chaturbate.com/${media.id}/") ?: return media
        val dec = decodeDossierQuotes(page)
        val status = fieldOf(dec, "room_status")
        val viewers = fieldOf(dec, "num_viewers")
        val title = fieldOf(dec, "room_title")
        val overview = buildString {
            if (status == "public") append("Live now")
            else append("Room currently ${status ?: "offline"}")
            viewers?.let { append(" · ").append(it).append(" viewers") }
            if (media.overview.isNullOrBlank()) {
                title?.takeIf { it.isNotBlank() }?.let { append("\n").append(it) }
            } else {
                append("\n").append(media.overview)
            }
        }.trim()
        return media.copy(
            overview = overview.ifBlank { media.overview },
            backdropUrl = media.posterUrl,
        )
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val page = HikariNet.getString("https://chaturbate.com/${media.id}/") ?: return emptyList()
        val hls = extractHlsSource(page) ?: return emptyList()
        return listOf(
            HikariStream(
                name = "Live",
                url = hls,
                headers = mapOf(
                    "Referer" to "https://chaturbate.com/${media.id}/",
                    "User-Agent" to HikariNet.browserHeaders.getValue("User-Agent"),
                ),
                isM3u8 = true,
            )
        )
    }

    // ------------------------------------------------------------------
    //  Dossier parsing
    // ------------------------------------------------------------------

    /** The dossier is one big double-quoted JS string; every inner quote is
     *  escaped as `\u0022`. Decode those so fields become matchable JSON. */
    private fun decodeDossierQuotes(page: String): String =
        page.replace("""\u0022""", "\"")

    /** Extracts `"key": "value"` (or a bare number) from the decoded dossier. */
    private fun fieldOf(decoded: String, key: String): String? {
        Regex("\"$key\"\\s*:\\s*\"((?:[^\"\\\\]|\\\\.)*?)\"")
            .find(decoded)?.let { return unescapeUnicode(it.groupValues[1]) }
        return Regex("\"$key\"\\s*:\\s*(-?\\d+)").find(decoded)?.groupValues?.get(1)
    }

    /** The signed LL-HLS playlist URL for the live broadcast. */
    private fun extractHlsSource(page: String): String? {
        val decoded = decodeDossierQuotes(page)
        return Regex(""""hls_source"\s*:\s*"((?:[^"\\]|\\.)*?)"""")
            .find(decoded)?.groupValues?.get(1)?.let { unescapeUnicode(it) }
    }

    private fun unescapeUnicode(s: String): String =
        Regex("""\\u([0-9a-fA-F]{4})""").replace(s) { m ->
            m.groupValues[1].toInt(16).toChar().toString()
        }

    private fun stripHtml(s: String): String =
        s.replace(Regex("""<[^>]+>"""), "").replace(Regex("""\s+"""), " ").trim()
}
