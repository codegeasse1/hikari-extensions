package com.hikari.ext.providers

import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import org.json.JSONArray
import org.json.JSONObject

/**
 * Shared AniList GraphQL helper (mirrors the bundle's HOMEPAGE/SEARCH/INFO
 * queries): POST https://graphql.anilist.co, parse media cards.
 */
object MrAnilist {
    const val URL = "https://graphql.anilist.co"

    private val headers = mapOf(
        "Content-Type" to "application/json",
        "Accept" to "application/json",
        "User-Agent" to "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36",
        "Origin" to "https://anilist.co",
        "Referer" to "https://anilist.co/",
    )

    private const val LIST_QUERY = """
        query (${'$'}page: Int, ${'$'}perPage: Int, ${'$'}sort: [MediaSort], ${'$'}format: MediaFormat, ${'$'}status: MediaStatus) {
            Page(page: ${'$'}page, perPage: ${'$'}perPage) {
                media(type: ANIME, sort: ${'$'}sort, format: ${'$'}format, status: ${'$'}status) {
                    id
                    title { romaji english native }
                    coverImage { large extraLarge }
                    format
                    episodes
                    status
                    seasonYear
                    averageScore
                    genres
                }
            }
        }
    """

    private const val SEARCH_QUERY = """
        query (${'$'}search: String, ${'$'}page: Int, ${'$'}perPage: Int) {
            Page(page: ${'$'}page, perPage: ${'$'}perPage) {
                media(search: ${'$'}search, type: ANIME, sort: SEARCH_MATCH) {
                    id
                    title { romaji english native }
                    coverImage { large extraLarge }
                    bannerImage
                    format
                    episodes
                    status
                    seasonYear
                    averageScore
                    genres
                    description(asHtml: false)
                }
            }
        }
    """

    private const val INFO_QUERY = """
        query (${'$'}id: Int) {
            Media(id: ${'$'}id, type: ANIME) {
                id
                title { romaji english native }
                description(asHtml: false)
                coverImage { large extraLarge }
                bannerImage
                format
                seasonYear
                episodes
                status
                averageScore
                genres
            }
        }
    """

    data class Card(
        val id: Int,
        val title: String,
        val poster: String?,
        val banner: String?,
        val year: Int?,
        val genres: List<String>,
        val isMovie: Boolean,
        val episodes: Int?,
        val overview: String?,
    )

    private suspend fun post(query: String, variables: JSONObject): JSONObject? {
        val body = JSONObject().put("query", query).put("variables", variables)
        return HikariNet.postJson(URL, body, headers)
    }

    suspend fun list(sort: List<String>, page: Int, perPage: Int = 24, format: String? = null, status: String? = null): List<Card> {
        val vars = JSONObject().put("page", page).put("perPage", perPage)
            .put("sort", JSONArray(sort))
        if (format != null) vars.put("format", format)
        if (status != null) vars.put("status", status)
        val res = post(LIST_QUERY, vars) ?: return emptyList()
        return cards(res.optJSONObject("data")?.optJSONObject("Page")?.optJSONArray("media"))
    }

    suspend fun search(q: String, page: Int, perPage: Int = 20): List<Card> {
        val vars = JSONObject().put("search", q).put("page", page).put("perPage", perPage)
        val res = post(SEARCH_QUERY, vars) ?: return emptyList()
        return cards(res.optJSONObject("data")?.optJSONObject("Page")?.optJSONArray("media"))
    }

    suspend fun info(id: Int): Card? {
        val res = post(INFO_QUERY, JSONObject().put("id", id)) ?: return null
        val m = res.optJSONObject("data")?.optJSONObject("Media") ?: return null
        return card(m)
    }

    private fun cards(arr: JSONArray?): List<Card> {
        if (arr == null) return emptyList()
        return buildList {
            for (i in 0 until arr.length()) {
                arr.optJSONObject(i)?.let { card(it) }?.let { add(it) }
            }
        }
    }

    private fun card(m: JSONObject): Card? {
        val id = m.optInt("id", 0).takeIf { it > 0 } ?: return null
        val t = m.optJSONObject("title")
        val title = t?.optString("english")?.takeIf { it.isNotBlank() }
            ?: t?.optString("romaji")?.takeIf { it.isNotBlank() }
            ?: t?.optString("native")?.takeIf { it.isNotBlank() } ?: return null
        val c = m.optJSONObject("coverImage")
        val poster = c?.optString("extraLarge")?.takeIf { it.startsWith("http") }
            ?: c?.optString("large")?.takeIf { it.startsWith("http") }
        val banner = m.optString("bannerImage").takeIf { it.startsWith("http") }
        val genres = m.optJSONArray("genres")?.let { g ->
            buildList { for (i in 0 until g.length()) g.optString(i).takeIf { it.isNotBlank() }?.let { add(it) } }
        } ?: emptyList()
        return Card(
            id = id,
            title = title,
            poster = poster,
            banner = banner,
            year = m.optInt("seasonYear", 0).takeIf { it > 0 },
            genres = genres,
            isMovie = m.optString("format").equals("MOVIE", true),
            episodes = m.optInt("episodes", 0).takeIf { it > 0 },
            overview = m.optString("description").takeIf { it.isNotBlank() }?.replace(Regex("<[^>]+>"), "")?.trim(),
        )
    }

    fun HikariMediaFromCard(prefix: String, c: Card): HikariMedia = HikariMedia(
        id = "$prefix:${c.id}",
        title = c.title,
        type = if (c.isMovie) HikariMediaType.MOVIE else HikariMediaType.SERIES,
        posterUrl = c.poster,
        year = c.year,
        overview = c.overview,
        genres = c.genres,
        backdropUrl = c.banner ?: c.poster,
    )
}
