package com.hikari.ext.providers

import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import com.hikari.ext.HikariSubtitle
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import android.util.Base64
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.json.JSONObject
import java.net.URLEncoder
import javax.crypto.Cipher
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec

/**
 * Castle TV (api.hlowb.com) — API-based Indian movies/TV, same backend the
 * "MeowTV"/"CineStream" forks use.
 *
 * The whole API speaks one encrypted envelope: every response body (and the
 * security-key handshake's own responses) is a JSON `{code,msg,data}` where
 * `data` is a base64 AES-128-CBC blob. The key is derived from the per-session
 * security key endpoint plus a fixed pepper, so nothing is ever sent in clear
 * and there's no WebView capture needed — plain HTTP + one AES decrypt is all
 * it takes. Verified live against api.hlowb.com.
 *
 * Endpoints:
 *  - GET  /v0.1/system/getSecurityKey/1?channel=IndiaA&clientType=1&lang=en-US
 *        → {data: "<base64 key>"} (NOT encrypted itself)
 *  - GET  /film-api/v1.1.0/movie/searchByKeyword?...&keyword=Q&page=1&size=30
 *  - GET  /film-api/v1.9.9/movie?channel=IndiaA&clientType=1&lang=en-US&movieId=ID&packageName=com.external.castle
 *  - POST /film-api/v2.0.1/movie/getVideo2 (JSON body, see below)
 *
 * Note: the plugin's `mainUrl` may carry a redirect-id segment (e.g.
 * https://api.hlowb.com/9919952593151901) — every endpoint 404s under that.
 * The real base is https://api.hlowb.com, which this provider always uses.
 */
class CastleProvider : HikariProvider {

    override val id = "castle"
    override val name = "Castle TV"
    override val mainUrl = "https://api.hlowb.com"
    override val description = "Castle TV (Use VLC) — Indian movies & TV via the official Castle API."
    override val tvTypes = setOf(HikariMediaType.MOVIE, HikariMediaType.SERIES)

    companion object {
        private const val BASE = "https://api.hlowb.com"
        private const val PACKAGE = "com.external.castle"
        private const val CHANNEL = "IndiaA"
        private const val APK_SIGN_KEY = "ED0955EB04E67A1D9F3305B95454FED485261475"
        private const val PEPPER = "T!BgJB"
        private const val CASTLE_UA = "okhttp/4.9.3"

        private val CLIENT = OkHttpClient.Builder().followRedirects(true).build()
    }

    private val headers = HikariNet.browserHeaders + mapOf(
        "User-Agent" to CASTLE_UA,
        "Accept" to "application/json",
        "Accept-Language" to "en-US,en;q=0.9",
        "Referer" to "$BASE/",
    )

    private var cachedKey: String? = null

    // ------------------------------------------------------------------
    //  Crypto helpers
    // ------------------------------------------------------------------

    private suspend fun securityKey(): String? {
        cachedKey?.let { return it }
        val url = "$BASE/v0.1/system/getSecurityKey/1?channel=$CHANNEL&clientType=1&lang=en-US"
        val key = HikariNet.getJson(url, headers)?.optString("data")?.takeIf { it.isNotBlank() }
        if (key != null) cachedKey = key
        return key
    }

    private fun decrypt(data: String, base64Key: String): String {
        val material = Base64.decode(base64Key, Base64.DEFAULT) + PEPPER.toByteArray(Charsets.UTF_8)
        val key = ByteArray(16)
        System.arraycopy(material, 0, key, 0, minOf(material.size, 16))
        val cipher = Cipher.getInstance("AES/CBC/PKCS5Padding")
        cipher.init(Cipher.DECRYPT_MODE, SecretKeySpec(key, "AES"), IvParameterSpec(key))
        return String(cipher.doFinal(Base64.decode(data, Base64.DEFAULT)), Charsets.UTF_8)
    }

    /** Unwraps the `{code,msg,data}` envelope: extracts data if it's an
     *  encrypted string, decrypts it, and returns the inner `data` object. */
    private suspend fun unwrap(raw: String, key: String): JSONObject? {
        val cipherText = runCatching {
            val o = JSONObject(raw)
            if (o.has("data") && o.get("data") is String) o.getString("data") else raw
        }.getOrDefault(raw)
        val dec = runCatching { decrypt(cipherText, key) }.getOrNull() ?: return null
        val obj = runCatching { JSONObject(dec) }.getOrNull() ?: return null
        return obj.optJSONObject("data") ?: obj
    }

    private suspend fun apiGet(url: String, key: String): JSONObject? =
        HikariNet.getString(url, headers)?.let { unwrap(it, key) }

    private suspend fun apiPost(url: String, body: JSONObject, key: String): JSONObject? {
        val raw = withContext(Dispatchers.IO) {
            runCatching {
                val req = Request.Builder().url(url)
                    .post(body.toString().toRequestBody("application/json; charset=utf-8".toMediaType()))
                    .apply { headers.forEach { (k, v) -> header(k, v) } }
                    .build()
                CLIENT.newCall(req).execute().use { it.body?.string() }
            }.getOrNull()
        } ?: return null
        return unwrap(raw, key)
    }

    // ------------------------------------------------------------------
    //  Catalog + search
    // ------------------------------------------------------------------

    private fun mediaFrom(row: JSONObject): HikariMedia {
        val type = when (row.optInt("movieType", -1)) {
            1, 3, 5 -> HikariMediaType.SERIES
            else -> HikariMediaType.MOVIE
        }
        return HikariMedia(
            id = row.optString("id").ifBlank { row.optString("redirectId") },
            title = row.optString("title"),
            type = type,
            posterUrl = row.optString("coverVerticalImage")
                .ifBlank { row.optString("coverHorizontalImage") }.ifBlank { null },
            backdropUrl = row.optString("coverHorizontalImage").ifBlank { null },
            overview = row.optString("briefIntroduction").ifBlank { null },
        )
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val key = securityKey() ?: return emptyList()
        val q = URLEncoder.encode(query, "UTF-8").replace("+", "%20")
        val url = "$BASE/film-api/v1.1.0/movie/searchByKeyword?channel=$CHANNEL&clientType=1" +
            "&keyword=$q&lang=en-US&mode=1&packageName=$PACKAGE&page=$page&size=30"
        val data = apiGet(url, key) ?: return emptyList()
        val rows = data.optJSONArray("rows") ?: return emptyList()
        return (0 until rows.length()).mapNotNull { rows.optJSONObject(it)?.let { r -> mediaFrom(r) } }
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val key = securityKey() ?: return media
        val d = apiGet(detailUrl(media.id), key) ?: return media
        return media.copy(
            title = d.optString("title").ifBlank { media.title },
            overview = d.optString("briefIntroduction").ifBlank { media.overview ?: "" }.ifBlank { media.overview },
            genres = d.optJSONArray("tags")
                ?.let { a -> (0 until a.length()).mapNotNull { a.optString(it).ifBlank { null } } }
                ?: media.genres,
            year = d.optString("publishTime").takeIf { it.length >= 4 }?.substring(0, 4)?.toIntOrNull() ?: media.year,
            posterUrl = d.optString("coverVerticalImage").ifBlank { media.posterUrl ?: "" }.ifBlank { media.posterUrl },
            backdropUrl = d.optString("coverHorizontalImage").ifBlank { media.backdropUrl },
        )
    }

    private fun detailUrl(movieId: String) =
        "$BASE/film-api/v1.9.9/movie?channel=$CHANNEL&clientType=1&lang=en-US&movieId=$movieId&packageName=$PACKAGE"

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val key = securityKey() ?: return null
        val d = apiGet(detailUrl(media.id), key) ?: return null
        val out = mutableListOf<HikariEpisode>()

        val seasons = d.optJSONArray("seasons")
        val seasonCount = seasons?.length() ?: 0
        if (seasons != null && seasonCount > 1) {
            for (i in 0 until seasonCount) {
                val s = seasons.optJSONObject(i) ?: continue
                val seasonMovieId = s.optString("movieId").ifBlank { continue }
                val seasonNum = s.optInt("number", i + 1)
                val sd = apiGet(detailUrl(seasonMovieId), key) ?: continue
                sd.optJSONArray("episodes")?.let { eps ->
                    for (j in 0 until eps.length()) {
                        eps.optJSONObject(j)?.let { ep -> out += epFrom(ep, seasonNum, seasonMovieId) }
                    }
                }
            }
        } else {
            val sourceMovieId = d.optString("id").ifBlank { media.id }
            val seasonNum = d.optInt("seasonNumber", 1)
            d.optJSONArray("episodes")?.let { eps ->
                for (j in 0 until eps.length()) {
                    eps.optJSONObject(j)?.let { ep -> out += epFrom(ep, seasonNum, sourceMovieId) }
                }
            }
        }
        out.sortWith(compareBy({ it.season }, { it.number }))
        return out
    }

    /** Episode id carries `<sourceMovieId>:<episodeId>` so getStreams can pick
     *  the right movieId (seasons have their own movieId on this API). */
    private fun epFrom(ep: JSONObject, season: Int, sourceMovieId: String): HikariEpisode {
        val epId = ep.optString("id")
        return HikariEpisode(
            number = ep.optInt("number", 1),
            id = "$sourceMovieId:$epId",
            name = ep.optString("title").ifBlank { null },
            image = ep.optString("coverImage").ifBlank { null },
            season = season,
        )
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val key = securityKey() ?: return emptyList()

        val (movieId, episodeId) = if (episode != null && episode.id.contains(":")) {
            episode.id.substringBefore(":") to episode.id.substringAfter(":")
        } else {
            var epId = episode?.id.orEmpty()
            if (epId.isBlank()) {
                // Movie playback — the API wants an episodeId, so take the
                // movie's own single episode (fall back to the movie id).
                val d = apiGet(detailUrl(media.id), key)
                epId = d?.optJSONArray("episodes")?.optJSONObject(0)?.optString("id")
                    ?.takeIf { it.isNotBlank() } ?: media.id
            }
            media.id to epId
        }

        // The episode's tracks decide which (language, shared-vs-individual)
        // video requests are valid — mirror the plugin's plan exactly.
        val tracks = mutableListOf<JSONObject>()
        var hasIndividual = false
        apiGet(detailUrl(movieId), key)?.optJSONArray("episodes")?.let { eps ->
            for (i in 0 until eps.length()) {
                val ep = eps.optJSONObject(i) ?: continue
                if (ep.optString("id") != episodeId) continue
                ep.optJSONArray("tracks")?.let { ts ->
                    for (j in 0 until ts.length()) {
                        ts.optJSONObject(j)?.let { t ->
                            tracks += t
                            if (t.optBoolean("existIndividualVideo", false)) hasIndividual = true
                        }
                    }
                }
                break
            }
        }

        val plan = mutableListOf<Pair<Int?, String>>()
        when {
            hasIndividual -> for (t in tracks) {
                plan += t.optInt("languageId", -1).takeIf { it != -1 } to
                    t.optString("languageName").ifBlank { t.optString("abbreviate") }
            }
            tracks.isNotEmpty() -> {
                val t = tracks.first()
                plan += t.optInt("languageId", -1).takeIf { it != -1 } to
                    t.optString("languageName").ifBlank { t.optString("abbreviate") }
            }
            else -> plan += null to ""
        }

        val resolutions = listOf(3, 2, 1)
        val qualityLabel = mapOf(3 to "1080p", 2 to "720p", 1 to "480p")
        val streams = mutableListOf<HikariStream>()
        val subtitles = mutableListOf<HikariSubtitle>()
        val seen = HashSet<String>()

        for ((langId, langName) in plan) {
            for (res in resolutions) {
                val body = JSONObject().apply {
                    put("mode", "1")
                    put("appMarket", "GuanWang")
                    put("clientType", "1")
                    put("woolUser", "false")
                    put("apkSignKey", APK_SIGN_KEY)
                    put("androidVersion", "13")
                    put("movieId", movieId)
                    put("episodeId", episodeId)
                    put("isNewUser", "true")
                    put("resolution", res.toString())
                    put("packageName", PACKAGE)
                    langId?.let { put("languageId", it.toString()) }
                }
                val url = "$BASE/film-api/v2.0.1/movie/getVideo2?clientType=1" +
                    "&packageName=$PACKAGE&channel=$CHANNEL&lang=en-US"
                val data = apiPost(url, body, key) ?: continue
                val videoUrl = data.optString("videoUrl").ifBlank { data.optString("url") }
                if (videoUrl.isBlank()) continue

                val q = qualityLabel[res] ?: "${res}p"
                val name = if (langName.isNotBlank()) "$q · $langName" else q
                if (seen.add(name)) {
                    streams += HikariStream(
                        name = name,
                        url = videoUrl,
                        headers = mapOf("Referer" to "$BASE/", "User-Agent" to CASTLE_UA),
                        isM3u8 = videoUrl.contains("m3u8"),
                        subtitles = subtitles.toList(),
                    )
                }
                if (subtitles.isEmpty()) {
                    data.optJSONArray("subtitles")?.let { arr ->
                        for (i in 0 until arr.length()) {
                            val s = arr.optJSONObject(i) ?: continue
                            val sUrl = s.optString("url")
                            if (sUrl.isNotBlank()) {
                                subtitles += HikariSubtitle(
                                    s.optString("abbreviate").ifBlank { s.optString("title") }.ifBlank { "Unknown" },
                                    sUrl,
                                )
                            }
                        }
                    }
                }
            }
        }
        return streams
    }
}
