package com.hikari.ext.providers

import android.util.Base64
import com.hikari.ext.HikariNet
import kotlinx.coroutines.delay
import org.json.JSONObject
import java.net.URLEncoder

/**
 * GoTaku site API (sealed JSON bodies via [GtCrypto]), mirroring GoTakuApi.
 */
object GtApi {
    private const val DEFAULT_SITE = "https://gotaku.to"

    @Volatile private var site = DEFAULT_SITE
    @Volatile private var lastDomainLoad = 0L

    suspend fun refreshDomain() {
        val now = System.currentTimeMillis()
        if (now - 5 * 60 * 1000L < lastDomainLoad) return
        lastDomainLoad = now
        try {
            val o = JSONObject(HikariNet.getString("https://cloudstreampluginhelper-default-rtdb.firebaseio.com/.json") ?: return)
            val v = o.optString("gotaku").trim().removeSuffix("/")
            if (v.startsWith("http")) site = v
        } catch (t: Throwable) { }
    }

    fun site(): String = site
    private fun api(): String = "${site()}/api/v1"

    val browserHeaders = mapOf(
        "User-Agent" to "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept" to "*/*",
        "Accept-Language" to "en-US,en;q=0.9",
        "Sec-Fetch-Dest" to "empty",
        "Sec-Fetch-Mode" to "cors",
        "Sec-Fetch-Site" to "cross-site",
        "sec-ch-ua" to "\"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
        "sec-ch-ua-mobile" to "?0",
        "sec-ch-ua-platform" to "\"Windows\"",
    )

    private fun siteHeaders(extra: Map<String, String> = emptyMap()): Map<String, String> {
        val h = browserHeaders.toMutableMap()
        h["Referer"] = "${site()}/"
        h["Sec-Fetch-Site"] = "same-origin"
        h["Accept"] = "application/json"
        h.putAll(extra)
        return h
    }

    private suspend fun getBody(url: String, headers: Map<String, String>, attempts: Int = 3): ByteArray? {
        repeat(attempts) { attempt ->
            try {
                val bytes = HikariNet.getBytes(url, headers)
                if (bytes != null) return bytes
            } catch (t: Throwable) { }
            delay(700L + attempt * 500L)
        }
        return null
    }

    private suspend fun fetchText(url: String, k: Boolean = false): String? {
        val fullUrl = if (k) "$url${if (url.contains('?')) '&' else '?'}k=1" else url
        val body = getBody(fullUrl, siteHeaders()) ?: return null
        val plain = GtCrypto.unseal(body) ?: body
        return String(plain, Charsets.UTF_8)
    }

    class ManifestInfo(
        val title: String?,
        val source: String,
        val token: String,
        val expiresAt: Long,
        val keySeed: ByteArray,
        val stamp: String,
        val segmentBytes: Int,
    )

    suspend fun fetchManifest(base: String, stamp: String): ManifestInfo? {
        val nonce = GtCrypto.newNonce()
        val path = GtCrypto.buildManifestPath(nonce, stamp)
        val body = getBody("$base/$path", siteHeaders(mapOf("Accept" to "text/plain"))) ?: return null
        val plain = GtCrypto.openManifestResponse(nonce, String(body, Charsets.UTF_8)) ?: return null
        return try {
            val node = JSONObject(String(plain, Charsets.UTF_8))
            val source = node.optString("source").takeIf { it.isNotBlank() } ?: return null
            val token = node.optString("token").takeIf { it.isNotBlank() } ?: return null
            val seedText = node.optString("keySeed").takeIf { it.isNotBlank() } ?: return null
            val seed = Base64.decode(
                seedText.replace('-', '+').replace('_', '/')
                    .let { it + "=".repeat((4 - it.length % 4) % 4) },
                Base64.DEFAULT,
            )
            ManifestInfo(
                title = node.optString("title").takeIf { it.isNotBlank() },
                source = source,
                token = token,
                expiresAt = node.optLong("expiresAt", 0),
                keySeed = seed,
                stamp = node.optString("stamp").takeIf { it.isNotBlank() } ?: stamp,
                segmentBytes = node.optJSONObject("obf")?.optInt("segmentBytes", 0) ?: 0,
            )
        } catch (t: Throwable) {
            null
        }
    }

    suspend fun fetchEmbed(episodeId: String, type: String): String? {
        val text = fetchText("${api()}/episodes/$episodeId/embed?type=$type", k = true) ?: return null
        return runCatching { JSONObject(text).optJSONObject("data")?.optString("url") }
            .getOrNull()?.takeIf { it.isNotBlank() }
    }

    data class EpisodeEntry(
        val id: String?,
        val number: Int?,
        val label: String?,
        val name: String?,
        val thumbnail: String?,
        val airedAt: String?,
        val isFiller: Boolean?,
        val sub: Boolean?,
        val dub: Boolean?,
    )

    suspend fun fetchEpisodes(titleId: String): List<EpisodeEntry> {
        val text = fetchText("${api()}/titles/$titleId/episodes", k = true) ?: return emptyList()
        return try {
            val arr = JSONObject(text).optJSONArray("data") ?: return emptyList()
            buildList {
                for (i in 0 until arr.length()) {
                    val e = arr.optJSONObject(i) ?: continue
                    add(
                        EpisodeEntry(
                            id = e.optString("id").takeIf { it.isNotBlank() },
                            number = e.optInt("number", 0).takeIf { it > 0 },
                            label = e.optString("label").takeIf { it.isNotBlank() },
                            name = e.optString("name").takeIf { it.isNotBlank() },
                            thumbnail = e.optString("thumbnail_url").takeIf { it.startsWith("http") },
                            airedAt = e.optString("aired_at").takeIf { it.isNotBlank() },
                            isFiller = if (e.has("is_filler")) e.optBoolean("is_filler", false) else null,
                            sub = if (e.has("sub")) e.optBoolean("sub", false) else null,
                            dub = if (e.has("dub")) e.optBoolean("dub", false) else null,
                        )
                    )
                }
            }
        } catch (t: Throwable) { emptyList() }
    }

    data class Genre(val name: String?)

    data class TitleEntry(
        val id: String?,
        val name: String?,
        val poster: String?,
        val backdrop: String?,
        val synopsis: String?,
        val format: String?,
        val ageRating: String?,
        val year: Int?,
        val duration: Int?,
        val genres: List<String>,
        val latestSub: Int?,
        val latestDub: Int?,
    )

    suspend fun fetchTitleDetail(titleId: String): TitleEntry? {
        val text = fetchText("${api()}/titles/$titleId") ?: return null
        return try {
            val t = JSONObject(text).optJSONObject("data")?.optJSONObject("title") ?: return null
            parseTitle(t)
        } catch (t: Throwable) { null }
    }

    suspend fun fetchTitles(params: Map<String, String>): Pair<List<TitleEntry>, Boolean> {
        val query = params.entries.joinToString("&") { "${it.key}=${URLEncoder.encode(it.value, "UTF-8")}" }
        val text = fetchText("${api()}/titles?$query") ?: return Pair(emptyList(), false)
        return try {
            val o = JSONObject(text)
            val arr = o.optJSONArray("data") ?: return Pair(emptyList(), false)
            val list = buildList {
                for (i in 0 until arr.length()) {
                    arr.optJSONObject(i)?.let { parseTitle(it) }?.let { add(it) }
                }
            }
            Pair(list, o.optJSONObject("meta")?.optBoolean("has_more", false) == true)
        } catch (t: Throwable) { Pair(emptyList(), false) }
    }

    private fun parseTitle(t: JSONObject): TitleEntry {
        val genres = t.optJSONArray("genres")?.let { arr ->
            buildList { for (i in 0 until arr.length()) arr.optJSONObject(i)?.optString("name")?.takeIf { it.isNotBlank() }?.let { add(it) } }
        } ?: emptyList()
        val counts = t.optJSONObject("episodes")
        return TitleEntry(
            id = t.optString("id").takeIf { it.isNotBlank() },
            name = t.optString("name").takeIf { it.isNotBlank() },
            poster = t.optString("poster_url").takeIf { it.startsWith("http") }
                ?: t.optString("backdrop_url").takeIf { it.startsWith("http") },
            backdrop = t.optString("backdrop_url").takeIf { it.startsWith("http") },
            synopsis = t.optString("synopsis").takeIf { it.isNotBlank() },
            format = t.optString("format").takeIf { it.isNotBlank() },
            ageRating = t.optString("age_rating").takeIf { it.isNotBlank() },
            year = t.optInt("year", 0).takeIf { it > 0 },
            duration = t.optInt("duration_minutes", 0).takeIf { it > 0 },
            genres = genres,
            latestSub = counts?.optInt("latest_sub", 0)?.takeIf { it > 0 },
            latestDub = counts?.optInt("latest_dub", 0)?.takeIf { it > 0 },
        )
    }
}
