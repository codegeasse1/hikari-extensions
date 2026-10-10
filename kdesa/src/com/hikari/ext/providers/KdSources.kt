package com.hikari.ext.providers

import com.hikari.ext.HikariNet
import com.hikari.ext.HikariStream
import com.hikari.ext.HikariSubtitle
import kotlinx.coroutines.delay
import org.json.JSONArray
import org.json.JSONObject
import java.net.URLEncoder
import java.text.Normalizer

/**
 * Kdesa's 9-source resolver fleet (port of KdesaSources).
 *
 * CornClick (TMDB JSON) / 7Movies (playback token + vaplayer) / 1Embed
 * (token + vidsrc/goated/emp/night) / VixSrc (api + tokenized playlist) /
 * Anidap (AniList id -> anidap api -> chad sub/dub providers) / TQQ anime
 * (AniKoto mirrors -> ajax episode/server -> getSources embeds) / Cuevana3
 * (Next.js props -> player var url) / Nova (CF API, per-language best) /
 * FSOnline (slug -> lazy player -> Filemoon/Doodstream). Shared tail:
 * filemoonInline (packed js via KdJsPacker) -> doodInline (pass_md5) ->
 * WebView capture. CF-walled hosts go through the rendered fetch. Masters
 * with separate audio renditions are emitted unsplit (Multi-Audio); plain
 * masters are handed to the player directly (it expands variants itself).
 */
internal class KdSources {

    companion object {
        private const val UA =
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        private const val TMDB_API = "https://api.themoviedb.org/3"
        private const val TMDB_BEARER =
            "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJjYTg3NmZkYmVhMjNhMzI3ODY0ZjRjN2U5MzMwZTYxNiIsIm5iZiI6MTc4MjIwOTQ0NC45OTksInN1YiI6IjZhM2E1YmE0ZmMzZGFiNGNmYzMzNjIxMCIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.WlSOswQDdxdbKu0jARJoruV6PlteoTXB1Oj4gRaibBI"
        private const val CORNCLICK = "https://cornclick.com"
        private const val SEVENMOVIES = "https://7movies.in"
        private const val SEVENMOVIES_EMBED = "https://embed.animecurx.tech"
        private const val ONEEMBED = "https://1embed.cc"
        private const val VIXSRC = "https://vixsrc.to"
        private const val NOVA = "https://novahd.cc"
        private const val FSONLINE = "https://www3.fsonline.app"
        private const val CUEVANA3 = "https://www.cuevana3.eu"
        private const val ANIDAP = "https://anidap.lol"
        private const val ANIDAP_API = "https://chad.anidap.lol"
        private val TQQ_MIRRORS = listOf(
            "https://anikototv.to",
            "https://anikoto.cz",
            "https://anikoto.me",
            "https://anikoto.net",
            "https://anikoto.se",
        )
    }

    private data class KdResp(val code: Int, val body: String)

    private fun hdr(vararg extra: Pair<String, String>): Map<String, String> {
        val map = mutableMapOf(
            "User-Agent" to UA,
            "Accept-Language" to "en-US,en;q=0.9",
        )
        for ((k, v) in extra) map[k] = v
        return map
    }

    private fun isWalled(body: String): Boolean {
        val b = body.lowercase()
        return (b.contains("just a moment") && b.contains("challenge-platform")) ||
            b.contains("checking your browser") ||
            b.contains("checking if the site connection is secure")
    }

    /** Plain fetch with status; CF-walled or failed pages retry rendered. */
    private suspend fun kdGet(url: String, headers: Map<String, String> = emptyMap()): KdResp? {
        try {
            val r = HikariNet.fetch(url, headers)
            if (r != null && r.status in 200..299 && r.body != null && !isWalled(r.body)) {
                return KdResp(r.status, r.body)
            }
            if (r != null && r.status == 403 || r != null && r.status == 503) {
                // fall through to the rendered fetch below
            } else if (r != null && r.status in 200..299 && r.body != null) {
                return KdResp(r.status, r.body)
            } else if (r != null && r.status !in 200..299 && r.status != 403 && r.status != 503) {
                return KdResp(r.status, r.body.orEmpty())
            }
        } catch (_: Exception) {
        }
        return try {
            val smart = HikariNet.getStringSmart(url, headers) ?: return null
            if (isWalled(smart)) return null
            KdResp(200, smart)
        } catch (_: Exception) {
            null
        }
    }

    private suspend fun kdPostJson(url: String, json: String, headers: Map<String, String>): String? {
        return try {
            HikariNet.postString(
                url, json,
                headers + mapOf("Content-Type" to "application/json; charset=utf-8"),
            )
        } catch (_: Exception) {
            null
        }
    }

    private suspend fun kdPostForm(url: String, form: Map<String, String>, headers: Map<String, String>): String? {
        return try {
            val body = form.entries.joinToString("&") {
                "${URLEncoder.encode(it.key, "UTF-8")}=${URLEncoder.encode(it.value, "UTF-8")}"
            }
            HikariNet.postString(
                url, body,
                headers + mapOf("Content-Type" to "application/x-www-form-urlencoded; charset=UTF-8"),
            )
        } catch (_: Exception) {
            null
        }
    }

    private fun jo(text: String): JSONObject? = try {
        JSONObject(text)
    } catch (_: Exception) {
        null
    }

    private fun strip(s: String): String =
        s.replace(Regex("<[^>]+>"), " ").replace(Regex("\\s+"), " ").trim()

    // -- HLS helpers ----------------------------------------------------------

    private data class HlsProbe(
        val audioLangs: List<String>,
        val maxHeight: Int,
        val variants: Int,
        val signature: String,
    )

    private fun prettifyLang(raw: String): String {
        return when (raw.trim().lowercase()) {
            "en", "eng", "english" -> "English"
            "ja", "jp", "jpn", "japanese" -> "Japanese"
            "hi", "hin", "hindi" -> "Hindi"
            "kn", "kan", "kannada" -> "Kannada"
            "ml", "mal", "malayalam" -> "Malayalam"
            "ta", "tam", "tamil" -> "Tamil"
            "te", "tel", "telugu" -> "Telugu"
            "es", "spa", "spanish", "castilian", "latino", "latin" -> "Spanish"
            "fr", "fre", "fra", "french" -> "French"
            "it", "ita", "italian" -> "Italian"
            "de", "ger", "deu", "german" -> "German"
            "pt", "por", "portuguese" -> "Portuguese"
            "ru", "rus", "russian" -> "Russian"
            "ar", "ara", "arabic" -> "Arabic"
            "ko", "kor", "korean" -> "Korean"
            "zh", "chi", "zho", "chinese" -> "Chinese"
            "audio", "default", "und", "original" -> "Original"
            else -> raw.trim()
        }
    }

    private fun audioSuffix(langs: List<String>): String =
        if (langs.size > 1) " (Multi-Audio: ${langs.joinToString(", ") { prettifyLang(it) }})" else ""

    private fun hlsAudioTag(line: String): String? {
        val lang = Regex("""LANGUAGE="([^"]+)"""").find(line)?.groupValues?.get(1)
        val name = Regex("NAME=\"([^\"]+)\"").find(line)?.groupValues?.get(1)
        return (lang ?: name ?: return null).takeIf { it.isNotBlank() }
    }

    private suspend fun probeHls(url: String, headers: Map<String, String>): HlsProbe? {
        return try {
            val text = HikariNet.getString(url, headers) ?: return null
            if (!text.startsWith("#EXTM3U")) return null
            val audioLangs = mutableListOf<String>()
            val hashParts = mutableListOf<String>()
            var maxHeight = 0
            var variants = 0
            for (raw in text.lines()) {
                val l = raw.trim()
                if (l.startsWith("#EXT-X-MEDIA:TYPE=AUDIO")) {
                    hlsAudioTag(l)?.let { audioLangs.add(it) }
                } else if (l.startsWith("#EXT-X-STREAM-INF")) {
                    variants++
                    Regex("""RESOLUTION=\d+x(\d+)""").find(l)?.groupValues?.get(1)
                        ?.toIntOrNull()?.let { if (it > maxHeight) maxHeight = it }
                } else if (!l.startsWith("#") && l.isNotBlank() && hashParts.size < 8) {
                    val path = l.split("?")[0].trimEnd('/')
                    val last2 = path.split("/").takeLast(2).joinToString("/")
                    if (last2.isNotBlank() && last2 != "/") hashParts.add(last2)
                }
            }
            HlsProbe(audioLangs.distinct(), maxHeight, variants, (audioLangs.sorted() + hashParts.sorted()).joinToString("|"))
        } catch (_: Exception) {
            null
        }
    }

    private suspend fun emitHls(
        label: String,
        url: String,
        referer: String,
        headers: Map<String, String>,
        callback: (HikariStream) -> Unit,
        probe: HlsProbe? = null,
    ): Int {
        val info = probe ?: probeHls(url, headers)
        val merged = if (headers.keys.none { it.equals("Referer", true) }) {
            headers + mapOf("Referer" to referer)
        } else headers
        if (info == null) {
            callback(HikariStream(name = label, url = url, headers = merged, isM3u8 = true))
            return 1
        }
        // demuxed-audio masters stay unsplit so every rendition is selectable
        val name = label + audioSuffix(info.audioLangs) +
            (if (info.maxHeight > 0 && info.audioLangs.isEmpty()) " ${info.maxHeight}p" else "")
        callback(HikariStream(name = name, url = url, headers = merged, isM3u8 = true))
        return 1
    }

    // -- fan-out --------------------------------------------------------------

    suspend fun resolveMovie(
        tmdbId: Int,
        title: String,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ): Boolean {
        var any = false
        any = sourceCornClick(tmdbId, null, null, subtitleCallback, callback) || any
        any = sourceSevenMovies(tmdbId, null, null, subtitleCallback, callback) || any
        any = sourceOneEmbed(tmdbId, null, null, subtitleCallback, callback) || any
        any = sourceVixSrc(tmdbId, null, null, subtitleCallback, callback) || any
        any = sourceAnidap(tmdbId, title, 1, subtitleCallback, callback) || any
        any = sourceTqq(title, 1, 1, subtitleCallback, callback) || any
        any = sourceCuevana3(tmdbId, title, null, null, subtitleCallback, callback) || any
        any = sourceNova(tmdbId, null, null, subtitleCallback, callback) || any
        any = sourceFsonline(tmdbId, title, null, null, subtitleCallback, callback) || any
        return any
    }

    suspend fun resolveShow(
        tmdbId: Int,
        title: String,
        season: Int,
        episode: Int,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ): Boolean {
        var any = false
        any = sourceCornClick(tmdbId, season, episode, subtitleCallback, callback) || any
        any = sourceSevenMovies(tmdbId, season, episode, subtitleCallback, callback) || any
        any = sourceOneEmbed(tmdbId, season, episode, subtitleCallback, callback) || any
        any = sourceVixSrc(tmdbId, season, episode, subtitleCallback, callback) || any
        any = sourceAnidap(tmdbId, title, episode, subtitleCallback, callback) || any
        any = sourceTqq(title, season, episode, subtitleCallback, callback) || any
        any = sourceCuevana3(tmdbId, title, season, episode, subtitleCallback, callback) || any
        any = sourceNova(tmdbId, season, episode, subtitleCallback, callback) || any
        any = sourceFsonline(tmdbId, title, season, episode, subtitleCallback, callback) || any
        return any
    }

    // -- CornClick ------------------------------------------------------------

    private suspend fun sourceCornClick(
        tmdbId: Int,
        season: Int?,
        episode: Int?,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ): Boolean {
        return try {
            val path = if (season != null && episode != null) "/player/tv/$tmdbId/$season/$episode"
            else "/player/movie/$tmdbId"
            val res = kdGet(
                "$CORNCLICK$path",
                hdr("Referer" to "$CORNCLICK/", "Origin" to CORNCLICK, "Accept" to "application/json"),
            ) ?: return false
            if (res.code != 200) return false
            val root = jo(res.body) ?: return false
            val sources = root.optJSONArray("sources") ?: return false
            var count = 0
            for (i in 0 until sources.length()) {
                val src = sources.optJSONObject(i) ?: continue
                val url = src.optString("url")
                if (url.isBlank() || !url.startsWith("http")) continue
                val provider = src.optJSONObject("provider")?.optString("id")?.ifBlank { "vaplayer" } ?: "vaplayer"
                val quality = src.optString("quality")
                val type = src.optString("type")
                if (type.equals("hls", true)) {
                    count += emitHls(
                        "$labelCc $provider", url, "$CORNCLICK/",
                        hdr("Referer" to "$CORNCLICK/", "Origin" to CORNCLICK), callback,
                    )
                } else {
                    callback(
                        HikariStream(
                            name = "$labelCc $provider $quality".trim(), url = url,
                            headers = hdr("Referer" to "$CORNCLICK/", "Origin" to CORNCLICK),
                        ),
                    )
                    count++
                }
            }
            val subs = root.optJSONArray("subtitles")
            if (subs != null) {
                for (i in 0 until subs.length()) {
                    val sub = subs.optJSONObject(i) ?: continue
                    val url = sub.optString("url")
                    if (url.isBlank() || url.endsWith(".gz")) continue
                    subtitleCallback(
                        HikariSubtitle(sub.optString("label").ifBlank { sub.optString("lang").ifBlank { "English" } }, url),
                    )
                }
            }
            count > 0
        } catch (_: Exception) {
            false
        }
    }

    private val labelCc = "CornClick"

    // -- 7Movies --------------------------------------------------------------

    private suspend fun sourceSevenMovies(
        tmdbId: Int,
        season: Int?,
        episode: Int?,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ): Boolean {
        val label = "7Movies"
        return try {
            val isShow = season != null && episode != null
            val body = JSONObject()
                .put("tmdbId", tmdbId)
                .put("type", if (isShow) "tv" else "movie")
            if (isShow) {
                body.put("season", season).put("episode", episode)
            }
            val tokenText = kdPostJson(
                "$SEVENMOVIES/api/playback-token", body.toString(),
                hdr("Origin" to SEVENMOVIES, "Referer" to "$SEVENMOVIES/", "Accept" to "application/json"),
            ) ?: return false
            val token = jo(tokenText)?.optString("token")
            if (token.isNullOrBlank()) return false
            val srcPath = if (isShow) "/api/source/tv/$tmdbId/$season/$episode"
            else "/api/source/movie/$tmdbId"
            val srcRes = kdGet(
                "$SEVENMOVIES_EMBED$srcPath?token=${URLEncoder.encode(token, "UTF-8")}&provider=vaplayer",
                hdr("Origin" to SEVENMOVIES, "Referer" to "$SEVENMOVIES/", "Accept" to "application/json"),
            ) ?: return false
            if (srcRes.code != 200) return false
            val root = jo(srcRes.body) ?: return false
            if (root.optBoolean("success", false) != true) return false
            val streams = root.optJSONArray("streams") ?: return false
            var count = 0
            val seenSig = mutableSetOf<String>()
            for (idx in 0 until streams.length()) {
                val stream = streams.optJSONObject(idx) ?: continue
                var url = stream.optString("url")
                if (url.isBlank() || !url.startsWith("http")) {
                    val proxyUrl = stream.optString("proxyUrl")
                    if (proxyUrl.isBlank()) continue
                    val full = if (proxyUrl.startsWith("http")) proxyUrl
                    else "$SEVENMOVIES_EMBED${if (proxyUrl.startsWith("/")) "" else "/"}$proxyUrl"
                    url = try {
                        val inner = java.net.URL(full).query?.split("&")
                            ?.firstOrNull { it.startsWith("url=") }
                            ?.substringAfter("url=")
                            ?.let { java.net.URLDecoder.decode(it, "UTF-8") }
                        if (inner != null && inner.startsWith("http")) inner else full
                    } catch (_: Exception) {
                        full
                    }
                }
                val provider = stream.optString("provider").ifBlank { "stream$idx" }
                val quality = root.optString("quality")
                val probe = probeHls(url, hdr())
                if (probe != null && !seenSig.add(probe.signature)) continue
                count += emitHls(
                    "$label $provider${if (quality.isNotBlank()) " $quality" else ""}",
                    url, "$SEVENMOVIES_EMBED/", hdr(), callback, probe,
                )
            }
            count > 0
        } catch (_: Exception) {
            false
        }
    }

    // -- 1Embed ----------------------------------------------------------------

    @Volatile
    private var oneEmbedToken: Pair<String, Long>? = null

    private suspend fun oneEmbedToken(): String? {
        val cached = oneEmbedToken
        if (cached != null && System.currentTimeMillis() < cached.second) return cached.first
        return try {
            val res = kdGet(
                "$ONEEMBED/api/token",
                hdr("Referer" to "$ONEEMBED/", "Accept" to "application/json"),
            ) ?: return null
            if (res.code != 200) return null
            val token = jo(res.body)?.optString("token")
            if (token.isNullOrBlank()) return null
            oneEmbedToken = token to (System.currentTimeMillis() + 25 * 60 * 1000)
            token
        } catch (_: Exception) {
            null
        }
    }

    private suspend fun sourceOneEmbed(
        tmdbId: Int,
        season: Int?,
        episode: Int?,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ): Boolean {
        val label = "1Embed"
        return try {
            val token = oneEmbedToken() ?: return false
            val isShow = season != null && episode != null
            var any = false
            for (server in listOf("vidsrc", "goated", "emp", "night")) {
                try {
                    val path = if (isShow) "/server/$server/id=$tmdbId?type=tv&season=$season&episode=$episode&_st=$token"
                    else "/server/$server/id=$tmdbId?type=movie&_st=$token"
                    val res = kdGet(
                        "$ONEEMBED$path",
                        hdr("Referer" to "$ONEEMBED/", "Origin" to ONEEMBED, "Accept" to "application/json"),
                    ) ?: continue
                    if (res.code != 200) continue
                    val root = jo(res.body) ?: continue
                    if (root.optBoolean("success", false) != true) continue
                    val streamUrl = root.optString("streamUrl").ifBlank {
                        root.optJSONObject("streams")?.optString("proxy_m3u8").orEmpty()
                    }.ifBlank {
                        root.optJSONObject("streams")?.optString("raw_m3u8").orEmpty()
                    }
                    if (streamUrl.isBlank() || !streamUrl.startsWith("http")) continue
                    val audioNames = ArrayList<String>()
                    root.optJSONArray("audioTracks")?.let { arr ->
                        for (i in 0 until arr.length()) {
                            val a = arr.optJSONObject(i) ?: continue
                            a.optString("name").ifBlank { a.optString("language") }
                                .takeIf { it.isNotBlank() }?.let { audioNames.add(it) }
                        }
                    }
                    val name = when {
                        audioNames.size > 1 -> "$label $server (Multi-Audio: ${audioNames.joinToString(", ")})"
                        audioNames.size == 1 -> "$label $server (${audioNames[0]})"
                        else -> "$label $server"
                    }
                    callback(
                        HikariStream(
                            name = name, url = streamUrl, isM3u8 = true,
                            headers = hdr("Referer" to "$ONEEMBED/"),
                        ),
                    )
                    any = true
                    root.optJSONArray("subtitles")?.let { subs ->
                        for (i in 0 until subs.length()) {
                            val sub = subs.optJSONObject(i) ?: continue
                            val subUrl = sub.optString("url").ifBlank { sub.optString("rawUrl") }
                            if (subUrl.isBlank()) continue
                            val subLabel = sub.optString("label").ifBlank { sub.optString("language") }
                            if (subLabel.isBlank()) continue
                            if (!subUrl.contains(".vtt") && !subUrl.contains(".srt")) continue
                            subtitleCallback(HikariSubtitle(subLabel, subUrl))
                        }
                    }
                } catch (_: Exception) {
                }
            }
            any
        } catch (_: Exception) {
            false
        }
    }

    // -- VixSrc -----------------------------------------------------------------

    private suspend fun sourceVixSrc(
        tmdbId: Int,
        season: Int?,
        episode: Int?,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ): Boolean {
        val label = "VixSrc"
        return try {
            val apiPath = if (season != null && episode != null) "/api/tv/$tmdbId/$season/$episode?lang=it"
            else "/api/movie/$tmdbId?lang=it"
            val apiRes = kdGet(
                "$VIXSRC$apiPath",
                hdr("Referer" to "$VIXSRC/", "Origin" to VIXSRC, "Accept" to "text/html,application/json,*/*"),
            ) ?: return false
            if (apiRes.code != 200) return false
            val src = jo(apiRes.body)?.optString("src")
            if (src.isNullOrBlank()) return false
            val embedUrl = if (src.startsWith("http")) src else VIXSRC + src
            val embedRes = kdGet(embedUrl, hdr("Referer" to "$VIXSRC/", "Origin" to VIXSRC))
                ?: return false
            if (embedRes.code != 200) return false
            val html = embedRes.body
            val masterUrl = Regex("""url:\s*'(https://[^']*playlist/[^']*)'""")
                .find(html)?.groupValues?.get(1)
            val token = Regex("""'token'\s*:\s*'([^']+)'""").find(html)?.groupValues?.get(1)
            val expires = Regex("""'expires'\s*:\s*'([^']+)'""").find(html)?.groupValues?.get(1)
            val asn = Regex("""'asn'\s*:\s*'([^']*)'""").find(html)?.groupValues?.get(1) ?: ""
            val canFhd = html.contains("window.canPlayFHD = true")
            if (masterUrl.isNullOrBlank() || token.isNullOrBlank() || expires.isNullOrBlank()) return false
            val sep = if (masterUrl.contains("?")) "&" else "?"
            var playlist = "$masterUrl${sep}token=$token&expires=$expires&asn=$asn"
            if (canFhd) playlist += "&h=1"
            callback(
                HikariStream(
                    name = "$label (Italian)", url = playlist, isM3u8 = true,
                    headers = hdr("Referer" to "$VIXSRC/", "Origin" to VIXSRC),
                ),
            )
            true
        } catch (_: Exception) {
            false
        }
    }

    // -- Anidap ------------------------------------------------------------------

    private fun normAnime(t: String): String =
        Normalizer.normalize(t, Normalizer.Form.NFD)
            .replace(Regex("[\\p{Mn}]"), "")
            .lowercase()
            .replace(Regex("[^a-z0-9]+"), " ")
            .trim()

    private suspend fun anilistSearch(title: String, year: Int?): Int? {
        return try {
            val query = """
                query (${'$'}search: String, ${'$'}type: MediaType) {
                  Page(page: 1, perPage: 20) {
                    media(search: ${'$'}search, type: ${'$'}type, sort: POPULARITY_DESC) {
                      id
                      seasonYear
                      title { romaji english native }
                    }
                  }
                }
            """.trimIndent()
            val payload = JSONObject()
                .put("query", query)
                .put("variables", JSONObject().put("search", title).put("type", "ANIME"))
                .toString()
            val text = kdPostJson(
                "https://graphql.anilist.co", payload,
                hdr("Accept" to "application/json"),
            ) ?: return null
            val media = jo(text)?.optJSONObject("data")?.optJSONObject("Page")?.optJSONArray("media")
                ?: return null
            if (media.length() == 0) return null
            val want = normAnime(title)
            var best = -1
            var bestId: Int? = null
            var firstId: Int? = null
            for (i in 0 until media.length()) {
                val item = media.optJSONObject(i) ?: continue
                val id = item.optInt("id", 0)
                if (id != 0 && firstId == null) firstId = id
                val titles = listOf("romaji", "english", "native").mapNotNull {
                    item.optJSONObject("title")?.optString(it)?.takeIf { it.isNotBlank() }?.let { t -> normAnime(t) }
                }
                var score = 0
                if (titles.contains(want)) score += 100
                else if (titles.any { it.contains(want) || want.contains(it) }) score += 50
                val sy = item.optInt("seasonYear", 0)
                if (year != null && sy > 0) score += maxOf(0, 20 - kotlin.math.abs(sy - year) * 4)
                if (score > best) {
                    best = score
                    bestId = if (id != 0) id else null
                }
            }
            bestId ?: firstId
        } catch (_: Exception) {
            null
        }
    }

    private suspend fun sourceAnidap(
        tmdbId: Int,
        title: String,
        episode: Int,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ): Boolean {
        val label = "Anidap"
        return try {
            val anilistId = anilistSearch(title, null) ?: return false
            val animeRes = kdGet(
                "$ANIDAP/api/anime/$anilistId",
                hdr("Referer" to "$ANIDAP/", "Origin" to ANIDAP, "Accept" to "application/json"),
            ) ?: return false
            if (animeRes.code != 200) return false
            val animeId = jo(animeRes.body)?.optJSONObject("data")?.optString("id")
            if (animeId.isNullOrBlank()) return false
            var count = 0
            outer@ for (audioType in listOf("sub", "dub")) {
                for (provider in listOf("yuki", "beep", "uwu")) {
                    try {
                        val srcRes = kdGet(
                            "$ANIDAP_API/rest/api/sources?id=${URLEncoder.encode(animeId, "UTF-8")}&epNum=$episode&type=$audioType&providerId=$provider",
                            hdr("Referer" to "$ANIDAP/", "Origin" to ANIDAP, "Accept" to "application/json"),
                        ) ?: continue
                        if (srcRes.code != 200) continue
                        val root = jo(srcRes.body) ?: continue
                        if (root.has("error") || root.optJSONArray("sources") == null) continue
                        val resHeaders = root.optJSONObject("headers")
                        val referer = resHeaders?.optString("Referer")?.ifBlank {
                            resHeaders.optString("referer")
                        }
                        val origin = resHeaders?.optString("Origin")?.ifBlank {
                            resHeaders.optString("origin")
                        }
                        val tracks = root.optJSONArray("tracks")
                        val sources = root.optJSONArray("sources") ?: continue
                        for (i in 0 until sources.length()) {
                            val src = sources.optJSONObject(i) ?: continue
                            val url = src.optString("url")
                            if (url.isBlank() || !url.startsWith("http")) continue
                            val streamHeaders = when {
                                !referer.isNullOrBlank() -> hdr(
                                    "Referer" to referer,
                                    "Origin" to (origin ?: ""),
                                ).filterValues { it.isNotBlank() }
                                Regex("""megap\.|norami\.|shiora\.|megaplay""").containsMatchIn(url) ->
                                    hdr("Referer" to "https://megaplay.buzz/", "Origin" to "https://megaplay.buzz")
                                Regex("""vivibebe|hawk\.aniwatchtv|anizara""").containsMatchIn(url) ->
                                    hdr("Referer" to "$ANIDAP/", "Origin" to ANIDAP)
                                else -> hdr()
                            }
                            val audioLabel = if (audioType == "dub") "English Dub" else "Japanese"
                            count += emitHls(
                                "$label $provider ($audioLabel)", url,
                                streamHeaders["Referer"].orEmpty(), streamHeaders, callback,
                            )
                            if (tracks != null) {
                                for (t in 0 until tracks.length()) {
                                    val track = tracks.optJSONObject(t) ?: continue
                                    val kind = track.optString("kind").ifBlank { "captions" }
                                    if (kind != "captions" && kind != "subtitles") continue
                                    val tUrl = track.optString("url")
                                    if (tUrl.isBlank() || tUrl.contains("thumbnail", true)) continue
                                    if (tUrl.contains(".vtt") || tUrl.contains(".srt")) {
                                        subtitleCallback(
                                            HikariSubtitle(
                                                track.optString("label").ifBlank {
                                                    track.optString("lang").ifBlank { "English" }
                                                }, tUrl,
                                            ),
                                        )
                                    }
                                }
                            }
                        }
                        if (count >= 2) break@outer
                    } catch (_: Exception) {
                    }
                }
            }
            count > 0
        } catch (_: Exception) {
            false
        }
    }

    // -- TQQ anime ----------------------------------------------------------------

    private data class TqqAnime(val slug: String, val title: String, val altTitle: String?, val animeId: String?)

    private fun tqqNorm(t: String): String =
        t.trim().lowercase().replace(Regex("['\":]"), "").replace(Regex("[^a-zA-Z0-9]+"), "_").trim('_')

    private fun tqqSeasonMarker(t: String, season: Int): Boolean {
        val n = tqqNorm(t)
        return n.contains("_season_$season") || n.contains("_${season}st_season") ||
            n.contains("_${season}nd_season") || n.contains("_${season}rd_season") ||
            n.contains("_${season}th_season")
    }

    private fun tqqSpecial(t: String): Boolean {
        val n = tqqNorm(t)
        return n.contains("_special") || n.contains("_ova") || n.contains("_ona") ||
            n.contains("_movie") || n.contains("_reawakening")
    }

    private fun tqqScore(candidate: String, target: String): Int {
        val a = tqqNorm(candidate)
        val b = tqqNorm(target)
        if (a == b) return 1000
        return if (a.contains(b) && b.length >= 4) 100
        else if (b.contains(a) && a.length >= 4) 80 else 0
    }

    private suspend fun tqqSearch(base: String, keyword: String): List<TqqAnime> {
        val res = kdGet(
            "$base/filter?keyword=${URLEncoder.encode(keyword, "UTF-8")}",
            hdr("Referer" to "$base/", "Accept" to "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"),
        ) ?: return emptyList()
        if (res.code != 200) return emptyList()
        val out = mutableListOf<TqqAnime>()
        // #list-items > .item blocks (nested divs: capture to the next item)
        val blocks = Regex(
            """<div[^>]*class="[^"]*\bitem\b[^"]*"[^>]*>([\s\S]*?)(?=<div[^>]*class="[^"]*\bitem\b|$|$|$)""",
        ).findAll(res.body)
        for (b in blocks) {
            val body = b.groupValues[1]
            val href = Regex("""<a[^>]*href="([^"]*/watch/([^"/]+)[^"]*)"""", RegexOption.IGNORE_CASE)
                .find(body)?.groupValues ?: continue
            val slug = href[2]
            if (slug.isBlank()) continue
            val title = Regex(
                """<a[^>]*class="[^"]*name[^"]*d-title[^"]*"[^>]*>([^<]*)<""",
                RegexOption.IGNORE_CASE,
            ).find(body)?.groupValues?.get(1)?.trim()
                ?: Regex(
                    """<a[^>]*class="[^"]*d-title[^"]*name[^"]*"[^>]*>([^<]*)<""",
                    RegexOption.IGNORE_CASE,
                ).find(body)?.groupValues?.get(1)?.trim() ?: continue
            if (title.isBlank()) continue
            val alt = Regex("""data-jp="([^"]+)"""").find(body)?.groupValues?.get(1)?.trim()?.takeIf { it.isNotBlank() }
            val aid = Regex("""data-tip="([^"]+)"""").find(body)?.groupValues?.get(1)?.trim()?.takeIf { it.isNotBlank() }
            out.add(TqqAnime(slug, title, alt, aid))
        }
        return out
    }

    private suspend fun sourceTqq(
        title: String,
        season: Int,
        episode: Int,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ): Boolean {
        for (base in TQQ_MIRRORS) {
            try {
                if (tqqMirror(base, title, season, episode, subtitleCallback, callback)) return true
            } catch (_: Exception) {
            }
        }
        return false
    }

    private data class TqqServer(val type: String, val name: String, val linkId: String)

    private suspend fun tqqMirror(
        base: String,
        title: String,
        season: Int,
        episode: Int,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ): Boolean {
        var candidates = tqqSearch(base, title)
        if (season > 1) candidates = candidates + tqqSearch(base, "$title Season $season")
        if (candidates.isEmpty()) return false
        val marked = candidates.filter { c ->
            listOf(c.title, c.altTitle).filterNotNull().any { tqqSeasonMarker(it, season) }
        }
        val pool = if (season > 1 && marked.isNotEmpty()) marked
        else {
            val nonSpecial = candidates.filter { c ->
                listOf(c.title, c.altTitle).filterNotNull().none { tqqSpecial(it) }
            }
            if (nonSpecial.isNotEmpty()) nonSpecial else candidates
        }
        var best: TqqAnime? = null
        var bestScore = 0
        for (c in pool) {
            val score = listOf(c.title, c.altTitle).filterNotNull()
                .maxOfOrNull { tqqScore(it, title) } ?: 0
            if (score > bestScore) {
                bestScore = score
                best = c
            }
        }
        if (best == null || bestScore < 80) return false
        val anime = best
        val watchRes = kdGet("$base/watch/${anime.slug}/ep-1", hdr("Referer" to "$base/", "Accept" to "text/html"))
            ?: return false
        if (watchRes.code != 200) return false
        val animeId = Regex("""id="watch-main"[^>]*data-id="(\d+)"""").find(watchRes.body)
            ?.groupValues?.get(1)?.takeIf { it.isNotBlank() } ?: anime.animeId
        if (animeId.isNullOrBlank()) return false
        val ajax = hdr(
            "Referer" to "$base/watch/${anime.slug}",
            "X-Requested-With" to "XMLHttpRequest",
            "Accept" to "application/json, text/javascript, */*; q=0.01",
        )
        val epRes = kdGet("$base/ajax/episode/list/$animeId", ajax) ?: return false
        if (epRes.code != 200) return false
        val epHtml = jo(epRes.body)?.optString("result")?.takeIf { it.isNotBlank() } ?: epRes.body
        var serverIds: String? = null
        for (m in Regex("""<a([^>]*)>""").findAll(epHtml)) {
            val attrs = m.groupValues[1]
            val num = Regex("""data-num="(\d+)"""").find(attrs)?.groupValues?.get(1)
            val ids = Regex("""data-ids="([^"]+)"""").find(attrs)?.groupValues?.get(1)
            if (num == episode.toString() && ids != null) {
                serverIds = ids
                break
            }
        }
        if (serverIds.isNullOrBlank()) return false
        val listRes = kdGet("$base/ajax/server/list?servers=${URLEncoder.encode(serverIds, "UTF-8")}", ajax)
            ?: return false
        if (listRes.code != 200) return false
        val listHtml = jo(listRes.body)?.optString("result")?.takeIf { it.isNotBlank() } ?: listRes.body
        val servers = mutableListOf<TqqServer>()
        Regex("""<div[^>]*class="type"[^>]*data-type="([^"]*)"[^>]*>([\s\S]*?)</div>""")
            .findAll(listHtml).forEach { tm ->
                val type = tm.groupValues[1].ifBlank { "sub" }.lowercase()
                Regex("""<li[^>]*data-link-id="([^"]*)"[^>]*>([\s\S]*?)</li>""")
                    .findAll(tm.groupValues[2]).forEach { li ->
                        val linkId = li.groupValues[1]
                        val name = strip(li.groupValues[2])
                        if (linkId.isNotBlank()) servers.add(TqqServer(type, name, linkId))
                    }
            }
        if (servers.isEmpty()) return false
        val typeOrder = mapOf("sub" to 0, "hsub" to 1, "dub" to 2)
        fun nameOrder(n: String): Int {
            val l = n.lowercase()
            return when {
                l.contains("hd-1") -> 0
                l.contains("vidplay") -> 1
                l.contains("vidstream") -> 2
                else -> 3
            }
        }
        servers.sortWith(compareBy({ typeOrder[it.type] ?: 3 }, { nameOrder(it.name) }))
        var emitted = 0
        val seenAudio = mutableSetOf<String>()
        for (server in servers) {
            val audioLang = when (server.type) {
                "dub" -> "en"
                else -> "ja"
            }
            if (seenAudio.contains(audioLang)) continue
            if (emitted >= 4) break
            try {
                val sRes = kdGet("$base/ajax/server?get=${URLEncoder.encode(server.linkId, "UTF-8")}", ajax)
                    ?: continue
                if (sRes.code != 200) continue
                val sRoot = jo(sRes.body) ?: continue
                var embedUrl = sRoot.optJSONObject("result")?.optString("url")
                if (embedUrl.isNullOrBlank() && sRoot.optJSONObject("result") != null) {
                    embedUrl = try {
                        jo(sRoot.optJSONObject("result").toString())?.optString("url")
                    } catch (_: Exception) {
                        null
                    }
                }
                if (embedUrl.isNullOrBlank()) continue
                var eu = embedUrl
                if (server.type == "dub") eu = eu.replace("/sub/", "/dub/")
                else if (eu.contains("/dub/")) eu = eu.replace("/dub/", "/sub/")
                if (tqqEmbed(eu, server.type, server.name, subtitleCallback, callback)) {
                    emitted++
                    seenAudio.add(audioLang)
                }
            } catch (_: Exception) {
            }
        }
        return emitted > 0
    }

    private suspend fun tqqEmbed(
        embedUrl: String,
        audioType: String,
        serverName: String,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ): Boolean {
        return try {
            val host = try {
                val u = java.net.URL(embedUrl)
                "${u.protocol}://${u.host}"
            } catch (_: Exception) {
                return false
            }
            val res = kdGet(
                embedUrl,
                hdr("Referer" to embedUrl, "Accept" to "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"),
            ) ?: return false
            if (res.code != 200) return false
            val html = res.body
            val dataId = Regex("""data-id="(\d+)"""").find(html)?.groupValues?.get(1)
                ?: Regex("""/stream/s-\d+/(\d+)""").find(embedUrl)?.groupValues?.get(1)
            if (dataId.isNullOrBlank()) return false
            val srcRes = kdGet(
                "$host/stream/getSources?id=$dataId",
                hdr(
                    "Referer" to embedUrl, "Origin" to host,
                    "X-Requested-With" to "XMLHttpRequest",
                    "Accept" to "application/json, text/javascript, */*; q=0.01",
                ),
            ) ?: return false
            if (srcRes.code != 200) return false
            val root = jo(srcRes.body) ?: return false
            val sourcesEl = if (root.has("sources")) root.get("sources") else return false
            val m3u8 = when (sourcesEl) {
                is JSONObject -> sourcesEl.optString("file")
                is JSONArray -> if (sourcesEl.length() > 0) sourcesEl.optJSONObject(0)?.optString("file") else null
                else -> null
            }
            if (m3u8.isNullOrBlank()) return false
            val audioLabel = when (audioType) {
                "dub" -> "English Dub"
                "hsub" -> "Japanese (HS)"
                else -> "Japanese Sub"
            }
            val playbackHeaders = hdr("Referer" to "$host/", "Origin" to host)
            val name = if (serverName.isBlank()) "TQQ" else "TQQ $serverName"
            val emitted = emitHls("$name ($audioLabel)", m3u8, "$host/", playbackHeaders, callback)
            if (emitted == 0) return false
            root.optJSONArray("tracks")?.let { tracks ->
                for (i in 0 until tracks.length()) {
                    val track = tracks.optJSONObject(i) ?: continue
                    val kind = track.optString("kind").ifBlank { "captions" }
                    if (kind != "captions" && kind != "subtitles") continue
                    val file = track.optString("url").ifBlank { track.optString("file") }
                    if (file.isBlank()) continue
                    val trackUrl = if (file.startsWith("http")) file else "$host/${file.removePrefix("/")}"
                    val subHeaders = when {
                        trackUrl.contains("lostproject.club") -> mapOf("Referer" to "https://megaplay.buzz/")
                        trackUrl.contains("nekostream.site") -> mapOf("Referer" to "$host/")
                        else -> playbackHeaders
                    }
                    subtitleCallback(
                        HikariSubtitle(track.optString("label").ifBlank { "English" }, trackUrl),
                    )
                    // HikariSubtitle carries no headers; hosts above accept plain fetches
                    if (subHeaders.isEmpty()) return@let
                }
            }
            true
        } catch (_: Exception) {
            false
        }
    }

    // -- Nova -------------------------------------------------------------------

    private suspend fun sourceNova(
        tmdbId: Int,
        season: Int?,
        episode: Int?,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ): Boolean {
        val label = "Nova"
        return try {
            val path = if (season != null && episode != null) "/api/sources?type=show&tmdbId=$tmdbId&season=$season&episode=$episode"
            else "/api/sources?type=movie&tmdbId=$tmdbId"
            val novaHeaders = hdr("Referer" to "$NOVA/", "Origin" to NOVA, "Accept" to "application/json")
            val playbackHeaders = hdr("Referer" to "$NOVA/", "Origin" to NOVA)
            var root: JSONObject? = null
            for (attempt in 1..3) {
                val res = kdGet("$NOVA$path", novaHeaders) ?: continue
                if (res.code != 200) {
                    if (res.code == 403 || res.code == 503) return false
                    continue
                }
                val parsed = jo(res.body) ?: continue
                if (parsed.optBoolean("ready", true) == false) {
                    delay(1200)
                    continue
                }
                root = parsed
                break
            }
            val r = root ?: return false
            val sources = r.optJSONArray("sources") ?: return false
            r.optJSONArray("subtitles")?.let { subs ->
                for (i in 0 until subs.length()) {
                    val sub = subs.optJSONObject(i) ?: continue
                    val url = sub.optString("url")
                    if (url.isBlank()) continue
                    val full = when {
                        url.startsWith("http") -> url
                        url.startsWith("/") -> NOVA + url
                        else -> continue
                    }
                    val subLabel = sub.optString("label").ifBlank { sub.optString("lang") }
                    if (subLabel.isBlank()) continue
                    if (!full.contains(".vtt") && !full.contains(".srt")) continue
                    subtitleCallback(HikariSubtitle(subLabel, full))
                }
            }
            data class NovaSrc(val url: String, val provider: String, val type: String, val quality: String, val lang: String)
            val all = mutableListOf<NovaSrc>()
            for (i in 0 until sources.length()) {
                val src = sources.optJSONObject(i) ?: continue
                val url = src.optString("url")
                if (url.isBlank()) continue
                val full = when {
                    url.startsWith("http") -> url
                    url.startsWith("/") -> NOVA + url
                    else -> continue
                }
                all.add(
                    NovaSrc(
                        full,
                        src.optString("provider").ifBlank { "edge" }.lowercase(),
                        src.optString("type").ifBlank { "hls" }.lowercase(),
                        src.optString("quality"), src.optString("language").trim(),
                    ),
                )
            }
            val providerRank = mapOf("titan" to 0, "orion" to 1, "vega" to 2, "falcon" to 3, "heron" to 4, "orca" to 5)
            fun qRank(q: String) = when {
                q.contains("2160") || q.contains("4k", true) -> 5
                q.contains("1080") -> 4
                q.contains("720") -> 3
                q.contains("480") -> 2
                else -> 3
            }
            val seenKeys = mutableSetOf<String>()
            val groups = all.filter { seenKeys.add("${it.provider}|${it.lang}|${it.quality}") }
                .groupBy { it.lang }
            var count = 0
            for ((lang, list) in groups) {
                val hls = list.filter { it.type != "mp4" }
                    .sortedWith(compareByDescending<NovaSrc> { qRank(it.quality) }.thenBy { providerRank[it.provider] ?: 9 })
                var bestUrl: String? = null
                var bestHeight = 0
                var probedLangs: List<String> = emptyList()
                var apiQuality = ""
                for (cand in hls.take(3)) {
                    val probe = probeHls(cand.url, playbackHeaders) ?: continue
                    bestUrl = cand.url
                    bestHeight = probe.maxHeight
                    probedLangs = probe.audioLangs
                    apiQuality = cand.quality
                    break
                }
                if (bestUrl != null) {
                    val langLabel = when {
                        lang.isNotBlank() -> prettifyLang(lang)
                        probedLangs.size == 1 -> prettifyLang(probedLangs[0])
                        else -> "Auto"
                    }
                    val qPart = when {
                        bestHeight > 0 -> " ${bestHeight}p"
                        apiQuality.isNotBlank() && !apiQuality.equals("Auto", true) -> " $apiQuality"
                        else -> ""
                    }
                    var name = "$label $langLabel$qPart"
                    if (probedLangs.size > 1) name += audioSuffix(probedLangs)
                    callback(HikariStream(name = name, url = bestUrl, headers = playbackHeaders, isM3u8 = true))
                    count++
                } else {
                    val mp4 = list.firstOrNull { it.type == "mp4" }
                    if (mp4 != null) {
                        val langLabel = if (lang.isNotBlank()) prettifyLang(lang) else "Auto"
                        val qPart = mp4.quality.takeIf { it.isNotBlank() && !it.equals("Auto", true) }
                            ?.let { " $it" }.orEmpty()
                        callback(
                            HikariStream(
                                name = "$label $langLabel$qPart (mp4)", url = mp4.url,
                                headers = playbackHeaders,
                            ),
                        )
                        count++
                    }
                }
            }
            count > 0
        } catch (_: Exception) {
            false
        }
    }

    // -- FSOnline ---------------------------------------------------------------

    private fun fsoSlug(title: String): String =
        Normalizer.normalize(title.trim(), Normalizer.Form.NFD)
            .lowercase()
            .replace(Regex("[^a-zA-Z0-9. ]+"), "")
            .replace(".", " ")
            .split(" ").filter { it.isNotBlank() }.joinToString("-")

    private suspend fun sourceFsonline(
        tmdbId: Int,
        title: String,
        season: Int?,
        episode: Int?,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ): Boolean {
        val label = "FSOnline"
        return try {
            val year = tmdbYear(tmdbId, season != null)
            val slugs = mutableListOf<String>()
            if (year != null) slugs.add(fsoSlug("$title $year"))
            slugs.add(fsoSlug(title))
            val pageHeaders = hdr(
                "Referer" to "$FSONLINE/", "Origin" to FSONLINE,
                "sec-fetch-dest" to "iframe", "sec-fetch-mode" to "navigate", "sec-fetch-site" to "cross-site",
            )
            var movieId: String? = null
            var usedSlug = ""
            for (slug in slugs) {
                val url = if (season != null && episode != null) "$FSONLINE/episoade/$slug-sezonul-$season-episodul-$episode/"
                else "$FSONLINE/film/$slug/"
                val res = kdGet(url, pageHeaders) ?: continue
                if (res.code != 200) continue
                movieId = Regex("""id="show_player_lazy"[^>]*movie-id="([^"]+)"""")
                    .find(res.body)?.groupValues?.get(1)
                    ?: Regex("""movie-id="([^"]+)"[^>]*id="show_player_lazy"""")
                        .find(res.body)?.groupValues?.get(1)
                if (movieId != null) {
                    usedSlug = slug
                    break
                }
            }
            if (movieId == null) return false
            val ajaxHtml = kdPostForm(
                "$FSONLINE/wp-admin/admin-ajax.php",
                mapOf("action" to "lazy_player", "movieID" to movieId),
                hdr(
                    "X-Requested-With" to "XMLHttpRequest",
                    "Referer" to "$FSONLINE/film/$usedSlug/",
                    "Origin" to FSONLINE,
                ),
            ) ?: return false
            val options = mutableMapOf<String, String>()
            Regex("""<li[^>]*class="[^"]*dooplay_player_option[^"]*"[^>]*data-vs="([^"]+)"[^>]*>([\s\S]*?)</li>""")
                .findAll(ajaxHtml).forEach { m ->
                    val name = strip(Regex("""<span[^>]*>(.*?)</span>""", RegexOption.DOT_MATCHES_ALL).find(m.groupValues[2])?.groupValues?.get(1).orEmpty())
                    if (name.isNotBlank() && m.groupValues[1].isNotBlank()) options[name] = m.groupValues[1]
                }
            var any = false
            for (hostName in listOf("Filemoon", "Doodstream")) {
                val embed = options[hostName] ?: continue
                if (resolveEmbed(embed, "$FSONLINE/", "$label $hostName", subtitleCallback, callback)) any = true
            }
            any
        } catch (_: Exception) {
            false
        }
    }

    // -- Cuevana3 -----------------------------------------------------------------

    private suspend fun tmdbTitle(type: String, tmdbId: Int, language: String): String? {
        return try {
            val res = kdGet(
                "$TMDB_API/$type/$tmdbId?language=$language",
                hdr("Authorization" to "Bearer $TMDB_BEARER", "Accept" to "application/json"),
            ) ?: return null
            if (res.code != 200) return null
            val root = jo(res.body) ?: return null
            root.optString("title").ifBlank { root.optString("name") }.ifBlank { null }
        } catch (_: Exception) {
            null
        }
    }

    private suspend fun tmdbYear(tmdbId: Int, isTv: Boolean): Int? {
        return try {
            val type = if (isTv) "tv" else "movie"
            val res = kdGet(
                "$TMDB_API/$type/$tmdbId?language=en-US",
                hdr("Authorization" to "Bearer $TMDB_BEARER", "Accept" to "application/json"),
            ) ?: return null
            if (res.code != 200) return null
            val root = jo(res.body) ?: return null
            (root.optString("release_date").ifBlank { root.optString("first_air_date") })
                .take(4).toIntOrNull()
        } catch (_: Exception) {
            null
        }
    }

    private fun cuevanaSlug(title: String): String =
        Normalizer.normalize(title, Normalizer.Form.NFD)
            .replace(Regex("[\\u0300-\\u036f]"), "")
            .lowercase()
            .replace(Regex("[^a-z0-9\\s-]"), "")
            .replace(Regex("\\s+"), "-")
            .replace(Regex("-+"), "-")
            .trim('-')

    private suspend fun sourceCuevana3(
        tmdbId: Int,
        title: String,
        season: Int?,
        episode: Int?,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ): Boolean {
        val label = "Cuevana3"
        return try {
            val isShow = season != null && episode != null
            val type = if (isShow) "tv" else "movie"
            val esTitle = tmdbTitle(type, tmdbId, "es-ES")
            val candidates = mutableListOf<String>()
            if (!esTitle.isNullOrBlank()) {
                cuevanaSlug(esTitle).takeIf { it.isNotBlank() }?.let { candidates.add(it) }
            }
            cuevanaSlug(title).takeIf { it.isNotBlank() && it != candidates.firstOrNull() }
                ?.let { candidates.add(it) }
            for (slug in candidates) {
                val url = if (isShow) "$CUEVANA3/episodio/$slug-temporada-$season-episodio-$episode"
                else "$CUEVANA3/ver-pelicula/$slug"
                val res = kdGet(url, hdr()) ?: continue
                if (res.code != 200) continue
                val html = res.body
                val jsonStart = html.indexOf("{\"props\":{\"pageProps\":")
                if (jsonStart == -1) continue
                val jsonEnd = html.indexOf("</script>", jsonStart)
                if (jsonEnd == -1) continue
                val root = jo(html.substring(jsonStart, jsonEnd).trim().trimEnd(';')) ?: continue
                val pageProps = root.optJSONObject("props")?.optJSONObject("pageProps") ?: continue
                val videosNode = if (isShow) pageProps.optJSONObject("episode")?.optJSONObject("videos")
                else pageProps.optJSONObject("thisMovie")?.optJSONObject("videos")
                if (videosNode == null) continue
                var any = false
                val keys = videosNode.keys()
                while (keys.hasNext()) {
                    val langField = keys.next()
                    val arr = videosNode.optJSONArray(langField) ?: continue
                    var langDone = false
                    for (i in 0 until arr.length()) {
                        if (langDone) break
                        val resultUrl = arr.optJSONObject(i)?.optString("result")
                        if (resultUrl.isNullOrBlank()) continue
                        try {
                            val playerRes = kdGet(resultUrl, hdr()) ?: continue
                            if (playerRes.code != 200) continue
                            val embed = Regex("""var url = '([^']+)'""")
                                .find(playerRes.body)?.groupValues?.get(1) ?: continue
                            val langLabel = when (langField) {
                                "latino" -> "Latino"
                                "spanish" -> "Spanish"
                                "english" -> "English"
                                "japanese" -> "Japanese"
                                else -> langField
                            }
                            val host = try {
                                java.net.URL(embed).host
                            } catch (_: Exception) {
                                continue
                            }
                            val hostLabel = when {
                                host.contains("filemoon") -> "Filemoon"
                                host.contains("streamwish") -> "StreamWish"
                                host.contains("vidhide") -> "VidHide"
                                host.contains("voe") -> "Voe"
                                else -> continue
                            }
                            if (resolveEmbed(embed, "$CUEVANA3/", "$label $langLabel ($hostLabel)", subtitleCallback, callback)) {
                                any = true
                                langDone = true
                            }
                        } catch (_: Exception) {
                        }
                    }
                }
                if (any) return true
            }
            false
        } catch (_:Exception) {
            false
        }
    }

    // -- shared embed tail ---------------------------------------------------------

    private suspend fun resolveEmbed(
        url: String,
        referer: String,
        labelPrefix: String,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ): Boolean {
        if (filemoonInline(url, labelPrefix, subtitleCallback, callback)) return true
        if (doodInline(url, labelPrefix, callback)) return true
        if (webViewIntercept(url, referer, labelPrefix, callback)) return true
        return false
    }

    private suspend fun filemoonInline(
        url: String,
        labelPrefix: String,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ): Boolean {
        return try {
            val mobileUa =
                "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Mobile Safari/537.36"
            val origin = try {
                val u = java.net.URL(url)
                "${u.protocol}://${u.host}"
            } catch (_: Exception) {
                return false
            }
            val pageHeaders = mapOf(
                "User-Agent" to mobileUa,
                "Accept" to "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "Referer" to "$origin/",
                "Accept-Language" to "en-GB,en-US;q=0.9,en;q=0.8",
                "Upgrade-Insecure-Requests" to "1",
            )
            val page = kdGet(url, pageHeaders) ?: return false
            if (page.code != 200) return false
            var iframeSrc = Regex("""<iframe[^>]*src="([^"]+)"""", RegexOption.IGNORE_CASE)
                .find(page.body)?.groupValues?.get(1)
            var scriptData: String? = null
            if (iframeSrc != null) {
                if (!iframeSrc.startsWith("http")) {
                    iframeSrc = "$origin${if (iframeSrc.startsWith("/")) "" else "/"}$iframeSrc"
                }
                val iframePage = kdGet(iframeSrc, pageHeaders)
                if (iframePage != null && iframePage.code == 200 &&
                    iframePage.body.contains("eval(function(p,a,c,k,e,d)")
                ) {
                    scriptData = iframePage.body
                }
            }
            if (scriptData == null && page.body.contains("eval(function(p,a,c,k,e,d)")) {
                scriptData = page.body
            }
            if (scriptData.isNullOrBlank()) return false
            val unpacked = KdJsPacker.parseAndUnpack(scriptData) ?: return false
            val m3u8 = Regex("""file:"([^"]+)"""").find(unpacked)?.groupValues?.get(1)
                ?: Regex("""file:\s*"([^"]+)"""").find(unpacked)?.groupValues?.get(1)
            if (m3u8.isNullOrBlank() || !m3u8.startsWith("http")) return false
            emitHls(
                labelPrefix, m3u8, "$origin/",
                mapOf("Referer" to "$origin/", "User-Agent" to mobileUa), callback,
            ) > 0
        } catch (_: Exception) {
            false
        }
    }

    private suspend fun doodInline(
        url: String,
        labelPrefix: String,
        callback: (HikariStream) -> Unit,
    ): Boolean {
        return try {
            val iphoneUa =
                "Mozilla/5.0 (iPhone; CPU iPhone OS 18_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.0 Mobile/15E148 Safari/604.1"
            val pageHeaders = mapOf(
                "User-Agent" to iphoneUa,
                "Accept" to "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language" to "en-US,en;q=0.9",
                "Connection" to "keep-alive",
            )
            val page = kdGet(url, pageHeaders) ?: return false
            if (page.code != 200) return false
            val html = page.body
            val passMd5 = Regex("""\$\.get\(['"](\/pass_md5\/[^'"]+)['"]""")
                .find(html)?.groupValues?.get(1)
            if (passMd5.isNullOrBlank()) return false
            val host = try {
                val u = java.net.URL(url)
                "${u.protocol}://${u.host}"
            } catch (_: Exception) {
                return false
            }
            val token = Regex("""token["']?\s*[:=]\s*["']([^"']+)["']""")
                .find(html)?.groupValues?.get(1)
            val md5Body = kdGet("$host$passMd5", pageHeaders + mapOf("Referer" to url))
                ?.body?.trim()
            if (md5Body.isNullOrBlank() || !md5Body.startsWith("http")) return false
            val random = buildString {
                val chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
                repeat(10) { append(chars[(0..61).random()]) }
            }
            val final = if (token != null) "$md5Body$random?token=$token&expiry=${System.currentTimeMillis()}"
            else md5Body
            callback(
                HikariStream(
                    name = labelPrefix, url = final,
                    headers = mapOf("Referer" to "$host/", "User-Agent" to iphoneUa),
                ),
            )
            true
        } catch (_: Exception) {
            false
        }
    }

    private suspend fun webViewIntercept(
        url: String,
        referer: String,
        labelPrefix: String,
        callback: (HikariStream) -> Unit,
    ): Boolean {
        return try {
            val found = HikariNet.resolveWithWebView(
                url,
                Regex("""(?i)\.(m3u8|mp4)(?:\?|$)"""),
                timeoutMs = 20_000,
                script = """document.querySelector('button,[role="button"],.jw-icon-display,.vds-play-button')?.click();""",
            )
            if (found.isEmpty()) return false
            var any = false
            for (r in found) {
                val headers = mapOf("Referer" to url)
                if (r.url.contains(".m3u8", ignoreCase = true)) {
                    if (emitHls(labelPrefix, r.url, url, r.headers + headers, callback) > 0) any = true
                } else if (r.url.contains(".mp4", ignoreCase = true)) {
                    callback(HikariStream(name = labelPrefix, url = r.url, headers = r.headers + headers))
                    any = true
                }
            }
            any
        } catch (_: Exception) {
            false
        }
    }
}
