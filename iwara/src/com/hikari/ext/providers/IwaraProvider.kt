package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.json.JSONArray
import org.json.JSONObject
import java.net.URLEncoder
import java.security.MessageDigest

/**
 * Iwara (iwara.tv) — the home of MMD / 3DCG / HMV anime videos, all
 * unauthenticated via the public JSON API.
 *
 *  - Rows come from the `videos` endpoint (`sort=date|trending|likes`), the
 *    dedicated `trending/video` endpoint, and tag-filtered lists
 *    (`videos?tag=<name>`, which also doubles as an anonymous search fallback
 *    since the site's `search` API 500s without a login).
 *  - Detail is `video/{id}` — its top-level `fileUrl` is a signed
 *    `filesq.iwara.tv/file/<uuid>?expires=…&hash=…` manifest URL.
 *  - The playable MP4s come from that manifest: GET it with
 *    `X-Version: <hex sha1 of "lastPathSegment_expires_<key>">` and it
 *    returns a JSON array of `{name, src:{view,download}, type}` quality
 *    variants (the signature key + flow mirror the site's own player and
 *    yt-dlp's iwara extractor). URLs are protocol-relative — prefix `https:`.
 *  - Posters: `https://files.iwara.tv/image/thumbnail/<fileId>/thumbnail-<NN>.jpg`
 *    (`NN` = the video's `thumbnail` index, clamped to `numThumbnails-1`).
 */
class IwaraProvider : HikariProvider {

    override val id = "iwara"
    override val name = "Iwara"
    override val mainUrl = "https://www.iwara.tv"
    override val description = "MMD, 3DCG, HMV and Blender anime videos — latest, trending and most-liked rows with direct MP4 streams. (Search falls back to tag matching since the site's text search needs a login.)"
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.MOVIE)

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? = null

    companion object {
        private const val API = "https://api.iwara.tv"
        private const val PAGE_SIZE = 20
        private const val SIGN_KEY = "mSvL05GfEmeEmsEYfGCnVpEjYgTJraJN"
        private const val THUMB_CDN = "https://files.iwara.tv/image/thumbnail"
        private const val CACHE_TTL_MS = 600_000L

        private val jsonCache = HashMap<String, Pair<Long, String>>()
        private val cacheMutex = Mutex()

        private val tagRows = listOf(
            "mikumikudance" to "MMD",
            "3dcg" to "3DCG",
            "blender" to "Blender",
            "koikatsu" to "Koikatsu",
            "honey_select" to "Honey Select",
            "sfm" to "Source Filmmaker",
            "virt_a_mate" to "Virt-A-Mate",
            "hmv" to "HMV",
        )

        // yt-dlp-style quality preference: higher index = better.
        private val qualityPref = listOf("preview", "120", "240", "360", "540", "720", "1080", "4k", "Source")
    }

    override fun catalogs(): List<HikariCatalog> = buildList {
        add(HikariCatalog("latest", "Latest", HikariMediaType.MOVIE))
        add(HikariCatalog("trending", "Trending", HikariMediaType.MOVIE))
        add(HikariCatalog("popular", "Popular", HikariMediaType.MOVIE))
        add(HikariCatalog("likes", "Most Liked", HikariMediaType.MOVIE))
        for ((tag, label) in tagRows) {
            add(HikariCatalog("tag_$tag", label, HikariMediaType.MOVIE, rawType = tag))
        }
    }

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val apiPage = (page - 1).coerceAtLeast(0)
        val query = when {
            catalog.id == "latest" -> "sort=date"
            catalog.id == "trending" -> null // dedicated endpoint below
            catalog.id == "popular" -> "sort=trending"
            catalog.id == "likes" -> "sort=likes"
            catalog.id.startsWith("tag_") -> "sort=date&tag=${encode(catalog.rawType)}"
            else -> "sort=date"
        }
        val json = if (catalog.id == "trending") {
            getJsonCached("$API/trending/video?limit=$PAGE_SIZE&page=$apiPage") ?: return emptyList()
        } else {
            getJsonCached("$API/videos?limit=$PAGE_SIZE&page=$apiPage&$query") ?: return emptyList()
        }
        return parseResults(json)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()

        // The site's text `search` API is login-gated (500 with no token);
        // still try it first, then fall back to tag matching so search always
        // returns something usable.
        val apiPage = (page - 1).coerceAtLeast(0)
        val json = getJsonCached(
            "$API/search?query=${encode(q)}&type=video&page=$apiPage&limit=$PAGE_SIZE",
            maxAgeMs = 30_000L,
        )
        val arr = json?.optJSONArray("results")
        if (arr == null || arr.length() == 0) {
            val tag = normalizeTag(q)
            if (tag.isBlank()) return emptyList()
            val tagJson = getJsonCached(
                "$API/videos?limit=$PAGE_SIZE&page=$apiPage&sort=date&tag=${encode(tag)}"
            ) ?: return emptyList()
            return parseResults(tagJson)
        }
        return parseResults(json!!)
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val d = getJsonCached("$API/video/${media.id}") ?: return media
        val title = d.optString("title").takeIf { it.isNotBlank() } ?: media.title
        val body = d.optString("body").takeIf { it.isNotBlank() }
        val user = d.optJSONObject("user")
        val userName = user?.optString("name")?.takeIf { it.isNotBlank() }
        val tags = d.optJSONArray("tags")
        val genres = if (tags != null) {
            buildList {
                for (i in 0 until tags.length()) {
                    tags.optJSONObject(i)?.optString("id")?.takeIf { it.isNotBlank() }?.let { add(it) }
                }
            }
        } else emptyList()
        val duration = d.optJSONObject("file")?.optLong("duration", 0L) ?: 0L

        val stats = buildString {
            if (d.optInt("numViews", 0) > 0) append("👁 ").append(d.optInt("numViews", 0)).append(" views")
            if (d.optInt("numLikes", 0) > 0) {
                if (isNotEmpty()) append("  ·  ")
                append("♥ ").append(d.optInt("numLikes", 0))
            }
            if (duration > 0) {
                if (isNotEmpty()) append("  ·  ")
                append("⏱ ").append(formatDuration(duration))
            }
            if (!userName.isNullOrBlank()) {
                if (isNotEmpty()) append("\n")
                append("Uploader: ").append(userName)
            }
        }.trim()
        val overview = buildString {
            if (body != null) {
                append(body)
                if (stats.isNotEmpty()) append("\n\n")
            }
            append(stats)
        }.trim()

        return media.copy(
            title = title,
            posterUrl = media.posterUrl ?: thumbnailUrl(d),
            year = yearOf(d.optString("createdAt")) ?: media.year,
            overview = overview.ifBlank { null },
            genres = if (genres.isNotEmpty()) genres else media.genres,
        )
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val d = getJsonCached("$API/video/${media.id}") ?: return emptyList()
        val fileUrl = d.optString("fileUrl").takeIf { it.startsWith("http") } ?: run {
            val embed = d.optString("embedUrl").takeIf { it.isNotBlank() }
            if (embed != null) {
                return listOf(HikariStream(name = "Open in browser", url = embed, externalUrl = true))
            }
            return emptyList()
        }

        val (lastSeg, expires) = try {
            val u = java.net.URI(fileUrl)
            u.path.substringAfterLast('/').substringBefore('?') to
                (queryParams(fileUrl)["expires"] ?: "")
        } catch (t: Throwable) {
            return emptyList()
        }
        if (lastSeg.isBlank() || expires.isBlank()) return emptyList()

        val xVersion = sha1Hex("${lastSeg}_${expires}_$SIGN_KEY")
        val manifest = HikariNet.getString(fileUrl, mapOf("X-Version" to xVersion)) ?: return emptyList()
        return parseStreams(manifest)
    }

    // ------------------------------------------------------------------
    //  Parsing helpers
    // ------------------------------------------------------------------

    private fun parseResults(json: JSONObject): List<HikariMedia> {
        val arr = json.optJSONArray("results") ?: return emptyList()
        val out = LinkedHashMap<String, HikariMedia>()
        for (i in 0 until arr.length()) {
            val v = arr.optJSONObject(i) ?: continue
            val id = v.optString("id")
            if (id.isBlank()) continue
            val title = v.optString("title").takeIf { it.isNotBlank() } ?: continue
            val user = v.optJSONObject("user")?.optString("name")?.takeIf { it.isNotBlank() }
            out[id] = HikariMedia(
                id = id,
                title = title,
                type = HikariMediaType.MOVIE,
                posterUrl = thumbnailUrl(v),
                year = yearOf(v.optString("createdAt")),
                genres = listOfNotNull(user?.let { "by $it" }),
            )
        }
        return out.values.toList()
    }

    /** Parses the `{name, src:{view,download}, type}` manifest array. */
    private fun parseStreams(manifest: String): List<HikariStream> {
        val items = runCatching {
            val root = if (manifest.trimStart().startsWith("[")) {
                JSONArray(manifest)
            } else {
                JSONArray().put(JSONObject(manifest))
            }
            root
        }.getOrNull() ?: return emptyList()

        val out = ArrayList<HikariStream>()
        for (i in 0 until items.length()) {
            val it = items.optJSONObject(i) ?: continue
            val src = it.optJSONObject("src")
            val rawUrl = when {
                src != null -> src.optString("view").takeIf { u -> u.isNotBlank() }
                    ?: src.optString("download").takeIf { u -> u.isNotBlank() }
                else -> it.optString("url").takeIf { u -> u.isNotBlank() }
            } ?: continue
            val streamUrl = if (rawUrl.startsWith("//")) "https:$rawUrl" else rawUrl
            val name = it.optString("name").takeIf { n -> n.isNotBlank() } ?: "Video"
            out.add(
                HikariStream(
                    name = if (name == "Source" || name.equals("source", true)) "Source" else "${name}p",
                    url = streamUrl,
                    headers = emptyMap(),
                    isM3u8 = false,
                )
            )
        }
        return out.sortedByDescending { qualityRank(it.name.removeSuffix("p")) }
    }

    private fun qualityRank(name: String): Int {
        val lower = name.lowercase()
        val idx = qualityPref.indexOfFirst { it.lowercase() == lower }
        return if (idx >= 0) idx else {
            name.toIntOrNull() ?: 0
        }
    }

    private fun thumbnailUrl(v: JSONObject): String? {
        val file = v.optJSONObject("file") ?: return null
        val fileId = file.optString("id").takeIf { it.isNotBlank() } ?: return null
        val maxIdx = file.optInt("numThumbnails", 16)
        val idx = v.optInt("thumbnail", 0).coerceIn(0, (maxIdx - 1).coerceAtLeast(0))
        return "$THUMB_CDN/$fileId/thumbnail-${idx.toString().padStart(2, '0')}.jpg"
    }

    private fun yearOf(iso: String): Int? =
        iso.take(4).toIntOrNull()

    private fun formatDuration(totalSec: Long): String {
        val h = totalSec / 3600
        val m = (totalSec % 3600) / 60
        val s = totalSec % 60
        return if (h > 0) "%d:%02d:%02d".format(h, m, s) else "%d:%02d".format(m, s)
    }

    private fun normalizeTag(q: String): String {
        val t = q.trim().lowercase().replace(Regex("[^a-z0-9]+"), "_").trim('_')
        return if (t.length in 1..40) t else ""
    }

    private fun queryParams(url: String): Map<String, String> {
        val idx = url.indexOf('?')
        if (idx < 0) return emptyMap()
        val out = HashMap<String, String>()
        for (pair in url.substring(idx + 1).split('&')) {
            val kv = pair.split('=', limit = 2)
            if (kv.size == 2 && kv[0].isNotBlank()) out[kv[0]] = kv[1]
        }
        return out
    }

    private fun encode(s: String): String =
        URLEncoder.encode(s, "UTF-8").replace("+", "%20")

    private fun sha1Hex(input: String): String {
        val md = MessageDigest.getInstance("SHA-1")
        val bytes = md.digest(input.toByteArray(Charsets.UTF_8))
        return bytes.joinToString("") { "%02x".format(it) }
    }

    /** Fetches a JSON API response once per TTL (searches use a short TTL). */
    private suspend fun getJsonCached(url: String, maxAgeMs: Long = CACHE_TTL_MS): JSONObject? {
        val now = System.currentTimeMillis()
        cacheMutex.withLock {
            jsonCache[url]?.let { (t, body) ->
                if (now - t < maxAgeMs) {
                    return runCatching { JSONObject(body) }.getOrNull()
                }
            }
        }
        val body = HikariNet.getString(url) ?: return null
        val parsed = runCatching { JSONObject(body) }.getOrNull() ?: return null
        cacheMutex.withLock {
            if (jsonCache.size > 60) jsonCache.clear()
            jsonCache[url] = now to body
        }
        return parsed
    }
}
