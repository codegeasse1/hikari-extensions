package com.hikari.ext.providers

import android.util.Base64
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
import java.security.MessageDigest
import java.security.SecureRandom
import javax.crypto.Cipher
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.SecretKeySpec

/**
 * Xanime (xanime.me) — anime / movies / OVA / ONA via an encrypted GraphQL API.
 *
 * Ported from KSHITIJ8473/raghav Xanime (CloudStream MainAPI) to a native
 * Hikari extension (no CloudStream dependency).
 *
 * API: POST {host}/z2/ with AES-256-GCM sealed {query, variables} envelope.
 * Responses are either plain JSON or a {iv, ct} envelope sealed with the same key.
 * Streams are direct HLS master playlists (probe for #EXTM3U), subtitles are
 * WEBVTT tracks listed per source.
 */
class XanimeProvider : HikariProvider {
    override val id = "xanime"
    override val name = "Xanime"
    override val mainUrl = "https://xanime.me"
    override val description = "Anime, movies, OVA/ONA from xanime.me — latest updates, top rated, genres, sub & dub, direct HLS."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    // sort=.. / type=.. / status=.. / audio=.. / genre=.. encoded as catalog rawType
    private val rows = listOf(
        "sort=field_update" to "Latest Updates",
        "sort=field_date_create" to "Recently Added",
        "sort=field_score" to "Top Rated",
        "type=Movie" to "Movies",
        "type=OVA" to "OVA",
        "type=ONA" to "ONA",
        "status=currently_airing" to "Currently Airing",
        "audio=dub" to "Dubbed",
        "genre=action" to "Action",
        "genre=comedy" to "Comedy",
        "genre=romance" to "Romance",
        "genre=fantasy" to "Fantasy",
        "genre=isekai" to "Isekai",
        "genre=slice_of_life" to "Slice of Life",
        "genre=supernatural" to "Supernatural"
    )

    override fun catalogs(): List<HikariCatalog> =
        rows.map { (data, label) -> HikariCatalog(data, label, HikariMediaType.SERIES, rawType = data) }

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        XApi.refreshDomain()
        val filters = parseFilters(catalog.rawType.ifBlank { catalog.id })
        val items = XApi.browse(
            sortby = filters["sort"],
            genre = filters["genre"],
            type = filters["type"],
            status = filters["status"],
            audio = filters["audio"],
            page = page
        )
        return items.mapNotNull { it.toMedia() }
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank()) return emptyList()
        if (page > 1) return emptyList()
        XApi.refreshDomain()
        return XApi.search(query.trim()).mapNotNull { it.toMedia() }
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        XApi.refreshDomain()
        val aniId = media.id.substringAfterLast("/").substringBefore("?").ifBlank { return media }
        val detail = XApi.detail(aniId) ?: return media
        val type = tvTypeOf(detail.types, hasDub = detail.audio.any { it == "dub" }, isMovieHint = detail.types.any { it.equals("Movie", true) })
        return media.copy(
            id = aniId,
            title = detail.title,
            type = type,
            posterUrl = detail.poster ?: media.posterUrl,
            backdropUrl = detail.background ?: media.backdropUrl,
            overview = detail.description ?: media.overview,
            genres = detail.genres.map { it.replace('_', ' ').replaceFirstChar { c -> c.uppercase() } }.ifEmpty { media.genres },
            year = detail.year ?: media.year
        )
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        XApi.refreshDomain()
        val aniId = media.id.substringAfterLast("/").substringBefore("?").ifBlank { return null }
        val episodes = XApi.episodes(aniId)
        if (episodes.isEmpty()) return null
        val subEps = ArrayList<HikariEpisode>()
        val dubEps = ArrayList<HikariEpisode>()
        var counter = 1
        val single = episodes.size == 1
        for (ep in episodes) {
            val hasSub = ep.audio.any { it == "sub" }
            val hasDub = ep.audio.any { it == "dub" }
            val hasRaw = ep.audio.any { it == "raw" }
            val number = if (ep.index > 0) ep.index else counter++
            if (hasSub || (!hasDub && hasRaw)) {
                val audio = if (hasSub) "sub" else "raw"
                val label = when {
                    single && audio == "sub" -> "Sub"
                    single -> "Raw"
                    audio == "sub" -> ep.title
                    else -> "${ep.title} (Raw)"
                }
                subEps.add(HikariEpisode(number = number, id = "${ep.epId}|$audio", name = label))
            }
            if (hasDub) {
                dubEps.add(HikariEpisode(number = number, id = "${ep.epId}|dub", name = if (single) "Dub" else ep.title))
            }
        }
        // sub first then dub, episode numbers already carry ordering
        return subEps + dubEps
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        XApi.refreshDomain()
        val raw = episode?.id ?: return emptyList()
        val parts = raw.split("|")
        if (parts.size != 2 || parts[0].isBlank() || parts[1].isBlank()) return emptyList()
        val epId = parts[0]
        val audio = parts[1]
        val sources = XApi.sources(epId).filter { it.type == audio }
        if (sources.isEmpty()) return emptyList()
        val out = ArrayList<HikariStream>()
        val sameNameCount = HashMap<String, Int>()
        for (source in sources) {
            val master = HikariNet.getString(source.path, mapOf("User-Agent" to XApi.USER_AGENT)) ?: continue
            if (!master.contains("#EXTM3U")) continue
            val count = (sameNameCount[source.name] ?: 0) + 1
            sameNameCount[source.name] = count
            val base = "Xanime ${source.name.ifBlank { "Server" }}"
            val nameLabel = if (count > 1) "$base $count" else base
            val label = "$nameLabel (${audioLabel(audio)})"
            val subs = if (source.tracks.isNotEmpty() && XApi.tracksAreReal(source)) {
                val seen = HashSet<String>()
                source.tracks.mapNotNull { t ->
                    if (!seen.add(t.url)) null else HikariSubtitle(t.label, t.url)
                }
            } else emptyList()
                HikariStream(
                    name = label,
                    url = source.path,
                    headers = mapOf("User-Agent" to XApi.USER_AGENT, "Referer" to XApi.host() + "/"),
                    subtitles = subs,
                    isM3u8 = true
                )
            )
        }
        return out
    }

    private fun parseFilters(data: String): Map<String, String> {
        val out = HashMap<String, String>()
        for (pair in data.split("&")) {
            val idx = pair.indexOf('=')
            if (idx > 0) out[pair.substring(0, idx)] = pair.substring(idx + 1)
        }
        return out
    }

    private fun tvTypeOf(types: List<String>, hasDub: Boolean = false, isMovieHint: Boolean = false): HikariMediaType {
        return if (types.any { it.equals("Movie", true) } || isMovieHint) HikariMediaType.MOVIE else HikariMediaType.SERIES
    }

    private fun XApi.TitleEntry.toMedia(): HikariMedia? {
        if (aniId.isBlank() || title.isBlank()) return null
        val type = if (types.any { it.equals("Movie", true) }) HikariMediaType.MOVIE else HikariMediaType.SERIES
        return HikariMedia(id = aniId, title = title, type = type, posterUrl = poster, year = year)
    }

    private fun audioLabel(audio: String): String = when (audio) {
        "sub" -> "SUB"
        "dub" -> "DUB"
        else -> "RAW"
    }
}

/** Encrypted GraphQL client for xanime.me (ported from XanimeApi, CloudStream-free). */
private object XApi {
    const val USER_AGENT =
        "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36"
    private const val DEFAULT_HOST = "https://xanime.me"
    private const val API_PATH = "/z2/"
    private const val OBFUSCATION_SECRET = "xanime-ph25-obfuscation-secret-key-2026"
    private const val PAGE_SIZE = 24
    private const val EP_PAGE_SIZE = 60
    private const val DOMAIN_URL = "https://cloudstreampluginhelper-default-rtdb.firebaseio.com/.json"

    @Volatile private var host = DEFAULT_HOST
    @Volatile private var lastDomainFetch = 0L
    private var cachedDomain: String? = null

    suspend fun refreshDomain() {
        val now = System.currentTimeMillis()
        if (now - lastDomainFetch < 5 * 60 * 1000L && cachedDomain != null) {
            cachedDomain?.let { host = it }
            return
        }
        try {
            val txt = HikariNet.getString(DOMAIN_URL) ?: return
            val obj = JSONObject(txt)
            val keys = obj.keys()
            while (keys.hasNext()) {
                val k = keys.next()
                if (k.equals("xanime", true) || k.equals("xanime_url", true) || k.equals("xanime_domain", true)) {
                    val v = obj.optString(k).trim().removeSuffix("/")
                    if (v.startsWith("http")) {
                        host = v
                        cachedDomain = v
                        break
                    }
                }
            }
        } catch (_: Exception) {
        } finally {
            lastDomainFetch = now
        }
    }

    fun host(): String = host
    private fun endpoint(): String = host + API_PATH

    private val aesKey by lazy {
        val digest = MessageDigest.getInstance("SHA-256").digest(OBFUSCATION_SECRET.toByteArray(Charsets.UTF_8))
        SecretKeySpec(digest, "AES")
    }
    private val random = SecureRandom()

    private fun seal(payload: JSONObject): String {
        val iv = ByteArray(12).also { random.nextBytes(it) }
        val cipher = Cipher.getInstance("AES/GCM/NoPadding")
        cipher.init(Cipher.ENCRYPT_MODE, aesKey, GCMParameterSpec(128, iv))
        val ct = cipher.doFinal(payload.toString().toByteArray(Charsets.UTF_8))
        return JSONObject()
            .put("v", 1)
            .put("iv", Base64.encodeToString(iv, Base64.NO_WRAP))
            .put("ct", Base64.encodeToString(ct, Base64.NO_WRAP))
            .toString()
    }

    private fun open(envelope: JSONObject): JSONObject? {
        return try {
            val iv = Base64.decode(envelope.optString("iv"), Base64.DEFAULT)
            val ct = Base64.decode(envelope.optString("ct"), Base64.DEFAULT)
            if (iv.isEmpty() || ct.isEmpty()) return null
            val cipher = Cipher.getInstance("AES/GCM/NoPadding")
            cipher.init(Cipher.DECRYPT_MODE, aesKey, GCMParameterSpec(128, iv))
            JSONObject(String(cipher.doFinal(ct), Charsets.UTF_8))
        } catch (_: Exception) { null }
    }

    private fun baseHeaders(): Map<String, String> = mapOf(
        "User-Agent" to USER_AGENT,
        "Accept" to "application/json",
        "Content-Type" to "application/json; charset=utf-8",
        "Origin" to host,
        "Referer" to "$host/"
    )

    private suspend fun post(payload: String): JSONObject? {
        val resp = HikariNet.postString(endpoint(), payload, baseHeaders()) ?: return null
        return try {
            val obj = JSONObject(resp)
            if (obj.has("ct")) open(obj) else obj
        } catch (_: Exception) { null }
    }

    private suspend fun query(query: String, variables: JSONObject): JSONObject? {
        val payload = seal(JSONObject().put("query", query).put("variables", variables))
        var result = post(payload) ?: post(payload)
        if (result == null) return null
        if (result.has("errors")) return null
        val data = result.optJSONObject("data") ?: return null
        val keys = data.keys()
        while (keys.hasNext()) {
            val key = keys.next()
            if (key.startsWith("get_") || key.startsWith("set_") || key.startsWith("put_") || key.startsWith("mut_")) {
                return data.optJSONObject(key)
            }
        }
        return null
    }

    class TitleEntry(val aniId: String, val title: String, val poster: String?, val year: Int?, val types: List<String>, val audio: Set<String>)
    class EpisodeEntry(val epId: String, val index: Int, val title: String, val audio: Set<String>)
    class Track(val label: String, val url: String)
    class Source(val name: String, val type: String, val path: String, val tracks: List<Track>)

    private const val SEARCH_QUERY = """
    query get_q27(${'$'}select: SearchAnime_Select) {
      get_q27(select: ${'$'}select) {
        items {
          id
          data {
            ani_id
            info_title
            urlCover600
            info_meta_type
            info_meta_year
            info_sou_types
          }
        }
      }
    }
    """
    private const val DETAIL_QUERY = """
    query Get_animesNode(${'$'}getAnimesNodeId: String!) {
      get_q02(id: ${'$'}getAnimesNodeId) {
        id
        data {
          info_title
          urlCover600
          urlCoverOri
          info_meta_year
          info_meta_type
          info_meta_genre
          info_meta_scores
          info_meta_status
          info_filmdesc
          info_sou_types
        }
      }
    }
    """
    private const val EPISODES_QUERY = """
    query get_q01(${'$'}select: AnimesEpisodesList_Select) {
      get_q01(select: ${'$'}select) {
        paging { total pages page init size }
        items {
          id
          data {
            ep_id
            ep_index
            ep_title
            sourcesNode_list { id data { src_type } }
          }
        }
      }
    }
    """
    private const val SOURCES_QUERY = """
    query get_q07(${'$'}select: Episodes_Select) {
      get_q07(select: ${'$'}select) {
        id
        data {
          ep_id
          sourcesNode_list {
            id
            data {
              src_name
              src_type
              souPath
              track { label trackPath }
            }
          }
        }
      }
    }
    """

    private fun JSONArray?.toStringList(): List<String> {
        this ?: return emptyList()
        val out = ArrayList<String>(length())
        for (i in 0 until length()) out.add(optString(i))
        return out
    }

    private fun parseTitles(node: JSONObject?): List<TitleEntry> {
        val out = ArrayList<TitleEntry>()
        node ?: return out
        val items = node.optJSONArray("items") ?: return out
        for (i in 0 until items.length()) {
            val entry = items.optJSONObject(i) ?: continue
            val data = entry.optJSONObject("data") ?: continue
            val aniId = data.optString("ani_id").ifBlank { entry.optString("id") }
            val title = data.optString("info_title")
            if (aniId.isBlank() || title.isBlank()) continue
            out.add(
                TitleEntry(
                    aniId, title,
                    data.optString("urlCover600").takeIf { it.isNotBlank() },
                    data.optString("info_meta_year").takeIf { it.isNotBlank() }?.toIntOrNull(),
                    data.optJSONArray("info_meta_type").toStringList(),
                    data.optJSONArray("info_sou_types").toStringList().map { it.lowercase() }.toSet()
                )
            )
        }
        return out
    }

    private fun searchSelect(params: Map<String, Any?>): JSONObject {
        val select = JSONObject()
        select.put("where", "browse")
        select.put("word", params["word"] ?: "")
        select.put("page", (params["page"] as? Int) ?: 1)
        select.put("size", (params["size"] as? Int) ?: PAGE_SIZE)
        select.put("sortby", params["sortby"] ?: "field_update")
        @Suppress("UNCHECKED_CAST")
        select.put("incGenres", JSONArray((params["incGenres"] as? List<*>) ?: emptyList<Any>()))
        select.put("excGenres", JSONArray())
        (params["origStatus"] as? String)?.let { select.put("origStatus", it) }
        (params["type"] as? String)?.let { select.put("type", it) }
        (params["sources"] as? String)?.let { select.put("sources", it) }
        return select
    }

    suspend fun browse(sortby: String? = null, genre: String? = null, type: String? = null, status: String? = null, audio: String? = null, page: Int): List<TitleEntry> {
        val map = HashMap<String, Any?>()
        map["page"] = page
        sortby?.let { map["sortby"] = it }
        genre?.let { map["incGenres"] = listOf(it) }
        type?.let { map["type"] = it }
        status?.let { map["origStatus"] = it }
        audio?.let { map["sources"] = it }
        val variables = JSONObject().put("select", searchSelect(map))
        return parseTitles(query(SEARCH_QUERY, variables))
    }

    suspend fun search(word: String): List<TitleEntry> {
        val variables = JSONObject().put("select", searchSelect(mapOf("word" to word, "sortby" to "field_score")))
        return parseTitles(query(SEARCH_QUERY, variables))
    }

    class Detail(val title: String, val poster: String?, val background: String?, val year: Int?, val types: List<String>, val genres: List<String>, val score: Int?, val status: String?, val audio: Set<String>, val description: String?)

    suspend fun detail(aniId: String): Detail? {
        val node = query(DETAIL_QUERY, JSONObject().put("getAnimesNodeId", aniId)) ?: return null
        val data = node.optJSONObject("data") ?: return null
        val title = data.optString("info_title")
        if (title.isBlank()) return null
        return Detail(
            title,
            data.optString("urlCover600").takeIf { it.isNotBlank() },
            data.optString("urlCoverOri").takeIf { it.isNotBlank() },
            data.optString("info_meta_year").takeIf { it.isNotBlank() }?.toIntOrNull(),
            data.optJSONArray("info_meta_type").toStringList(),
            data.optJSONArray("info_meta_genre").toStringList(),
            data.optInt("info_meta_scores").takeIf { it > 0 },
            data.optString("info_meta_status").takeIf { it.isNotBlank() },
            data.optJSONArray("info_sou_types").toStringList().map { it.lowercase() }.toSet(),
            data.optString("info_filmdesc").takeIf { it.isNotBlank() }
        )
    }

    suspend fun episodes(aniId: String): List<EpisodeEntry> {
        val first = episodePage(aniId, 1) ?: return emptyList()
        if (first.pages <= 1) return first.episodes
        val collected = ArrayList<EpisodeEntry>(first.episodes)
        var page = 2
        while (page <= first.pages) {
            val end = minOf(page + 3, first.pages)
            val batch = coroutineScope { (page..end).map { async { episodePage(aniId, it) } }.awaitAll() }
            batch.forEach { if (it != null) collected.addAll(it.episodes) }
            page = end + 1
        }
        val seen = HashSet<String>()
        return collected.filter { seen.add(it.epId) }.sortedBy { it.index }
    }

    private class EpisodePage(val pages: Int, val episodes: List<EpisodeEntry>)

    private fun parseEpisodePage(node: JSONObject?): EpisodePage? {
        node ?: return null
        val items = node.optJSONArray("items") ?: return null
        val out = ArrayList<EpisodeEntry>(items.length())
        for (i in 0 until items.length()) {
            val entry = items.optJSONObject(i) ?: continue
            val data = entry.optJSONObject("data") ?: continue
            val epId = data.optString("ep_id")
            if (epId.isBlank()) continue
            val audio = HashSet<String>()
            data.optJSONArray("sourcesNode_list")?.let { arr ->
                for (j in 0 until arr.length()) {
                    arr.optJSONObject(j)?.optJSONObject("data")?.optString("src_type")
                        ?.takeIf { it.isNotBlank() }?.let { audio.add(it.lowercase()) }
                }
            }
            out.add(
                EpisodeEntry(
                    epId, data.optInt("ep_index"),
                    data.optString("ep_title").takeIf { it.isNotBlank() } ?: "Episode ${data.optInt("ep_index")}",
                    audio
                )
            )
        }
        val pages = node.optJSONObject("paging")?.optInt("pages")?.takeIf { it > 0 } ?: 1
        return EpisodePage(pages, out)
    }

    private suspend fun episodePage(aniId: String, page: Int): EpisodePage? {
        val select = JSONObject().put("ani_id", aniId).put("init", EP_PAGE_SIZE).put("size", EP_PAGE_SIZE).put("page", page)
        return parseEpisodePage(query(EPISODES_QUERY, JSONObject().put("select", select)))
    }

    suspend fun sources(epId: String): List<Source> {
        val node = query(SOURCES_QUERY, JSONObject().put("select", JSONObject().put("id", epId))) ?: return emptyList()
        val data = node.optJSONObject("data") ?: return emptyList()
        val list = data.optJSONArray("sourcesNode_list") ?: return emptyList()
        val out = ArrayList<Source>(list.length())
        for (i in 0 until list.length()) {
            val src = list.optJSONObject(i)?.optJSONObject("data") ?: continue
            val path = src.optString("souPath").takeIf { it.startsWith("http") } ?: continue
            val tracks = ArrayList<Track>()
            src.optJSONArray("track")?.let { arr ->
                for (j in 0 until arr.length()) {
                    val t = arr.optJSONObject(j) ?: continue
                    val url = t.optString("trackPath").takeIf { it.startsWith("http") } ?: continue
                    tracks.add(Track(t.optString("label").ifBlank { "Subtitle" }, url))
                }
            }
            out.add(Source(src.optString("src_name").ifBlank { "Server" }, src.optString("src_type").lowercase(), path, tracks))
        }
        return out
    }

    suspend fun tracksAreReal(source: Source): Boolean {
        val first = source.tracks.firstOrNull() ?: return true
        val txt = HikariNet.getString(first.url, mapOf("User-Agent" to USER_AGENT)) ?: return true
        return txt.contains("WEBVTT")
    }
}
