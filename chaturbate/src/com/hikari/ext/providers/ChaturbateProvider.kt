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
 * The room-list API ignores every filter param for anonymous clients (gender,
 * tag, search are all ignored — verified), so the provider fetches a large
 * pool of live rooms (8 pages × 100, cached ~90s and shared by every catalog
 * row and search) and buckets them into category rows client-side: gender
 * (Women/Couples/Men/Trans), tags (Teen, Big Boobs, Latina, Mature, Goth,
 * Curvy, Ebony, New) and Asian via tag + subject + country.
 */
class ChaturbateProvider : HikariProvider {

    override val id = "chaturbate"
    override val name = "Chaturbate (Hikari)"
    override val mainUrl = "https://chaturbate.com"
    override val description = "Live cam rooms — public broadcasts in the built-in player."
    override val iconUrl: String? = null
    override val tvTypes = setOf(HikariMediaType.MOVIE)

    private data class PoolRoom(
        val media: HikariMedia,
        val gender: String,
        val subject: String,
        val tags: Set<String>,
        val country: String,
        val isNew: Boolean,
    )

    companion object {
        private const val POOL_PAGES = 8
        private const val POOL_PAGE_SIZE = 100
        private const val CACHE_TTL_MS = 90_000L
        private const val PAGE_SIZE = 60

        private val ASIAN_COUNTRIES = setOf("KR", "CN", "JP", "HK", "TW", "TH", "VN", "PH", "SG", "MY", "ID")
        private val SUBJECT_ASIAN = listOf("korean", "chinese", "japanese", "asian", "thai", "filipino", "vietnamese")
    }

    // Shared live-room pool, refreshed every CACHE_TTL_MS. All catalog rows and
    // search draw from it, so a home refresh costs one pool build, not one per row.
    private var pool: List<PoolRoom> = emptyList()
    private var poolFetchedAt: Long = 0
    private val poolMutex = Mutex()

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("live", "Live Rooms", HikariMediaType.MOVIE),
        HikariCatalog("women", "Women", HikariMediaType.MOVIE),
        HikariCatalog("couples", "Couples", HikariMediaType.MOVIE),
        HikariCatalog("men", "Men", HikariMediaType.MOVIE),
        HikariCatalog("trans", "Trans", HikariMediaType.MOVIE),
        HikariCatalog("teen", "Teen", HikariMediaType.MOVIE),
        HikariCatalog("bigboobs", "Big Boobs", HikariMediaType.MOVIE),
        HikariCatalog("asian", "Asian", HikariMediaType.MOVIE),
        HikariCatalog("latina", "Latina", HikariMediaType.MOVIE),
        HikariCatalog("mature", "Mature & Milf", HikariMediaType.MOVIE),
        HikariCatalog("goth", "Goth & Alt", HikariMediaType.MOVIE),
        HikariCatalog("curvy", "Curvy & Thick", HikariMediaType.MOVIE),
        HikariCatalog("ebony", "Ebony", HikariMediaType.MOVIE),
        HikariCatalog("new", "New Models", HikariMediaType.MOVIE),
    )

    // ------------------------------------------------------------------
    //  Catalog / search over the shared pool
    // ------------------------------------------------------------------

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val list = filtered(catalog.id)
        val start = (page - 1) * PAGE_SIZE
        if (list.isEmpty() || start >= list.size) return emptyList()
        return list.subList(start, minOf(start + PAGE_SIZE, list.size))
    }

    private suspend fun filtered(catId: String): List<HikariMedia> {
        val rooms = poolRooms()
        if (catId == "live") return rooms.map { it.media }
        val out = mutableListOf<HikariMedia>()
        for (r in rooms) {
            if (inCategory(r, catId)) out += r.media
        }
        return out
    }

    private fun inCategory(r: PoolRoom, catId: String): Boolean = when (catId) {
        "women" -> r.gender == "f"
        "couples" -> r.gender == "c"
        "men" -> r.gender == "m"
        "trans" -> r.gender == "s"
        "teen" -> r.tags.contains("teen")
        "bigboobs" -> r.tags.any { it == "bigboobs" || it == "bigtits" || it == "hugeboobs" }
        "asian" -> r.tags.contains("asian") ||
            SUBJECT_ASIAN.any { r.subject.contains(it) } ||
            r.country in ASIAN_COUNTRIES
        "latina" -> r.tags.contains("latina") || r.subject.contains("latina") || r.subject.contains("hispanic")
        "mature" -> r.tags.any { it == "mature" || it == "milf" || it == "cougar" } ||
            r.subject.contains("milf") || r.subject.contains("mature")
        "goth" -> r.tags.any { it == "goth" || it == "alt" || it == "emo" } ||
            r.subject.contains("goth") || r.subject.contains("gothic")
        "curvy" -> r.tags.any { it == "curvy" || it == "thick" || it == "bbw" }
        "ebony" -> r.tags.contains("ebony") || r.subject.contains("ebony") || r.subject.contains("black")
        "new" -> r.isNew || r.tags.contains("new")
        else -> false
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim().lowercase()
        if (q.isBlank()) return emptyList()
        val out = LinkedHashMap<String, HikariMedia>()
        for (r in poolRooms()) {
            if (r.media.id.contains(q) || r.media.title.contains(q) ||
                r.subject.contains(q) || r.tags.any { it.contains(q) }
            ) {
                out[r.media.id] = r.media
            }
        }
        return out.values.take(PAGE_SIZE * 2)
    }

    // ------------------------------------------------------------------
    //  Pool fetching (shared, TTL-cached)
    // ------------------------------------------------------------------

    private suspend fun poolRooms(): List<PoolRoom> {
        val now = System.currentTimeMillis()
        if (pool.isNotEmpty() && now - poolFetchedAt < CACHE_TTL_MS) return pool
        return poolMutex.withLock {
            val now2 = System.currentTimeMillis()
            if (pool.isNotEmpty() && now2 - poolFetchedAt < CACHE_TTL_MS) return@withLock pool
            val fresh = mutableListOf<PoolRoom>()
            val seen = HashSet<String>()
            for (page in 0 until POOL_PAGES) {
                val json = runCatching {
                    HikariNet.getJson(
                        "https://chaturbate.com/api/ts/roomlist/room-list/?limit=$POOL_PAGE_SIZE&offset=${page * POOL_PAGE_SIZE}"
                    )
                }.getOrNull() ?: break
                val arr = json.optJSONArray("rooms") ?: break
                var any = false
                for (i in 0 until arr.length()) {
                    val r = arr.optJSONObject(i) ?: continue
                    val media = roomToMedia(r) ?: continue
                    if (!seen.add(media.id)) continue
                    fresh += PoolRoom(
                        media = media,
                        gender = r.optString("gender"),
                        subject = r.optString("room_subject").lowercase(),
                        tags = r.optJSONArray("tags")?.let { a ->
                            (0 until a.length()).mapNotNull { a.optString(it).ifBlank { null } }.toSet()
                        } ?: emptySet(),
                        country = r.optString("country").uppercase(),
                        isNew = r.optBoolean("is_new", false),
                    )
                    any = true
                }
                if (!any) break
            }
            if (fresh.isNotEmpty()) {
                pool = fresh
                poolFetchedAt = now2
            }
            pool
        }
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
    //  Meta + streams (from the room page's embedded dossier)
    // ------------------------------------------------------------------

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val page = HikariNet.getString("https://chaturbate.com/${media.id}/") ?: return media
        val dec = decodeDossierQuotes(page)
        val status = fieldOf(dec, "room_status")
        val viewers = fieldOf(dec, "num_viewers")
        val overview = buildString {
            if (status == "public") append("Live now")
            else append("Room currently ${status ?: "offline"}")
            viewers?.let { append(" · ").append(it).append(" viewers") }
            if (!media.overview.isNullOrBlank()) append("\n").append(media.overview)
        }.trim()
        return media.copy(overview = overview.ifBlank { media.overview })
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
