package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import android.util.Base64
import java.net.URLEncoder
import java.text.SimpleDateFormat
import java.util.Locale
import org.json.JSONArray
import org.json.JSONObject

/**
 * LiveTV — IPTV channels + live sports events.
 *
 * Native port of KSHITIJ8473/raghav LIVETVProvider (LIVETV + LiveEvents +
 * ProviderManager rolled into one extension, since the plugin registers its
 * sub-providers dynamically from the same backend):
 *  - Channels: the default M3U playlist plus every provider playlist listed
 *    by the backend's encrypted categories.txt (AES via LvCrypto, verbatim
 *    port of LIVETVCryptoUtils). Playlist blobs that come back encrypted are
 *    decrypted before M3U parsing.
 *  - Live events: the backend's encrypted events.txt, grouped by category
 *    (Cricket/Football/…), with cricify match-card posters; per-event
 *    <slug>.txt carries the server links (url + optional "|Header=V" pairs,
 *    .mpd + KID:KEY clearkey entries are labeled [DRM] since the Hikari
 *    player contract has no DRM fields).
 *
 * NOTE: the backend base URL normally arrives via Firebase Remote Config
 * whose keys live in the source's BuildConfig (not in the repo), so this
 * port tries the source's default base URLs (adsflw.xyz, playztv2828.store)
 * directly. If the operator moves hosts, events go empty while the M3U
 * playlist rows keep working.
 */
class LiveTVProvider : HikariProvider {

    override val id = "livetv"
    override val name = "LiveTV"
    override val mainUrl = "https://fifabd.site"
    override val description = "Live TV channels and sports events from IPTV playlists."
    override val version = 2
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private companion object {
        val BASE_URLS = listOf("https://adsflw.xyz", "https://playztv2828.store")
        const val DEFAULT_PLAYLIST = "https://fifabd.site/OPLLX7/LIVE2.m3u"
        const val DALVIK_UA = "Dalvik/2.1.0 (Linux; U; Android 10; SM-A505F)"
        const val MOB_UA =
            "Mozilla/5.0 (Linux; Android 10; Pixel 3 XL) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36"
        const val CARD = "https://live-card-png.cricify.workers.dev/?"
    }

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("events", "🔴 Live Events", HikariMediaType.SERIES),
        HikariCatalog("cricket", "🏏 Cricket", HikariMediaType.SERIES),
        HikariCatalog("football", "⚽ Football", HikariMediaType.SERIES),
        HikariCatalog("all", "All Channels", HikariMediaType.SERIES),
        HikariCatalog("sports", "Sports", HikariMediaType.SERIES),
        HikariCatalog("news", "News", HikariMediaType.SERIES),
        HikariCatalog("entertainment", "Entertainment", HikariMediaType.SERIES),
        HikariCatalog("movies", "Movies", HikariMediaType.SERIES),
        HikariCatalog("kids", "Kids", HikariMediaType.SERIES),
    )

    // ------------------------------------------------------------- backend

    @Volatile private var providersCache: List<JSONObject>? = null

    private suspend fun fetchDecrypted(path: String): String? {
        for (base in BASE_URLS) {
            try {
                val body = HikariNet.getString("$base/$path", mapOf("User-Agent" to DALVIK_UA))
                    ?: continue
                if (body.isBlank()) continue
                LvCrypto.decrypt(body)?.let { return it }
            } catch (_: Exception) {
            }
        }
        return null
    }

    private suspend fun providers(): List<JSONObject> {
        providersCache?.let { return it }
        val out = ArrayList<JSONObject>()
        try {
            val txt = fetchDecrypted("categories.txt") ?: return out
            val arr = JSONArray(txt)
            for (i in 0 until arr.length()) {
                val cat = try {
                    JSONObject(arr.optJSONObject(i)?.optString("cat") ?: continue)
                } catch (_: Exception) {
                    continue
                }
                if (cat.optBoolean("visible", true) == false) continue
                out.add(cat)
            }
        } catch (_: Exception) {
        }
        providersCache = out
        return out
    }

    private data class LiveEvent(
        val title: String, val image: String?, val slug: String, val cat: String,
        val teamA: String?, val teamB: String?, val teamAFlag: String?, val teamBFlag: String?,
        val eventCat: String?, val eventName: String?, val eventLogo: String?,
        val start: String?, val end: String?,
    )

    private suspend fun liveEvents(): List<LiveEvent> {
        val out = ArrayList<LiveEvent>()
        try {
            val txt = fetchDecrypted("events.txt") ?: return out
            val arr = JSONArray(txt)
            for (i in 0 until arr.length()) {
                val ev = try {
                    JSONObject(arr.optJSONObject(i)?.optString("event") ?: continue)
                } catch (_: Exception) {
                    continue
                }
                if (ev.optBoolean("visible", true) == false) continue
                val links = ev.optString("links")
                if (links.isBlank()) continue
                val names = ev.optJSONArray("link_names")
                out.add(
                    LiveEvent(
                        title = ev.optString("eventName").ifBlank { "Event ${i + 1}" },
                        image = ev.optString("eventLogo").ifBlank { null },
                        slug = links.substringBeforeLast("."),
                        cat = ev.optString("category").ifBlank { "Other" },
                        teamA = ev.optString("teamAName").ifBlank { null },
                        teamB = ev.optString("teamBName").ifBlank { null },
                        teamAFlag = ev.optString("teamAFlag").ifBlank { null },
                        teamBFlag = ev.optString("teamBFlag").ifBlank { null },
                        eventCat = ev.optString("category").ifBlank { null },
                        eventName = ev.optString("eventName").ifBlank { null },
                        eventLogo = ev.optString("eventLogo").ifBlank { null },
                        start = parseDateTime(ev.optString("date").ifBlank { null }, ev.optString("time").ifBlank { null }),
                        end = parseDateTime(ev.optString("end_date").ifBlank { null }, ev.optString("end_time").ifBlank { null }),
                    ),
                )
                if (names != null) {
                    // formats kept implicitly via slug streams
                }
            }
        } catch (_: Exception) {
        }
        return out.filter { it.slug.isNotBlank() }
    }

    private fun parseDateTime(date: String?, time: String?): String? {
        if (date == null || time == null) return null
        return try {
            val parts = date.split("/")
            if (parts.size == 3) "${parts[2]}/${parts[1]}/${parts[0]} $time +0000" else null
        } catch (_: Exception) {
            null
        }
    }

    private fun eventMs(s: String?): Long? {
        if (s.isNullOrBlank()) return null
        return try {
            SimpleDateFormat("yyyy/MM/dd HH:mm:ss Z", Locale.US).parse(s)?.time
        } catch (_: Exception) {
            null
        }
    }

    private fun isLive(e: LiveEvent): Boolean {
        val now = System.currentTimeMillis()
        val s = eventMs(e.start) ?: return false
        val en = eventMs(e.end) ?: (s + 3 * 3600_000L)
        return now in s..en
    }

    private fun isEnded(e: LiveEvent): Boolean {
        val en = eventMs(e.end) ?: return false
        return System.currentTimeMillis() > en
    }

    private fun displayTitle(e: LiveEvent): String =
        if (e.teamA != null && e.teamB != null) "${e.teamA} vs ${e.teamB}"
        else e.eventName ?: e.title

    private fun eventStatus(e: LiveEvent): String = when {
        isLive(e) -> "🔴 LIVE "
        isEnded(e) -> "✅ Ended "
        else -> ""
    }

    private fun cardUrl(e: LiveEvent): String? {
        return try {
            val enc: (String) -> String = { URLEncoder.encode(it, "UTF-8") }
            val time = try {
                e.start?.let {
                    val df = SimpleDateFormat("yyyy/MM/dd HH:mm:ss Z", Locale.US)
                    val disp = SimpleDateFormat("MMM dd, yyyy hh:mm a", Locale.US)
                    df.parse(it)?.let { d -> enc(disp.format(d)) } ?: ""
                } ?: ""
            } catch (_: Exception) {
                ""
            }
            buildString {
                append(CARD)
                append("title=" + enc(e.eventName ?: e.title))
                append("&teamA=" + enc(e.teamA ?: "Team A"))
                append("&teamB=" + enc(e.teamB ?: "Team B"))
                if (!e.teamAFlag.isNullOrBlank()) append("&teamAImg=${e.teamAFlag}")
                if (!e.teamBFlag.isNullOrBlank()) append("&teamBImg=${e.teamBFlag}")
                if (!e.eventLogo.isNullOrBlank()) append("&eventLogo=${e.eventLogo}")
                if (time.isNotBlank()) append("&time=$time")
                append("&isLive=${isLive(e)}")
                append("&isEnded=${isEnded(e)}")
            }
        } catch (_: Exception) {
            e.image
        }
    }

    private fun eventMedia(e: LiveEvent): HikariMedia {
        val title = eventStatus(e) + displayTitle(e)
        val payload = JSONObject().put("slug", e.slug).put("title", displayTitle(e))
        val id = "liveev:" + Base64.encodeToString(payload.toString().toByteArray(), Base64.URL_SAFE or Base64.NO_WRAP)
        return HikariMedia(id = id, title = title, type = HikariMediaType.SERIES, posterUrl = cardUrl(e) ?: e.image)
    }

    // -------------------------------------------------------------- channels

    private data class Channel(
        val title: String, val url: String, val logo: String?,
        val group: String, val headers: Map<String, String>,
    )

    private suspend fun allChannels(): List<Channel> {
        val out = ArrayList<Channel>()
        val seen = HashSet<String>()
        // Default playlist.
        try {
            val body = HikariNet.getString(DEFAULT_PLAYLIST, mapOf("User-Agent" to MOB_UA))
            if (body != null) {
                for (c in parseM3u(body)) if (seen.add(c.url)) out.add(c)
            }
        } catch (_: Exception) {
        }
        // Provider playlists (m3u-style providers; custom/event providers feed the events rows).
        try {
            for (p in providers()) {
                val type = p.optString("type", "custom")
                if (type == "custom") continue
                val link = p.optString("api").ifBlank { continue }
                try {
                    val raw = HikariNet.getString(link, mapOf("User-Agent" to DALVIK_UA)) ?: continue
                    val body = LvCrypto.decrypt(raw) ?: raw
                    for (c in parseM3u(body)) if (seen.add(c.url)) out.add(c)
                } catch (_: Exception) {
                }
            }
        } catch (_: Exception) {
        }
        // Custom providers expose channel lists through their catLink endpoints.
        try {
            for (p in providers()) {
                if (p.optString("type", "custom") != "custom") continue
                val link = p.optString("api").ifBlank { continue }
                for (e in customChannels(link)) {
                    if (!seen.add(e.slug)) continue
                    out.add(
                        Channel(
                            title = e.title, url = "slug:${e.slug}", logo = e.image,
                            group = e.cat, headers = emptyMap(),
                        ),
                    )
                }
            }
        } catch (_: Exception) {
        }
        return out
    }

    private suspend fun customChannels(catLink: String): List<LiveEvent> {
        val out = ArrayList<LiveEvent>()
        try {
            val txt = fetchDecrypted(catLink) ?: return out
            val arr = JSONArray(txt)
            for (i in 0 until arr.length()) {
                val c = try {
                    JSONObject(arr.optJSONObject(i)?.optString("channel") ?: continue)
                } catch (_: Exception) {
                    continue
                }
                if (c.optBoolean("visible", true) == false) continue
                val links = c.optString("links")
                if (links.isBlank()) continue
                out.add(
                    LiveEvent(
                        title = c.optString("name").ifBlank { "Channel ${i + 1}" },
                        image = c.optString("logo").ifBlank { null },
                        slug = links.substringBeforeLast(".", links),
                        cat = "Custom",
                        teamA = null, teamB = null, teamAFlag = null, teamBFlag = null,
                        eventCat = null, eventName = c.optString("name"), eventLogo = c.optString("logo"),
                        start = null, end = null,
                    ),
                )
            }
        } catch (_: Exception) {
        }
        return out
    }

    private fun parseM3u(body: String): List<Channel> {
        val out = ArrayList<Channel>()
        var name = ""
        var logo = ""
        var group = ""
        for (line in body.lines()) {
            val l = line.trim()
            if (l.startsWith("#EXTINF")) {
                name = l.substringAfterLast(",").trim()
                logo = Regex("""tvg-logo="([^"]+)"""").find(l)?.groupValues?.get(1).orEmpty()
                group = Regex("""group-title="([^"]+)"""").find(l)?.groupValues?.get(1).orEmpty()
            } else if (l.startsWith("http")) {
                if (name.isNotBlank()) {
                    val (url, headers) = splitLink(l)
                    out.add(Channel(name, url, logo.takeIf { it.startsWith("http") }, group, headers))
                    if (out.size >= 3000) break
                }
                name = ""
            }
        }
        return out
    }

    private fun splitLink(link: String): Pair<String, Map<String, String>> {
        if (!link.contains("|")) return link.replace("%2F", "/") to emptyMap()
        val parts = link.split("|", limit = 2)
        val url = parts[0].trim().replace("%2F", "/")
        val headers = LinkedHashMap<String, String>()
        if (parts.size > 1) {
            for (pair in parts[1].split("&")) {
                val kv = pair.split("=", limit = 2)
                if (kv.size == 2) {
                    val k = kv[0].trim()
                    val name = when (k.lowercase()) {
                        "cookie" -> "Cookie"
                        "origin" -> "Origin"
                        "user-agent" -> "User-Agent"
                        "referer" -> "Referer"
                        else -> k
                    }
                    headers[name] = kv[1].trim()
                }
            }
        }
        return url to headers
    }

    // -------------------------------------------------------------- catalogs

    private fun matchGroup(c: Channel, row: String): Boolean {
        val g = c.group.lowercase()
        val t = c.title.lowercase()
        return when (row) {
            "all" -> true
            "sports" -> g.contains("sport") || t.contains("sport") || g.contains("cricket") || g.contains("football")
            "news" -> g.contains("news") || t.contains("news")
            "entertainment" -> g.contains("entertain") || g.contains("movie").not() && (g.contains("general") || g.contains("music") || t.contains("tv "))
            "movies" -> g.contains("movie") || g.contains("film") || t.contains("movie")
            "kids" -> g.contains("kid") || g.contains("cartoon") || g.contains("toon") || t.contains("kids")
            else -> true
        }
    }

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        if (page > 2) return emptyList()
        return try {
            when (catalog.id) {
                "events" -> liveEvents().sortedByDescending { isLive(it) }.map { eventMedia(it) }
                "cricket" -> liveEvents().filter {
                    (it.eventCat ?: it.cat).contains("cricket", true)
                }.sortedByDescending { isLive(it) }.map { eventMedia(it) }
                "football" -> liveEvents().filter {
                    (it.eventCat ?: it.cat).contains("football", true) ||
                        (it.eventCat ?: it.cat).contains("soccer", true)
                }.sortedByDescending { isLive(it) }.map { eventMedia(it) }
                else -> {
                    val channels = allChannels().filter { matchGroup(it, catalog.id) }
                    val pageItems = channels.drop((page.coerceAtLeast(1) - 1) * 500).take(500)
                    pageItems.map { c ->
                        val id = if (c.url.startsWith("slug:")) {
                            val payload = JSONObject().put("slug", c.url.removePrefix("slug:")).put("title", c.title)
                            "liveev:" + Base64.encodeToString(payload.toString().toByteArray(), Base64.URL_SAFE or Base64.NO_WRAP)
                        } else {
                            "livetv:" + c.url
                        }
                        HikariMedia(
                            id = id, title = c.title, type = HikariMediaType.SERIES,
                            posterUrl = c.logo,
                            genres = if (c.group.isNotBlank()) listOf(c.group) else emptyList(),
                        )
                    }
                }
            }
        } catch (_: Exception) {
            emptyList()
        }
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank() || page > 1) return emptyList()
        val q = query.trim().lowercase()
        val out = ArrayList<HikariMedia>()
        try {
            out.addAll(liveEvents().filter {
                listOfNotNull(it.title, it.teamA, it.teamB, it.eventName, it.eventCat)
                    .joinToString(" ").lowercase().contains(q)
            }.map { eventMedia(it) })
            out.addAll(allChannels().filter { it.title.lowercase().contains(q) }.map { c ->
                HikariMedia(
                    id = "livetv:" + c.url, title = c.title, type = HikariMediaType.SERIES,
                    posterUrl = c.logo,
                    genres = if (c.group.isNotBlank()) listOf(c.group) else emptyList(),
                )
            })
        } catch (_: Exception) {
        }
        return out.distinctBy { it.id }
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? =
        listOf(HikariEpisode(number = 1, id = media.id, name = "Watch Live", image = media.posterUrl))

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val ref = episode?.id ?: media.id
        // Event / custom-provider channel: resolve slug.txt server links.
        if (ref.startsWith("liveev:")) {
            return try {
                val o = JSONObject(String(Base64.decode(ref.removePrefix("liveev:"), Base64.URL_SAFE)))
                eventStreams(o.optString("slug"), o.optString("title", media.title))
            } catch (_: Exception) {
                emptyList()
            }
        }
        val url = ref.substringAfter("livetv:", ref)
        if (url.startsWith("slug:")) return eventStreams(url.removePrefix("slug:"), media.title)
        if (!url.startsWith("http")) return emptyList()
        val fixed = url.replace(Regex("(?i)%2f"), "/")
        return listOf(
            HikariStream(
                name = media.title, url = fixed,
                headers = mapOf("User-Agent" to MOB_UA),
                isM3u8 = fixed.contains("m3u8"), isMpd = fixed.contains("mpd"),
            ),
        )
    }

    private suspend fun eventStreams(slug: String, title: String): List<HikariStream> {
        if (slug.isBlank()) return emptyList()
        val out = ArrayList<HikariStream>()
        try {
            val txt = fetchDecrypted("$slug.txt") ?: return out
            val arr = JSONArray(txt)
            for (i in 0 until arr.length()) {
                val s = arr.optJSONObject(i) ?: continue
                val server = s.optString("name").ifBlank { "Server ${i + 1}" }
                val raw = s.optString("link").ifBlank { continue }
                val (url, headers) = splitLink(raw)
                if (!url.startsWith("http")) continue
                val fixed = url.replace(Regex("(?i)%2f"), "/")
                val finalHeaders = headers.toMutableMap()
                if (!finalHeaders.containsKey("User-Agent")) finalHeaders["User-Agent"] = MOB_UA
                if (fixed.contains(".mpd")) {
                    val drm = s.optString("api").split(":")
                    val nm = if (drm.size == 2) "$server [DRM]" else server
                    out.add(HikariStream(name = "$title — $nm", url = fixed, headers = finalHeaders, isMpd = true))
                } else {
                    out.add(
                        HikariStream(
                            name = "$title — $server", url = fixed, headers = finalHeaders,
                            isM3u8 = fixed.contains("m3u8"),
                        ),
                    )
                }
            }
        } catch (_: Exception) {
        }
        return out
    }
}
