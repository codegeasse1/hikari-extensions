package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import org.json.JSONArray
import org.json.JSONObject
import java.io.ByteArrayOutputStream
import java.net.HttpURLConnection
import java.net.URL
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale
import java.util.concurrent.ConcurrentHashMap

/**
 * Streamed.pk — live sports streams.
 *
 * Faithful native port of KSHITIJ8473/raghav StreamedPk, verified live Oct 2026:
 *  - GET /api/matches/all (240 matches, all with sources; categories football,
 *    american-football, basketball, hockey, motor-sports, fight, cricket,
 *    tennis, golf, rugby, baseball, other). 60s cache + 10min stale fallback,
 *    3 attempts with a pause; ddos-guard cookies kept in an in-memory jar.
 *  - 12 catalog rows (Live Now + 11 sport rows), search over the cached list.
 *  - GET /api/stream/{source}/{id} variants (streamNo/language/hd/embedUrl);
 *    each embed.st embed is resolved with the source's stealth media-hook
 *    script through WebView capture (.m3u8 + master.txt), emitted as M3U8
 *    with the embed host as Referer/Origin.
 */
class StreamedPkProvider : HikariProvider {

    override val id = "streamedpk"
    override val name = "Streamed.pk"
    override val mainUrl = "https://streamed.pk"
    override val description = "Live sports streams from Streamed.pk — football, basketball, fight, F1 and more."
    override val version = 2
    override val tvTypes = setOf(HikariMediaType.SERIES)

    private val ua =
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"

    private val jar = ConcurrentHashMap<String, String>()

    private suspend fun apiGet(url: String): String? {
        // hardened client first; raw cookie-jar client as fallback
        return try {
            val h = mutableMapOf(
                "User-Agent" to ua,
                "Accept" to "application/json, text/plain, */*",
                "Referer" to "$mainUrl/",
                "Origin" to mainUrl,
            )
            if (jar.isNotEmpty()) h["Cookie"] = jar.entries.joinToString("; ") { "${it.key}=${it.value}" }
            HikariNet.getString(url, h)
        } catch (_: Exception) {
            null
        } ?: rawGet(url)
    }

    private fun rawGet(url: String): String? {
        return try {
            val conn = (URL(url).openConnection() as HttpURLConnection).apply {
                requestMethod = "GET"
                connectTimeout = 15_000
                readTimeout = 20_000
                instanceFollowRedirects = true
                setRequestProperty("User-Agent", ua)
                setRequestProperty("Accept", "application/json, text/plain, */*")
                setRequestProperty("Referer", "$mainUrl/")
                setRequestProperty("Origin", mainUrl)
                if (jar.isNotEmpty()) {
                    setRequestProperty("Cookie", jar.entries.joinToString("; ") { "${it.key}=${it.value}" })
                }
            }
            val code = conn.responseCode
            conn.headerFields["Set-Cookie"]?.forEach { raw ->
                val pair = raw.substringBefore(";").trim()
                val eq = pair.indexOf('=')
                if (eq > 0) {
                    val v = pair.substring(eq + 1)
                    if (v.isNotBlank()) jar[pair.substring(0, eq)] = v
                }
            } ?: conn.headerFields.entries.firstOrNull { it.key.equals("set-cookie", true) }
                ?.value?.forEach { raw ->
                    val pair = raw.substringBefore(";").trim()
                    val eq = pair.indexOf('=')
                    if (eq > 0) {
                        val v = pair.substring(eq + 1)
                        if (v.isNotBlank()) jar[pair.substring(0, eq)] = v
                    }
                }
            if (code !in 200..299) {
                conn.disconnect()
                return null
            }
            val bytes = conn.inputStream.use { ins ->
                val buf = ByteArrayOutputStream()
                val tmp = ByteArray(8192)
                while (true) {
                    val n = ins.read(tmp)
                    if (n < 0) break
                    buf.write(tmp, 0, n)
                }
                buf.toByteArray()
            }
            conn.disconnect()
            String(bytes, Charsets.UTF_8)
        } catch (_: Exception) {
            null
        }
    }

    private data class SpkSource(val source: String, val id: String)

    private data class SpkMatch(
        val id: String,
        val title: String,
        val category: String?,
        val date: Long?,
        val poster: String?,
        val sources: List<SpkSource>,
    )

    private fun parseMatches(text: String): List<SpkMatch> {
        val out = ArrayList<SpkMatch>()
        try {
            val arr = JSONArray(text)
            for (i in 0 until arr.length()) {
                val o = arr.optJSONObject(i) ?: continue
                val id = o.optString("id")
                val title = o.optString("title")
                if (id.isBlank() || title.isBlank()) continue
                val srcs = ArrayList<SpkSource>()
                val sa = o.optJSONArray("sources")
                if (sa != null) {
                    for (j in 0 until sa.length()) {
                        val s = sa.optJSONObject(j) ?: continue
                        val sn = s.optString("source")
                        val sid = s.optString("id")
                        if (sn.isNotBlank() && sid.isNotBlank()) srcs.add(SpkSource(sn, sid))
                    }
                }
                var poster = o.optString("poster").ifBlank { null }
                if (poster != null && poster.startsWith("/")) poster = mainUrl + poster
                out.add(
                    SpkMatch(
                        id, title,
                        o.optString("category").ifBlank { null },
                        o.optLong("date", 0L).takeIf { it > 0 },
                        poster, srcs,
                    ),
                )
            }
        } catch (_: Exception) {
        }
        return out
    }

    private data class Cached(val matches: List<SpkMatch>, val at: Long)

    @Volatile
    private var cache: Cached? = null

    private suspend fun fetchAllMatches(): List<SpkMatch> {
        val now = System.currentTimeMillis()
        cache?.let { if (now - it.at < 60_000L) return it.matches }
        var last: List<SpkMatch>? = null
        repeat(3) { attempt ->
            try {
                val text = apiGet("$mainUrl/api/matches/all")
                    ?: throw java.io.IOException("streamed.pk unreachable")
                val matches = parseMatches(text)
                cache = Cached(matches, now)
                return matches
            } catch (_: Exception) {
                if (attempt < 2) kotlinx.coroutines.delay(1500L)
                else last = cache?.takeIf { now - it.at < 600_000L }?.matches
            }
        }
        return last ?: throw java.io.IOException("streamed.pk could not be reached")
    }

    private fun posterFor(category: String?, poster: String?): String {
        if (!poster.isNullOrBlank()) return poster
        return when (category?.lowercase()) {
            "football" -> "https://images.unsplash.com/photo-1508098682722-e99c43a406b2?w=500"
            "basketball" -> "https://images.unsplash.com/photo-1546519638-68e109498ffc?w=500"
            "american-football" -> "https://images.unsplash.com/photo-1587280501635-68a0e82cd5ff?w=500"
            "hockey" -> "https://images.unsplash.com/photo-1515703407324-5f753eed2411?w=500"
            "baseball" -> "https://images.unsplash.com/photo-1530541930197-ff16ac917b0e?w=500"
            "motor-sports" -> "https://images.unsplash.com/photo-1568605117036-5fe5e7bab0b7?w=500"
            "fight" -> "https://images.unsplash.com/photo-1517838277536-f5f99be501cd?w=500"
            "tennis" -> "https://images.unsplash.com/photo-1595435934249-5df7ed86e1c0?w=500"
            "rugby" -> "https://images.unsplash.com/photo-1534353436294-0dbd4bdac845?w=500"
            "cricket" -> "https://images.unsplash.com/photo-1531415074968-036ba1b575da?w=500"
            else -> "https://images.unsplash.com/photo-1461896836934-ffe607ba8211?w=500"
        }
    }

    private fun dateStr(ts: Long?): String {
        if (ts == null || ts <= 0) return "soon"
        return try {
            val sdf = SimpleDateFormat("dd MMM, HH:mm", Locale.US)
            sdf.format(Date(ts))
        } catch (_: Exception) {
            "soon"
        }
    }

    private fun encodeId(m: SpkMatch): String {
        val o = JSONObject()
            .put("id", m.id)
            .put("title", m.title)
            .put("category", m.category ?: "")
            .put("date", m.date ?: 0L)
            .put("poster", m.poster ?: "")
        val sa = JSONArray()
        for (s in m.sources) sa.put(JSONObject().put("source", s.source).put("id", s.id))
        o.put("sources", sa)
        var b64 = android.util.Base64.encodeToString(
            o.toString().toByteArray(Charsets.UTF_8),
            android.util.Base64.URL_SAFE or android.util.Base64.NO_PADDING or android.util.Base64.NO_WRAP,
        )
        return "spk$b64"
    }

    private fun decodeId(id: String): SpkMatch? {
        return try {
            if (!id.startsWith("spk")) return null
            var b64 = id.removePrefix("spk").replace('-', '+').replace('_', '/')
            while (b64.length % 4 != 0) b64 += "="
            val o = JSONObject(
                String(android.util.Base64.decode(b64, android.util.Base64.DEFAULT), Charsets.UTF_8),
            )
            val srcs = ArrayList<SpkSource>()
            val sa = o.optJSONArray("sources")
            if (sa != null) {
                for (i in 0 until sa.length()) {
                    val s = sa.optJSONObject(i) ?: continue
                    srcs.add(SpkSource(s.optString("source"), s.optString("id")))
                }
            }
            SpkMatch(
                o.optString("id"), o.optString("title"),
                o.optString("category").ifBlank { null },
                o.optLong("date", 0L).takeIf { it > 0 },
                o.optString("poster").ifBlank { null }, srcs,
            )
        } catch (_: Exception) {
            null
        }
    }

    private fun toMedia(m: SpkMatch): HikariMedia {
        val live = m.sources.isNotEmpty()
        val title = if (live) "[LIVE] ${m.title}" else "${m.title} [Starts: ${dateStr(m.date)}]"
        return HikariMedia(
            id = encodeId(m), title = title, type = HikariMediaType.SERIES,
            posterUrl = posterFor(m.category, m.poster),
            overview = m.category?.let { "Category: $it. Starts: ${dateStr(m.date)}" },
        )
    }

    private data class Cat(val id: String, val name: String, val match: String?)

    private val cats = listOf(
        Cat("live", "Live Now", null),
        Cat("football", "Football", "football"),
        Cat("american-football", "American Football", "american-football"),
        Cat("basketball", "Basketball", "basketball"),
        Cat("hockey", "Hockey", "hockey"),
        Cat("motor-sports", "Motor Sports", "motor-sports"),
        Cat("fight", "Fight (UFC, Boxing)", "fight"),
        Cat("cricket", "Cricket", "cricket"),
        Cat("tennis", "Tennis", "tennis"),
        Cat("baseball", "Baseball", "baseball"),
        Cat("rugby", "Rugby", "rugby"),
        Cat("other", "Other Sports", "other"),
    )

    override fun catalogs(): List<HikariCatalog> =
        cats.map { HikariCatalog(it.id, it.name, HikariMediaType.SERIES) }

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val matches = try {
            fetchAllMatches()
        } catch (_: Exception) {
            return emptyList()
        }
        val cat = cats.firstOrNull { it.id == catalog.id } ?: return emptyList()
        val live = matches.filter { it.sources.isNotEmpty() }
        val pool = if (cat.match == null) live
        else live.filter { it.category.equals(cat.match, true) }
        val p = page.coerceAtLeast(1)
        return pool.drop((p - 1) * 60).take(60).map { toMedia(it) }
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank()) return emptyList()
        val matches = try {
            fetchAllMatches()
        } catch (_: Exception) {
            return emptyList()
        }
        val q = query.trim()
        val hits = matches.filter {
            it.title.contains(q, true) || (it.category?.contains(q, true) ?: false)
        }
        val p = page.coerceAtLeast(1)
        return hits.drop((p - 1) * 60).take(60).map { toMedia(it) }
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val ev = decodeId(media.id) ?: return media
        return try {
            val fresh = fetchAllMatches().firstOrNull { it.id == ev.id } ?: return media
            toMedia(fresh).copy(id = media.id)
        } catch (_: Exception) {
            media
        }
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val ev = decodeId(media.id) ?: return null
        return listOf(HikariEpisode(number = 1, id = media.id, name = ev.title, image = media.posterUrl))
    }

    private val capture = Regex("""(?i)\.m3u8(?:\?|$)|master\.txt""")

    // the source's stealth hook: media/src sets navigate to the playlist so
    // the interceptor sees it even when the player hides the URL
    private val hookScript = """
        (function() {
            if (window.__interceptor_installed) return "already_installed";
            Object.defineProperty(window, '__interceptor_installed', {
                value: true, writable: true, configurable: true, enumerable: false
            });
            function triggerInterception(url) {
                if (!url) return;
                var urlStr = (url && typeof url.toString === 'function') ? url.toString() : url;
                if (urlStr.indexOf('m3u8') !== -1 || urlStr.indexOf('master.txt') !== -1) {
                    window.location.href = urlStr;
                }
            }
            try {
                var d = Object.getOwnPropertyDescriptor(HTMLMediaElement.prototype, 'src');
                if (d && d.set) {
                    var origSet = d.set;
                    Object.defineProperty(HTMLMediaElement.prototype, 'src', {
                        set: function(val) { triggerInterception(val); return origSet.apply(this, arguments); },
                        configurable: true, enumerable: true
                    });
                }
            } catch(e) {}
            try {
                var d2 = Object.getOwnPropertyDescriptor(HTMLSourceElement.prototype, 'src');
                if (d2 && d2.set) {
                    var origSet2 = d2.set;
                    Object.defineProperty(HTMLSourceElement.prototype, 'src', {
                        set: function(val) { triggerInterception(val); return origSet2.apply(this, arguments); },
                        configurable: true, enumerable: true
                    });
                }
            } catch(e) {}
        })();
    """.trimIndent()

    private fun playHeaders(embedUrl: String): Map<String, String> {
        val host = try {
            val u = java.net.URI(embedUrl)
            "${u.scheme}://${u.host}"
        } catch (_: Exception) {
            "https://embed.st"
        }
        return mapOf(
            "User-Agent" to "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Mobile Safari/537.36",
            "Referer" to "$host/",
            "Origin" to host,
            "Accept" to "*/*",
        )
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val ev = decodeId(episode?.id ?: media.id) ?: return emptyList()
        if (ev.sources.isEmpty()) return emptyList()
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        coroutineScope {
            ev.sources.map { src ->
                async(Dispatchers.IO) {
                    val found = ArrayList<HikariStream>()
                    try {
                        val text = apiGet("$mainUrl/api/stream/${src.source}/${src.id}") ?: return@async found
                        val arr = JSONArray(text)
                        for (i in 0 until arr.length()) {
                            val st = arr.optJSONObject(i) ?: continue
                            val sn = st.optInt("streamNo", i + 1)
                            val lang = st.optString("language").takeIf { it.isNotBlank() }
                                ?.let { " ($it)" }.orEmpty()
                            val hd = if (st.optBoolean("hd", false)) " [HD]" else ""
                            val embed = st.optString("embedUrl")
                            if (embed.isBlank()) continue
                            val name = "Server $sn - ${src.source.uppercase()}$lang$hd"
                            try {
                                val results = HikariNet.resolveWithWebView(
                                    embed, capture, timeoutMs = 45_000, script = hookScript,
                                )
                                for (r in results) {
                                    found.add(
                                        HikariStream(
                                            name = "[Streamed] $name", url = r.url,
                                            headers = playHeaders(embed), isM3u8 = true,
                                        ),
                                    )
                                }
                            } catch (_: Exception) {
                            }
                        }
                    } catch (_: Exception) {
                    }
                    found
                }
            }.forEach { d ->
                try {
                    for (s in d.await()) if (seen.add(s.url)) out.add(s)
                } catch (_: Exception) {
                }
            }
        }
        return out
    }
}
