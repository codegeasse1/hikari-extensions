package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import org.json.JSONObject
import java.net.URLEncoder

/**
 * AnimeDrive — Hindi / Dual-Audio / English-subbed anime (WordPress).
 *
 * Site flow (verified live Nov 2026):
 *  - animedrive.cc list/search/detail pages are plain WordPress (Astra):
 *    article.ast-archive-post cards, h2.entry-title links, post-thumb images.
 *  - Each detail page has ONE "Download / Watch" button (a.anime-dl-btn)
 *    pointing at a link.animedrive.cc landing page.
 *  - The landing page has NO iframes and NO direct media. Its "Episodes"
 *    section (.adc-ep blocks, EP badge + .adc-epname) holds per-episode
 *    buttons: HubCloud links (/dl/<token>, Cloudflare-challenged for plain
 *    HTTP — skipped) and FilePress links (new3.filepress.lat/file/<id>).
 *  - FilePress is a FileBee SPA: details live at
 *    {origin}/api/file/get/<id> and downloads resolve via
 *    POST {origin}/api/file/downlaod[/2] with
 *    {"id","method","captchaValue":null} as application/json.
 *    captchaRequired=false on these files, so the API path works without a
 *    browser. WebView capture is the fallback.
 */
class AnimeDriveProvider : HikariProvider {

    override val id = "animedrive"
    override val name = "AnimeDrive"
    override val mainUrl = "https://animedrive.cc"
    override val description = "Hindi, dual-audio and English-subbed anime from AnimeDrive."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val ua =
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

    private fun pageHeaders(referer: String? = null): Map<String, String> {
        val h = mutableMapOf(
            "User-Agent" to ua,
            "Accept" to "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        )
        if (referer != null) h["Referer"] = referer
        return h
    }

    private suspend fun siteGet(url: String, referer: String? = null): String? =
        HikariNet.getStringSmart(url, pageHeaders(referer))

    private fun strip(s: String): String =
        s.replace(Regex("<[^>]+>"), " ").replace(Regex("\\s+"), " ").trim()

    private fun unesc(s: String): String {
        var t = s.replace("&amp;", "&").replace("&#039;", "'").replace("&#39;", "'")
            .replace("&quot;", "\"").replace("&lt;", "<").replace("&gt;", ">")
            .replace("&nbsp;", " ").replace("&#038;", "&")
        t = Regex("&#(\\d+);").replace(t) {
            runCatching { it.groupValues[1].toInt().toChar().toString() }.getOrDefault(it.value)
        }
        t = Regex("&#x([0-9a-fA-F]+);").replace(t) {
            runCatching { it.groupValues[1].toInt(16).toChar().toString() }.getOrDefault(it.value)
        }
        return t.trim()
    }

    private fun firstRealImg(block: String): String? {
        for (m in Regex("""<img[^>]+(?:data-src|src)="([^"]+)"""", RegexOption.IGNORE_CASE).findAll(block)) {
            var src = m.groupValues[1].trim()
            if (src.isBlank() || src.startsWith("data:") || src.contains("\${")) continue
            if (src.startsWith("//")) src = "https:$src"
            if (src.startsWith("http")) return src
        }
        return null
    }

    private data class Card(val title: String, val url: String, val poster: String?, val isMovie: Boolean)

    /** WordPress archive cards (home, category, search). */
    private fun cards(html: String): List<Card> {
        val out = ArrayList<Card>()
        val seen = HashSet<String>()
        val blocks = Regex(
            """<article[^>]*class="([^"]*)"[^>]*>(.*?)</article>""",
            setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
        ).findAll(html)
        for (b in blocks) {
            val artClass = b.groupValues[1]
            if (!artClass.contains("ast-archive-post")) continue
            val body = b.groupValues[2]
            val link = Regex(
                """<h2[^>]*class="[^"]*entry-title[^"]*"[^>]*>.*?<a[^>]*href="([^"]+)"""",
                setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
            ).find(body)?.groupValues?.get(1)?.trim()?.takeIf { it.startsWith("http") } ?: continue
            if (!seen.add(link)) continue
            val rawTitle = Regex(
                """<h2[^>]*class="[^"]*entry-title[^"]*"[^>]*>(.*?)</h2>""",
                setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
            ).find(body)?.groupValues?.get(1) ?: continue
            val title = unesc(strip(rawTitle)).takeIf { it.isNotBlank() } ?: continue
            // The article's own category classes decide movie vs series —
            // never the whole page (the nav menu links /category/movie/ everywhere).
            val movie = Regex("""\bcategory-movie\b""").containsMatchIn(artClass)
            out.add(Card(title, link, firstRealImg(body), movie))
        }
        return out
    }

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("latest", "Latest Anime", HikariMediaType.SERIES),
        HikariCatalog("hindi", "Hindi Dubbed", HikariMediaType.SERIES),
        HikariCatalog("dual", "Dual Audio / Multi", HikariMediaType.SERIES),
        HikariCatalog("subbed", "English Subbed", HikariMediaType.SERIES),
        HikariCatalog("movies", "Movies", HikariMediaType.MOVIE),
    )

    private fun catalogBase(id: String, d: String): String = when (id) {
        "hindi" -> "$d/category/hindi-dubbed-anime/"
        "dual" -> "$d/category/dual-audio-anime/"
        "subbed" -> "$d/category/english-sub-anime-download/"
        "movies" -> "$d/category/movie/"
        else -> "$d/"
    }

    private fun toMedia(c: Card, fallbackType: HikariMediaType): HikariMedia =
        HikariMedia(
            id = c.url, title = c.title,
            type = if (c.isMovie) HikariMediaType.MOVIE else fallbackType,
            posterUrl = c.poster,
        )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val d = mainUrl
        val p = page.coerceAtLeast(1)
        val base = catalogBase(catalog.id, d)
        val url = if (p <= 1) base else base.trimEnd('/') + "/page/$p/"
        val html = siteGet(url, "$d/") ?: return emptyList()
        return cards(html).map { toMedia(it, catalog.type) }
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (query.isBlank()) return emptyList()
        val d = mainUrl
        val out = ArrayList<HikariMedia>()
        val seen = HashSet<String>()
        val enc = URLEncoder.encode(query.trim(), "UTF-8")
        val maxPage = page.coerceIn(1, 3)
        for (p in 1..maxPage) {
            try {
                val html = siteGet("$d/page/$p/?s=$enc", "$d/") ?: break
                val items = cards(html)
                if (items.isEmpty()) break
                for (c in items) {
                    if (seen.add(c.url)) out.add(toMedia(c, HikariMediaType.SERIES))
                }
                if (items.size < 4) break
            } catch (_: Exception) {
                break
            }
        }
        return out
    }

    private fun metaTag(html: String, prop: String): String? {
        val m = Regex(
            """<meta[^>]*property="$prop"[^>]*content="([^"]+)"""",
            RegexOption.IGNORE_CASE,
        ).find(html) ?: Regex(
            """<meta[^>]*content="([^"]+)"[^>]*property="$prop"""",
            RegexOption.IGNORE_CASE,
        ).find(html) ?: return null
        return unesc(m.groupValues[1]).takeIf { it.isNotBlank() }
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val d = mainUrl
        val html = siteGet(media.id, "$d/") ?: return media
        val title = metaTag(html, "og:title")?.substringBefore(" - AnimeDrive")
            ?: Regex(
                """<h1[^>]*>(.*?)</h1>""",
                setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
            ).find(html)?.groupValues?.get(1)?.let { unesc(strip(it)) }
            ?: media.title
        val poster = metaTag(html, "og:image") ?: firstRealImg(html) ?: media.posterUrl
        val plot = metaTag(html, "og:description")?.takeIf { it.length > 10 } ?: media.overview
        val genres = Regex("""<a[^>]*href="[^"]*/category/[^"]*"[^>]*>([^<]+)</a>""", RegexOption.IGNORE_CASE)
            .findAll(html).map { unesc(it.groupValues[1].trim()) }.filter {
                it.isNotBlank() && !it.contains("Download", true) && !it.contains("Anime", true)
            }.distinct().take(8).toList()
        val type = run {
            val artClass = Regex("""<article[^>]*class="([^"]*)"""", RegexOption.IGNORE_CASE)
                .find(html)?.groupValues?.get(1).orEmpty()
            if (Regex("""\bcategory-movie\b""").containsMatchIn(artClass)) HikariMediaType.MOVIE else media.type
        }
        return media.copy(
            title = title.takeIf { it.isNotBlank() } ?: media.title,
            type = type, posterUrl = poster, backdropUrl = poster,
            overview = plot, genres = genres.ifEmpty { media.genres },
        )
    }

    /** The link.animedrive.cc landing URL for a detail page (or null). */
    private fun linkPageOf(detailHtml: String): String? {
        val btn = Regex(
            """<a[^>]*class="[^"]*anime-dl-btn[^"]*"[^>]*href="([^"]+)"""",
            RegexOption.IGNORE_CASE,
        ).find(detailHtml)?.groupValues?.get(1)
        if (btn != null && btn.contains("link.animedrive.cc")) return unesc(btn)
        return Regex(
            """href="(https://link\.animedrive\.cc/[^"]+)"""",
            RegexOption.IGNORE_CASE,
        ).find(detailHtml)?.groupValues?.get(1)?.let { unesc(it) }
    }

    private data class EpLinks(val badge: Int, val name: String, val files: List<FileLink>)
    private data class FileLink(val url: String, val fileId: String, val label: String)

    private fun filepressId(href: String): String? =
        Regex("""/file/([A-Za-z0-9]{6,64})""").find(href)?.groupValues?.get(1)

    /** Parses the landing page's .adc-ep blocks into episodes with FilePress files. */
    private fun episodesOf(linkHtml: String): List<EpLinks> {
        val out = ArrayList<EpLinks>()
        val chunks = linkHtml.split(Regex("""<div[^>]*class="[^"]*adc-ep[^"]*"[^>]*>""")).drop(1)
        var order = 0
        for (ch in chunks) {
            order++
            val badge = Regex("""<span class="adc-badge">[^0-9]*([0-9]{1,4})""")
                .find(ch)?.groupValues?.get(1)?.toIntOrNull() ?: order
            val epName = Regex("""<span class="adc-epname">([^<]*)</span>""")
                .find(ch)?.groupValues?.get(1)?.let { unesc(strip(it)) }
                .takeIf { !it.isNullOrBlank() } ?: "Episode $badge"
            val files = ArrayList<FileLink>()
            val seen = HashSet<String>()
            for (a in Regex(
                """<a[^>]*href="([^"]+)"[^>]*>(.*?)</a>""",
                setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
            ).findAll(ch)) {
                val href = unesc(a.groupValues[1].trim())
                if (!href.contains("filepress", true)) continue // HubCloud /dl/ is CF-challenged: skip
                val fid = filepressId(href) ?: continue
                if (!seen.add(fid)) continue
                val label = unesc(strip(a.groupValues[2])).takeIf { it.isNotBlank() } ?: fid
                files.add(FileLink(href, fid, label))
            }
            if (files.isEmpty()) continue
            out.add(EpLinks(badge, epName, files))
        }
        out.sortBy { it.badge }
        return out
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val d = mainUrl
        val detailHtml = siteGet(media.id, "$d/") ?: return null
        val linkPage = linkPageOf(detailHtml) ?: run {
            // No landing page: the detail page itself may list files (rare).
            val direct = episodesOf(detailHtml)
            if (direct.isEmpty()) return null
            return direct.mapIndexed { i, e ->
                HikariEpisode(
                    number = e.badge, id = media.id + "#adcep=" + i,
                    name = e.name, image = media.posterUrl,
                )
            }
        }
        val linkHtml = siteGet(linkPage, media.id) ?: return null
        val eps = episodesOf(linkHtml)
        if (eps.isEmpty()) {
            return listOf(HikariEpisode(number = 1, id = "$linkPage#adcep=0", name = media.title, image = media.posterUrl))
        }
        return eps.mapIndexed { i, e ->
            HikariEpisode(
                number = e.badge, id = "$linkPage#adcep=" + i,
                name = e.name, image = media.posterUrl,
            )
        }
    }

    // ---------- FilePress resolution ----------

    private val v2Methods = setOf(
        "publicDownlaod", "privateDownlaod", "publicUserDownlaod",
        "indexDownlaod", "cloudDownlaod", "cloudR2Downlaod",
    )

    private fun apiOrigin(fileUrl: String): String {
        val host = Regex("""https?://([^/]+)""").find(fileUrl)?.groupValues?.get(1) ?: "new3.filepress.lat"
        return "https://$host"
    }

    private fun pickMethods(options: JSONObject?): List<String> {
        val preferred = listOf("dotFlixDownlaod", "indexDownlaod", "publicDownlaod", "cloudR2Downlaod")
        if (options == null) return preferred
        return preferred.filter { key ->
            val flag = when (key) {
                "dotFlixDownlaod" -> "dotFlixDownlaod"
                "indexDownlaod" -> "indexDownlaod"
                "publicDownlaod" -> "publicDownlaod"
                "cloudR2Downlaod" -> "cloudDownload"
                else -> key
            }
            options.optBoolean(flag, true)
        }.ifEmpty { preferred }
    }

    private fun extractUrl(data: Any?): String? {
        if (data is String) {
            val t = data.trim()
            return t.takeIf { it.startsWith("http") }
        }
        if (data is JSONObject) {
            for (k in listOf("url", "downloadUrl", "link", "fileUrl", "directUrl", "download_url")) {
                val v = data.optString(k)
                if (v.startsWith("http")) return v
            }
        }
        return null
    }

    /** Resolves one FilePress file page to a direct media URL (or null). */
    private suspend fun resolveFilePress(fileUrl: String, fileId: String): String? {
        val origin = apiOrigin(fileUrl)
        val details = try {
            HikariNet.getJson("$origin/api/file/get/$fileId")
        } catch (_: Exception) {
            null
        }
        val options = details?.optJSONObject("data")?.optJSONObject("downloadOptions")
        val apiHeaders = mapOf(
            "User-Agent" to ua,
            "Accept" to "application/json, */*;q=0.8",
            "Origin" to origin,
            "Referer" to fileUrl,
        )
        for (m in pickMethods(options)) {
            try {
                val payload = JSONObject()
                    .put("id", fileId)
                    .put("method", m)
                    .put("captchaValue", JSONObject.NULL)
                    .put("referrer", fileUrl)
                    .toString()
                val initial = HikariNet.postString(
                    "$origin/api/file/downlaod/", payload, apiHeaders,
                    "application/json; charset=utf-8",
                ) ?: continue
                val jo = runCatching { JSONObject(initial) }.getOrNull() ?: continue
                extractUrl(jo.opt("data"))?.let { return it }
                // Final step (V2 methods use downlaod2, others repeat downlaod).
                val finalEp = if (v2Methods.contains(m)) "downlaod2" else "downlaod"
                val finalResp = HikariNet.postString(
                    "$origin/api/file/$finalEp/", payload, apiHeaders,
                    "application/json; charset=utf-8",
                ) ?: continue
                val fjo = runCatching { JSONObject(finalResp) }.getOrNull() ?: continue
                extractUrl(fjo.opt("data"))?.let { return it }
            } catch (_: Exception) {
            }
        }
        return null
    }

    private val capture = Regex("""https?://[^"' \s]+\.m3u8[^"' \s]*|https?://[^"' \s]+\.mp4[^"' \s]*""")

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val d = mainUrl
        val ref = episode?.id ?: media.id
        val linkPage = ref.substringBefore("#adcep=")
        val epIndex = ref.substringAfter("#adcep=", "0").toIntOrNull() ?: 0
        val linkHtml = if (linkPage.startsWith("http") && linkPage != media.id) {
            siteGet(linkPage, media.id)
        } else {
            val detailHtml = siteGet(media.id, "$d/")
            val lp = detailHtml?.let { linkPageOf(it) }
            lp?.let { siteGet(it, media.id) } ?: detailHtml
        } ?: return emptyList()
        val eps = episodesOf(linkHtml)
        val files = eps.getOrNull(epIndex)?.files
            ?: eps.firstOrNull()?.files
            ?: return emptyList()
        // Warm the host session once (the FileBee download API is
        // cookie-aware; the web app always visits the file page first).
        files.firstOrNull()?.url?.let { warm ->
            runCatching { HikariNet.getString(warm, mapOf("User-Agent" to ua, "Referer" to linkPage)) }
        }
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        for (f in files) {
            try {
                val direct = resolveFilePress(f.url, f.fileId)
                if (direct != null && seen.add(direct)) {
                    out.add(
                        HikariStream(
                            name = "[FP] ${f.label}", url = direct,
                            headers = mapOf("Referer" to f.url, "User-Agent" to ua),
                            isM3u8 = direct.contains("m3u8"),
                        ),
                    )
                    continue
                }
            } catch (_: Exception) {
            }
            try {
                for (r in HikariNet.resolveWithWebView(f.url, capture, timeoutMs = 45_000)) {
                    if (!seen.add(r.url)) continue
                    out.add(
                        HikariStream(
                            name = "[FP] ${f.label}", url = r.url, headers = r.headers,
                            isM3u8 = r.url.contains("m3u8"),
                        ),
                    )
                }
            } catch (_: Exception) {
            }
        }
        return out
    }
}
