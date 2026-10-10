package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import org.json.JSONObject
import java.net.URLDecoder
import java.net.URLEncoder

/**
 * AnimeTH (anime-th.com) — Thai-subbed/dubbed anime.
 *
 * Faithful native port of KSHITIJ8473/raghav AnimeTH (jsoup selectors
 * translated 1:1 to regexes — jsoup is not on the Hikari compile classpath):
 * home sections, /scoretop/, /category/{th}/, /genre/{en}/ rows, ajax + query
 * search, JSON-LD detail pages, a.ep-item episode lists, sub/dub counterpart
 * pairing, and the base/{epId} -> playback sid -> fileId -> FILE_BASE/file2
 * HLS master flow.
 */
class AnimeTHProvider : HikariProvider {

    override val id = "animeth"
    override val name = "AnimeTH"
    override val mainUrl = "https://anime-th.com"
    override val description = "Thai anime from AnimeTH — latest updates, popular, Thai sub & dub categories, genres, direct HLS."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    companion object {
        private const val BASE = "https://anime-th.com"
        private const val UA =
            "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36"
        private const val STREAM_BASE = "https://streaming.tonytonychopper.com"
        private const val FILE_BASE = "https://anime.tonytonychopper.net"

        private const val MARKER_DUB = "dub"
        private const val MARKER_SUB = "sub"
        private val DUB_WORDS = listOf("พากย์ไทย", "เสียงไทย")
        private val SUB_WORDS = listOf("ซับไทย", "บรรยายไทย")
        private val NAME_SUFFIXES = DUB_WORDS + SUB_WORDS
        private val CATEGORY_LANG = mapOf(
            "พากย์ไทย" to MARKER_DUB,
            "ซับไทย" to MARKER_SUB,
            "อนิเมะจีนซับไทย" to MARKER_SUB,
        )
        private const val SECTION_LATEST = "อนิเมะอัพเดต"
        private const val SECTION_REVIEWED = "อนิเมะรีวิวเยอะสุด"
        private const val MIN_COUNTERPART_SCORE = 0.55

        private val EP_ID_REGEX = Regex("watch/([A-Za-z0-9]+)(?:\\.html)?/?(?:[?#].*)?$")
        private val EP_NUM_REGEX = Regex("ตอนที่[\\s_]*(\\d+)")
        private val FILE_ID_REGEX = Regex("anime\\.tonytonychopper\\.net/v2/([A-Za-z0-9]+)")
        private val RX_PLAYBACK_SID = Regex("playback/v/([A-Za-z0-9]+)/")
        private val CLEAN_WORDS = listOf(
            "(Thai)", "อัพเดตตอนล่าสุด", "อัพเดตล่าสุด", "อัพเดตทุกตอน", "รวมทุกภาค", "ครบทุกตอน",
        )
        private val RX_THAI_WORD = Regex("thai", RegexOption.IGNORE_CASE)
        private val RX_GROUPS = Regex("\\([^)]*\\)|\\[[^\\]]*\\]|\\{[^}]*\\}")
        private val RX_ORDINAL_SEASON = Regex("(\\d+)(st|nd|rd|th)\\s+season", RegexOption.IGNORE_CASE)
        private val RX_SEASON_EN = Regex("season\\s*(\\d+)", RegexOption.IGNORE_CASE)
        private val RX_SEASON_TH = Regex("ภาค\\s*(\\d+)")
        private val RX_SEASON_ALT_TH = Regex("ซีซั่น\\s*(\\d+)(\\s*-\\s*\\d+)?")
        private val RX_YEAR_TH = Regex("ปี\\s*(\\d+)(\\s*-\\s*\\d+)?")
        private val RX_PART_EN = Regex("part\\s*(\\d+)", RegexOption.IGNORE_CASE)
        private val RX_DASH_BANG = Regex("[-!]")
        private val RX_SEASON_TOKEN = Regex("(?:^|\\s)([sp]\\d+)(?:$|\\s)")

        fun similarity(a: String, b: String): Double {
            val ta = a.split(" ").filter { it.isNotBlank() }.toSet()
            val tb = b.split(" ").filter { it.isNotBlank() }.toSet()
            if (ta.isEmpty() || tb.isEmpty()) return 0.0
            return ta.intersect(tb).size.toDouble() / ta.union(tb).size
        }

        fun seasonToken(cleaned: String): String? = RX_SEASON_TOKEN.find(cleaned)?.groupValues?.get(1)

        fun cleanTitle(raw: String): String {
            var t = raw
            for (w in NAME_SUFFIXES) t = t.replace(w, " ")
            for (w in CLEAN_WORDS) t = t.replace(w, " ")
            t = RX_THAI_WORD.replace(t, " ")
            t = RX_GROUPS.replace(t, " ")
            t = RX_ORDINAL_SEASON.replace(t, "s$1")
            t = RX_SEASON_EN.replace(t, "s$1")
            t = RX_SEASON_TH.replace(t, "s$1")
            t = RX_SEASON_ALT_TH.replace(t, "s$1")
            t = RX_YEAR_TH.replace(t, "p$1")
            t = RX_PART_EN.replace(t, "p$1")
            t = RX_DASH_BANG.replace(t, " ")
            return t.split(" ").filter { it.isNotBlank() }.joinToString(" ").lowercase()
        }

        private fun siteHeaders(referer: String? = null, xhr: Boolean = false): Map<String, String> {
            val h = linkedMapOf("User-Agent" to UA, "Accept" to "*/*")
            if (xhr) h["X-Requested-With"] = "XMLHttpRequest"
            if (referer != null) h["Referer"] = referer
            return h
        }
    }

    // ------------------------------------------------------------------
    // Catalogs (mirrors the source mainPage — 16 rows)
    // ------------------------------------------------------------------

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("home_latest", "Latest Updates", HikariMediaType.SERIES),
        HikariCatalog("top", "Popular", HikariMediaType.SERIES),
        HikariCatalog("cat_ซับไทย", "Thai Sub", HikariMediaType.SERIES),
        HikariCatalog("cat_พากย์ไทย", "Thai Dub", HikariMediaType.SERIES),
        HikariCatalog("cat_อนิเมะจีนซับไทย", "Chinese Anime (Sub)", HikariMediaType.SERIES),
        HikariCatalog("home_reviewed", "Most Reviewed", HikariMediaType.SERIES),
        HikariCatalog("genre_action", "Action", HikariMediaType.SERIES),
        HikariCatalog("genre_adventure", "Adventure", HikariMediaType.SERIES),
        HikariCatalog("genre_comedy", "Comedy", HikariMediaType.SERIES),
        HikariCatalog("genre_drama", "Drama", HikariMediaType.SERIES),
        HikariCatalog("genre_fantasy", "Fantasy", HikariMediaType.SERIES),
        HikariCatalog("genre_isekai", "Isekai", HikariMediaType.SERIES),
        HikariCatalog("genre_mecha", "Mecha", HikariMediaType.SERIES),
        HikariCatalog("genre_romance", "Romance", HikariMediaType.SERIES),
        HikariCatalog("genre_horror", "Horror", HikariMediaType.SERIES),
        HikariCatalog("genre_sports", "Sports", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        // The site rows are single-page grids (same as the CloudStream source).
        if (page > 1) return emptyList()
        return when {
            catalog.id == "home_latest" || catalog.id == "home_reviewed" -> homeSection(catalog.id)
            catalog.id == "top" -> {
                val html = get("$BASE/scoretop/") ?: return emptyList()
                parseAnchors(html, requireBlock = false)
            }
            catalog.id.startsWith("cat_") -> {
                val html = get("$BASE/category/${catalog.id.removePrefix("cat_")}/") ?: return emptyList()
                parseAnchors(html, requireBlock = true)
            }
            catalog.id.startsWith("genre_") -> {
                val html = get("$BASE/genre/${catalog.id.removePrefix("genre_")}/") ?: return emptyList()
                parseAnchors(html, requireBlock = true)
            }
            else -> emptyList()
        }
    }

    private suspend fun homeSection(which: String): List<HikariMedia> {
        val wanted = if (which == "home_latest") SECTION_LATEST else SECTION_REVIEWED
        val html = get(BASE) ?: return emptyList()
        for (sec in splitSections(html)) {
            val h2 = Regex("<h2[^>]*>([\\s\\S]*?)</h2>").find(sec)?.groupValues?.get(1)
                ?.let { stripTags(it) }?.trim() ?: continue
            if (h2 != wanted) continue
            return parseAnchors(sec, requireBlock = false)
        }
        return emptyList()
    }

    // ------------------------------------------------------------------
    // Search (ajax + query POST, merged like the source)
    // ------------------------------------------------------------------

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        if (page > 1) return emptyList()
        val q = query.trim()
        if (q.isEmpty()) return emptyList()
        val bySlug = LinkedHashMap<String, HikariMedia>()
        runCatching { quickAjax(q) }.getOrNull()?.forEach { bySlug[slugKey(it.id)] = it }
        runCatching { queryCards(q) }.getOrNull()?.forEach { bySlug.putIfAbsent(slugKey(it.id), it) }
        val needle = q.lowercase()
        return bySlug.values.sortedByDescending { m ->
            val t = m.title.lowercase()
            when {
                t == needle -> 3
                t.startsWith(needle) -> 2
                t.contains(needle) -> 1
                else -> 0
            }
        }
    }

    private suspend fun quickAjax(q: String): List<HikariMedia> {
        val body = HikariNet.getString(
            "$BASE/vendor/search-ajax.php?q=" + URLEncoder.encode(q, "UTF-8"),
            siteHeaders("$BASE/", xhr = true),
        ) ?: return emptyList()
        val arr = runCatching { JSONObject(body).optJSONArray("results") } .getOrNull() ?: return emptyList()
        return buildList {
            for (i in 0 until arr.length()) {
                val r = arr.optJSONObject(i) ?: continue
                val slug = r.optString("slug").ifBlank { continue }
                val title = r.optString("title").trim().ifEmpty { continue }
                var cover = r.optString("cover")
                if (cover.isNotBlank() && !cover.startsWith("http")) cover = "$BASE/$cover"
                add(mediaFor(title, slug, cover.takeIf { it.startsWith("http") }, r.optString("category")))
            }
        }
    }

    private suspend fun queryCards(q: String): List<HikariMedia> {
        val html = HikariNet.postString(
            "$BASE/query/",
            "query=" + URLEncoder.encode(q, "UTF-8"),
            siteHeaders("$BASE/search/", xhr = true) + mapOf("Content-Type" to "application/x-www-form-urlencoded"),
        ) ?: return emptyList()
        return parseAnchors(html, requireBlock = true)
    }

    // ------------------------------------------------------------------
    // Meta + episodes (with sub/dub counterpart pairing)
    // ------------------------------------------------------------------

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val slug = slugFromHref(media.id).ifBlank { return media }
        val html = get("$BASE/anime/$slug/") ?: return media
        val ld = parseJsonLd(html)
        val title = ld?.optString("name")?.trim()?.takeIf { it.isNotEmpty() }
            ?: Regex("<h1[^>]*>([\\s\\S]*?)</h1>").find(html)?.groupValues?.get(1)?.let { stripTags(it) }
            ?: media.title
        val poster = ld?.optString("image")?.takeIf { it.isNotEmpty() } ?: media.posterUrl
        val banner = Regex("<img[^>]*src=\"([^\"]*?/banner/[^\"]*)\"").find(html)?.groupValues?.get(1)
        val overview = ld?.optString("description")?.takeIf { it.isNotEmpty() } ?: media.overview
        val year = ld?.optString("datePublished")?.takeIf { it.length >= 4 }?.substring(0, 4)?.toIntOrNull() ?: media.year
        val genres = ld?.optJSONArray("genre")?.let { arr ->
            buildList { for (i in 0 until arr.length()) arr.optString(i).takeIf { it.isNotEmpty() }?.let { add(it) } }
        } ?: media.genres
        return media.copy(
            title = title, posterUrl = poster, backdropUrl = banner ?: poster,
            overview = overview, genres = genres, year = year,
        )
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val slug = slugFromHref(media.id).ifBlank { return null }
        val html = get("$BASE/anime/$slug/") ?: return null
        val titleTag = Regex("<title[^>]*>([\\s\\S]*?)</title>").find(html)?.groupValues?.get(1)?.trim() ?: return null
        if (titleTag.contains("404")) return null
        val title = parseJsonLd(html)?.optString("name")?.trim()?.takeIf { it.isNotEmpty() }
            ?: Regex("<h1[^>]*>([\\s\\S]*?)</h1>").find(html)?.groupValues?.get(1)?.let { stripTags(it) }
            ?: media.title

        val epItems = parseEpItems(html)
        if (epItems.isEmpty()) return null
        val ownLang = detectLang(titleTag, slug) ?: epItems.firstNotNullOfOrNull { langFromName(it.name) }

        val counterpart = findCounterpart(title, slug, ownLang)
        val counterpartLang = counterpart?.let { loadCounterpartLang(it) }
        val counterpartItems = if (counterpart != null && counterpartLang != null && counterpartLang != ownLang) {
            loadCounterpartEpisodes(counterpart)
        } else emptyList()

        val isMovie = parseJsonLd(html)?.optString("@type") == "Movie" ||
            sidebarValue(html, "รูปแบบ").equals("Movie", ignoreCase = true) ||
            (epItems.size == 1 && !epItems[0].name.contains("ตอนที่"))

        val own = buildEpisodes(epItems, ownLang ?: MARKER_SUB, isMovie, title)
        val merged = buildEpisodes(counterpartItems, counterpartLang ?: MARKER_SUB, isMovie, title)
        val subEps = if (ownLang == MARKER_DUB) merged else own
        val dubEps = if (ownLang == MARKER_DUB) own else merged
        if (subEps.isEmpty() && dubEps.isEmpty()) return null
        return subEps + dubEps
    }

    private data class EpItem(val epId: String, val name: String, val num: Int)

    private fun buildEpisodes(items: List<EpItem>, lang: String, isMovie: Boolean, title: String): List<HikariEpisode> =
        items.map { item ->
            val data = JSONObject().put("epId", item.epId).put("lang", lang).toString()
            HikariEpisode(
                number = item.num,
                id = "th|$data|$lang",
                name = if (isMovie) title else stripMarkers(item.name),
            )
        }

    private suspend fun findCounterpart(title: String, slug: String, ownLang: String?): String? {
        if (ownLang == null) return null
        val base = cleanTitle(title)
        val baseToken = seasonToken(base)
        val words = base.split(" ").filter { it.isNotBlank() && it.all { c -> c in 'a'..'z' } }
        val q = (if (words.size >= 2) words.take(4) else base.split(" ").take(3)).joinToString(" ")
        if (q.isBlank()) return null
        var best: String? = null
        var bestScore = 0.0
        runCatching { quickAjaxRaw(q) }.getOrNull()?.forEach { (candSlug, candTitle, category) ->
            if (candSlug.isBlank() || candTitle.isEmpty() || candSlug == slug) return@forEach
            val lang = detectLang(candTitle, candSlug) ?: CATEGORY_LANG[category] ?: return@forEach
            if (lang == ownLang) return@forEach
            val candClean = cleanTitle(candTitle)
            val candToken = seasonToken(candClean)
            if (baseToken != null && candToken != null && baseToken != candToken) return@forEach
            val score = similarity(base, candClean)
            if (score > bestScore) { bestScore = score; best = candSlug }
        }
        runCatching { queryCards(q) }.getOrNull()?.forEach { resp ->
            val candSlug = slugFromHref(resp.id)
            if (candSlug == slug) return@forEach
            val lang = detectLang(resp.title, candSlug) ?: return@forEach
            if (lang == ownLang) return@forEach
            val candClean = cleanTitle(resp.title)
            val candToken = seasonToken(candClean)
            if (baseToken != null && candToken != null && baseToken != candToken) return@forEach
            val score = similarity(base, candClean)
            if (score > bestScore) { bestScore = score; best = candSlug }
        }
        return best?.takeIf { bestScore >= MIN_COUNTERPART_SCORE }
    }

    private data class AjaxRow(val slug: String, val title: String, val category: String)

    private suspend fun quickAjaxRaw(q: String): List<AjaxRow> {
        val body = HikariNet.getString(
            "$BASE/vendor/search-ajax.php?q=" + URLEncoder.encode(q, "UTF-8"),
            siteHeaders("$BASE/", xhr = true),
        ) ?: return emptyList()
        val arr = runCatching { JSONObject(body).optJSONArray("results") }.getOrNull() ?: return emptyList()
        return buildList {
            for (i in 0 until arr.length()) {
                val r = arr.optJSONObject(i) ?: continue
                add(AjaxRow(r.optString("slug"), r.optString("title").trim(), r.optString("category")))
            }
        }
    }

    private suspend fun loadCounterpartLang(slug: String): String? {
        val html = get("$BASE/anime/$slug/") ?: return null
        val title = Regex("<title[^>]*>([\\s\\S]*?)</title>").find(html)?.groupValues?.get(1)?.trim() ?: return null
        return detectLang(title, slug)
    }

    private suspend fun loadCounterpartEpisodes(slug: String): List<EpItem> {
        val html = get("$BASE/anime/$slug/") ?: return emptyList()
        return parseEpItems(html)
    }

    // ------------------------------------------------------------------
    // Streams: base/{epId} -> playback sid -> fileId -> HLS master
    // ------------------------------------------------------------------

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val epRef = episode?.id ?: return emptyList()
        val payload = runCatching { JSONObject(epRef.substringAfter("th|").substringBeforeLast("|")) }.getOrNull()
            ?: return emptyList()
        val epId = payload.optString("epId").ifBlank { return emptyList() }
        val label = if (payload.optString("lang", MARKER_SUB) == MARKER_DUB) "DUB" else "SUB"

        val fileId = resolveFileId(epId) ?: return emptyList()
        val masterUrl = "$FILE_BASE/file2/$fileId/"
        var master = fetchMaster(fileId)
        if (!master.startsWith("#EXTM3U")) {
            kotlinx.coroutines.delay(1500)
            master = fetchMaster(fileId)
        }
        if (!master.startsWith("#EXTM3U")) return emptyList()
        val playHeaders = mapOf(
            "User-Agent" to UA,
            "Referer" to "$FILE_BASE/v2/$fileId",
            "Accept" to "*/*",
        )
        return listOf(
            HikariStream(
                name = "AnimeTH $label",
                url = masterUrl,
                headers = playHeaders,
                isM3u8 = true,
            )
        )
    }

    private suspend fun resolveFileId(epId: String): String? {
        return try {
            val baseJs = HikariNet.getString("$BASE/base/$epId/", siteHeaders("$BASE/watch/$epId.html")) ?: return null
            val sid = RX_PLAYBACK_SID.find(baseJs)?.groupValues?.get(1) ?: return null
            kotlinx.coroutines.delay(400)
            val page = HikariNet.getString("$STREAM_BASE/playback/v/$sid/", siteHeaders("$BASE/")) ?: return null
            FILE_ID_REGEX.find(page)?.groupValues?.get(1)
        } catch (t: Throwable) { null }
    }

    private suspend fun fetchMaster(fileId: String): String {
        return HikariNet.getString("$FILE_BASE/file2/$fileId/", siteHeaders("$FILE_BASE/v2/$fileId")) ?: ""
    }

    // ------------------------------------------------------------------
    // HTML parsing (regex equivalents of the source jsoup selectors)
    // ------------------------------------------------------------------

    private suspend fun get(url: String): String? {
        var last: String? = null
        repeat(2) { attempt ->
            last = HikariNet.getString(url, siteHeaders())
                ?: HikariNet.getStringSmart(url, siteHeaders())
            if (last != null) return last
            if (attempt == 0) kotlinx.coroutines.delay(1200)
        }
        return last
    }

    private fun splitSections(html: String): List<String> {
        val out = ArrayList<String>()
        var idx = 0
        while (true) {
            val s = html.indexOf("<section", idx)
            if (s < 0) break
            val e = html.indexOf("</section>", s)
            if (e < 0) break
            out.add(html.substring(s, e))
            idx = e + 10
        }
        return out
    }

    private fun parseAnchors(html: String, requireBlock: Boolean): List<HikariMedia> {
        val out = LinkedHashMap<String, HikariMedia>()
        for (chunk in html.split("<a ").drop(1)) {
            val tag = chunk.substringBefore(">")
            if (!tag.contains("/anime/")) continue
            if (requireBlock && !tag.contains("block")) continue
            val href = Regex("href=\"([^\"]+)\"").find(tag)?.groupValues?.get(1) ?: continue
            val slug = slugFromHref(href)
            if (slug.isBlank()) continue
            val body = chunk.substringAfter(">", "").substringBefore("</a>")
            val imgTag = Regex("<img[\\s\\S]*?>").find(body)?.value ?: ""
            val title = Regex("<h3[^>]*>([\\s\\S]*?)</h3>").find(body)?.groupValues?.get(1)
                ?.let { stripTags(it) }?.takeIf { it.isNotEmpty() }
                ?: Regex("alt=\"([^\"]+)\"").find(imgTag)?.groupValues?.get(1)?.trim()?.takeIf { it.isNotEmpty() }
                ?: continue
            val poster = Regex("data-src=\"([^\"]+)\"").find(imgTag)?.groupValues?.get(1)
                ?: Regex("src=\"([^\"]+)\"").find(imgTag)?.groupValues?.get(1)
            val category = if (body.contains("cate-ribbon")) {
                Regex("cate-ribbon[^>]*>([^<]*)").find(body)?.groupValues?.get(1)?.trim()
            } else null
            val key = slug.lowercase()
            if (!out.containsKey(key)) out[key] = mediaFor(title, slug, poster?.takeIf { it.startsWith("http") }, category)
        }
        return out.values.toList()
    }

    private fun mediaFor(title: String, slug: String, poster: String?, category: String?): HikariMedia {
        val lang = detectLang(title, slug) ?: category?.let { CATEGORY_LANG[it] }
        return HikariMedia(
            id = "$BASE/anime/$slug/",
            title = title,
            type = HikariMediaType.SERIES,
            posterUrl = poster,
        )
    }

    private fun parseEpItems(html: String): List<EpItem> {
        val out = ArrayList<EpItem>()
        for (chunk in html.split("<a ").drop(1)) {
            val tag = chunk.substringBefore(">")
            if (!tag.contains("ep-item")) continue
            val href = Regex("href=\"([^\"]+)\"").find(tag)?.groupValues?.get(1) ?: continue
            val epId = EP_ID_REGEX.find(href)?.groupValues?.get(1) ?: continue
            val rawName = stripTags(chunk.substringAfter(">", "").substringBefore("</a>"))
            val num = EP_NUM_REGEX.find(rawName)?.groupValues?.get(1)?.toIntOrNull() ?: 1
            out.add(EpItem(epId, rawName, num))
        }
        return out
    }

    private fun parseJsonLd(html: String): JSONObject? {
        val raw = Regex("<script[^>]*type=\"application/ld\\+json\"[^>]*>([\\s\\S]*?)</script>")
            .find(html)?.groupValues?.get(1) ?: return null
        val trimmed = raw.trim()
        return runCatching {
            if (trimmed.startsWith("[")) org.json.JSONArray(trimmed).optJSONObject(0) else JSONObject(trimmed)
        }.getOrNull()
    }

    private fun sidebarValue(html: String, label: String): String {
        val marker = ">$label</div>"
        val idx = html.indexOf(marker)
        if (idx < 0) return ""
        val tail = html.substring(idx + marker.length, minOf(idx + marker.length + 600, html.length))
        return Regex(">([^<]{1,120})<").find(tail)?.groupValues?.get(1)?.trim() ?: ""
    }

    private fun slugKey(url: String): String = slugFromHref(url).lowercase()

    private fun slugFromHref(href: String): String {
        val clean = href.substringBefore("?").trimEnd('/')
        val idx = clean.lastIndexOf("/anime/")
        if (idx < 0) return ""
        return clean.substring(idx + "/anime/".length)
    }

    private fun detectLang(title: String, slug: String): String? {
        if (DUB_WORDS.any { title.contains(it) }) return MARKER_DUB
        if (SUB_WORDS.any { title.contains(it) }) return MARKER_SUB
        val text = title + " " + runCatching { URLDecoder.decode(slug, "UTF-8") }.getOrDefault(slug).replace("-", " ")
        if (DUB_WORDS.any { text.contains(it) }) return MARKER_DUB
        if (SUB_WORDS.any { text.contains(it) }) return MARKER_SUB
        if (text.contains("sub", ignoreCase = true)) return MARKER_SUB
        return null
    }

    private fun langFromName(name: String): String? {
        if (DUB_WORDS.any { name.contains(it) }) return MARKER_DUB
        if (SUB_WORDS.any { name.contains(it) }) return MARKER_SUB
        return null
    }

    private fun stripMarkers(name: String): String {
        var t = name.trim()
        while (true) {
            val suffix = NAME_SUFFIXES.firstOrNull { t.endsWith(it) } ?: break
            t = t.removeSuffix(suffix).trim()
        }
        return t
    }

    private fun stripTags(s: String): String = s
        .replace(Regex("<[^>]+>"), " ")
        .replace("&quot;", "\"").replace("&#039;", "'").replace("&#39;", "'")
        .replace("&#038;", "&").replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">").replace("&nbsp;", " ")
        .replace(Regex("\\s+"), " ")
        .trim()
}
