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
 * 91社 (9273.m256ed7n.cc) — community post walls as JSON-LD ItemLists.
 *
 * - catalogs: /tag/<slug>/ (~tags), pages /tag/<slug>/<n>/.
 * - each item links /post/<id>/; covers come from the post-media-cell
 *   markup (plain images, time-bound auth query).
 * - post page embeds a media manifest: video cells carry
 *   data-video="/api/posts/<id>/playback#<idx>".
 * - streams: GET the playback path (same shape as the manifest src).
 * - search: /search/<q>/ (tag-shaped pages, same parser).
 */
class She91Provider : HikariProvider {

    override val id = "she91"
    override val name = "91社"
    override val mainUrl = "https://9273.m256ed7n.cc"
    override val description = "91社: 网红黑料与成人视频社区, 标签墙 + 搜索 — 直接 m3u8, 无限翻页."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.MOVIE)

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? = null

    companion object {
        private val CELL_RE =
            Regex("data-src=\"([^\"]+)\"[^>]*?data-video=\"(/api/posts/(\\d+)/playback)[^\"]*\"")
        private val VIDEO_CELL_RE = Regex("data-video=\"(/api/posts/(\\d+)/playback)[^\"]*\"")
        private val MANIFEST_RE = Regex("\"video\"\\s*:\\s*\"(/api/posts/\\d+/playback[^\"]*)\"")
        private val SRC_RE = Regex("\"src\"\\s*:\\s*\"(https?[^\"]+)\"")
        private val POSTER_RE = Regex("<meta[^>]*property=\"og:image\"[^>]*content=\"([^\"]+)\"")
        private val DESC_RE = Regex("<meta[^>]*name=\"description\"[^>]*content=\"([^\"]+)\"")
    }

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("guochan", "国产", HikariMediaType.MOVIE),
        HikariCatalog("zhubo", "主播", HikariMediaType.MOVIE),
        HikariCatalog("wuma", "无码", HikariMediaType.MOVIE),
        HikariCatalog("nvshen", "女神", HikariMediaType.MOVIE),
        HikariCatalog("jvid", "JVID", HikariMediaType.MOVIE),
        HikariCatalog("fuliji", "福利姬", HikariMediaType.MOVIE),
        HikariCatalog("fc2", "FC2", HikariMediaType.MOVIE),
        HikariCatalog("heisi", "黑丝", HikariMediaType.MOVIE),
        HikariCatalog("nvdaxuesheng", "女大学生", HikariMediaType.MOVIE),
        HikariCatalog("renqi", "人妻", HikariMediaType.MOVIE),
        HikariCatalog("aiduanju", "AI短剧", HikariMediaType.MOVIE),
        HikariCatalog("juqing", "剧情", HikariMediaType.MOVIE),
        HikariCatalog("jipin", "极品", HikariMediaType.MOVIE),
        HikariCatalog("cos", "COS", HikariMediaType.MOVIE),
        HikariCatalog("yujie", "御姐", HikariMediaType.MOVIE),
        HikariCatalog("hougong", "后宫", HikariMediaType.MOVIE),
        HikariCatalog("qingquneiyi", "情趣内衣", HikariMediaType.MOVIE),
        HikariCatalog("nenxue", "嫩穴", HikariMediaType.MOVIE),
        HikariCatalog("7bosstanhua", "7BOSS探花", HikariMediaType.MOVIE),
        HikariCatalog("luchu", "露出", HikariMediaType.MOVIE),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val path = if (page <= 1) "/tag/${catalog.id}/" else "/tag/${catalog.id}/page/$page/"
        val html = HikariNet.getStringSmart(mainUrl + path, referer()) ?: return emptyList()
        return parseList(html)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        val enc = URLEncoder.encode(q, "UTF-8").replace("+", "%20")
        val path = if (page <= 1) "/search/$enc/" else "/search/$enc/page/$page/"
        val html = HikariNet.getStringSmart(mainUrl + path, referer()) ?: return emptyList()
        return parseList(html)
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val html = HikariNet.getStringSmart("$mainUrl/post/${media.id}/", referer()) ?: return media
        val desc = DESC_RE.find(html)?.groupValues?.get(1)?.let { unesc(it) }?.trim().orEmpty()
        val cover = manifestCover(html) ?: POSTER_RE.find(html)?.groupValues?.get(1)?.let { unesc(it) }
        return media.copy(
            posterUrl = (cover?.takeIf { it.isNotBlank() } ?: media.posterUrl),
            overview = desc,
        )
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val html = HikariNet.getStringSmart("$mainUrl/post/${media.id}/", referer()) ?: return emptyList()
        val paths = MANIFEST_RE.findAll(html).map { it.groupValues[1] }.toList().distinct()
        if (paths.isEmpty()) return emptyList()
        val out = ArrayList<HikariStream>()
        for (p in paths) {
            val url = playbackUrl(p) ?: continue
            out.add(
                HikariStream(
                    name = if (paths.size == 1) "91社" else "线路${out.size + 1}",
                    url = url,
                    headers = mapOf("Referer" to "$mainUrl/"),
                    isM3u8 = url.contains(".m3u8"),
                )
            )
        }
        return out
    }

    // ---- walls ----

    private data class Card(val id: String, val title: String, val cover: String?)

    private fun parseList(html: String): List<HikariMedia> {
        val names = jsonLdNames(html)
        val covers = HashMap<String, String>()
        for (m in CELL_RE.findAll(html)) {
            covers[m.groupValues[3]] = m.groupValues[1]
        }
        val out = ArrayList<HikariMedia>()
        val seen = HashSet<String>()
        for ((url, name) in names) {
            val id = Regex("/post/(\\d+)/").find(url)?.groupValues?.get(1) ?: continue
            if (!seen.add(id)) continue
            out.add(
                HikariMedia(
                    id = id,
                    title = name,
                    type = HikariMediaType.MOVIE,
                    posterUrl = covers[id],
                )
            )
        }
        return out
    }

    // ---- detail ----

    private fun manifestCover(postHtml: String): String? {
        // First video cell's src is the video cover.
        val cells = CELL_RE.findAll(postHtml).toList()
        if (cells.isNotEmpty()) return cells[0].groupValues[1]
        return SRC_RE.find(postHtml)?.groupValues?.get(1)?.let { unesc(it) }
    }

    private suspend fun playbackUrl(path: String): String? {
        val clean = path.substringBefore("#")
        val body = HikariNet.getString(mainUrl + clean, referer()) ?: return null
        val t = body.trim()
        if (t.startsWith("#EXTM3U")) {
            // Rare: the endpoint answers the playlist itself.
            return mainUrl + clean
        }
        val root = runCatching { JSONObject(t) }.getOrNull() ?: return null
        // Try the usual shapes: {url}, {m3u8_url}, {data:{url...}}, {data:{list:[...]}}.
        val cands = ArrayList<String>()
        cands.add(root.optString("url"))
        cands.add(root.optString("m3u8_url"))
        cands.add(root.optString("src"))
        cands.add(root.optString("playUrl"))
        val data = root.optJSONObject("data")
        if (data != null) {
            cands.add(data.optString("url"))
            cands.add(data.optString("m3u8_url"))
            cands.add(data.optString("src"))
            val list = data.optJSONArray("list")
            if (list != null) {
                for (k in 0 until list.length()) {
                    val o = list.optJSONObject(k) ?: continue
                    cands.add(o.optString("url"))
                    cands.add(o.optString("m3u8_url"))
                    cands.add(o.optString("src"))
                }
            }
        }
        val hit = cands.firstOrNull { it.isNotBlank() && it.startsWith("http") }
        if (hit != null) return hit
        // Last resort: first http(s) URL that looks like a stream.
        val m = Regex("(https?[^\"'\\s]+\\.m3u8[^\"'\\s]*)").find(t)
            ?: Regex("(https?[^\"'\\s]+\\.mp4[^\"'\\s]*)").find(t)
        return m?.groupValues?.get(1)
    }

    // ---- shared helpers ----

    private fun referer(): Map<String, String> = mapOf("Referer" to "$mainUrl/")

    private fun unesc(s: String): String = s
        .replace("&quot;", "\"").replace("&#34;", "\"").replace("&#39;", "'")
        .replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")

    private fun jsonLdNames(html: String): Map<String, String> {
        val out = LinkedHashMap<String, String>()
        var i = 0
        while (true) {
            i = html.indexOf("<script type=\"application/ld+json\">", i)
            if (i < 0) break
            val s = i + "<script type=\"application/ld+json\">".length
            val e = html.indexOf("</script>", s)
            if (e < 0) break
            runCatching {
                collectItems(JSONObject(html.substring(s, e)), out)
            }
            i = e + 9
        }
        return out
    }

    private fun collectItems(o: JSONObject, out: MutableMap<String, String>) {
        if (o.optString("@type") == "ItemList") {
            val arr = o.optJSONArray("itemListElement") ?: return
            for (k in 0 until arr.length()) {
                val it = arr.optJSONObject(k) ?: continue
                val inner = it.optJSONObject("item")
                val name = (inner?.optString("name") ?: it.optString("name")).trim()
                val url = (inner?.optString("url") ?: it.optString("url")).trim()
                if (name.isNotBlank() && url.isNotBlank() && !out.containsKey(url)) out[url] = name
            }
            return
        }
        val g = o.optJSONArray("@graph")
        if (g != null) {
            for (k in 0 until g.length()) {
                g.optJSONObject(k)?.let { collectItems(it, out) }
            }
        }
        o.optJSONObject("mainEntity")?.let { collectItems(it, out) }
    }
}
