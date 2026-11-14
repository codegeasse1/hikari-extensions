package com.hikari.ext.providers

import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariNet
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import java.net.URLEncoder

/**
 * 91看片网 (ddza-e1.91kpw222.sbs, /91kp) — classic MacCMS.
 *
 * - catalogs: /91kp/index.php/vod/type/id/<tid>.html,
 *   pages .../page/<n>.html.
 * - cards: dl > dt (cover data-original) + dd (h3 title), detail links.
 * - detail: play-group items (label + /vod/play/id/<id>/sid/<s>/nid/<n>).
 * - play page: player_aaaa JSON carries the direct m3u8/mp4 url.
 * - search: /91kp/index.php/vod/search.html?wd=<q> (page via &page=<n>).
 */
class Kpw91Provider : HikariProvider {

    override val id = "kpw91"
    override val name = "91KanPian"
    override val mainUrl = "https://ddza-e1.91kpw222.sbs"
    override val description = "91KanPian: Chinese/JP/KR/Western and 20+ more categories + search — direct m3u8, unlimited pages."
    override val version = 2
    override val tvTypes = setOf(HikariMediaType.MOVIE)

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? = null

    companion object {
        private const val BASE = "/91kp"
        private val BROWSER_UA by lazy { HikariNet.browserHeaders.getValue("User-Agent") }
        private val CARD_RE = Regex(
            "<dl[^>]*>\\s*<dt[^>]*>\\s*<a[^>]*href=\"([^\"]+/vod/detail/id/(\\d+)\\.html)\"[^>]*>" +
                "[\\s\\S]{0,900}?<img[^>]*?(?:data-original|src)=\"([^\"]+)\"" +
                "[\\s\\S]{0,900}?</dt>\\s*<dd>\\s*<a[^>]*>(?:<h3>)?([^<]{2,120})(?:</h3>)?"
        )
        private val BLOCK_ID_RE = Regex("/vod/detail/id/(\\d+)\\.html")
        private val BLOCK_IMG_RE = Regex("(?:data-original|src)=\"(https?://[^\"]+)\"")
        private val BLOCK_H3_RE = Regex("<h3>([^<]{2,120})</h3>")
        private val PLAY_RE = Regex(
            "href=\"([^\"]+/vod/play/id/(\\d+)/sid/(\\d+)/nid/(\\d+)\\.html)\"[^>]*>([^<]{1,24})</a>"
        )
        private val PLAYER_URL_RE = Regex("\"url\"\\s*:\\s*\"(https?:[^\"]+)\"")
        private val DETAIL_TITLE_RE = Regex("<title>([^<]{2,150})</title>")
        private val DESC_RE =
            Regex("<meta[^>]*name=\"description\"[^>]*content=\"([^\"]{10,600})\"")
    }

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("37", "抖阴视频", HikariMediaType.MOVIE),
        HikariCatalog("39", "网曝黑料", HikariMediaType.MOVIE),
        HikariCatalog("34", "国产主播", HikariMediaType.MOVIE),
        HikariCatalog("36", "明星换脸", HikariMediaType.MOVIE),
        HikariCatalog("28", "国产传媒", HikariMediaType.MOVIE),
        HikariCatalog("26", "国产视频", HikariMediaType.MOVIE),
        HikariCatalog("80", "网红主播", HikariMediaType.MOVIE),
        HikariCatalog("121", "OnlyFans", HikariMediaType.MOVIE),
        HikariCatalog("81", "国产剧情", HikariMediaType.MOVIE),
        HikariCatalog("82", "国产自拍", HikariMediaType.MOVIE),
        HikariCatalog("83", "国产探花", HikariMediaType.MOVIE),
        HikariCatalog("84", "网曝吃瓜", HikariMediaType.MOVIE),
        HikariCatalog("30", "日本无码", HikariMediaType.MOVIE),
        HikariCatalog("29", "日本有码", HikariMediaType.MOVIE),
        HikariCatalog("27", "中文字幕", HikariMediaType.MOVIE),
        HikariCatalog("48", "韩国主播", HikariMediaType.MOVIE),
        HikariCatalog("31", "欧美无码", HikariMediaType.MOVIE),
        HikariCatalog("119", "日本动漫", HikariMediaType.MOVIE),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val path = if (page <= 1) {
            "$BASE/index.php/vod/type/id/${catalog.id}.html"
        } else {
            "$BASE/index.php/vod/type/id/${catalog.id}/page/$page.html"
        }
        val html = HikariNet.getString(mainUrl + path, referer()) ?: return emptyList()
        return parseCards(html)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        val enc = URLEncoder.encode(q, "UTF-8")
        val path = if (page <= 1) {
            "$BASE/index.php/vod/search.html?wd=$enc"
        } else {
            "$BASE/index.php/vod/search.html?wd=$enc&page=$page"
        }
        val html = HikariNet.getString(mainUrl + path, referer()) ?: return emptyList()
        return parseCards(html)
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val html = HikariNet.getString(
            "$mainUrl$BASE/index.php/vod/detail/id/${media.id}.html", referer()
        ) ?: return media
        val rawTitle = DETAIL_TITLE_RE.find(html)?.groupValues?.get(1)?.trim().orEmpty()
        val title = rawTitle.split("详情介绍", "-").firstOrNull()?.trim().orEmpty()
        val desc = DESC_RE.find(html)?.groupValues?.get(1)?.let { unesc(it) }?.trim().orEmpty()
        return media.copy(
            title = title.ifBlank { media.title },
            overview = desc,
        )
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val html = HikariNet.getString(
            "$mainUrl$BASE/index.php/vod/detail/id/${media.id}.html", referer()
        ) ?: return emptyList()
        val plays = PLAY_RE.findAll(html).toList().distinctBy { it.groupValues[1] }
        if (plays.isEmpty()) return emptyList()
        val out = ArrayList<HikariStream>()
        for (p in plays) {
            val url = playM3u8(p.groupValues[1]) ?: continue
            out.add(
                HikariStream(
                    name = p.groupValues[5].trim().ifBlank { "91看片网" },
                    url = url,
                    headers = mapOf("Referer" to "$mainUrl/"),
                    isM3u8 = url.contains(".m3u8"),
                )
            )
        }
        return out
    }

    // ---- internals ----

    private fun parseCards(html: String): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        val seen = HashSet<String>()
        for (m in CARD_RE.findAll(html)) {
            val id = m.groupValues[2]
            if (!seen.add(id)) continue
            val cover = m.groupValues[3].trim().takeIf { it.startsWith("http") }
            val title = unesc(m.groupValues[4]).trim().takeIf { it.isNotBlank() } ?: continue
            out.add(
                HikariMedia(
                    id = id,
                    title = title,
                    type = HikariMediaType.MOVIE,
                    posterUrl = cover,
                )
            )
        }
        if (out.isNotEmpty()) return out
        // Fallback: split on <dl blocks (menu <dl>s have no detail links).
        for (block in html.split("<dl")) {
            if (!block.contains("/vod/detail/id/")) continue
            val idm = BLOCK_ID_RE.find(block) ?: continue
            val id = idm.groupValues[1]
            if (!seen.add(id)) continue
            val cover = BLOCK_IMG_RE.find(block)?.groupValues?.get(1)?.trim()
                ?.takeIf { it.startsWith("http") }
            val title = BLOCK_H3_RE.find(block)?.groupValues?.get(1)
                ?.let { unesc(it) }?.trim()?.takeIf { it.isNotBlank() } ?: continue
            out.add(
                HikariMedia(
                    id = id,
                    title = title,
                    type = HikariMediaType.MOVIE,
                    posterUrl = cover,
                )
            )
        }
        return out
    }

    private suspend fun playM3u8(playPath: String): String? {
        val html = HikariNet.getString(mainUrl + playPath, referer()) ?: return null
        val raw = PLAYER_URL_RE.find(html)?.groupValues?.get(1) ?: return null
        val url = raw.replace("\\/", "/").trim()
        return url.takeIf { it.startsWith("http") }
    }

    private fun referer(): Map<String, String> = mapOf(
        "Referer" to "$mainUrl/",
        "User-Agent" to BROWSER_UA,
    )

    private fun unesc(s: String): String = s
        .replace("&quot;", "\"").replace("&#34;", "\"").replace("&#39;", "'")
        .replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
}
