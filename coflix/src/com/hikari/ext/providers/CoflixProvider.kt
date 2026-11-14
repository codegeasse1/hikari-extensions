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
 * Coflix (coflix.esq) — French films / series / drames (K-Drama), WordPress +
 * MangaDex theme, verified live Oct 2026:
 *  - Rows: /films/, /series/, /drames/; home has "Derniers episodes" (.md-ep-row
 *    with S01 . Ep. N badges) + Films/Series .md-manga-card carousels.
 *  - Cards: <a class="md-card-link" href="https://coflix.esq/?p=N"> + sibling
 *    .md-manga-card-name + img data-src/src (TMDB image URLs).
 *  - Detail pages embed <div class="cfp-player" data-cfp='{"rest":".../wp-json/coflix/v1/resolve","params":{"tmdb":..,"type":"movie","year":"..","pid":..}}'>.
 *  - Playback JACKPOT: GET {rest}?tmdb=..&type=..&year=..&pid=..[&season=..&episode=..]
 *    returns {ok, mode:"iframe", host, lang, url(embed), poster, backdrop,
 *    servers:[{host,lang,direct,active}], downloads:[{url,name,lang}]} — verified
 *    live (af-coflix-resolve.json: Vidzy embed + 11 servers + 1fichier/MegaUp/Voe
 *    download links for "The Fix" 2026).
 *  - Series detail pages carry one cfp-player per episode (params include
 *    season/episode); movie pages carry a single player.
 */
class CoflixProvider : HikariProvider {

    override val id = "coflix"
    override val name = "Coflix"
    override val mainUrl = "https://coflix.esq"
    override val description = "Films et series en streaming VF/VOSTFR — films, series, drames."
    override val version = 1
    override val tvTypes = setOf(HikariMediaType.SERIES, HikariMediaType.MOVIE)

    private val UA =
        "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36"

    companion object {
        val BASES = listOf("https://coflix.esq", "https://coflix.upn.one")
        var base = BASES[0]
    }

    private fun h(referer: String = "$base/"): Map<String, String> =
        mapOf("User-Agent" to UA, "Referer" to referer, "Accept-Language" to "fr-FR,fr;q=0.9")

    private suspend fun get(path: String): String? {
        for (b in BASES) {
            val t = HikariNet.getStringSmart(if (path.startsWith("http")) path else b + path, h("$b/"))
            if (t != null) {
                base = b
                return t
            }
        }
        return null
    }

    override fun catalogs(): List<HikariCatalog> = listOf(
        HikariCatalog("latest-ep", "Derniers episodes", HikariMediaType.SERIES),
        HikariCatalog("films", "Films", HikariMediaType.MOVIE),
        HikariCatalog("series", "Series", HikariMediaType.SERIES),
        HikariCatalog("drames", "Drames", HikariMediaType.SERIES),
        HikariCatalog("latest-mov", "Films recents", HikariMediaType.MOVIE),
        HikariCatalog("latest-tv", "Series recentes", HikariMediaType.SERIES),
    )

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        val p = page.coerceAtLeast(1)
        if (catalog.id == "latest-ep") {
            val html = get(if (p > 1) "/page/$p/" else "/") ?: return emptyList()
            val eps = parseEpRows(html)
            if (eps.isNotEmpty()) return eps
            return parseCards(html)
        }
        val section = when (catalog.id) {
            "films", "latest-mov" -> "films"
            "drames" -> "drames"
            else -> "series"
        }
        val path = if (p > 1) "/$section/page/$p/" else "/$section/"
        val html = get(path) ?: get(if (p > 1) "/page/$p/" else "/") ?: return emptyList()
        val cards = parseCards(html)
        if (cards.isNotEmpty()) return cards
        return parseEpRows(html)
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank() || page > 1) return emptyList()
        val html = get("/?s=${URLEncoder.encode(q, "UTF-8")}") ?: return emptyList()
        val cards = parseCards(html)
        if (cards.isNotEmpty()) return cards
        return parseEpRows(html)
    }

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val html = HikariNet.getStringSmart(media.id, h()) ?: return media
        val ogTitle = Regex("<meta[^>]+property=\"og:title\"[^>]+content=\"([^\"]+)\"")
            .find(html)?.groupValues?.get(1)?.let { unescape(it) }
        val ogImage = Regex("<meta[^>]+property=\"og:image\"[^>]+content=\"([^\"]+)\"")
            .find(html)?.groupValues?.get(1)
        val ogDesc = Regex("<meta[^>]+(?:property=\"og:description\"|name=\"description\")[^>]+content=\"([^\"]+)\"")
            .find(html)?.groupValues?.get(1)?.let { unescape(it) }
        val isMovie = media.id.contains("film", true) ||
            Regex("type[\"']?\\s*[:=]\\s*[\"']?movie").containsMatchIn(html.take(20000))
        return media.copy(
            title = ogTitle?.takeIf { it.isNotBlank() } ?: media.title,
            posterUrl = ogImage?.takeIf { it.startsWith("http") || it.startsWith("//") }?.let { fixUrl(it) } ?: media.posterUrl,
            backdropUrl = ogImage?.takeIf { it.startsWith("http") || it.startsWith("//") }?.let { fixUrl(it) } ?: media.backdropUrl,
            overview = ogDesc?.takeIf { it.isNotBlank() && !it.startsWith("Regarder des Films") },
            type = if (isMovie && media.type == HikariMediaType.SERIES) HikariMediaType.SERIES else media.type,
        )
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? {
        val html = HikariNet.getStringSmart(media.id, h()) ?: return null
        val players = parseCfpPlayers(html)
        if (players.size > 1) {
            return players.mapIndexed { idx, pl ->
                val s = pl.season ?: 1
                val e = pl.episode ?: (idx + 1)
                HikariEpisode(
                    number = e,
                    season = s,
                    id = media.id + "#cfp$idx",
                    name = pl.label?.takeIf { it.isNotBlank() } ?: "S${s} E$e",
                    image = media.posterUrl,
                )
            }
        }
        if (players.size == 1) {
            return listOf(HikariEpisode(number = 1, id = media.id + "#cfp0", name = media.title, image = media.posterUrl))
        }
        // No player block (search stub / episode-row link): single playable item.
        return listOf(HikariEpisode(number = 1, id = media.id + "#full", name = media.title, image = media.posterUrl))
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val ref = (episode?.id ?: media.id).substringBefore("#")
        val epIdx = (episode?.id ?: "").substringAfter("#cfp", "").toIntOrNull() ?: 0
        val html = HikariNet.getStringSmart(ref, h()) ?: return emptyList()
        val players = parseCfpPlayers(html)
        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        if (players.isNotEmpty()) {
            val pl = players.getOrElse(epIdx) { players[0] }
            resolvePlayer(pl, out, seen)
            if (out.isNotEmpty()) return out
        }
        // Fallback: generic iframe/embed scrape on the detail page.
        val frames = Regex("<iframe[^>]+src=[\"']([^\"']+)[\"']").findAll(html)
            .map { it.groupValues[1] }.filter { it.startsWith("http") }.distinct().toList()
        for (f in frames) {
            resolveEmbed(f, f, out, seen)
        }
        if (out.isEmpty()) {
            try {
                val hits = HikariNet.resolveWithWebView(
                    ref,
                    Regex("https?://[^\"'\\s]+?\\.(?:m3u8|mp4)(?:[?#][^\"'\\s]*)?"),
                    timeoutMs = 45_000,
                )
                for (hit in hits) {
                    if (hit.url.isBlank() || !seen.add(hit.url)) continue
                    out.add(HikariStream(name = "Coflix", url = hit.url, headers = hit.headers, isM3u8 = hit.url.contains(".m3u8", true)))
                }
            } catch (t: Throwable) { }
        }
        return out
    }

    // ------------------------------------------------------------------
    // Parsing helpers.
    // ------------------------------------------------------------------

    data class CfpPlayer(
        val rest: String,
        val tmdb: String,
        val type: String,
        val year: String,
        val pid: String,
        val season: Int?,
        val episode: Int?,
        val label: String?,
    )

    fun parseCards(html: String): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        val seen = HashSet<String>()
        // md-card-link anchors carry the ?p= detail URL; title sits in the
        // sibling .md-manga-card-name element, poster in a nearby <img>.
        val linkRe = Regex("<a[^>]+href=\"([^\"]+)\"[^>]*class=\"md-card-link\"[^>]*>")
        for (m in linkRe.findAll(html)) {
            val url = fixUrl(m.groupValues[1])
            if (!url.startsWith("http") || !seen.add(url)) continue
            val window = html.substring(m.range.last, (m.range.last + 2500).coerceAtMost(html.length))
            val before = html.substring(0.coerceAtLeast(m.range.first - 2500), m.range.first)
            val title = Regex("<p class=\"md-manga-card-name\">(.*?)</p>", RegexOption.DOT_MATCHES_ALL).find(window)
                ?.groupValues?.get(1)?.let { stripTags(unescape(it)) }?.trim()
                ?: Regex("aria-label=\"([^\"]+)\"").find(m.value)?.groupValues?.get(1)?.let { unescape(it) }?.trim()
                ?: continue
            if (title.isBlank()) continue
            val img = (Regex("<img[^>]+data-src=\"([^\"]+)\"").find(before.reversed().take(1500).reversed())
                ?: Regex("<img[^>]+src=\"([^\"]+)\"").find(before.takeLast(1500))
                ?: Regex("<img[^>]+(?:data-src|src)=\"([^\"]+)\"").find(window))
                ?.groupValues?.get(1)?.let { fixUrl(it) }?.takeIf { it.startsWith("http") }
            val isSeries = url.contains("/series", true) || window.contains("md-ep-type--serie") ||
                title.contains("S0", true) || title.contains("Saison", true)
            out.add(
                HikariMedia(
                    id = url,
                    title = title,
                    type = if (isSeries) HikariMediaType.SERIES else HikariMediaType.MOVIE,
                    posterUrl = img,
                )
            )
        }
        return out
    }

    fun parseEpRows(html: String): List<HikariMedia> {
        val out = ArrayList<HikariMedia>()
        val seen = HashSet<String>()
        val rowRe = Regex("<a[^>]+href=\"([^\"]+)\"[^>]*class=\"md-ep-row\"[^>]*>(.*?)</a>", RegexOption.DOT_MATCHES_ALL)
        for (m in rowRe.findAll(html)) {
            val url = fixUrl(m.groupValues[1])
            if (!url.startsWith("http") || !seen.add(url)) continue
            val block = m.groupValues[2]
            val title = Regex("<p class=\"md-ep-title\">(.*?)</p>", RegexOption.DOT_MATCHES_ALL).find(block)
                ?.groupValues?.get(1)?.let { stripTags(unescape(it)) }?.trim()?.takeIf { it.isNotBlank() } ?: continue
            val badge = Regex("S(\\d+)\\s*[·.]\\s*[ÉE]p\\.\\s*(\\d+)", RegexOption.IGNORE_CASE).find(block)
            val se = if (badge != null) " S${badge.groupValues[1]} E${badge.groupValues[2]}" else ""
            val img = Regex("<img[^>]+src=\"([^\"]+)\"").find(block)?.groupValues?.get(1)?.let { fixUrl(it) }
                ?.takeIf { it.startsWith("http") }
            out.add(HikariMedia(id = url, title = "$title$se", type = HikariMediaType.SERIES, posterUrl = img))
        }
        return out
    }

    fun parseCfpPlayers(html: String): List<CfpPlayer> {
        val out = ArrayList<CfpPlayer>()
        // data-cfp='{...}' with HTML-escaped quotes (&quot;).
        val re = Regex("data-cfp=\"(.*?)\"", RegexOption.DOT_MATCHES_ALL)
        for (m in re.findAll(html)) {
            val raw = m.groupValues[1].replace("&quot;", "\"").replace("&#039;", "'").replace("&amp;", "&")
            val o = runCatching { JSONObject(raw) }.getOrNull() ?: continue
            val rest = o.optString("rest")
            if (!rest.startsWith("http")) continue
            val p = o.optJSONObject("params") ?: JSONObject()
            out.add(
                CfpPlayer(
                    rest = rest,
                    tmdb = p.optString("tmdb").ifBlank { p.optInt("tmdb", 0).takeIf { it > 0 }?.toString() ?: "" },
                    type = p.optString("type").ifBlank { "movie" },
                    year = p.optString("year"),
                    pid = p.optString("pid").ifBlank { p.optInt("pid", 0).takeIf { it > 0 }?.toString() ?: "" },
                    season = p.optInt("season", -1).takeIf { it >= 0 } ?: p.optInt("saison", -1).takeIf { it >= 0 },
                    episode = p.optInt("episode", -1).takeIf { it >= 0 },
                    label = o.optString("label").takeIf { it.isNotBlank() },
                )
            )
        }
        return out
    }

    private suspend fun resolvePlayer(pl: CfpPlayer, out: MutableList<HikariStream>, seen: HashSet<String>) {
        val qs = StringBuilder()
        if (pl.tmdb.isNotBlank()) qs.append("tmdb=${URLEncoder.encode(pl.tmdb, "UTF-8")}&")
        if (pl.type.isNotBlank()) qs.append("type=${URLEncoder.encode(pl.type, "UTF-8")}&")
        if (pl.year.isNotBlank()) qs.append("year=${URLEncoder.encode(pl.year, "UTF-8")}&")
        if (pl.pid.isNotBlank()) qs.append("pid=${URLEncoder.encode(pl.pid, "UTF-8")}&")
        if (pl.season != null) qs.append("season=${pl.season}&")
        if (pl.episode != null) qs.append("episode=${pl.episode}&")
        val url = pl.rest + "?" + qs.toString().trimEnd('&')
        val txt = HikariNet.getString(url, h()) ?: return
        val root = runCatching { JSONObject(txt) }.getOrNull() ?: return
        // Primary embed iframe.
        val embed = root.optString("url")
        val host = root.optString("host")
        val lang = root.optString("lang")
        if (embed.startsWith("http")) {
            val tag = "Coflix" + (if (host.isNotBlank()) " $host" else "") + (if (lang.isNotBlank()) " $lang" else "")
            resolveEmbed(embed, url, out, seen, tag)
        }
        // Download mirrors double as backup streams.
        val dls = root.optJSONArray("downloads")
        if (dls != null) {
            for (i in 0 until dls.length()) {
                val d = dls.optJSONObject(i) ?: continue
                val u = d.optString("url")
                if (!u.startsWith("http") || !seen.add("dl:$u")) continue
                val nm = d.optString("name").ifBlank { "Download" }
                val lg = d.optString("lang")
                out.add(HikariStream(name = "Coflix $nm${if (lg.isNotBlank()) " $lg" else ""}", url = u, headers = mapOf("User-Agent" to UA, "Referer" to "$base/")))
            }
        }
    }

    private suspend fun resolveEmbed(embedUrl: String, referer: String, out: MutableList<HikariStream>, seen: HashSet<String>, tag: String = "Coflix") {
        val page = HikariNet.getString(embedUrl, mapOf("User-Agent" to UA, "Referer" to referer))
        if (page != null) {
            for (m in Regex("https?://[^\\s\"'<>\\\\]+\\.(?:m3u8|mp4)[^\\s\"'<>\\\\]*").findAll(page)) {
                val u = m.value
                if (u.startsWith("http") && seen.add(u)) {
                    out.add(HikariStream(name = tag, url = u, headers = mapOf("User-Agent" to UA, "Referer" to embedUrl), isM3u8 = u.contains(".m3u8")))
                }
            }
            if (out.isNotEmpty()) return
        }
        try {
            val hits = HikariNet.resolveWithWebView(
                embedUrl,
                Regex("https?://[^\"'\\s]+?\\.(?:m3u8|mp4)(?:[?#][^\"'\\s]*)?"),
                timeoutMs = 45_000,
            )
            for (hit in hits) {
                if (hit.url.isBlank() || !seen.add(hit.url)) continue
                out.add(HikariStream(name = tag, url = hit.url, headers = hit.headers, isM3u8 = hit.url.contains(".m3u8", true)))
            }
        } catch (t: Throwable) { }
    }

    private fun fixUrl(u: String): String {
        if (u.startsWith("//")) return "https:$u"
        return u
    }

    private fun stripTags(s: String): String =
        s.replace(Regex("<[^>]+>"), " ").replace(Regex("\\s+"), " ").trim()

    private fun unescape(s: String): String =
        s.replace("&#039;", "'").replace("&#8217;", "'").replace("&#8211;", "-")
            .replace("&#038;", "&").replace("&amp;", "&").replace("&#8230;", "...")
            .replace("&quot;", "\"").replace("&nbsp;", " ")
}
