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
import java.net.URLEncoder
import java.security.MessageDigest
import java.security.SecureRandom
import javax.crypto.Cipher
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec

/**
 * Krx18 (krx18.com) — DooPlay-theme erotic/JAV movie site.
 *
 *  - Archive pages (`/movies/`, `/movies/?orderby=…`, `/movies/page/N/`) and
 *    WordPress search (`/?s=…`) are grids of `<article class="item">` cards
 *    carrying the numeric post id, poster and title.
 *  - The DooPlay REST API serves A–Z browsing (`/wp-json/dooplay/glossary/`)
 *    and, per server, the player embed URL
 *    (`/wp-json/dooplayer/v2/<post>/<type>/<nume>` → `{embed_url}`).
 *  - The primary server (play.playkrx18.site, a 9stream JWPlayer embed) hides
 *    its source behind an AES-encrypted POST API, so it is resolved over plain
 *    HTTP here: the embed page carries OpenSSL-AES-encrypted `idfile`/`iduser`
 *    constants, which are decrypted and re-encrypted into a signed config blob
 *    for `POST <api>/playiframe`; the response's hex-encrypted `data` decrypts
 *    to the m3u8 URL. Loadvid/mov18plus embeds are protected differently and
 *    fall back to the generic WebView m3u8/mp4 capture.
 */
class Krx18Provider : HikariProvider {

    override val id = "krx18"
    override val name = "Krx18"
    override val mainUrl = "https://krx18.com"
    override val description = "Erotic/JAV movies, DooPlay-powered, with multi-server JWPlayer embeds."
    override val tvTypes: Set<HikariMediaType> = setOf(HikariMediaType.MOVIE)
    override val version = 3

    companion object {
        private const val BASE = "https://krx18.com"
        private const val CACHE_TTL_MS = 600_000L
        private val htmlCache = HashMap<String, Pair<Long, String>>()
        private val cacheMutex = Mutex()
        private val streamCapture = Regex("""https?://[^"'\s]+?\.(?:m3u8|mp4)(?:[?#][^"'\s]*)?""")

        // 9stream / play.playkrx18.site embedded keys (reverse-engineered from
        // the player bundle; stable across page loads).
        private const val KEY_IDFILE = "jcLycoRJT6OWjoWspgLMOZwS3aSS0lEn"
        private const val KEY_IDUSER = "PZZ3J3LDbLT0GY7qSA5wW5vchqgpO36O"
        private const val KEY_CONFIG = "vlVbUQhkOhoSfyteyzGeeDzU0BHoeTyZ"
        private const val KEY_RESPONSE = "oJwvmmVBajMaRCTklxbfjavpQO7SZpsL"
        private const val MD5_SALT = "KRWN3AdgmxEMcd2vLN1ju9qKe8Feco5h"
    }

    override fun catalogs(): List<HikariCatalog> = buildList {
        add(HikariCatalog("latest", "Latest Updates", HikariMediaType.MOVIE))
        add(HikariCatalog("popular", "Most Popular", HikariMediaType.MOVIE))
        add(HikariCatalog("top-rated", "Top Rated", HikariMediaType.MOVIE))
        for (c in 'a'..'z') {
            add(HikariCatalog("letter-$c", "A–Z · ${c.uppercase()}", HikariMediaType.MOVIE))
        }
    }

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        return when {
            catalog.id == "latest" -> parseArchive("$BASE/movies/", page)
            catalog.id == "popular" -> parseArchive("$BASE/movies/?orderby=views", page)
            catalog.id == "top-rated" -> parseArchive("$BASE/movies/?orderby=rating", page)
            catalog.id.startsWith("letter-") -> parseGlossary(catalog.id.removePrefix("letter-"))
            else -> emptyList()
        }
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> {
        val q = query.trim()
        if (q.isBlank()) return emptyList()
        val encoded = URLEncoder.encode(q, "UTF-8").replace("+", "%20")
        return parseArchive("$BASE/?s=$encoded", page)
    }

    // ------------------------------------------------------------------
    //  Meta + streams
    // ------------------------------------------------------------------

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val pageUrl = media.rawType.takeIf { it.isNotBlank() } ?: return media
        val html = getCached(pageUrl) ?: return media
        val title = metaProperty(html, "og:title")?.let { unescapeEntities(it) }
            ?.takeIf { it.isNotBlank() } ?: media.title
        val overview = metaProperty(html, "og:description")?.let { unescapeEntities(it) }
            ?.takeIf { it.isNotBlank() }
        val poster = metaProperty(html, "og:image")
            ?.takeIf { it.isNotBlank() } ?: media.posterUrl
        val year = Regex("""<div class="data">[\s\S]*?<h3[^>]*>[\s\S]*?</h3>[\s\S]*?<span>(\d{4})</span>""")
            .find(html)?.groupValues?.get(1)?.toIntOrNull() ?: media.year
        val genres = Regex("""nav class="genres"[\s\S]*?</nav>""")
            .find(html)?.value
            ?.let { g ->
                Regex("""href="[^"]*"[\s\S]*?>([^<]+)</a>""")
                    .findAll(g).mapNotNull { m ->
                        unescapeEntities(m.groupValues[1]).trim().takeIf { it.isNotBlank() }
                    }.toList()
            }
            ?: emptyList()
        return media.copy(
            title = title,
            posterUrl = poster,
            overview = overview ?: media.overview,
            year = year,
            genres = genres,
        )
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val pageUrl = media.rawType.takeIf { it.isNotBlank() } ?: return emptyList()
        val html = getCached(pageUrl) ?: return emptyList()
        val postId = Regex("""data-postid='(\d+)'""").find(html)?.groupValues?.get(1) ?: return emptyList()
        // Server options: <li class='dooplay_player_option' data-type='movie' data-post='87592' data-nume='1'>…<span class='server'>playkrx18.site</span>
        val options = Regex(
            """<li[^>]*class=['"][^'"]*dooplay_player_option[^'"]*['"][^>]*data-type=['"]([^'"]+)['"][^>]*data-post=['"](\d+)['"][^>]*data-nume=['"](\d+)['"][\s\S]*?<span class=['"]server['"]>([^<]*)</span>"""
        ).findAll(html).map { m ->
            Triple(m.groupValues[2], m.groupValues[1], m.groupValues[3]) to m.groupValues[4].trim()
        }.toList()
        if (options.isEmpty()) return emptyList()

        val out = ArrayList<HikariStream>()
        val seen = HashSet<String>()
        for ((opt, label) in options) {
            val (post, type, nume) = opt
            if (type != "movie") continue
            val playerJson = getCached("$BASE/wp-json/dooplayer/v2/$post/$type/$nume") ?: continue
            val embed = runCatching { JSONObject(playerJson).optString("embed_url") }.getOrNull()
                ?.takeIf { it.startsWith("http") } ?: continue
            val targets = if (embed.contains("<iframe")) {
                Regex("""src="([^"]+)"""").find(embed)?.groupValues?.get(1) ?: continue
            } else embed
            val streams = try {
                resolveEmbed(targets, label)
            } catch (t: Throwable) {
                emptyList()
            }
            for (s in streams) {
                if (s.url.isBlank() || !seen.add(s.url)) continue
                out.add(s)
            }
            if (out.isNotEmpty()) break // first working server is enough
        }
        return out
    }

    /**
     * Resolves one server's embed to playable streams. The 9stream
     * (play.playkrx18.site) server is decrypted over plain HTTP; unknown embed
     * hosts fall back to the WebView capture. Loadvid and mov18plus embeds are
     * skipped outright: loadvid's manifest is fetched as blob content by its
     * own JS (never a capturable URL), and mov18plus redirects away unless it
     * is embedded in an iframe — so their WebView attempts can only burn time.
     */
    private suspend fun resolveEmbed(embed: String, label: String): List<HikariStream> {
        if (embed.contains("playkrx18.site", ignoreCase = true)) {
            val direct = resolvePlaykrx18(embed)
            if (direct.isNotEmpty()) return direct
        }
        if (embed.contains("loadvid.com", ignoreCase = true) ||
            embed.contains("mov18plus.cloud", ignoreCase = true)
        ) {
            return emptyList()
        }
        return try {
            HikariNet.resolveWithWebView(embed, streamCapture, timeoutMs = 45_000).map { h ->
                HikariStream(
                    name = if (label.isBlank()) "Server" else "Server · $label",
                    url = h.url,
                    headers = h.headers,
                    isM3u8 = h.url.contains(".m3u8", ignoreCase = true),
                )
            }
        } catch (t: Throwable) {
            emptyList()
        }
    }

    /**
     * play.playkrx18.site "9stream" embed over plain HTTP. Mirrors the page's
     * own JS: decrypt `idfile`/`iduser` (OpenSSL AES), encrypt the config
     * object (OpenSSL AES), POST `data=<enc>|<md5(enc+salt)>` to the play API,
     * then decrypt the hex-encoded `data` in the JSON response to get the m3u8.
     */
    private suspend fun resolvePlaykrx18(embed: String): List<HikariStream> {
        val page = getCached(embed) ?: return emptyList()
        val apiBase = Regex("""const DOMAIN_API = ['"]([^'"]+)['"]""").find(page)?.groupValues?.get(1)
            ?.takeIf { it.startsWith("http") } ?: return emptyList()
        val idfileEnc = Regex("""const idfile_enc = ["']([0-9a-fA-F]+)["']""").find(page)?.groupValues?.get(1)
            ?: return emptyList()
        val iduserEnc = Regex("""const idUser_enc = ["']([0-9a-fA-F]+)["']""").find(page)?.groupValues?.get(1)
            ?: return emptyList()
        val idfile = openSslDecryptHex(idfileEnc, KEY_IDFILE) ?: return emptyList()
        val iduser = openSslDecryptHex(iduserEnc, KEY_IDUSER) ?: return emptyList()

        // Config must serialize in EXACTLY this key order — the API verifies
        // MD5 over the encrypted blob, so the plaintext must match the page's
        // JSON.stringify order byte for byte (current bundle sends only these
        // four keys; `domain_play`/`jwplayer` are no longer part of the
        // playiframe payload, and `platform` falls back to "noplf").
        val config = buildString {
            append("{\"idfile\":\"").append(jsonEscape(idfile)).append("\"")
            append(",\"iduser\":\"").append(jsonEscape(iduser)).append("\"")
            append(",\"platform\":\"noplf\"")
            append(",\"hlsSupport\":true")
            append("}")
        }

        // The page hex-encodes the OpenSSL blob before signing/sending:
        // `data = hex(Salted__+salt+ct) | md5(hex + MD5_SALT)`.
        val encryptedHex = openSslEncryptHex(config.toString(), KEY_CONFIG) ?: return emptyList()
        val signature = md5Hex(encryptedHex + MD5_SALT)
        val body = "data=" + URLEncoder.encode("$encryptedHex|$signature", "UTF-8")

        val origin = runCatching { "https://" + Regex("""https?://([^/]+)""").find(embed)?.groupValues?.get(1) }
            .getOrDefault(embed)
        val headers = mapOf(
            "User-Agent" to HikariNet.browserHeaders["User-Agent"].orEmpty(),
            "Referer" to embed,
            "Origin" to origin,
            "X-Requested-With" to "XMLHttpRequest",
            "Accept" to "application/json, text/javascript, */*; q=0.01",
        )
        val resp = HikariNet.postString(
            apiBase + "/playiframe",
            body,
            headers,
            "application/x-www-form-urlencoded; charset=UTF-8",
        ) ?: return emptyList()
        val json = runCatching { JSONObject(resp) }.getOrNull() ?: return emptyList()
        val status = json.optInt("status", -1)
        val type = json.optString("type")
        val data = json.optString("data").takeIf { it.isNotBlank() } ?: return emptyList()
        // The success path is status==1; status==0 can also carry a
        // `type:"url-m3u8-encv1"` payload.
        if (status != 1 && type != "url-m3u8-encv1") return emptyList()
        val playlist = openSslDecryptHex(data, KEY_RESPONSE)?.trim() ?: return emptyList()
        if (!playlist.startsWith("http")) return emptyList()
        return listOf(
            HikariStream(
                name = "Server · playkrx18",
                url = playlist,
                headers = mapOf(
                    "User-Agent" to HikariNet.browserHeaders["User-Agent"].orEmpty(),
                    "Referer" to embed,
                ),
                isM3u8 = playlist.contains(".m3u8", ignoreCase = true),
            )
        )
    }

    // ------------------------------------------------------------------
    //  Crypto (OpenSSL "Salted__" AES via EVP_BytesToKey, MD5) — mirrors the
    //  embed's CryptoJS calls exactly.
    // ------------------------------------------------------------------

    private fun openSslDecryptHex(hex: String, passphrase: String): String? {
        var raw = hexToBytes(hex)
        if (raw == null && hex.startsWith("U2FsdGVkX1")) {
            raw = runCatching { HikariNet.base64Decode(hex) }.getOrNull()
        }
        raw = raw ?: return null
        if (raw.size <= 16) return null
        val keyIv = evpBytesToKey(passphrase, raw.copyOfRange(8, 16))
        return try {
            val cipher = Cipher.getInstance("AES/CBC/PKCS5Padding")
            cipher.init(Cipher.DECRYPT_MODE, SecretKeySpec(keyIv, 0, 32, "AES"), IvParameterSpec(keyIv, 32, 16))
            String(cipher.doFinal(raw, 16, raw.size - 16), Charsets.UTF_8)
        } catch (t: Throwable) {
            null
        }
    }

    private fun openSslEncryptHex(plain: String, passphrase: String): String? {
        val salt = ByteArray(8).also { SecureRandom().nextBytes(it) }
        val keyIv = evpBytesToKey(passphrase, salt)
        val ct = try {
            val cipher = Cipher.getInstance("AES/CBC/PKCS5Padding")
            cipher.init(Cipher.ENCRYPT_MODE, SecretKeySpec(keyIv, 0, 32, "AES"), IvParameterSpec(keyIv, 32, 16))
            cipher.doFinal(plain.toByteArray(Charsets.UTF_8))
        } catch (t: Throwable) {
            return null
        }
        val out = ByteArray(16 + ct.size)
        val magic = "Salted__".toByteArray(Charsets.US_ASCII)
        System.arraycopy(magic, 0, out, 0, 8)
        System.arraycopy(salt, 0, out, 8, 8)
        System.arraycopy(ct, 0, out, 16, ct.size)
        return hexEncode(out)
    }

    private fun hexEncode(bytes: ByteArray): String = buildString {
        for (b in bytes) append(String.format("%02x", b))
    }

    /** EVP_BytesToKey: MD5-chained passphrase+salt, first 48 bytes → key(32)+iv(16). */
    private fun evpBytesToKey(passphrase: String, salt: ByteArray): ByteArray {
        val md = MessageDigest.getInstance("MD5")
        val out = java.io.ByteArrayOutputStream()
        var prev = ByteArray(0)
        val pass = passphrase.toByteArray(Charsets.UTF_8)
        while (out.size() < 48) {
            md.reset()
            md.update(prev)
            md.update(pass)
            md.update(salt)
            prev = md.digest()
            out.write(prev)
        }
        return out.toByteArray()
    }

    private fun hexToBytes(s: String): ByteArray? {
        if (s.length % 2 != 0) return null
        val out = ByteArray(s.length / 2)
        for (i in out.indices) {
            out[i] = s.substring(i * 2, i * 2 + 2).toInt(16).toByte()
        }
        return out
    }

    private fun md5Hex(s: String): String {
        val md = MessageDigest.getInstance("MD5")
        return md.digest(s.toByteArray(Charsets.UTF_8)).joinToString("") { "%02x".format(it) }
    }

    private fun jsonEscape(s: String): String = buildString {
        for (c in s) {
            when (c) {
                '\\' -> append("\\\\")
                '"' -> append("\\\"")
                '\n' -> append("\\n")
                '\r' -> append("\\r")
                '\t' -> append("\\t")
                else -> append(c)
            }
        }
    }

    // ------------------------------------------------------------------
    //  Listing parsers
    // ------------------------------------------------------------------

    private suspend fun parseArchive(url: String, page: Int): List<HikariMedia> {
        val pageUrl = if (page <= 1) url else insertPage(url, page)
        val html = getCached(pageUrl) ?: return emptyList()
        val out = LinkedHashMap<String, HikariMedia>()
        for (chunk in html.split(Regex("""<article class="item"""")).drop(1)) {
            val link = Regex("""<a href="(https://krx18\.com/movies/[^"]+)"""").find(chunk)?.groupValues?.get(1) ?: continue
            val img = Regex("""<img\s+src="([^"]+)"""").find(chunk)?.groupValues?.get(1) ?: continue
            var title: String? = Regex("""<h3 class="title">([\s\S]*?)</h3>""").find(chunk)?.groupValues?.get(1)
                ?.let { unescapeEntities(it).trim() }
            if (title.isNullOrBlank()) {
                title = Regex("""<h3><a href="[^"]*">([^<]+)</a></h3>""").find(chunk)?.groupValues?.get(1)
                    ?.let { unescapeEntities(it).trim() }
            }
            if (title.isNullOrBlank()) {
                title = Regex("""<img\s+src="[^"]+" alt="([^"]*)"""").find(chunk)?.groupValues?.get(1) ?: ""
            }
            if (title.isNullOrBlank()) continue
            val id = Regex("""id="post-(\d+)"""").find(chunk)?.groupValues?.get(1)
                ?: link.substringAfterLast("/").trimEnd('/')
            if (id.isBlank()) continue
            out[id] = HikariMedia(
                id = id,
                title = title,
                type = HikariMediaType.MOVIE,
                posterUrl = httpsify(img),
                rawType = link,
            )
        }
        return out.values.toList()
    }

    /** DooPlay A–Z glossary: `{ "<postid>": {title,url,img,year} }`. Needs the
     *  page nonce, fetched fresh from the cached home page (WP nonces rotate). */
    private suspend fun parseGlossary(letter: String): List<HikariMedia> {
        val nonce = Regex("""var dtGonza = \{[\s\S]*?"nonce":"([^"]+)"""")
            .find(getCached("$BASE/") ?: return emptyList())?.groupValues?.get(1) ?: return emptyList()
        val json = getCached("$BASE/wp-json/dooplay/glossary/?nonce=$nonce&term=$letter&type=movies")
            ?: return emptyList()
        val obj = runCatching { JSONObject(json) }.getOrNull() ?: return emptyList()
        val out = ArrayList<HikariMedia>()
        val keys = obj.keys()
        while (keys.hasNext()) {
            val id = keys.next()
            val v = obj.optJSONObject(id) ?: continue
            val title = v.optString("title").ifBlank { continue }
            out.add(
                HikariMedia(
                    id = id,
                    title = unescapeEntities(title),
                    type = HikariMediaType.MOVIE,
                    posterUrl = httpsify(v.optString("img")).takeIf { it.startsWith("http") },
                    year = v.optString("year").toIntOrNull(),
                    rawType = v.optString("url"),
                )
            )
        }
        return out.sortedBy { it.title }
    }

    private fun insertPage(url: String, page: Int): String {
        val q = url.indexOf('?')
        val base = if (q >= 0) url.substring(0, q) else url
        val query = if (q >= 0) url.substring(q) else ""
        return "${base.trimEnd('/')}/page/$page/$query"
    }

    private fun httpsify(url: String): String =
        if (url.startsWith("http://")) "https://" + url.substring(7) else url

    // ------------------------------------------------------------------
    //  HTML helpers
    // ------------------------------------------------------------------

    private suspend fun getCached(url: String): String? {
        val now = System.currentTimeMillis()
        cacheMutex.withLock {
            htmlCache[url]?.let { (t, html) ->
                if (now - t < CACHE_TTL_MS) return html
            }
        }
        val html = HikariNet.getStringSmart(url) ?: return null
        cacheMutex.withLock {
            if (htmlCache.size > 60) htmlCache.clear()
            htmlCache[url] = now to html
        }
        return html
    }

    private fun metaProperty(html: String, property: String): String? =
        Regex("""<meta\s+property="$property"\s+content="([^"]*)"""").find(html)?.groupValues?.get(1)

    private fun unescapeEntities(s: String): String = s
        .replace("&quot;", "\"")
        .replace("&#039;", "'")
        .replace("&#39;", "'")
        .replace("&#038;", "&")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&nbsp;", " ")
        .replace(Regex("""&#(\d+);""")) { m ->
            m.groupValues[1].toIntOrNull()?.let { runCatching { it.toChar().toString() }.getOrNull() } ?: m.value
        }
        .replace(Regex("""\s+"""), " ")
        .trim()
}
