package com.hikari.ext.providers

import com.hikari.ext.HikariStream
import org.json.JSONObject
import java.net.URI

/**
 * The gd mirror lists the same film on several file hosts and exposes one
 * file id per host; each id then resolves through that host's own pipeline.
 * Port of the source's MMIqsmart (+ MMXvidStyle); jsoup replaced with regex.
 */
internal object MmIqsmart {

    private const val API = "https://streams.iqsmartgames.com"
    private const val PLAYER = "https://pro.iqsmartgames.com"

    private data class FileEntry(val slug: String, val name: String, val size: String)

    private data class MirrorLink(
        val siteUrl: String,
        val friendlyName: String,
        val code: String,
    )

    private fun shortLabel(filename: String): String {
        val cleaned = filename
            .replace(Regex("(?i)\\b(multi|ddp[\\d.]*|atmos|h[ .]?265|hevc|x265|x264|avc|esub|hindi|web[ .-]?dl|webrip|hdrip|hdtc|hc)\\b"), " ")
            .replace(Regex("\\s+"), " ")
            .trim()
        return if (cleaned.length > 26) cleaned.take(26).trim() + "..." else cleaned
    }

    private suspend fun fetchFiles(embedUrl: String): List<FileEntry> {
        val html = MmNet.get(embedUrl, referer = "https://multimovies.garden/") ?: return emptyList()

        fun varValue(name: String): String =
            Regex("let\\s+$name\\s*=\\s*\"([^\"]*)\"").find(html)?.groupValues?.get(1).orEmpty()

        val finalId = varValue("FinalID")
        val idType = varValue("idType")
        val key = varValue("myKey")
        if (finalId.isBlank() || key.isBlank()) return emptyList()

        val season = varValue("season")
        val episode = varValue("epname")
        val apiPath = if (season.isNotBlank() && episode.isNotBlank()) {
            "$API/myseriesapi?$idType=${MmNet.urlEncode(finalId)}" +
                "&season=$season&epname=${MmNet.urlEncode(episode)}&key=$key"
        } else {
            "$API/mymovieapi?$idType=${MmNet.urlEncode(finalId)}&key=$key"
        }

        val body = MmNet.get(apiPath, referer = "$API/") ?: return emptyList()
        val data = try {
            JSONObject(body).optJSONArray("data")
        } catch (_: Exception) {
            null
        } ?: return emptyList()

        val out = mutableListOf<FileEntry>()
        for (i in 0 until data.length()) {
            val item = data.optJSONObject(i) ?: continue
            val slug = item.optString("fileslug")
            if (slug.isBlank()) continue
            out.add(FileEntry(slug, item.optString("filename"), item.optString("fsize")))
        }
        return out
    }

    private suspend fun fetchMirrors(slug: String): List<MirrorLink> {
        val body = MmNet.postForm(
            "$PLAYER/embedhelper2.php",
            mapOf(
                "sid" to slug,
                "UserFavSite" to "",
                "currentDomain" to """["streams.iqsmartgames.com","pro.iqsmartgames.com"]""",
            ),
            referer = "$PLAYER/",
        ) ?: return emptyList()

        val root = try {
            JSONObject(body)
        } catch (_: Exception) {
            return emptyList()
        }
        val sources = root.optJSONObject("sources") ?: return emptyList()
        val codes = try {
            JSONObject(String(MmCrypto.b64urlDecode(root.optString("mresult")), Charsets.UTF_8))
        } catch (_: Exception) {
            return emptyList()
        }

        val out = mutableListOf<MirrorLink>()
        for (key in codes.keys()) {
            val src = sources.optJSONObject(key) ?: continue
            val siteUrl = src.optString("siteUrl")
            val code = codes.optString(key)
            if (siteUrl.isBlank() || code.isBlank()) continue
            out.add(MirrorLink(siteUrl, src.optString("friendlyName"), code))
        }
        return out
    }

    private suspend fun dispatchMirror(
        mirror: MirrorLink,
        label: String,
        callback: (HikariStream) -> Unit,
    ): Boolean {
        val pageUrl = MmNet.abs(mirror.siteUrl, mirror.code)
        val host = MmNet.hostOf(pageUrl)
        return when {
            host in setOf(
                "multimovies.rpmhub.site",
                "multimovies.embedseek.xyz",
                "multimovies.p2pplay.pro",
                "server1.uns.bio",
            ) -> MmCineverse.resolveApiV1(host, mirror.code, label, callback)

            host == "bysetayico.com" -> MmFilemoonBridge.resolveByse(label, mirror.code, callback)

            MmXvidStyle.isXvidStyle(host) -> MmXvidStyle.resolve(pageUrl, label, callback)

            else -> MmXvidStyle.resolveGeneric(pageUrl, label, callback)
        }
    }

    suspend fun resolve(
        embedUrl: String,
        labelPrefix: String,
        callback: (HikariStream) -> Unit,
    ): Boolean {
        val files = fetchFiles(embedUrl)
        if (files.isEmpty()) return false
        var any = false
        for ((index, file) in files.withIndex()) {
            val mirrors = try {
                fetchMirrors(file.slug)
            } catch (_: Exception) {
                emptyList()
            }
            val fileLabel = shortLabel(file.name).ifBlank { "File ${index + 1}" }
            for (mirror in mirrors) {
                try {
                    any = dispatchMirror(mirror, "$labelPrefix $fileLabel", callback) || any
                } catch (_: Exception) {
                    // single dead mirror is expected, keep the rest
                }
            }
        }
        return any
    }

    // filesforever links are iqsmart file ids in disguise, the landing page only
    // confirms the id before the shared mirror helper takes over
    suspend fun resolveFilesforever(
        embedUrl: String,
        labelPrefix: String,
        callback: (HikariStream) -> Unit,
    ): Boolean {
        val code = embedUrl.trimEnd('/').substringAfterLast('/')
        if (code.isBlank()) return false

        val page = MmNet.get(embedUrl, referer = "https://multimovies.garden/")
        val sid = if (page != null) {
            Regex("id=\"gdmrfid\"\\s+value=\"([^\"]+)\"").find(page)?.groupValues?.get(1)
                ?: Regex("let\\s+FinalID\\s*=\\s*\"([^\"]+)\"").find(page)?.groupValues?.get(1)
                ?: code
        } else code

        val mirrors = try {
            fetchMirrors(sid)
        } catch (_: Exception) {
            emptyList()
        }
        var any = false
        for (mirror in mirrors) {
            try {
                any = dispatchMirror(mirror, "$labelPrefix $sid", callback) || any
            } catch (_: Exception) {
                // keep going on a single dead mirror
            }
        }
        return any
    }
}

/** Xvidstyle frontends expose their playlist through a dean edwards packed jw setup. */
internal object MmXvidStyle {

    private val knownHosts = setOf(
        "hanerix.com",
        "morencius.com",
        "vibuxer.com",
        "n1mwq.org",
    )

    fun isXvidStyle(host: String): Boolean = host in knownHosts

    private fun extractLinks(unpacked: String, base: String): List<String> {
        val absolute = LinkedHashSet<String>()
        val relative = LinkedHashSet<String>()
        for (m in Regex("\"(?:hls\\d|file|mp4)\"\\s*:\\s*\"([^\"]+)\"").findAll(unpacked)) {
            val url = MmNet.deEsc(m.groupValues[1])
            if (url.isBlank()) continue
            if (url.startsWith("http")) absolute.add(url) else relative.add(MmNet.abs(base, url))
        }
        for (m in Regex("https?://[^\"'\\s\\\\]+\\.m3u8[^\"'\\s\\\\]*").findAll(unpacked)) {
            absolute.add(MmNet.deEsc(m.groupValues.first()))
        }
        // the cdn copies outlive the same origin stream tokens, list them first
        return (absolute + relative).filter { it.startsWith("http") }
    }

    suspend fun resolve(
        pageUrl: String,
        label: String,
        callback: (HikariStream) -> Unit,
    ): Boolean {
        val base = try {
            val uri = URI(pageUrl)
            "${uri.scheme}://${uri.host}"
        } catch (_: Exception) {
            return false
        }
        val html = MmNet.get(pageUrl, referer = "https://pro.iqsmartgames.com/") ?: return false
        val unpacked = MmJsPacker.parseAndUnpack(html) ?: return false
        val links = extractLinks(unpacked, base)
        for (url in links) {
            callback(
                HikariStream(
                    name = label,
                    url = url,
                    headers = mapOf("Referer" to "$base/"),
                    isM3u8 = url.contains(".m3u8"),
                )
            )
        }
        return links.isNotEmpty()
    }

    // unknown hosts still get a plain m3u8 sweep of the raw page
    suspend fun resolveGeneric(
        pageUrl: String,
        label: String,
        callback: (HikariStream) -> Unit,
    ): Boolean {
        val html = MmNet.get(pageUrl) ?: return false
        val urls = Regex("https?://[^\"'\\s\\\\]+\\.m3u8[^\"'\\s\\\\]*")
            .findAll(html)
            .map { MmNet.deEsc(it.groupValues.first()) }
            .toSet()
        for (url in urls) {
            callback(HikariStream(name = label, url = url, isM3u8 = true))
        }
        return urls.isNotEmpty()
    }
}
