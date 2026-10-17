package com.hikari.ext.providers

import com.hikari.ext.HikariNet
import com.hikari.ext.HikariStream
import com.hikari.ext.HikariSubtitle
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import org.json.JSONArray
import org.json.JSONObject
import java.net.URLDecoder
import java.util.concurrent.atomic.AtomicBoolean

private fun docHeaders(referer: String?): Map<String, String> =
    TmfNet.browserHeaders(referer).filterKeys { it != "User-Agent" && it != "Referer" }

// hosts are resolved here instead of through a generic extractor, whose result depends on load order
internal object TmfSources {

    class Stream(
        val name: String,
        val url: String,
        val isM3u8: Boolean = false,
        val headers: Map<String, String> = emptyMap(),
    )

    private val MEDIA_CAPTURE = Regex("""(?i)\.(m3u8|mp4|mkv)(?:[?#]|$)""")

    private fun absolute(link: String, base: String): String =
        if (link.startsWith("http")) link else base.trimEnd('/') + "/" + link.removePrefix("/")

    private fun b64d(s: String): String? = try {
        var t = s.trim()
        while (t.length % 4 != 0) t += "="
        String(android.util.Base64.decode(t, android.util.Base64.DEFAULT), Charsets.UTF_8)
    } catch (_: Exception) {
        null
    }

    private suspend fun redirectOf(url: String, referer: String?, hops: Int = 6): String? {
        var current = url
        repeat(hops) {
            val res = TmfNet.request(
                "GET", current,
                referer = referer,
                extraHeaders = docHeaders(referer),
                followRedirects = false,
            ) ?: return null
            val loc = res.header("location").orEmpty()
            if (loc.isEmpty()) return current
            current = when {
                loc.startsWith("http") -> loc
                loc.startsWith("/") -> TmfNet.originOf(current) + loc
                else -> return null
            }
        }
        return current
    }

    // fastdl serves a redirect stub with the drive link in the reurl variable,
    // the hubcdn wiki host serves the same stub with one extra base64 hop in r;
    // fastdl.rest serves the same stub from /embed.php?download=
    suspend fun resolveFastDl(url: String): List<Stream> {
        return try {
            val text = TmfNet.get(url, "https://nexdrive.fit/", docHeaders("https://nexdrive.fit/"))
                ?: return emptyList()
            if (text.contains("File is Deleted") || text.contains("Something went wrong")) {
                return emptyList()
            }
            val reurl = Regex("""var\s+reurl\s*=\s*"([^"]+)"""").find(text)?.groupValues?.get(1)
                ?: Regex("""'(https://fastdl\.[^']*dl\.php\?link=[^']+)'"""").find(text)?.groupValues?.get(1)
                ?: Regex("""'(https://hubcdn\.[^']*dl\.php\?link=[^']+)'"""").find(text)?.groupValues?.get(1)
                ?: return emptyList()
            val carrier = Regex("""[?&]r=([A-Za-z0-9+/=_-]+)""").find(reurl)?.groupValues?.get(1)
                ?.let { wrapped ->
                    val padded = if (wrapped.length % 4 > 0) {
                        wrapped + "=".repeat(4 - wrapped.length % 4)
                    } else wrapped
                    b64d(padded)
                } ?: reurl
            val googleUrl = Regex("""link=(https?://[^&"']+)""").find(carrier)?.groupValues?.get(1)
                ?: return emptyList()
            val direct = URLDecoder.decode(googleUrl, "UTF-8")
            if (!direct.startsWith("http")) return emptyList()
            listOf(Stream("G-Direct", direct, false, mapOf("Referer" to TmfNet.originOf(url) + "/")))
        } catch (_: Exception) {
            emptyList()
        }
    }

    // older vcloud pages carry a div.main h4 a hop first, the current ones
    // keep the target in a script variable that is either double base64 or a
    // plain url, and when the target is not the file itself it is a hub page
    suspend fun resolveVCloud(url: String): List<Stream> {
        return try {
            val page = TmfNet.get(url, "https://nexdrive.fit/", docHeaders("https://nexdrive.fit/"))
                ?: return emptyList()
            val base = TmfNet.originOf(url)
            var link: String? = null

            val hop = Regex(
                """<div[^>]*class="[^"]*\bmain\b[^"]*"[^>]*>.*?<h4[^>]*>.*?<a[^>]+href="([^"]+)"""",
                setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
            ).find(page)?.groupValues?.get(1)?.trim()
            if (!hop.isNullOrBlank()) {
                val hopPage = TmfNet.get(absolute(hop, base), base, docHeaders(base))
                if (hopPage != null) link = extractVCloudLink(hopPage)
            }
            if (link.isNullOrBlank()) link = extractVCloudLink(page)

            if (link.isNullOrBlank() && url.contains("/video/")) {
                link = Regex(
                    """<div[^>]*class="[^"]*\bvd\b[^"]*"[^>]*>.*?<a[^>]+href="([^"]+)"""",
                    setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
                ).find(page)?.groupValues?.get(1)?.trim()
            }
            if (link.isNullOrBlank()) return emptyList()

            val target = absolute(link, base)
            if (!target.startsWith("http")) return emptyList()
            vcloudTarget(target, base)
        } catch (_: Exception) {
            emptyList()
        }
    }

    private suspend fun vcloudTarget(target: String, base: String): List<Stream> {
        val targetPage = TmfNet.get(target, base, docHeaders(base)) ?: return emptyList()
        val hub = hubStreams(targetPage, TmfNet.originOf(target), target, 0)
        if (hub.isNotEmpty()) return hub
        if (target.contains("drive.google.com") || target.contains("googleusercontent")) {
            return listOf(Stream("V-Cloud", target))
        }
        return emptyList()
    }

    private fun extractVCloudLink(page: String): String? {
        for (m in Regex(
            """<script(?![^>]*src)[^>]*>(.*?)</script>""",
            setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
        ).findAll(page)) {
            val script = m.groupValues[1]
            if (!script.contains("url")) continue
            Regex("""var\s+url\s*=\s*atob\s*\(\s*atob\s*\(\s*['"]([^'"]+)['"]\s*\)\s*\)""")
                .find(script)?.groupValues?.get(1)
                ?.let { encoded ->
                    val decoded = b64d(encoded)?.let { b64d(it) }
                    if (decoded != null && decoded.startsWith("http")) return decoded
                }
            Regex("""var\s+url\s*=\s*['"]([^'"]*)['"]""").find(script)?.groupValues?.get(1)
                ?.takeIf { it.startsWith("http") }
                ?.let { return it }
        }
        return Regex(
            """<div[^>]*class="[^"]*card-body[^"]*"[^>]*>.*?<h2[^>]*>.*?<a[^>]*class="[^"]*btn[^"]*"[^>]+href="([^"]+)"""",
            setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
        ).find(page)?.groupValues?.get(1)?.trim()?.takeIf { it.startsWith("http") }
    }

    // mcloud.best hides its vcloud target behind a download button whose
    // onclick double-dec salmon atob payload, then navigates to it
    suspend fun resolveMCloud(url: String): List<Stream> {
        return try {
            val page = TmfNet.get(url, TmfNet.originOf(url) + "/", docHeaders(TmfNet.originOf(url)))
                ?: return emptyList()
            val base = TmfNet.originOf(url)
            val dbl = Regex("""atob\s*\(\s*atob\s*\(\s*['"]([^'"]+)['"]""").find(page)?.groupValues?.get(1)
            var target = dbl?.let { b64d(it)?.let { once -> b64d(once) } }
            if (target.isNullOrBlank()) {
                val single = Regex("""atob\s*\(\s*['"]([^'"]{32,})['"]""").find(page)?.groupValues?.get(1)
                target = single?.let { b64d(it) }
            }
            if (target.isNullOrBlank() || !target.startsWith("http")) return emptyList()
            target = target.replace(
                Regex("""https?://(?:www\.)?vcloud\.beer""", RegexOption.IGNORE_CASE),
                base,
            )
            vcloudTarget(target, base)
        } catch (_: Exception) {
            emptyList()
        }
    }

    // m-drive.lol is a vegadrive bridge: buttons carry hashes into a base64
    // urlMap of one.vegadrive.me provider urls, each resolving like the
    // source's vegadrive provider pages
    suspend fun resolveMDrive(url: String): List<Stream> {
        return try {
            val page = TmfNet.get(url, TmfNet.originOf(url) + "/", docHeaders(TmfNet.originOf(url)))
                ?: return emptyList()
            val labels = Regex(
                """<button[^>]*data-hash="([^"]+)"[^>]*>(.*?)</button>""",
                setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
            ).findAll(page).associate { m ->
                m.groupValues[1] to m.groupValues[2].replace(Regex("<[^>]+>"), "").replace(Regex("\\s+"), " ").trim()
            }
            val mapRaw = Regex("""(?:const|var|let)\s+urlMap\s*=\s*(\[.*?\])\s*;""", RegexOption.DOT_MATCHES_ALL)
                .find(page)?.groupValues?.get(1) ?: return emptyList()
            val arr = JSONArray(mapRaw)
            val out = mutableListOf<Stream>()
            for (i in 0 until arr.length()) {
                val e = arr.optJSONObject(i) ?: continue
                val target = b64d(e.optString("url")) ?: continue
                if (!target.startsWith("http")) continue
                val label = labels[e.optString("hash")].orEmpty()
                try {
                    out.addAll(vegaProviderFlow(target, label, url))
                } catch (_: Exception) {
                }
            }
            out.distinctBy { it.url }
        } catch (_: Exception) {
            emptyList()
        }
    }

    private suspend fun vegaProviderFlow(providerUrl: String, label: String, referer: String): List<Stream> {
        val res = TmfNet.request(
            "GET", providerUrl,
            referer = referer,
            extraHeaders = docHeaders(referer),
            followRedirects = false,
        ) ?: return emptyList()
        val loc = res.header("location").orEmpty()
        if (loc.isEmpty()) return emptyList()
        val final = when {
            loc.startsWith("http") -> loc
            loc.startsWith("/") -> TmfNet.originOf(providerUrl) + loc
            else -> return emptyList()
        }
        val tag = if (label.isBlank()) "M-Drive" else "M-Drive $label"
        return when {
            final.contains("googleusercontent") || final.contains("drive.google.com") ->
                listOf(Stream(tag, final))
            final.contains("tgfiles") ->
                listOf(Stream("$tag Telegram", final))
            final.contains("pixeldrain.com/u/") -> {
                val id = final.substringBefore("?").substringBefore("#").substringAfterLast("/")
                if (id.isBlank()) emptyList()
                else listOf(Stream("$tag Pixeldrain", "https://pixeldrain.com/api/file/$id"))
            }
            final.contains("bzzhr.co") ->
                listOf(Stream("$tag Buzzheavier", final))
            final.contains("gofile.io") ->
                resolveGofile(final).map { Stream("$tag Gofile", it.url, it.isM3u8, it.headers) }
            TmfNet.probeFile(final, referer) ->
                listOf(Stream(tag, final))
            else -> emptyList()
        }
    }

    // the vegadrive share page lists one bridge provider per host, vegadrop
    // (skydrop) streams the drive file itself and the rest land on their own
    // partner pages, providers come and go so each result is checked against
    // the host it is supposed to be on
    suspend fun resolveVegaDrive(url: String): List<Stream> {
        return try {
            TmfNet.get(url, "https://nexdrive.fit/", docHeaders("https://nexdrive.fit/")) ?: return emptyList()
            val base = TmfNet.originOf(url)
            val token = url.substringAfter("/s/").substringBefore("?")
            if (token.isBlank() || token == url) return emptyList()

            val out = mutableListOf<Stream>()

            val drop = resolveVegaProvider(base, token, "skydrop", url)
            if (drop != null && drop.contains("googleusercontent") &&
                !drop.substringAfterLast("/").contains(".zip", true)
            ) {
                out.add(Stream("V-Drive Vegadrop (10Gbps)", drop))
            }

            val pixel = resolveVegaProvider(base, token, "pixeldrain", url)
            if (pixel != null && pixel.contains("pixeldrain.com/u/")) {
                val id = pixel.substringBefore("?").substringBefore("#").substringAfterLast("/")
                if (id.isNotBlank()) {
                    out.add(Stream("V-Drive Pixeldrain", "https://pixeldrain.com/api/file/$id"))
                }
            }

            val buzz = resolveVegaProvider(base, token, "buzzheavier", url)
            if (buzz != null && buzz.contains("bzzhr.co")) {
                out.add(Stream("V-Drive Buzzheavier", buzz))
            }

            val telegram = resolveVegaProvider(base, token, "telegram", url)
            if (telegram != null && telegram.contains("tgfiles")) {
                out.add(Stream("V-Drive Telegram", telegram))
            }

            out.distinctBy { it.url }
        } catch (_: Exception) {
            emptyList()
        }
    }

    // the provider pages only answer when the share page is sent as referer,
    // without it they bounce straight back to the picker
    private suspend fun resolveVegaProvider(
        base: String,
        token: String,
        provider: String,
        referer: String,
    ): String? {
        val start = if (provider == "skydrop") {
            "$base/go/$token/skydrop"
        } else {
            "$base/d/$token/$provider"
        }
        return redirectOf(start, referer, hops = 6)
    }

    private val FILEPRESS_ID = Regex("""/file/([a-f0-9]{16,40})""")
    private const val FILEBEE_API = "https://filebee.xyz/api"

    // filepress is a react app whose html pages sit behind an interactive
    // turnstile while the json api is open: file/get describes the file (its
    // name is the only reliable zip pack detector), downlaod/ queues a task
    // or answers instantly depending on the method, downlaod2/ turns a
    // finished task into the link
    suspend fun resolveFilePress(url: String): List<Stream> {
        val id = FILEPRESS_ID.find(url)?.groupValues?.get(1) ?: return emptyList()
        return try {
            val infoRes = TmfNet.request(
                "GET", "$FILEBEE_API/file/get/$id",
                referer = "https://filebee.xyz/",
                extraHeaders = docHeaders("https://filebee.xyz/"),
            )
            if (infoRes == null || infoRes.code !in 200..299) return emptyList()
            val info = try {
                JSONObject(infoRes.body).optJSONObject("data") ?: return emptyList()
            } catch (_: Exception) {
                return emptyList()
            }
            val name = info.optString("name")
            if (isArchiveName(name)) return emptyList()

            val out = mutableListOf<Stream>()

            // dotflix mirrors the drive file and serves it as an instant link
            val dotflix = filePressDownload(id, "dotFlixDownlaod")
            if (dotflix != null && dotflix.startsWith("http")) {
                val direct = resolveDotFlix(dotflix)
                if (direct != null && direct.startsWith("http")) {
                    out.add(Stream("FilePress Instant", direct))
                }
            }

            val telegram = filePressDownload(id, "telegramDownload")
            if (telegram != null && telegram.startsWith("http")) {
                out.add(Stream("FilePress Telegram", telegram))
            }

            // the index worker proxies through its own host with a short
            // lived link, it is only emitted when the file actually answers
            val indexTask = filePressDownload(id, "indexDownlaod")
            if (indexTask != null && indexTask.matches(Regex("[a-f0-9]{16,40}"))) {
                val link = filePressFinal(indexTask, "indexDownlaod")
                if (link != null && TmfNet.probeFile(link, "https://filebee.xyz/")) {
                    out.add(Stream("FilePress Direct", link))
                }
            }

            out.distinctBy { it.url }
        } catch (_: Exception) {
            emptyList()
        }
    }

    private suspend fun filePressDownload(id: String, method: String): String? {
        return try {
            val body = JSONObject()
                .put("captchaValue", "")
                .put("id", id)
                .put("method", method)
            val res = TmfNet.postJson(
                "$FILEBEE_API/file/downlaod/",
                body.toString(),
                referer = "https://filebee.xyz/",
                extraHeaders = mapOf(
                    "Accept" to "application/json",
                    "Origin" to "https://filebee.xyz",
                ),
            ) ?: return null
            if (res.code !in 200..299) return null
            val parsed = try {
                JSONObject(res.body)
            } catch (_: Exception) {
                return null
            }
            if (!parsed.optBoolean("status")) return null
            when (val data = parsed.opt("data")) {
                is String -> data.takeIf { it.isNotBlank() }
                else -> null
            }
        } catch (_: Exception) {
            null
        }
    }

    private suspend fun filePressFinal(taskId: String, method: String): String? {
        return try {
            val body = JSONObject()
                .put("captchaValue", "")
                .put("id", taskId)
                .put("method", method)
            val res = TmfNet.postJson(
                "$FILEBEE_API/file/downlaod2/",
                body.toString(),
                referer = "https://filebee.xyz/",
                extraHeaders = mapOf(
                    "Accept" to "application/json",
                    "Origin" to "https://filebee.xyz",
                ),
            ) ?: return null
            if (res.code !in 200..299) return null
            val parsed = try {
                JSONObject(res.body)
            } catch (_: Exception) {
                return null
            }
            if (!parsed.optBoolean("status")) return null
            when (val data = parsed.opt("data")) {
                is String -> data.takeIf { it.startsWith("http") }
                is JSONArray -> (0 until data.length())
                    .firstNotNullOfOrNull { i ->
                        data.optString(i).takeIf { it.startsWith("http") }
                    }
                else -> null
            }
        } catch (_: Exception) {
            null
        }
    }

    // the dotflix share page carries a per file code in a btoa call, the
    // reversed base64 of it is posted to the extract endpoint which answers
    // with the drive file url
    private suspend fun resolveDotFlix(shareUrl: String): String? {
        return try {
            val text = TmfNet.get(
                shareUrl, "https://new2.dotflix.shop/",
                docHeaders("https://new2.dotflix.shop/"),
            ) ?: return null
            val code = Regex("""btoa\('([^']+)'\)""").find(text)?.groupValues?.get(1)
                ?: return null
            val obfuscated = android.util.Base64.encodeToString(
                code.toByteArray(), android.util.Base64.NO_WRAP,
            ).reversed()
            val alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
            val requestId = (1..13).map { alphabet.random() }.joinToString("")
            val timestamp = System.currentTimeMillis().toString()
            val body = JSONObject()
                .put("requestId", requestId)
                .put("timestamp", timestamp)
                .put("data", obfuscated)
            val res = TmfNet.postJson(
                "https://dotflix.store/api/extract-download",
                body.toString(),
                referer = "https://new2.dotflix.shop/",
                extraHeaders = mapOf(
                    "Accept" to "application/json",
                    "X-Request-ID" to requestId,
                    "X-Timestamp" to timestamp,
                    "Origin" to "https://new2.dotflix.shop",
                ),
            ) ?: return null
            if (res.code !in 200..299) return null
            val parsed = try {
                JSONObject(res.body)
            } catch (_: Exception) {
                return null
            }
            if (!parsed.optBoolean("success")) return null
            parsed.optString("downloadUrl").takeIf { it.startsWith("http") }
        } catch (_: Exception) {
            null
        }
    }

    // gofile share links resolve through a throwaway guest account
    suspend fun resolveGofile(url: String): List<Stream> {
        return try {
            val id = url.substringAfter("/d/").substringBefore("?").substringBefore("#").trim()
            if (id.isBlank() || id == url) return emptyList()
            val acc = TmfNet.postJson(
                "https://api.gofile.io/accounts", "{}",
                referer = "https://gofile.io/",
                extraHeaders = mapOf("Accept" to "application/json", "Origin" to "https://gofile.io"),
            ) ?: return emptyList()
            if (acc.code !in 200..299) return emptyList()
            val token = JSONObject(acc.body).optJSONObject("data")?.optString("token").orEmpty()
            if (token.isBlank()) return emptyList()
            val srvRes = TmfNet.get("https://api.gofile.io/servers", "https://gofile.io/")
            val server = try {
                val arr = JSONObject(srvRes ?: "").optJSONObject("data")?.optJSONArray("servers")
                (0 until (arr?.length() ?: 0)).map { arr!!.optJSONObject(it)?.optString("name").orEmpty() }
                    .firstOrNull { it.isNotBlank() }
            } catch (_: Exception) {
                null
            } ?: "store1"
            val content = TmfNet.request(
                "GET", "https://$server.gofile.io/contents/$id?wt=$token",
                referer = "https://gofile.io/",
                extraHeaders = mapOf(
                    "Accept" to "application/json",
                    "Authorization" to "Bearer $token",
                ),
            ) ?: return emptyList()
            if (content.code !in 200..299) return emptyList()
            val children = JSONObject(content.body).optJSONObject("data")?.optJSONObject("children")
                ?: return emptyList()
            val out = mutableListOf<Stream>()
            for (key in children.keys()) {
                val link = children.optJSONObject(key)?.optString("link").orEmpty()
                if (link.startsWith("http")) out.add(Stream("Gofile", link))
            }
            out.distinctBy { it.url }
        } catch (_: Exception) {
            emptyList()
        }
    }

    // ads and site plumbing that sit next to the real download buttons on
    // the hub pages, none of them carry a file
    private val hubJunk = Regex(
        "tinyurl|t\\.me|telegram|/tg/|winexch|a-ads|snvhost|one\\.one\\.one\\.one|" +
            "google\\.com/search|hubcloud\\.fans|drive/admin",
    )

    private val pxlRegex = Regex("""var\s+pxl\s*=\s*["']([^"']+)["']""")

    // the pixel button href is a dead placeholder that stays the same for
    // every file, the real pixeldrain link sits in the pxl variable of the
    // page, following the placeholder is what hands every quality the same
    // dead file id
    private fun pixelFileUrl(pageHtml: String, buttonHref: String): String? {
        val pxl = pxlRegex.find(pageHtml)?.groupValues?.get(1)
        val link = pxl?.takeIf { it.startsWith("http") } ?: buttonHref
        if (!link.startsWith("http")) return null
        if (link.contains("download", true)) return link
        val id = link.substringBefore("?").substringBefore("#").substringAfterLast("/")
        if (id.isBlank()) return null
        return "${TmfNet.originOf(link)}/api/file/$id"
    }

    private fun isArchiveName(name: String): Boolean =
        Regex("""(?i)\.(zip|rar|7z)\s*$""").containsMatchIn(name.trim())

    private val anchorRe = Regex(
        """<a([^>]*)>(.*?)</a>""",
        setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
    )
    private val btnHrefRe = Regex("""href="([^"]+)"""", RegexOption.IGNORE_CASE)

    // the old hub layout with a generate button first and the full server
    // list behind it, still used by the vcloud target pages
    private suspend fun hubStreams(html: String, base: String, pageUrl: String, depth: Int): List<Stream> {
        val out = mutableListOf<Stream>()
        val header = Regex(
            """<div[^>]*class="[^"]*card-header[^"]*"[^>]*>(.*?)</div>""",
            setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
        ).find(html)?.groupValues?.get(1)?.replace(Regex("<[^>]+>"), "")?.trim().orEmpty()
        if (isArchiveName(header)) return emptyList()

        val buttons = anchorRe.findAll(html).map { m ->
            Triple(
                btnHrefRe.find(m.groupValues[1])?.groupValues?.get(1)?.trim().orEmpty(),
                m.groupValues[2].replace(Regex("<[^>]+>"), " ").replace(Regex("\\s+"), " ").trim(),
                m.groupValues[1],
            )
        }.filter { (href, _, attrs) ->
            href.isNotBlank() && (attrs.contains("btn", true) || attrs.contains("download", true))
        }.toList()

        val generate = buttons.firstOrNull { it.second.contains("generate", true) }?.first
        if (!generate.isNullOrBlank() && depth < 3) {
            try {
                val genPage = TmfNet.get(absolute(generate, base), pageUrl, docHeaders(pageUrl))
                if (genPage != null) {
                    val nested = hubStreams(genPage, base, pageUrl, depth + 1)
                    if (nested.isNotEmpty()) return nested
                }
            } catch (_: Exception) {
            }
        }

        if (pageUrl.contains("/video/") && depth < 3) {
            val inner = Regex(
                """<div[^>]*class="[^"]*\bvd\b[^"]*"[^>]*>.*?<a[^>]+href="([^"]+)"""",
                setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
            ).find(html)?.groupValues?.get(1)
            if (!inner.isNullOrBlank()) {
                try {
                    val innerPage = TmfNet.get(absolute(inner, base), base, docHeaders(base))
                    if (innerPage != null) {
                        val nested = hubStreams(innerPage, base, inner, depth + 1)
                        if (nested.isNotEmpty()) return nested
                    }
                } catch (_: Exception) {
                }
            }
        }

        for ((href, text, _) in buttons) {
            try {
                out.addAll(hubButton(href, text, base, html))
            } catch (_: Exception) {
            }
        }
        return out.distinctBy { it.url }
    }

    private suspend fun hubButton(href: String, text: String, base: String, pageHtml: String): List<Stream> {
        val t = text.lowercase()
        val link = href.trim()
        if (link.isBlank() || hubJunk.containsMatchIn(link)) return emptyList()
        val abs = absolute(link, base)
        return try {
            when {
                t.contains("fslv2") -> listOf(Stream("FSLv2", abs))
                t.contains("fsl") -> listOf(Stream("FSL Server", abs))
                t.contains("buzzserver") -> {
                    val dlink = TmfNet.request(
                        "GET", "$abs/download",
                        referer = abs,
                        extraHeaders = docHeaders(abs),
                        followRedirects = false,
                    )?.header("hx-redirect") ?: ""
                    if (dlink.isNotBlank()) {
                        listOf(Stream("BuzzServer", absolute(dlink, base)))
                    } else emptyList()
                }
                t.contains("10gbps") -> {
                    val target = redirectOf(abs, base) ?: return emptyList()
                    val direct = if (target.contains("link=")) target.substringAfter("link=") else target
                    if (direct.startsWith("http")) listOf(Stream("10Gbps", direct)) else emptyList()
                }
                t.contains("instant download") || t.contains("instant dl") -> {
                    val loc = TmfNet.request(
                        "GET", abs,
                        referer = base,
                        extraHeaders = docHeaders(base),
                        followRedirects = false,
                    )?.header("location")?.trim()
                    val direct = when {
                        loc == null -> null
                        loc.contains("url=") -> loc.substringAfter("url=")
                        loc.contains("link=") -> loc.substringAfter("link=")
                        else -> loc
                    }?.takeIf { it.startsWith("http") }
                    if (direct != null) listOf(Stream("Instant Download", direct)) else emptyList()
                }
                t.contains("pixeldra") || t.contains("pixelserver") || t.contains("pixel server") ||
                    link.contains("pixeldra") -> {
                    val final = pixelFileUrl(pageHtml, link) ?: return emptyList()
                    listOf(Stream("Pixeldrain", final))
                }
                t.contains("s3 server") || t.contains("mega server") || t.contains("pdl") ->
                    listOf(Stream(text.trim().ifBlank { "Server" }, abs))
                t.contains("download file") || t.contains("download now") ->
                    listOf(Stream("Download File", abs))
                link.contains("fastdl.") || link.contains("hubcdn.") -> resolveFastDl(abs)
                else -> emptyList()
            }
        } catch (_: Exception) {
            emptyList()
        }
    }

    fun resolves(href: String): Boolean =
        href.contains("fastdl.") || href.contains("vcloud.") ||
            href.contains("mcloud.") || href.contains("m-drive.") ||
            href.contains("vegadrive.") || href.contains("filebee.") ||
            href.contains("filepress.") || href.contains("fpgo.") ||
            href.contains("hubcloud.") || href.contains("gofile.")

    suspend fun resolveOne(href: String): List<Stream> = when {
        href.contains("fastdl.") || href.contains("hubcdn.") -> resolveFastDl(href)
        href.contains("mcloud.") -> resolveMCloud(href)
        href.contains("m-drive.") -> resolveMDrive(href)
        href.contains("vcloud.") -> resolveVCloud(href)
        href.contains("vegadrive.") -> resolveVegaDrive(href)
        href.contains("filebee.") || href.contains("filepress.") || href.contains("fpgo.") ->
            resolveFilePress(href)
        href.contains("gofile.") -> resolveGofile(href)
        else -> emptyList()
    }

    suspend fun emitAll(
        hrefs: List<String>,
        qualityHint: Int?,
        info: String,
        referer: String,
        callback: (HikariStream) -> Unit,
    ): Boolean {
        val emitted = AtomicBoolean(false)
        coroutineScope {
            hrefs.distinct().map { href ->
                async(Dispatchers.IO) {
                    if (resolves(href)) {
                        val streams = try {
                            resolveOne(href)
                        } catch (_: Exception) {
                            emptyList()
                        }
                        for (s in streams) {
                            val label = if (info.isBlank()) s.name else "${s.name} · $info"
                            val name = if (qualityHint != null) "$label · ${qualityHint}p" else label
                            callback(HikariStream(name = name, url = s.url, headers = s.headers, isM3u8 = s.isM3u8))
                            emitted.set(true)
                        }
                    } else {
                        // anything else (transferit, 1fichier, vikingfile, megaup,
                        // stray hubs): let the page's own player fire in a real
                        // WebView and capture the file it requests
                        try {
                            val found = HikariNet.resolveWithWebView(
                                href, MEDIA_CAPTURE, timeoutMs = 45_000,
                            )
                            for (r in found) {
                                callback(
                                    HikariStream(
                                        name = if (info.isBlank()) "WebView" else "WebView · $info",
                                        url = r.url,
                                        headers = r.headers,
                                        isM3u8 = r.url.contains(".m3u8"),
                                    )
                                )
                                emitted.set(true)
                            }
                        } catch (_: Exception) {
                        }
                    }
                }
            }.awaitAll()
        }
        return emitted.get()
    }
}
