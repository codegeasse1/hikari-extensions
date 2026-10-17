package com.hikari.ext.providers

import com.hikari.ext.HikariStream
import com.hikari.ext.HikariSubtitle
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withTimeoutOrNull
import org.json.JSONArray
import org.json.JSONObject
import java.net.URLDecoder
import java.util.concurrent.Semaphore

private val hubJunk = Regex(
    "tinyurl|t\\.me|telegram|/tg/|winexch|a-ads|snvhost|one\\.one\\.one\\.one|" +
        "google\\.com/search|hubcloud\\.fans|drive/admin",
)
private val hubPxlRegex = Regex("""var\s+pxl\s*=\s*["']([^"']+)["']""")

private fun hubPixelFileUrl(pageHtml: String, buttonHref: String): String? {
    val pxl = hubPxlRegex.find(pageHtml)?.groupValues?.get(1)
    val link = pxl?.takeIf { it.startsWith("http") } ?: buttonHref
    if (!link.startsWith("http")) return null
    if (link.contains("download", true)) return link
    val id = link.substringBefore("?").substringBefore("#").substringAfterLast("/")
    if (id.isBlank()) return null
    return "${JpNet.getBaseUrl(link)}/api/file/$id?download"
}

private val hubAnchorRe = Regex(
    """<a([^>]*)>(.*?)</a>""",
    setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
)
private val hubHrefRe = Regex("""href="([^"]+)"""", RegexOption.IGNORE_CASE)

private data class HubBtn(val href: String, val text: String, val attrs: String)

private fun hubButtons(html: String, onlyBtn: Boolean): List<HubBtn> =
    hubAnchorRe.findAll(html).mapNotNull { m ->
        val attrs = m.groupValues[1]
        val href = hubHrefRe.find(attrs)?.groupValues?.get(1)?.trim().orEmpty()
        if (href.isBlank()) return@mapNotNull null
        if (onlyBtn && !attrs.contains("btn", true) && !attrs.contains("download", true)) return@mapNotNull null
        val text = m.groupValues[2].replace(Regex("<[^>]+>"), " ").replace(Regex("\\s+"), " ").trim()
        HubBtn(href, text, attrs)
    }.toList()

private fun hubCardHeader(html: String): String =
    Regex(
        """<div[^>]*class="[^"]*card-header[^"]*"[^>]*>(.*?)</div>""",
        setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
    ).find(html)?.groupValues?.get(1)?.replace(Regex("<[^>]+>"), " ")?.replace(Regex("\\s+"), " ")?.trim().orEmpty()

private fun isArchiveName(name: String): Boolean =
    Regex("""(?i)\.(zip|rar|7z)\s*$""").containsMatchIn(name.trim())

/** Hub-Cloud pages (hubcloud.ist/.foo and mirrors): generate button -> server list. */
internal object JpHubCloud {

    suspend fun resolve(
        url: String,
        referer: String?,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ) {
        try {
            val html = JpNet.get(url, referer, JpNet.headers(referer)) ?: return
            emitHubServers(html, JpNet.getBaseUrl(url), url, subtitleCallback, callback)
        } catch (_: Exception) {
        }
    }

    suspend fun emitHubServers(
        html: String,
        base: String,
        pageUrl: String,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ): Boolean {
        var emitted = false
        val header = hubCardHeader(html)
        val size = Regex("""<i[^>]*id="size"[^>]*>(.*?)</i>""", RegexOption.IGNORE_CASE)
            .find(html)?.groupValues?.get(1)?.replace(Regex("<[^>]+>"), "")?.trim().orEmpty()
        if (Regex("""(?i)\.(zip|rar|7z)\s*$""").containsMatchIn(header.trim())) return false
        val quality = JpNet.getIndexQuality(header)
        val labelExtras = listOf(header, size).filter { it.isNotBlank() }.joinToString(" ")

        val generate = hubButtons(html, true)
            .firstOrNull { it.text.contains("generate", true) }?.href
        if (!generate.isNullOrBlank()) {
            try {
                val genPage = JpNet.get(JpNet.absolute(generate, base), pageUrl, JpNet.headers(pageUrl))
                if (genPage != null && emitGeneratedServers(genPage, labelExtras, quality, subtitleCallback, callback)) return true
            } catch (_: Exception) {
            }
        }

        val inner = when {
            pageUrl.contains("/video/") -> Regex(
                """<div[^>]*class="[^"]*\bvd\b[^"]*"[^>]*>.*?<a[^>]+href="([^"]+)"""",
                setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
            ).find(html)?.groupValues?.get(1)
            else -> null
        }
        if (!inner.isNullOrBlank()) {
            try {
                val innerPage = JpNet.get(JpNet.absolute(inner, base), base, JpNet.headers(base))
                if (innerPage != null && emitHubServers(innerPage, base, inner, subtitleCallback, callback)) return true
            } catch (_: Exception) {
            }
        }

        val found = mutableListOf<Pair<String, String>>()
        for (btn in hubButtons(html, true)) {
            val text = btn.text
            val link = btn.href
            if (hubJunk.containsMatchIn(link)) continue
            val label = text.lowercase()
            val abs = JpNet.absolute(link, base)
            when {
                label.contains("fslv2") -> found.add("FSLv2 [$labelExtras]" to abs)
                label.contains("fsl") -> found.add("FSL Server [$labelExtras]" to abs)
                label.contains("buzzserver") -> {
                    try {
                        val dlink = JpNet.request(
                            "GET", "$abs/download", referer = abs,
                            extraHeaders = JpNet.docHeaders(abs), followRedirects = false,
                        )?.header("hx-redirect") ?: ""
                        if (dlink.isNotBlank()) {
                            found.add("BuzzServer [$labelExtras]" to JpNet.absolute(dlink, JpNet.getBaseUrl(abs)))
                        }
                    } catch (_: Exception) {
                    }
                }
                label.contains("10gbps") -> {
                    try {
                        val target = JpNet.resolveRedirectTarget(abs, base)
                        if (target != null) {
                            val direct = if (target.contains("link=")) target.substringAfter("link=") else target
                            if (direct.startsWith("http")) found.add("10Gbps [Download] [$labelExtras]" to direct)
                        }
                    } catch (_: Exception) {
                    }
                }
                label.contains("instant download") || label.contains("instant dl") -> {
                    try {
                        val loc = JpNet.request(
                            "GET", abs, referer = base,
                            extraHeaders = JpNet.docHeaders(base), followRedirects = false,
                        )?.header("location")?.trim()
                        val direct = when {
                            loc == null -> null
                            loc.contains("url=") -> loc.substringAfter("url=")
                            loc.contains("link=") -> loc.substringAfter("link=")
                            else -> loc
                        }?.takeIf { it.startsWith("http") }
                        if (direct != null) found.add("Instant Download [$labelExtras]" to direct)
                    } catch (_: Exception) {
                    }
                }
                label.contains("pixeldra") || label.contains("pixelserver") || label.contains("pixel server") || link.contains("pixeldra") -> {
                    hubPixelFileUrl(html, link)?.let { found.add("Pixeldrain [$labelExtras]" to it) }
                }
                label.contains("s3 server") || label.contains("mega server") || label.contains("pdl") ->
                    found.add("${text.trim()} [$labelExtras]" to abs)
                label.contains("download file") ->
                    found.add("Download File [$labelExtras]" to abs)
                link.contains("gofile.io") -> {
                    try {
                        JpGoFile.resolve(abs, base, subtitleCallback, callback)
                        emitted = true
                    } catch (_: Exception) {
                    }
                }
                abs.startsWith("http") -> {
                    try {
                        JpNet.emitSiteLink("justplay", abs, labelExtras, null, base, callback)
                        emitted = true
                    } catch (_: Exception) {
                    }
                }
            }
        }
        if (emitVerified(found, quality, callback)) emitted = true
        return emitted
    }

    suspend fun emitVerified(
        found: List<Pair<String, String>>,
        quality: Int,
        callback: (HikariStream) -> Unit,
    ): Boolean {
        if (found.isEmpty()) return false
        val alive = withTimeoutOrNull(30_000L) {
            coroutineScope {
                val gate = Semaphore(6)
                found.distinctBy { it.second }.map { (name, url) ->
                    async(Dispatchers.IO) {
                        gate.acquire()
                        try {
                            if (JpNet.alive(url, false)) name to url else null
                        } finally {
                            gate.release()
                        }
                    }
                }.mapNotNull { runCatching { it.await() }.getOrNull() }
            }
        } ?: emptyList()
        for ((name, url) in alive) {
            val q = if (quality > 0) " · ${quality}p" else ""
            callback(HikariStream(name = "$name$q", url = url))
        }
        return alive.isNotEmpty()
    }

    private suspend fun emitGeneratedServers(
        html: String,
        labelExtras: String,
        quality: Int,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ): Boolean {
        var emitted = false
        val found = mutableListOf<Pair<String, String>>()
        val centerBlocks = Regex(
            """<(center|div[^>]*class="[^"]*\bvd\b[^"]*"[^>]*)>(.*?)</\1>""",
            setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
        ).findAll(html).map { it.groupValues[2] }.joinToString("\n") + "\n" + html
        for (btn in hubButtons(centerBlocks, false)) {
            val text = btn.text.lowercase()
            val href = btn.href
            if (!href.startsWith("http") || hubJunk.containsMatchIn(href)) continue
            when {
                text.contains("fsl") || href.contains("cloudflarestorage.com") || href.contains(".r2.dev/") ||
                    href.endsWith(".r2.dev") ->
                    found.add("FSL Server [$labelExtras]" to href)
                text.contains("pixelserver") || text.contains("pixeldra") || href.contains("pixeldrain") ->
                    hubPixelFileUrl(html, href)?.let { found.add("Pixeldrain [$labelExtras]" to it) }
                text.contains("10gbps") -> {
                    try {
                        val target = JpNet.resolveRedirectTarget(href)
                        if (target != null) {
                            val direct = if (target.contains("link=")) target.substringAfter("link=") else target
                            if (direct.startsWith("http")) found.add("10Gbps [Download] [$labelExtras]" to direct)
                        }
                    } catch (_: Exception) {
                    }
                }
                text.contains("instant download") || text.contains("instant dl") -> {
                    try {
                        val loc = JpNet.request(
                            "GET", href, extraHeaders = JpNet.headers(),
                            followRedirects = false,
                        )?.header("location")?.trim()
                        val direct = when {
                            loc == null -> null
                            loc.contains("url=") -> loc.substringAfter("url=")
                            loc.contains("link=") -> loc.substringAfter("link=")
                            else -> loc
                        }?.takeIf { it.startsWith("http") }
                        if (direct != null) found.add("Instant Download [$labelExtras]" to direct)
                    } catch (_: Exception) {
                    }
                }
                text.contains("download file") || text.contains("download now") || text.contains("download] ") ->
                    found.add("Download File [$labelExtras]" to href)
                href.contains("gofile.io") -> {
                    try {
                        JpGoFile.resolve(href, "", subtitleCallback, callback)
                        emitted = true
                    } catch (_: Exception) {
                    }
                }
                href.contains(".workers.dev/") ->
                    found.add("HubCloud Mirror [$labelExtras]" to href)
            }
        }
        if (emitVerified(found, quality, callback)) emitted = true
        return emitted
    }
}

/** V-Cloud pages: hop page -> script var url (double-base64 or plain) -> hub file list. */
internal object JpVCloud {

    suspend fun resolve(
        url: String,
        referer: String?,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ) {
        try {
            val html = JpNet.fetchWithCf(url, referer) ?: return
            val base = JpNet.getBaseUrl(url)
            var link: String? = null

            val hop = Regex(
                """<div[^>]*class="[^"]*\bmain\b[^"]*"[^>]*>.*?<h4[^>]*>.*?<a[^>]+href="([^"]+)"""",
                setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
            ).find(html)?.groupValues?.get(1)?.trim()
            if (!hop.isNullOrBlank()) {
                val hopPage = try {
                    JpNet.fetchWithCf(JpNet.absolute(hop, base), base)
                } catch (_: Exception) {
                    null
                }
                if (hopPage != null) link = extractVCloudLink(hopPage)
            }
            if (link.isNullOrBlank()) link = extractVCloudLink(html)

            if (link.isNullOrBlank() && url.contains("/video/")) {
                link = Regex(
                    """<div[^>]*class="[^"]*\bvd\b[^"]*"[^>]*>.*?<a[^>]+href="([^"]+)"""",
                    setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
                ).find(html)?.groupValues?.get(1)?.trim()
            }
            if (link.isNullOrBlank()) return

            val abs = JpNet.absolute(link, base)
            if (!abs.startsWith("http")) return

            val targetPage = try {
                JpNet.fetchWithCf(abs, base)
            } catch (_: Exception) {
                null
            } ?: return
            val emitted = JpHubCloud.emitHubServers(targetPage, JpNet.getBaseUrl(abs), abs, subtitleCallback, callback)
            if (!emitted && (abs.contains("drive.google.com") || abs.contains("googleusercontent"))) {
                callback(
                    HikariStream(
                        name = "V-Cloud",
                        url = abs,
                        headers = mapOf("Referer" to "https://vcloud.fit/"),
                    )
                )
            }
        } catch (_: Exception) {
        }
    }

    fun extractVCloudLink(html: String): String? {
        for (m in Regex(
            """<script(?![^>]*src)[^>]*>(.*?)</script>""",
            setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
        ).findAll(html)) {
            val script = m.groupValues[1]
            if (!script.contains("url")) continue
            Regex("""var\s+url\s*=\s*atob\s*\(\s*atob\s*\(\s*['"]([^'"]+)['"]\s*\)\s*\)""")
                .find(script)?.groupValues?.get(1)
                ?.let { encoded ->
                    val decoded = JpNet.b64d(encoded)?.let { JpNet.b64d(it) }
                    if (decoded != null && decoded.startsWith("http")) return decoded
                }
            Regex("""var\s+url\s*=\s*['"]([^'"]*)['"]""").find(script)?.groupValues?.get(1)
                ?.takeIf { it.startsWith("http") }
                ?.let { return it }
        }
        return Regex(
            """<div[^>]*class="[^"]*card-body[^"]*"[^>]*>.*?<h2[^>]*>.*?<a[^>]*class="[^"]*btn[^"]*"[^>]+href="([^"]+)"""",
            setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
        ).find(html)?.groupValues?.get(1)?.trim()?.takeIf { it.startsWith("http") }
    }
}

/** VegaDrive share links (-> skydrop/pixeldrain/buzzheavier/telegram providers). */
internal object JpVegaDrive {

    suspend fun resolve(
        url: String,
        referer: String?,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ) {
        try {
            // m-drive.lol bridge pages carry a urlMap of provider urls instead
            if (JpNet.hostOf(url).contains("m-drive.")) {
                JpMDrive.resolve(url, referer, subtitleCallback, callback)
                return
            }
            JpNet.get(url, "https://nexdrive.fit/", JpNet.docHeaders("https://nexdrive.fit/")) ?: return
            val base = JpNet.getBaseUrl(url)
            val token = url.substringAfter("/s/").substringBefore("?")
            if (token.isBlank() || token == url) return

            val drop = providerLink(base, token, "skydrop")
            if (drop != null && drop.contains("googleusercontent") &&
                !drop.substringAfterLast("/").contains(".zip", true)
            ) {
                callback(HikariStream(name = "V-Drive Vegadrop (10Gbps)", url = drop))
            }

            val pixel = providerLink(base, token, "pixeldrain")
            if (pixel != null && pixel.contains("pixeldrain.com/u/")) {
                val id = pixel.substringBefore("?").substringBefore("#").substringAfterLast("/")
                if (id.isNotBlank()) {
                    callback(HikariStream(name = "V-Drive Pixeldrain", url = "https://pixeldrain.com/api/file/$id"))
                }
            }

            val buzz = providerLink(base, token, "buzzheavier")
            if (buzz != null && buzz.contains("bzzhr.co")) {
                callback(HikariStream(name = "V-Drive Buzzheavier", url = buzz))
            }

            val telegram = providerLink(base, token, "telegram")
            if (telegram != null && telegram.contains("tgfiles")) {
                callback(HikariStream(name = "V-Drive Telegram", url = telegram))
            }
        } catch (_: Exception) {
        }
    }

    private suspend fun providerLink(base: String, token: String, provider: String): String? {
        val start = if (provider == "skydrop") "$base/go/$token/skydrop" else "$base/d/$token/$provider"
        return try {
            JpNet.resolveRedirectTarget(start, "$base/")
        } catch (_: Exception) {
            null
        }
    }
}

/** m-drive.lol bridge: urlMap -> one.vegadrive.me provider urls. */
internal object JpMDrive {

    suspend fun resolve(
        url: String,
        referer: String?,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ) {
        try {
            val page = JpNet.get(url, JpNet.getBaseUrl(url) + "/", JpNet.docHeaders(JpNet.getBaseUrl(url))) ?: return
            val labels = Regex(
                """<button[^>]*data-hash="([^"]+)"[^>]*>(.*?)</button>""",
                setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
            ).findAll(page).associate { m ->
                m.groupValues[1] to m.groupValues[2].replace(Regex("<[^>]+>"), "").replace(Regex("\\s+"), " ").trim()
            }
            val mapRaw = Regex("""(?:const|var|let)\s+urlMap\s*=\s*(\[.*?\])\s*;""", RegexOption.DOT_MATCHES_ALL)
                .find(page)?.groupValues?.get(1) ?: return
            val arr = JSONArray(mapRaw)
            for (i in 0 until arr.length()) {
                val e = arr.optJSONObject(i) ?: continue
                val target = JpNet.b64d(e.optString("url")) ?: continue
                if (!target.startsWith("http")) continue
                val label = labels[e.optString("hash")].orEmpty()
                try {
                    for (s in vegaProviderFlow(target, label, url)) callback(s)
                } catch (_: Exception) {
                }
            }
        } catch (_: Exception) {
        }
    }

    private suspend fun vegaProviderFlow(providerUrl: String, label: String, referer: String): List<HikariStream> {
        val res = JpNet.request(
            "GET", providerUrl, referer = referer,
            extraHeaders = JpNet.docHeaders(referer), followRedirects = false,
        ) ?: return emptyList()
        val loc = res.header("location").orEmpty()
        if (loc.isEmpty()) return emptyList()
        val final = when {
            loc.startsWith("http") -> loc
            loc.startsWith("/") -> JpNet.getBaseUrl(providerUrl) + loc
            else -> return emptyList()
        }
        val tag = if (label.isBlank()) "M-Drive" else "M-Drive $label"
        return when {
            final.contains("googleusercontent") || final.contains("drive.google.com") ->
                listOf(HikariStream(name = tag, url = final))
            final.contains("tgfiles") ->
                listOf(HikariStream(name = "$tag Telegram", url = final))
            final.contains("pixeldrain.com/u/") -> {
                val id = final.substringBefore("?").substringBefore("#").substringAfterLast("/")
                if (id.isBlank()) emptyList()
                else listOf(HikariStream(name = "$tag Pixeldrain", url = "https://pixeldrain.com/api/file/$id"))
            }
            final.contains("bzzhr.co") ->
                listOf(HikariStream(name = "$tag Buzzheavier", url = final))
            final.contains("gofile.io") -> {
                val out = mutableListOf<HikariStream>()
                JpGoFile.resolve(final, referer, {}, { out.add(it) })
                out.map { it.copy(name = "$tag ${it.name}") }
            }
            JpNet.fileAlive(final, referer) ->
                listOf(HikariStream(name = tag, url = final))
            else -> emptyList()
        }
    }
}

/** mcloud.best: download button double-atob payload -> vcloud pipeline. */
internal object JpMCloud {

    suspend fun resolve(
        url: String,
        referer: String?,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ) {
        try {
            val base = JpNet.getBaseUrl(url)
            val page = JpNet.get(url, "$base/", JpNet.docHeaders(base)) ?: return
            val dbl = Regex("""atob\s*\(\s*atob\s*\(\s*['"]([^'"]+)['"]""").find(page)?.groupValues?.get(1)
            var target = dbl?.let { JpNet.b64d(it)?.let { once -> JpNet.b64d(once) } }
            if (target.isNullOrBlank()) {
                val single = Regex("""atob\s*\(\s*['"]([^'"]{32,})['"]""").find(page)?.groupValues?.get(1)
                target = single?.let { JpNet.b64d(it) }
            }
            if (target.isNullOrBlank() || !target.startsWith("http")) return
            target = target.replace(Regex("""https?://(?:www\.)?vcloud\.beer""", RegexOption.IGNORE_CASE), base)
            val targetPage = JpNet.fetchWithCf(target, base) ?: return
            val hubbed = JpHubCloud.emitHubServers(targetPage, JpNet.getBaseUrl(target), target, subtitleCallback, callback)
            if (!hubbed) {
                val link = JpVCloud.extractVCloudLink(targetPage)
                if (!link.isNullOrBlank() && link.startsWith("http")) {
                    val tp = JpNet.fetchWithCf(JpNet.absolute(link, base), base)
                    if (tp != null) JpHubCloud.emitHubServers(tp, base, link, subtitleCallback, callback)
                }
            }
        } catch (_: Exception) {
        }
    }
}

/** FilePress/FileBee JSON API (+DotFlix extract). */
internal object JpFilePress {
    private val fileId = Regex("""/file/([a-f0-9]{16,40})""")
    private const val API = "https://filebee.xyz/api"

    suspend fun resolve(
        url: String,
        referer: String?,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ) {
        val id = fileId.find(url)?.groupValues?.get(1) ?: return
        try {
            val infoRes = JpNet.request(
                "GET", "$API/file/get/$id",
                referer = "https://filebee.xyz/", extraHeaders = JpNet.docHeaders("https://filebee.xyz/"),
            ) ?: return
            if (infoRes.code !in 200..299) return
            val info = try {
                JSONObject(infoRes.body).optJSONObject("data") ?: return
            } catch (_: Exception) {
                return
            }
            if (isArchiveName(info.optString("name"))) return

            val dotflix = download(id, "dotFlixDownlaod")
            if (dotflix != null && dotflix.startsWith("http")) {
                val direct = resolveDotFlix(dotflix)
                if (direct != null && direct.startsWith("http")) {
                    callback(HikariStream(name = "FilePress Instant", url = direct))
                }
            }

            val telegram = download(id, "telegramDownload")
            if (telegram != null && telegram.startsWith("http")) {
                callback(HikariStream(name = "FilePress Telegram", url = telegram))
            }

            val task = download(id, "indexDownlaod")
            if (task != null && task.matches(Regex("[a-f0-9]{16,40}"))) {
                val link = final(task, "indexDownlaod")
                if (link != null && JpNet.fileAlive(link, "https://filebee.xyz/")) {
                    callback(HikariStream(name = "FilePress Direct", url = link))
                }
            }
        } catch (_: Exception) {
        }
    }

    private suspend fun download(id: String, method: String): String? {
        return try {
            val body = JSONObject().put("captchaValue", "").put("id", id).put("method", method)
            val res = JpNet.postJson(
                "$API/file/downlaod/", body.toString(),
                referer = "https://filebee.xyz/",
                extraHeaders = mapOf("Accept" to "application/json", "Origin" to "https://filebee.xyz"),
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

    private suspend fun final(taskId: String, method: String): String? {
        return try {
            val body = JSONObject().put("captchaValue", "").put("id", taskId).put("method", method)
            val res = JpNet.postJson(
                "$API/file/downlaod2/", body.toString(),
                referer = "https://filebee.xyz/",
                extraHeaders = mapOf("Accept" to "application/json", "Origin" to "https://filebee.xyz"),
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
                    .firstNotNullOfOrNull { i -> data.optString(i).takeIf { it.startsWith("http") } }
                else -> null
            }
        } catch (_: Exception) {
            null
        }
    }

    private suspend fun resolveDotFlix(shareUrl: String): String? {
        return try {
            val text = JpNet.get(shareUrl, "https://new2.dotflix.shop/", JpNet.docHeaders("https://new2.dotflix.shop/")) ?: return null
            val code = Regex("""btoa\('([^']+)'\)""").find(text)?.groupValues?.get(1) ?: return null
            val obfuscated = android.util.Base64.encodeToString(code.toByteArray(), android.util.Base64.NO_WRAP).reversed()
            val alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
            val requestId = (1..13).map { alphabet.random() }.joinToString("")
            val timestamp = System.currentTimeMillis().toString()
            val body = JSONObject().put("requestId", requestId).put("timestamp", timestamp).put("data", obfuscated)
            val res = JpNet.postJson(
                "https://dotflix.store/api/extract-download", body.toString(),
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
}

/** FastDl / HubCdn redirect stubs (var reurl -> drive link). */
internal object JpDirectStub {
    suspend fun resolve(url: String, referer: String?): String? {
        try {
            val text = JpNet.get(url, referer ?: "https://nexdrive.fit/", JpNet.headers(referer)) ?: return null
            val embedded = Regex("""var\s+reurl\s*=\s*"([^"]+)"""").find(text)?.groupValues?.get(1)
                ?: Regex("""'(https://fastdl[^']*dl\.php\?link=[^']+)'"""").find(text)?.groupValues?.get(1)
                ?: return null
            val wrapped = Regex("""[?&]r=([A-Za-z0-9+/=_-]+)""").find(embedded)?.groupValues?.get(1)
            val carrier = if (wrapped != null) {
                val padded = if (wrapped.length % 4 > 0) wrapped + "=".repeat(4 - wrapped.length % 4) else wrapped
                JpNet.b64d(padded) ?: return null
            } else embedded
            val encoded = Regex("""link=(https?://[^&"']+)""").find(carrier)?.groupValues?.get(1) ?: return null
            return URLDecoder.decode(encoded, "UTF-8").takeIf { it.startsWith("http") }
        } catch (_: Exception) {
            return null
        }
    }
}

internal object JpFastDl {
    suspend fun resolve(
        url: String,
        referer: String?,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ) {
        val direct = JpDirectStub.resolve(url, referer) ?: return
        callback(
            HikariStream(
                name = "G-Direct",
                url = direct,
                headers = mapOf("Referer" to "https://fastdl.zip/"),
            )
        )
    }
}

internal object JpHubCdn {
    suspend fun resolve(
        url: String,
        referer: String?,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ) {
        val direct = JpDirectStub.resolve(url, referer) ?: return
        callback(
            HikariStream(
                name = "G-Direct",
                url = direct,
                headers = mapOf("Referer" to "https://hubcdn.wiki/"),
            )
        )
    }
}

/** HbLinks index pages: entry-content anchors resolved through the shared router. */
internal object JpHbLinks {
    suspend fun resolve(
        url: String,
        referer: String?,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ) {
        try {
            val html = JpNet.fetchWithCf(url, referer) ?: return
            val entry = Regex(
                """<div[^>]*class="[^"]*entry-content[^"]*"[^>]*>(.*?)</div>\s*(?:</article>|<footer|$)""",
                setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
            ).find(html)?.groupValues?.get(1) ?: html
            val seen = mutableSetOf<String>()
            for (m in Regex("""<a[^>]+href="([^"]+)"""", RegexOption.IGNORE_CASE).findAll(entry)) {
                val href = m.groupValues[1].trim()
                if (!href.startsWith("http") || !seen.add(href)) continue
                JpNet.emitOwnLink("justplay", href, "", null, url, subtitleCallback, callback)
            }
        } catch (_: Exception) {
        }
    }
}

/** Hubdrive pages: hubcloud anchors resolved natively. */
internal object JpHubDrive {
    suspend fun resolve(
        url: String,
        referer: String?,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ) {
        try {
            val html = JpNet.fetchWithCf(url, referer) ?: return
            val seen = mutableSetOf<String>()
            for (m in Regex("""<a[^>]+href="([^"]+)"""", RegexOption.IGNORE_CASE).findAll(html)) {
                val href = m.groupValues[1].trim()
                if (!href.startsWith("http") || href.contains("/tg/") || !seen.add(href)) continue
                if (href.contains("hubcloud")) {
                    JpNet.emitOwnLink("justplay", href, "", null, url, subtitleCallback, callback)
                }
            }
        } catch (_: Exception) {
        }
    }
}

/** GoFile share links through a throwaway guest account. */
internal object JpGoFile {
    suspend fun resolve(
        url: String,
        referer: String?,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ) {
        try {
            val code = url.substringAfterLast("/").substringBefore("#").substringBefore("?").trim()
            if (code.isBlank()) return
            val api = "https://api.gofile.io"
            val accBody = JpNet.postJson("$api/accounts", "{}", referer = "https://gofile.io/")?.body ?: return
            val token = JSONObject(accBody).optJSONObject("data")?.optString("token").orEmpty()
            if (token.isBlank()) return
            val srvBody = JpNet.get("$api/servers", "https://gofile.io/") ?: ""
            var server = "store1"
            try {
                val arr = JSONObject(srvBody).optJSONObject("data")?.optJSONArray("servers")
                for (i in 0 until (arr?.length() ?: 0)) {
                    val n = arr!!.optJSONObject(i)?.optString("name").orEmpty()
                    if (n.isNotBlank()) {
                        server = n
                        break
                    }
                }
            } catch (_: Exception) {
            }
            val content = JpNet.request(
                "GET", "https://$server.gofile.io/contents/$code?wt=$token",
                referer = "https://gofile.io/",
                extraHeaders = mapOf("Accept" to "application/json", "Authorization" to "Bearer $token"),
            ) ?: return
            if (content.code !in 200..299) return
            val children = JSONObject(content.body).optJSONObject("data")?.optJSONObject("children") ?: return
            for (key in children.keys()) {
                val child = children.optJSONObject(key) ?: continue
                val link = child.optString("link").takeIf { it.startsWith("http") } ?: continue
                val name = child.optString("name")
                val size = child.optLong("size", 0L)
                val sizeText = if (size > 0) "${size / 1024 / 1024} MB" else ""
                val inner = listOf(name.take(60), sizeText).filter { it.isNotBlank() }.joinToString(" | ")
                callback(
                    HikariStream(
                        name = if (inner.isBlank()) "GoFile" else "GoFile [$inner]",
                        url = link,
                        headers = mapOf("Authorization" to "Bearer $token"),
                    )
                )
            }
        } catch (_: Exception) {
        }
    }
}

/** CryptoJS-compatible AES-CBC (Salted__ + EVP_BytesToKey MD5). Port of PlayCrypto. */
internal object JpCrypto {

    private fun evpBytesToKey(password: ByteArray, salt: ByteArray, keyLen: Int = 32, ivLen: Int = 16): Pair<ByteArray, ByteArray> {
        val md = java.security.MessageDigest.getInstance("MD5")
        val out = ArrayList<Byte>(keyLen + ivLen)
        var prev = ByteArray(0)
        while (out.size < keyLen + ivLen) {
            md.reset()
            md.update(prev)
            md.update(password)
            md.update(salt)
            prev = md.digest()
            out.addAll(prev.toList())
        }
        return out.subList(0, keyLen).toByteArray() to out.subList(keyLen, keyLen + ivLen).toByteArray()
    }

    fun aesEncrypt(plain: String, passphrase: String): String? = try {
        val salt = java.security.SecureRandom().generateSeed(8)
        val (key, iv) = evpBytesToKey(passphrase.toByteArray(Charsets.UTF_8), salt)
        val cipher = javax.crypto.Cipher.getInstance("AES/CBC/PKCS5Padding")
        cipher.init(
            javax.crypto.Cipher.ENCRYPT_MODE,
            javax.crypto.spec.SecretKeySpec(key, "AES"),
            javax.crypto.spec.IvParameterSpec(iv),
        )
        val out = "Salted__".toByteArray(Charsets.UTF_8) + salt + cipher.doFinal(plain.toByteArray(Charsets.UTF_8))
        android.util.Base64.encodeToString(out, android.util.Base64.NO_WRAP)
            .replace("+", "-").replace("/", "_").replace("=", "")
    } catch (_: Exception) {
        null
    }

    fun aesDecrypt(data: String, passphrase: String): String? = try {
        val b64 = data.trim().replace("-", "+").replace("_", "/")
            .let { if (it.length % 4 != 0) it + "=".repeat(4 - it.length % 4) else it }
        val raw = android.util.Base64.decode(b64, android.util.Base64.DEFAULT)
        if (raw.size < 17 || String(raw, 0, 8, Charsets.UTF_8) != "Salted__") return null
        val salt = raw.copyOfRange(8, 16)
        val (key, iv) = evpBytesToKey(passphrase.toByteArray(Charsets.UTF_8), salt)
        val cipher = javax.crypto.Cipher.getInstance("AES/CBC/PKCS5Padding")
        cipher.init(
            javax.crypto.Cipher.DECRYPT_MODE,
            javax.crypto.spec.SecretKeySpec(key, "AES"),
            javax.crypto.spec.IvParameterSpec(iv),
        )
        String(cipher.doFinal(raw.copyOfRange(16, raw.size)), Charsets.UTF_8)
    } catch (_: Exception) {
        null
    }
}

/** GDFlix/GDLink pages: instant/10gbps/drivebot/cloud/direct/fsl/pixeldrain/gofile buttons. */
internal object JpGdFlix {

    suspend fun resolve(
        url: String,
        referer: String?,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ) {
        try {
            val html = JpNet.get(url, referer, JpNet.headers(referer)) ?: return
            val base = JpNet.getBaseUrl(url)
            val fileName = Regex(
                """<li[^>]*class="[^"]*list-group-item[^"]*"[^>]*>([^<]*Name[^<]*)</li>""",
                RegexOption.IGNORE_CASE,
            ).find(html)?.groupValues?.get(1)?.substringAfter("Name :")?.trim()
                ?.ifBlank {
                    Regex("""<title>([^<]*)""", RegexOption.IGNORE_CASE).find(html)
                        ?.groupValues?.get(1)?.substringAfter("GDFlix |")?.trim()
                }.orEmpty()
            val sizeText = Regex(
                """<li[^>]*class="[^"]*list-group-item[^"]*"[^>]*>([^<]*Size[^<]*)</li>""",
                RegexOption.IGNORE_CASE,
            ).find(html)?.groupValues?.get(1)?.substringAfter("Size :")?.substringBefore("|")?.trim().orEmpty()
            val quality = JpNet.getIndexQuality(fileName)

            suspend fun emit(link: String, server: String) {
                if (!link.startsWith("http")) return
                val extras = listOf(fileName, sizeText).filter { it.isNotBlank() }.joinToString(" ")
                val q = if (quality > 0) " · ${quality}p" else ""
                callback(
                    HikariStream(
                        name = "GDFlix $server [$extras]$q",
                        url = link,
                        headers = mapOf("Referer" to base),
                    )
                )
            }

            for (btn in hubButtons(html, true)) {
                val text = btn.text
                val link = btn.href
                if (link.isBlank() || link.startsWith("#")) continue
                val abs = JpNet.absolute(link, base)
                when {
                    text.contains("Login To DL", true) || link.startsWith("/login") -> {}
                    text.contains("Instant DL", true) || text.contains("Instant Download", true) -> {
                        try {
                            val loc = JpNet.request(
                                "GET", abs, referer = url,
                                extraHeaders = JpNet.headers(url), followRedirects = false,
                            )?.header("location")?.trim()
                            val direct = when {
                                loc == null -> null
                                loc.contains("url=") -> loc.substringAfter("url=")
                                loc.contains("link=") -> loc.substringAfter("link=")
                                else -> loc
                            }?.takeIf { it.startsWith("http") }
                            if (direct != null) emit(direct, "Instant Download")
                        } catch (_: Exception) {
                        }
                    }
                    text.contains("10GBPS", true) -> {
                        try {
                            val target = JpNet.resolveRedirectTarget(abs, url)
                            if (target != null) {
                                val direct = if (target.contains("link=")) target.substringAfter("link=") else target
                                emit(direct, "10Gbps")
                            }
                        } catch (_: Exception) {
                        }
                    }
                    text.contains("DRIVEBOT", true) || link.contains("drivebot") ->
                        driveBot(abs, fileName, sizeText, quality, callback)
                    text.contains("FAST CLOUD", true) || text.contains("CLOUD DOWNLOAD", true) ||
                        text.contains("ZIPDISK", true) -> {
                        try {
                            val cloudPage = JpNet.get(abs, url, JpNet.headers(url))
                            val card = cloudPage?.let {
                                Regex(
                                    """<div[^>]*class="[^"]*card-body[^"]*"[^>]*>.*?<a[^>]+href="([^"]+)"""",
                                    setOf(RegexOption.DOT_MATCHES_ALL, RegexOption.IGNORE_CASE),
                                ).find(it)?.groupValues?.get(1)?.trim()
                            }
                            if (!card.isNullOrBlank() && card.startsWith("http")) emit(card, "Cloud")
                        } catch (_: Exception) {
                        }
                    }
                    text.contains("DIRECT DL", true) || text.contains("DIRECT SERVER", true) ->
                        emit(abs, "Direct")
                    text.contains("FSL", true) -> emit(abs, "FSL")
                    link.contains("pixeldrain") -> {
                        val pixelBase = JpNet.getBaseUrl(link)
                        val final = if (link.contains("download", true)) link
                        else "$pixelBase/api/file/${link.substringAfterLast("/").substringBefore("?")}?download"
                        emit(final, "Pixeldrain")
                    }
                    link.contains("gofile") ->
                        JpGoFile.resolve(abs, url, subtitleCallback, callback)
                }
            }
        } catch (_: Exception) {
        }
    }

    private suspend fun driveBot(
        url: String,
        fileName: String,
        sizeText: String,
        quality: Int,
        callback: (HikariStream) -> Unit,
    ) {
        try {
            val body = JpNet.get(url, null, JpNet.headers()) ?: return
            val token = Regex("""formData\.append\('token', '([a-f0-9]+)'""").find(body)?.groupValues?.get(1)
            val downloadId = Regex("""fetch\('/download\?id=([a-zA-Z0-9/+]+)'""").find(body)?.groupValues?.get(1)
            if (token == null || downloadId == null) return
            val answer = JpNet.postForm(
                JpNet.absolute("/download?id=$downloadId", JpNet.getBaseUrl(url)),
                mapOf("token" to token),
                referer = url,
                extraHeaders = mapOf("User-Agent" to PLAY_UA),
            )?.body ?: return
            val link = Regex(""""url"\s*:\s*"(.*?)"""").find(answer)?.groupValues?.get(1)?.replace("\\", "")
            if (!link.isNullOrBlank() && link.startsWith("http")) {
                val extras = listOf(fileName, sizeText).filter { it.isNotBlank() }.joinToString(" ")
                val q = if (quality > 0) " · ${quality}p" else ""
                callback(HikariStream(name = "GDFlix DriveBot [$extras]$q", url = link))
            }
        } catch (_: Exception) {
        }
    }
}

/** NxShare (nxsha.space) encrypted scraper API. Port of PlayNxsha (Jackson -> org.json). */
internal object JpNxsha {
    private const val PASSPHRASE = "S8x!Jk4ZP1uG8\$my"
    private const val BASE = "https://nxsha.space"
    private const val TMDB_PROXY = "https://db.speedracelight.com/3"

    private suspend fun apiGet(path: String, payload: Map<String, String>, referer: String): String? {
        val obj = payload.toMutableMap()
        obj["_req_ts"] = System.currentTimeMillis().toString()
        obj["_req_salt"] = (1..10).map { ('a' + (0..35).random()) }.joinToString("")
        val q = JpCrypto.aesEncrypt(JSONObject(obj as Map<String, String>).toString(), PASSPHRASE) ?: return null
        return try {
            JpNet.get("$BASE$path?q=${JpNet.urlEncode(q)}", referer, JpNet.headers(referer))
        } catch (_: Exception) {
            null
        }
    }

    private fun decodeHash(body: String?): JSONObject? {
        if (body.isNullOrBlank()) return null
        val hash = Regex(""""_hash"\s*:\s*"([^"]+)"""").find(body)?.groupValues?.get(1) ?: return null
        val plain = JpCrypto.aesDecrypt(hash, PASSPHRASE) ?: return null
        return try {
            JSONObject(plain)
        } catch (_: Exception) {
            null
        }
    }

    private suspend fun imdbToTmdb(imdbId: String): String? {
        val body = try {
            JpNet.get("$TMDB_PROXY/find/$imdbId?external_source=imdb_id")
        } catch (_: Exception) {
            return null
        } ?: return null
        return Regex(""""movie_results"\s*:\s*\[\s*\{[^}]*?"id"\s*:\s*(\d+)""")
            .find(body)?.groupValues?.get(1)
    }

    suspend fun resolve(
        embedUrl: String,
        label: String,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ): Boolean {
        return try {
            val tvMatch = Regex("""/embed/tv/(\d+)/(\d+)/(\d+)""").find(embedUrl)
            val movieMatch = Regex("""/embed/movie/(tt\d+)""").find(embedUrl)
            var tmdbId: String? = null
            var imdbId = ""
            var type = "movie"
            var season = ""
            var episode = ""
            if (tvMatch != null) {
                tmdbId = tvMatch.groupValues[1]
                season = tvMatch.groupValues[2]
                episode = tvMatch.groupValues[3]
                type = "tv"
            } else if (movieMatch != null) {
                imdbId = movieMatch.groupValues[1]
                tmdbId = imdbToTmdb(imdbId)
                    ?: Regex("""/embed/movie/(\d+)""").find(embedUrl)?.groupValues?.get(1)
            } else {
                return false
            }
            val tmdb = tmdbId ?: return false

            val base = mapOf(
                "tmdbId" to tmdb, "imdb_id" to imdbId, "type" to type,
                "season" to season, "episode" to episode,
            )
            val serversBody = apiGet("/api/servers", base, embedUrl) ?: return false
            val servers = decodeHash(serversBody)?.optJSONArray("servers") ?: return false

            var any = false
            coroutineScope {
                (0 until servers.length()).map { si ->
                    async(Dispatchers.IO) {
                        val server = servers.optJSONObject(si) ?: return@async
                        if (server.optBoolean("webSupport", true) == false) return@async
                        val scraper = server.optString("scraper").ifBlank { return@async }
                        val sourcesBody = try {
                            apiGet("/api/sources", base + mapOf("ex_lang" to "false", "provider" to scraper), embedUrl)
                        } catch (_: Exception) {
                            null
                        } ?: return@async
                        val sources = decodeHash(sourcesBody)?.optJSONArray("sources") ?: return@async
                        for (i in 0 until sources.length()) {
                            val src = sources.optJSONObject(i) ?: continue
                            val url = src.optString("url").trim().takeIf { it.startsWith("http") } ?: continue
                            if (src.optBoolean("isEmbed")) continue
                            val name = JpLabels.buildLabel("multimovies", "", "$label ${server.optString("name")}")
                            val isM3u8 = when (src.optString("type").lowercase()) {
                                "mp4", "video" -> false
                                else -> true
                            }
                            val qualityNum = Regex("(2160|1080|720|480|360)")
                                .find(src.optString("label").ifBlank { src.optString("quality") })
                                ?.groupValues?.get(1)?.toIntOrNull()
                            val q = qualityNum?.let { " · ${it}p" } ?: ""
                            callback(
                                HikariStream(
                                    name = "$name$q",
                                    url = url,
                                    headers = mapOf("Referer" to "$BASE/"),
                                    isM3u8 = isM3u8,
                                    isMpd = src.optString("type").lowercase() == "mpd" ||
                                        src.optString("type").lowercase() == "dash",
                                )
                            )
                            any = true
                        }
                    }
                }.forEach { it.join() }
            }

            try {
                val subsBody = apiGet("/api/subtitles", base, embedUrl)
                decodeHash(subsBody)?.optJSONArray("subtitles")?.let { arr ->
                    for (i in 0 until arr.length()) {
                        val sub = arr.optJSONObject(i) ?: continue
                        val uri = sub.optString("uri").trim().takeIf { it.startsWith("http") } ?: continue
                        val name = sub.optString("title").takeIf { it.isNotBlank() }
                            ?: sub.optString("language").takeIf { it.isNotBlank() } ?: "English"
                        subtitleCallback(HikariSubtitle(name, uri))
                    }
                }
            } catch (_: Exception) {
            }
            any
        } catch (_: Exception) {
            false
        }
    }
}

/** Vidout github hls index (+ cdn/github subtitles). Port of PlayVidout. */
internal object JpVidout {
    private const val REFERER = "https://vidout.pages.dev/"
    private const val GITHUB_RAW = "https://raw.githubusercontent.com/Watchout2025/api/refs/heads/main"

    private val langNames = mapOf(
        "eng" to "English", "hin" to "Hindi", "spa" to "Spanish", "fre" to "French",
        "ger" to "German", "ita" to "Italian", "por" to "Portuguese", "rus" to "Russian",
        "zho" to "Chinese", "ara" to "Arabic", "kor" to "Korean", "jpn" to "Japanese",
        "tam" to "Tamil", "tel" to "Telugu", "kan" to "Kannada", "mal" to "Malayalam",
    )

    private suspend fun getText(url: String): String? = try {
        val r = JpNet.request("GET", url, referer = REFERER, extraHeaders = JpNet.headers())
        if (r != null && r.code in 200..299) r.body else null
    } catch (_: Exception) {
        null
    }

    suspend fun resolve(
        embedUrl: String,
        label: String,
        subtitleCallback: (HikariSubtitle) -> Unit,
        callback: (HikariStream) -> Unit,
    ): Boolean {
        return try {
            val tvMatch = Regex("""/tv/(\d+)/(?:s(\d+)|(\d+))/(?:e(\d+)|(\d+))""", RegexOption.IGNORE_CASE)
                .find(embedUrl)
            val movieMatch = Regex("""/movie/(tt\d+|\d+)""", RegexOption.IGNORE_CASE).find(embedUrl)

            var streamUrl: String? = null
            var tmdbId: String? = null
            var season: Int? = null
            var episode: Int? = null
            if (tvMatch != null) {
                tmdbId = tvMatch.groupValues[1]
                season = tvMatch.groupValues[2].ifEmpty { tvMatch.groupValues[3] }.toIntOrNull()
                episode = tvMatch.groupValues[4].ifEmpty { tvMatch.groupValues[5] }.toIntOrNull()
                if (tmdbId == null || season == null || episode == null) return false
                val body = getText("$GITHUB_RAW/hls/tv/$tmdbId/S$season.json") ?: return false
                streamUrl = Regex(""""$episode"\s*:\s*"([^"]+)"""").find(body)?.groupValues?.get(1)
            } else if (movieMatch != null) {
                val id = movieMatch.groupValues[1]
                tmdbId = if (id.startsWith("tt")) {
                    val body = try {
                        JpNet.get("https://db.speedracelight.com/3/find/$id?external_source=imdb_id")
                    } catch (_: Exception) {
                        return false
                    } ?: return false
                    Regex(""""movie_results"\s*:\s*\[\s*\{[^}]*?"id"\s*:\s*(\d+)""")
                        .find(body)?.groupValues?.get(1)
                } else id
                streamUrl = tmdbId?.let { getText("$GITHUB_RAW/hls/movie/$it")?.trim()?.takeIf { s -> s.startsWith("http") } }
            } else {
                return false
            }

            var url = streamUrl ?: return false
            url = JpNet.deEsc(url)
            val lower = url.lowercase()
            if (url.contains("#")) return false
            if (lower.endsWith(".txt") && !lower.contains("/hls3/")) return false
            if (!lower.contains(".m3u8") && !lower.endsWith(".txt") && !lower.contains("/stream/")) {
                return false
            }

            val name = JpLabels.buildLabel("multimovies", "", label)
            callback(
                HikariStream(name = name, url = url, headers = mapOf("Referer" to REFERER), isM3u8 = true)
            )
            loadCdnSubtitles(url, subtitleCallback)
            loadGithubSubtitles(tmdbId, season, episode, subtitleCallback)
            true
        } catch (_: Exception) {
            false
        }
    }

    private suspend fun loadCdnSubtitles(streamUrl: String, subtitleCallback: (HikariSubtitle) -> Unit) {
        try {
            val m = Regex("""/([^/]+)/hls3/([^/]+)/([^/]+)/([^/]+)_(?:,|[nhl]/)""").find(streamUrl) ?: return
            val (srv, prefix, folderId, filePrefix) = m.destructured
            for ((code, name) in langNames) {
                val cdn = "https://$srv.acek-cdn.com/vtt/$prefix/$folderId/${filePrefix}_$code.vtt"
                subtitleCallback(HikariSubtitle(name, cdn))
            }
        } catch (_: Exception) {
        }
    }

    private suspend fun loadGithubSubtitles(
        tmdbId: String?,
        season: Int?,
        episode: Int?,
        subtitleCallback: (HikariSubtitle) -> Unit,
    ) {
        if (tmdbId == null) return
        try {
            val path = if (season != null && episode != null) {
                "sub/tv/$tmdbId/$season/$episode/subtitles.json"
            } else {
                "sub/movie/$tmdbId/subtitles.json"
            }
            val body = getText("$GITHUB_RAW/$path") ?: return
            for (m in Regex(""""([a-z]{2})"\s*:\s*"(https?[^"]+)"""").findAll(body)) {
                subtitleCallback(HikariSubtitle(m.groupValues[1].uppercase(), JpNet.deEsc(m.groupValues[2])))
            }
        } catch (_: Exception) {
        }
    }
}
