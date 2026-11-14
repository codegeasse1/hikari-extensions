package com.hikari.ext.providers

import com.hikari.ext.HikariNet
import kotlinx.coroutines.runBlocking
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.OutputStream
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.URI
import java.net.URLDecoder
import java.net.URLEncoder
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

/**
 * Localhost HLS proxy for Senshi (vidcloud/anidap CDN) masters: pins the
 * chosen variant + audio mode, rewrites playlists/keys/segments through the
 * proxy, decrypts EM3U8v1 playlists on the fly. Upstream via HikariNet.
 */
object SnProxy {

    private const val MAX_STREAMS = 12
    private const val HLS_TYPE = "application/vnd.apple.mpegurl"

    private var serverSocket: ServerSocket? = null
    private var serverPort = 0
    @Volatile private var serverRunning = false

    private val pool: ExecutorService = Executors.newCachedThreadPool()
    private val streams = ConcurrentHashMap<String, StreamEntry>()
    private val order = ArrayDeque<String>()
    private val orderLock = Any()

    class StreamEntry(
        val id: String,
        val masterUrl: String,
        val masterContent: String,
        val headers: Map<String, String>,
    ) {
        val servedPlaylists = ConcurrentHashMap<String, String>()
    }

    @Synchronized
    private fun ensureServerRunning(): Int {
        if (serverRunning && serverPort > 0) return serverPort
        try {
            val socket = ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"))
            serverSocket = socket
            serverPort = socket.localPort
            serverRunning = true
            Thread {
                while (serverRunning) {
                    try {
                        val conn = socket.accept()
                        pool.execute { handleRequest(conn) }
                    } catch (t: Throwable) { }
                }
            }.apply { isDaemon = true }.start()
        } catch (t: Throwable) { }
        return serverPort
    }

    @Synchronized
    fun register(masterUrl: String, masterContent: String, headers: Map<String, String>): String? {
        val port = ensureServerRunning()
        if (port == 0) return null
        val id = MessageDigest.getInstance("MD5")
            .digest(masterUrl.toByteArray())
            .joinToString("") { "%02x".format(it) }
            .take(10)
        synchronized(orderLock) {
            if (!streams.containsKey(id)) order.addLast(id)
            while (order.size > MAX_STREAMS) {
                order.removeFirstOrNull()?.let { streams.remove(it) }
            }
            streams[id] = StreamEntry(id, masterUrl, masterContent, headers)
        }
        return "http://127.0.0.1:$port/$id"
    }

    private fun handleRequest(conn: Socket) {
        try {
            conn.soTimeout = 20000
            val reader = BufferedReader(InputStreamReader(conn.getInputStream()))
            val requestLine = reader.readLine() ?: return
            val rawPath = requestLine.split(" ").getOrNull(1) ?: return
            val requestHeaders = mutableMapOf<String, String>()
            while (true) {
                val line = reader.readLine() ?: break
                if (line.isEmpty()) break
                val idx = line.indexOf(':')
                if (idx > 0) requestHeaders[line.substring(0, idx).trim().lowercase()] = line.substring(idx + 1).trim()
            }
            val pathOnly = rawPath.substringBefore("?")
            val segments = pathOnly.split("/").filter { it.isNotEmpty() }
            if (segments.size < 3) {
                send404(conn)
                return
            }
            val entry = streams[segments[0]] ?: run { send404(conn); return }
            when (segments[1]) {
                "m" -> {
                    val mode = segments.getOrNull(2) ?: "sub"
                    val variant = segments.getOrNull(3)?.toIntOrNull() ?: 0
                    val rewritten = rewriteMaster(entry, mode, variant)
                    if (rewritten == null) send404(conn)
                    else sendBytes(conn, rewritten.toByteArray(Charsets.UTF_8), HLS_TYPE)
                }
                "p" -> {
                    val target = decodeUrl(segments.drop(2).joinToString("/"))
                    if (target == null) send404(conn) else servePlaylist(conn, entry, target)
                }
                "s" -> {
                    val target = decodeUrl(segments.drop(2).joinToString("/"))
                    if (target == null) send404(conn) else serveSegment(conn, entry, target, requestHeaders["range"])
                }
                "k" -> {
                    val target = decodeUrl(segments.drop(2).joinToString("/"))
                    if (target == null) send404(conn) else serveKey(conn, entry, target)
                }
                else -> send404(conn)
            }
        } catch (t: Throwable) { } finally {
            try { conn.close() } catch (t: Throwable) { }
        }
    }

    private val uriAttr = Regex("URI=\"([^\"]+)\"")
    private val languageAttr = Regex("LANGUAGE=\"([^\"]*)\"")
    private val nameAttr = Regex("NAME=\"([^\"]*)\"")

    private fun isEnglishAudio(line: String, uri: String): Boolean {
        val lang = languageAttr.find(line)?.groupValues?.get(1)?.lowercase() ?: ""
        val name = nameAttr.find(line)?.groupValues?.get(1)?.lowercase() ?: ""
        if (lang.startsWith("en")) return true
        if (name.contains("english")) return true
        val u = uri.lowercase()
        return u.contains("1_en") || u.contains("_en") || u.contains("/en/") || u.contains("-en")
    }

    private fun markDefault(line: String, yes: Boolean): String {
        val value = if (yes) "YES" else "NO"
        return if (line.contains("DEFAULT=")) line.replace(Regex("DEFAULT=[^,]*"), "DEFAULT=$value")
        else "$line,DEFAULT=$value"
    }

    private fun rewriteMaster(entry: StreamEntry, mode: String, variantIdx: Int): String? {
        try {
            val lines = entry.masterContent.split("\n").map { it.trim() }.filter { it.isNotEmpty() }
            val base = URI(entry.masterUrl.substringBefore("?"))
            val audioIdx = mutableListOf<Int>()
            for (i in lines.indices) {
                if (lines[i].startsWith("#EXT-X-MEDIA") && lines[i].contains("TYPE=AUDIO")) audioIdx.add(i)
            }
            val keepAudio = mutableSetOf<Int>()
            if (audioIdx.isNotEmpty()) {
                val uriOf = { i: Int -> uriAttr.find(lines[i])?.groupValues?.get(1)?.let { resolveUri(it, base) } ?: "" }
                val english = audioIdx.filter { isEnglishAudio(lines[it], uriOf(it)) }
                when {
                    mode == "dub" && english.isNotEmpty() -> keepAudio.addAll(english)
                    mode == "dub" -> keepAudio.addAll(audioIdx)
                    else -> {
                        val nonEnglish = audioIdx.filter { it !in english }
                        if (nonEnglish.isNotEmpty()) keepAudio.addAll(nonEnglish) else keepAudio.addAll(audioIdx)
                    }
                }
            }
            val variantBlocks = mutableListOf<Pair<Int, Int>>()
            var pending = -1
            for (i in lines.indices) {
                if (lines[i].startsWith("#EXT-X-STREAM-INF")) pending = i
                else if (pending >= 0 && !lines[i].startsWith("#")) {
                    variantBlocks.add(pending to i)
                    pending = -1
                }
            }
            if (variantBlocks.isEmpty()) return entry.masterContent
            val chosen = variantBlocks.getOrNull(variantIdx) ?: variantBlocks.first()
            val skipLines = mutableSetOf<Int>()
            variantBlocks.forEach { (si, ui) ->
                if (si != chosen.first) {
                    skipLines.add(si)
                    skipLines.add(ui)
                }
            }
            val out = StringBuilder()
            out.append("#EXTM3U\n")
            var firstAudioKept = true
            for (i in lines.indices) {
                if (i in skipLines) continue
                val line = lines[i]
                when {
                    line == "#EXTM3U" -> { }
                    line.startsWith("#EXT-X-MEDIA") -> {
                        if (line.contains("TYPE=AUDIO")) {
                            if (i !in keepAudio) continue
                            out.append(markDefault(rewriteRenditionLine(line, base, entry), firstAudioKept)).append('\n')
                            firstAudioKept = false
                        } else out.append(rewriteRenditionLine(line, base, entry)).append('\n')
                    }
                    line.startsWith("#EXT-X-SESSION-KEY") -> out.append(rewriteAttr(line, base, entry, "k")).append('\n')
                    line.startsWith("#EXT-X-STREAM-INF") -> out.append(line).append('\n')
                    !line.startsWith("#") && i == chosen.second -> {
                        val abs = resolveUri(line, base) ?: return null
                        out.append(proxyPath(entry, abs, "p")).append('\n')
                    }
                    else -> out.append(line).append('\n')
                }
            }
            return out.toString()
        } catch (t: Throwable) {
            return null
        }
    }

    private fun rewriteRenditionLine(line: String, base: URI, entry: StreamEntry): String {
        val m = uriAttr.find(line) ?: return line
        val abs = resolveUri(m.groupValues[1], base) ?: return line
        return line.replaceRange(m.range, "URI=\"${proxyPath(entry, abs, "p")}\"")
    }

    private fun rewriteAttr(line: String, base: URI, entry: StreamEntry, route: String): String {
        val m = uriAttr.find(line) ?: return line
        val abs = resolveUri(m.groupValues[1], base) ?: return line
        return line.replaceRange(m.range, "URI=\"${proxyPath(entry, abs, route)}\"")
    }

    private fun servePlaylist(conn: Socket, entry: StreamEntry, target: String) {
        try {
            entry.servedPlaylists[target]?.let {
                sendBytes(conn, it.toByteArray(Charsets.UTF_8), HLS_TYPE)
                return
            }
            val body = runBlocking { HikariNet.getString(target, entry.headers) } ?: run { send404(conn); return }
            val playlist: String = if (SnCrypt.isEncrypted(body)) {
                SnCrypt.decrypt(body) ?: run { send404(conn); return }
            } else body
            if (!playlist.startsWith("#EXTM3U")) {
                sendBytes(conn, playlist.toByteArray(Charsets.UTF_8), "application/octet-stream")
                return
            }
            val base = URI(target.substringBefore("?"))
            val out = StringBuilder()
            for (raw in playlist.split("\n")) {
                val line = raw.trim()
                if (line.isEmpty()) continue
                when {
                    line.startsWith("#EXT-X-KEY") -> out.append(rewriteAttr(line, base, entry, "k")).append('\n')
                    line.startsWith("#EXT-X-MAP") -> out.append(rewriteAttr(line, base, entry, "s")).append('\n')
                    line.startsWith("#EXT-X-SESSION-KEY") -> out.append(rewriteAttr(line, base, entry, "k")).append('\n')
                    !line.startsWith("#") -> {
                        val abs = resolveUri(line, base) ?: line
                        out.append(proxyPath(entry, abs, "s")).append('\n')
                    }
                    else -> out.append(line).append('\n')
                }
            }
            val rewritten = out.toString()
            entry.servedPlaylists[target] = rewritten
            sendBytes(conn, rewritten.toByteArray(Charsets.UTF_8), HLS_TYPE)
        } catch (t: Throwable) {
            send404(conn)
        }
    }

    private fun serveSegment(conn: Socket, entry: StreamEntry, target: String, range: String?) {
        try {
            // Fetch the full segment, then slice locally so seeking always works.
            val bytes = runBlocking { HikariNet.getBytes(target, entry.headers) } ?: run { send404(conn); return }
            sendBytesWithRange(conn, bytes, "video/mp2t", range, upstreamRange = null)
        } catch (t: Throwable) { }
    }

    private fun serveKey(conn: Socket, entry: StreamEntry, target: String) {
        val bytes = runBlocking { HikariNet.getBytes(target, entry.headers) }
        if (bytes == null) {
            send404(conn)
            return
        }
        sendBytes(conn, bytes, "application/octet-stream")
    }

    private fun proxyPath(entry: StreamEntry, abs: String, route: String): String {
        val enc = URLEncoder.encode(abs, "UTF-8")
        return "/${entry.id}/$route/$enc"
    }

    private fun resolveUri(ref: String, base: URI): String? {
        return try { base.resolve(ref).toString() } catch (t: Throwable) { null }
    }

    private fun decodeUrl(seg: String): String? {
        return try { URLDecoder.decode(seg, "UTF-8") } catch (t: Throwable) { null }
    }

    private fun parseRange(range: String?, total: Long): Pair<Long, Long>? {
        if (range == null || total < 0) return null
        val m = Regex("bytes=(\\d*)-(\\d*)").find(range.trim()) ?: return null
        val start = m.groupValues[1].toLongOrNull()
        val end = m.groupValues[2].toLongOrNull()
        return when {
            start != null && end != null && end >= start -> start to minOf(end, total - 1)
            start != null -> start to (total - 1)
            end != null -> maxOf(0L, total - end) to (total - 1)
            else -> null
        }
    }

    private fun sendBytesWithRange(conn: Socket, bytes: ByteArray, contentType: String, range: String?, upstreamRange: String?) {
        try {
            val out: OutputStream = conn.getOutputStream()
            val span = parseRange(range, bytes.size.toLong())
            if (span != null) {
                val (s, e) = span
                if (s >= bytes.size) {
                    out.write("HTTP/1.1 416 Range Not Satisfiable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".toByteArray(Charsets.ISO_8859_1))
                    out.flush()
                    return
                }
                out.write(
                    ("HTTP/1.1 206 Partial Content\r\nContent-Type: $contentType\r\nContent-Length: ${e - s + 1}\r\nContent-Range: bytes $s-$e/${bytes.size}\r\nAccept-Ranges: bytes\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n").toByteArray(Charsets.ISO_8859_1)
                )
                out.write(bytes, s.toInt(), (e - s + 1).toInt())
            } else {
                sendBytes(conn, bytes, contentType)
                return
            }
            out.flush()
        } catch (t: Throwable) { }
    }

    private fun sendBytes(conn: Socket, bytes: ByteArray, contentType: String) {
        try {
            val out: OutputStream = conn.getOutputStream()
            val head = "HTTP/1.1 200 OK\r\nContent-Type: $contentType\r\nContent-Length: ${bytes.size}\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n"
            out.write(head.toByteArray(Charsets.ISO_8859_1))
            out.write(bytes)
            out.flush()
        } catch (t: Throwable) { }
    }

    private fun send404(conn: Socket) {
        try {
            val out: OutputStream = conn.getOutputStream()
            out.write("HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".toByteArray(Charsets.ISO_8859_1))
            out.flush()
        } catch (t: Throwable) { }
    }
}
