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

/**
 * HanimeTV (hanime.tv) — curated 720p/1080p hentai, Astro SSR front-end.
 *
 *  - the homepage server-renders four video rows (Recent Uploads, New
 *    Releases, Trending, Random) with full card grids — those are the
 *    catalogs,
 *  - `/browse/` and search are client-side (the guest API behind them is
 *    Cloudflare-blocked), so those aren't scrapable server-side — search is
 *    intentionally not wired up,
 *  - the video page's player (HTVPlayer) is client-side too, so streams are
 *    captured by loading the page in a real WebView and catching the HLS /
 *    DASH / MP4 request the player makes.
 */
class HanimetvProvider : HikariProvider {

    override val id = "hanimetv"
    override val name = "HanimeTV"
    override val mainUrl = "https://hanime.tv"
    override val version = 8
    override val description = "Curated 720p/1080p hentai — new releases, trending and random."
    override val tvTypes = setOf(HikariMediaType.MOVIE)

    private data class RowSpec(val id: String, val label: String)
    private data class Card(val media: HikariMedia, val start: Int)

    companion object {
        private const val BASE = "https://hanime.tv"
        private const val CACHE_TTL_MS = 600_000L
        private val htmlCache = HashMap<String, Pair<Long, String>>()
        private val cacheMutex = Mutex()

        private val ROW_SPECS = listOf(
            RowSpec("recent-uploads", "Recent Uploads"),
            RowSpec("new-releases", "New Releases"),
            RowSpec("trending", "Trending"),
            RowSpec("random", "Random"),
        )
    }

    override fun catalogs(): List<HikariCatalog> = ROW_SPECS.map {
        HikariCatalog(it.id, it.label, HikariMediaType.MOVIE)
    }

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> {
        if (page > 1) return emptyList()
        val spec = ROW_SPECS.find { it.id == catalog.id } ?: return emptyList()
        val cards = cards() ?: return emptyList()
        val titles = titles()
        return cards.filter { c ->
            val title = titles.lastOrNull { it.first <= c.start }?.second ?: ""
            title == spec.label
        }.map { it.media }
    }

    override suspend fun search(query: String, page: Int): List<HikariMedia> = emptyList()

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val page = getCached("$BASE/videos/hentai/${media.id}") ?: return media
        val backdrop = metaProperty(page, "og:image")?.takeIf { it.startsWith("https://hanime-cdn.com/") }
        val duration = Regex("\"duration\":\"PT(\\d+)M(\\d+)S\"").find(page)
        val runtime = duration?.let { m ->
            val mins = m.groupValues[1].toIntOrNull() ?: 0
            val secs = m.groupValues[2].toIntOrNull() ?: 0
            "Runtime: ${"%d:%02d".format(mins, secs)}"
        }
        val overview = listOfNotNull(runtime).joinToString(" · ")
        return media.copy(
            backdropUrl = backdrop ?: media.backdropUrl,
            overview = overview.ifBlank { media.overview },
        )
    }

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> {
        val watchUrl = "$BASE/videos/hentai/${media.id}"
        // The player only serves its stream after a signed/encrypted handshake
        // (`POST auth.hanime.tv/api/v11/handshake`, AES-GCM envelope + a WASM
        // request signature) and a preroll ad — so waiting for the video
        // request to happen naturally is fragile. Instead this script runs
        // inside the page (where the WASM signature + session cookies exist),
        // performs the handshake itself, decrypts the `x-token` response, and
        // exfiltrates each source URL through a dummy image request that the
        // app's WebView capture picks up. No playback/ad dependency at all.
        val slug = media.id
        val siteScript = buildString {
            append("""(function(){
  if (window.__htvExfil) return; window.__htvExfil = 1;
  var SLUG = """)
            append("\"" + slug.replace("\\", "\\\\").replace("\"", "\\\"") + "\"")
            append(""";
  var SECRET = "htv-insecure-handshake-v1";
  var EXTRA = "htv-insecure-v1";
  var te = new TextEncoder(), td = new TextDecoder();
  function b64u(b){ var s=""; for(var i=0;i<b.length;i+=0x8000)s+=String.fromCharCode.apply(null,b.subarray(i,i+0x8000)); return btoa(s).replace(/\+/g,"-").replace(/\//g,"_").replace(/=+$/,""); }
  function b64d(s){ var t=String(s||"").replace(/-/g,"+").replace(/_/g,"/"); t=t.padEnd(Math.ceil(t.length/4)*4,"="); var r=atob(t),u=new Uint8Array(r.length); for(var i=0;i<r.length;i++)u[i]=r.charCodeAt(i); return u; }
  function key(usage){ return crypto.subtle.digest("SHA-256",te.encode(SECRET)).then(function(d){ return crypto.subtle.importKey("raw",d,{name:"AES-GCM"},false,usage); }); }
  function encrypt(obj){ var iv=crypto.getRandomValues(new Uint8Array(12)); return key(["encrypt"]).then(function(k){ return crypto.subtle.encrypt({name:"AES-GCM",iv:iv,additionalData:te.encode(EXTRA),tagLength:128},k,te.encode(JSON.stringify(obj))); }).then(function(ct){ ct=new Uint8Array(ct); var env={v:1,alg:"AES-256-GCM",iv:b64u(iv),tag:b64u(ct.slice(-16)),data:b64u(ct.slice(0,-16))}; return b64u(te.encode(JSON.stringify(env))); }); }
  function decrypt(tok){ var j=JSON.parse(td.decode(b64d(tok))); return key(["decrypt"]).then(function(k){ var a=[].slice.call(b64d(j.data)),b=[].slice.call(b64d(j.tag)),full=new Uint8Array(a.concat(b)); return crypto.subtle.decrypt({name:"AES-GCM",iv:b64d(j.iv),additionalData:te.encode(EXTRA),tagLength:128},k,full); }).then(function(pt){ return td.decode(pt); }); }
  function emit(){ try{ if(typeof window.Emit==="function"){ window.Emit("e",{}); return; } window.dispatchEvent(new CustomEvent("e",{detail:{}})); }catch(e){} }
  function getCsrf(){ try{ if(window.S && window.S.csrf_token) return Promise.resolve(window.S.csrf_token); }catch(e){} return fetch("https://ct.hanime.tv/csrf-token",{credentials:"include"}).then(function(r){ return r.json(); }).then(function(j){ return j.csrf_token||""; }).catch(function(){ return ""; }); }
  function exfil(url){ try{ if(url){ new Image().src="https://m.capture/x?u="+encodeURIComponent(url); } }catch(e){} }
  function waitFor(fn,ms){ return new Promise(function(res){ var t0=Date.now(); (function poll(){ if(fn()){ res(true); return; } if(Date.now()-t0>ms){ res(false); return; } setTimeout(poll,500); })(); }); }
  function fire(){
    emit();
    Promise.all([
      waitFor(function(){ return !!(window.ssignature && window.stime); }, 25000),
      waitFor(function(){ return !!(window.crypto && window.crypto.subtle); }, 25000)
    ]).then(function(){
      getCsrf().then(function(csrf){
        encrypt({timestamp_unix:parseInt(Date.now()/1000,10),directive:"htv_player_handshake",slug:SLUG}).then(function(token){
          return fetch("https://auth.hanime.tv/api/v11/handshake",{method:"POST",credentials:"include",headers:{"Content-Type":"application/json","Accept":"application/json","x-signature-version":"web2","x-signature":window.ssignature||"","x-time":String(window.stime||""),"x-csrf-token":csrf},body:JSON.stringify({token:token})});
        }).then(function(r){
          var xt=r.headers.get("x-token");
          if(!xt) return null;
          return decrypt(xt);
        }).then(function(plain){
          if(!plain) return;
          try{ var d=JSON.parse(plain); (d.sources||[]).forEach(function(s){ if(s){ var u=(s&&(s.src||s.url))||""; if(u){ u=new URL(u,location.href).href; exfil(u); } } }); }catch(e){}
        }).catch(function(){});
      });
    });
  }
  if(document.readyState==="complete") fire(); else window.addEventListener("load", fire);
  setTimeout(fire, 4000);
})();""")
        }
        val captured = HikariNet.resolveWithWebView(
            watchUrl,
            capture = Regex("""https://m\.capture/x\?u=([^&"'\s]+)"""),
            additional = listOf(
                Regex("""https://[^\s"'\\<>]+\.m3u8(\?[^\s"'\\<>]*)?"""),
                Regex("""https://[^\s"'\\<>]+/manifest\.mpd(\?[^\s"'\\<>]*)?"""),
                Regex("""https://[^\s"'\\<>]+\.mp4(\?[^\s"'\\<>]*)?"""),
            ),
            timeoutMs = 75_000,
            script = siteScript,
        )
        val exfil = captured.filter { it.url.startsWith("https://m.capture/x") }
        if (exfil.isNotEmpty()) {
            return exfil.mapNotNull { hit ->
                val enc = Regex("""\?u=([^&"'\s]+)""").find(hit.url)?.groupValues?.get(1) ?: return@mapNotNull null
                val realUrl = runCatching { java.net.URLDecoder.decode(enc, "UTF-8") }.getOrNull()
                    ?: return@mapNotNull null
                if (realUrl.isBlank()) return@mapNotNull null
                // The handshake sometimes returns relative paths (`/hls/…`);
                // resolve them against the site like the web player does.
                val resolved = if (realUrl.startsWith("/")) "$BASE$realUrl" else realUrl
                val isHls = isHlsUrl(resolved)
                val isMpd = resolved.contains(".mpd")
                HikariStream(
                    name = if (isHls) "HLS" else if (isMpd) "DASH" else "MP4",
                    url = resolved,
                    headers = hit.headers + mapOf("Referer" to "$BASE/"),
                    isM3u8 = isHls,
                    isMpd = isMpd,
                )
            }.distinctBy { it.url }
        }
        captured.firstOrNull { !it.url.startsWith("https://m.capture/") }?.let { hit ->
            val isHls = isHlsUrl(hit.url)
            val isMpd = hit.url.contains(".mpd")
            return listOf(
                HikariStream(
                    name = if (isHls) "HLS" else if (isMpd) "DASH" else "MP4",
                    url = hit.url,
                    headers = hit.headers + mapOf("Referer" to "$BASE/"),
                    isM3u8 = isHls,
                    isMpd = isMpd,
                )
            )
        }
        return emptyList()
    }

    // ------------------------------------------------------------------
    //  Home parsing
    // ------------------------------------------------------------------

    /** Row heading titles with their byte offsets (document order). */
    private suspend fun titles(): List<Pair<Int, String>> {
        val html = getCached("$BASE/") ?: return emptyList()
        val re = Regex("""(?:[^A-Za-z0-9_])title&quot;:\[0,&quot;([^&]+)&quot;\]""")
        return re.findAll(html).mapNotNull { m ->
            val title = unescape(m.groupValues[1]).trim()
            if (title.isBlank()) null else m.range.first to title
        }.toList()
    }

    private suspend fun cards(): List<Card>? {
        val html = getCached("$BASE/") ?: return null
        val re = Regex("""<a href="/videos/hentai/([^"]+)" title="[^"]*" class="relative block overflow-hidden[^"]*"[^>]*>""")
        val out = mutableListOf<Card>()
        for (m in re.findAll(html)) {
            val slug = m.groupValues[1]
            if (slug.isBlank()) continue
            val start = m.range.last
            val endIdx = html.indexOf("</a>", start)
            val block = html.substring(start, if (endIdx < 0) html.length else endIdx)
            val poster = Regex("""src="(https://hanime-cdn\.com/[^"]+)""")
                .find(block)?.groupValues?.get(1)
            val title = Regex("""<h3[^>]*>([\s\S]*?)</h3>""")
                .find(block)?.groupValues?.get(1)?.let { unescape(it) } ?: slug
            out += Card(
                HikariMedia(
                    id = slug,
                    title = title,
                    type = HikariMediaType.MOVIE,
                    posterUrl = poster,
                ),
                start,
            )
        }
        return out
    }

    private suspend fun getCached(url: String): String? {
        val now = System.currentTimeMillis()
        cacheMutex.withLock {
            htmlCache[url]?.let { (t, html) -> if (now - t < CACHE_TTL_MS) return html }
        }
        val html = HikariNet.getStringSmart(url) ?: return null
        // Never cache a WAF challenge page — after the user completes the
        // Cloudflare verification in the app's WebView, the retry must re-fetch
        // for real (caching the challenge HTML would keep things empty for the
        // whole 10-minute TTL).
        if (looksLikeChallenge(html)) return null
        cacheMutex.withLock {
            if (htmlCache.size > 30) htmlCache.clear()
            htmlCache[url] = now to html
        }
        return html
    }

    private fun looksLikeChallenge(html: String): Boolean {
        val probe = html.take(30_000)
        return CHALLENGE_MARKERS.any { probe.contains(it, ignoreCase = true) }
    }

    private val CHALLENGE_MARKERS = listOf(
        "cf-chl-", "challenge-platform", "cf-browser-verification", "cf-mitigated",
        "cf-turnstile", "Just a moment", "Attention Required!", "enablejs",
        "Pardon Our Interruption", "Checking your browser", "Verify you are human",
        "verify you are human", "hcaptcha", "h-captcha", "Access Denied", "datadome",
    )

    private fun metaProperty(html: String, prop: String): String? =
        Regex("""<meta\s+property="[^"]*$prop[^"]*"\s+content="([^"]*)"""")
            .find(html)?.groupValues?.get(1)

    /** True when [url] points at an HLS stream: an explicit .m3u8, the site's
     *  `/hls/<id>/<token>` manifest path, or the legacy hls.hanime.tv host. */
    private fun isHlsUrl(url: String): Boolean =
        url.contains(".m3u8", ignoreCase = true) ||
            url.contains("/hls/", ignoreCase = true) ||
            url.contains("hls.hanime.tv", ignoreCase = true)

    private fun unescape(s: String): String = s
        .replace("&amp;", "&")
        .replace("&quot;", "\"")
        .replace("&#039;", "'")
        .replace("&#39;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&nbsp;", " ")
        .replace(Regex("""\s+"""), " ")
        .trim()
}
