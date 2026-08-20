@file:OptIn(com.lagradost.cloudstream3.InternalAPI::class)

package com.hikari.ext.providers

import android.content.Context
import android.content.res.AssetManager
import android.content.res.Resources
import com.hikari.app.HikariApp
import com.hikari.ext.HikariCatalog
import com.hikari.ext.HikariEpisode
import com.hikari.ext.HikariMedia
import com.hikari.ext.HikariMediaType
import com.hikari.ext.HikariProvider
import com.hikari.ext.HikariStream
import com.hikari.ext.HikariSubtitle
import com.lagradost.cloudstream3.AnimeLoadResponse
import com.lagradost.cloudstream3.AnimeSearchResponse
import com.lagradost.cloudstream3.APIHolder
import com.lagradost.cloudstream3.Episode
import com.lagradost.cloudstream3.LoadResponse
import com.lagradost.cloudstream3.MainAPI
import com.lagradost.cloudstream3.MainPageRequest
import com.lagradost.cloudstream3.MovieLoadResponse
import com.lagradost.cloudstream3.MovieSearchResponse
import com.lagradost.cloudstream3.SearchResponse
import com.lagradost.cloudstream3.SubtitleFile
import com.lagradost.cloudstream3.TvSeriesLoadResponse
import com.lagradost.cloudstream3.TvSeriesSearchResponse
import com.lagradost.cloudstream3.TvType
import com.lagradost.cloudstream3.plugins.BasePlugin
import com.lagradost.cloudstream3.plugins.Plugin
import com.lagradost.cloudstream3.utils.ExtractorLink
import com.lagradost.cloudstream3.utils.Qualities
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import java.io.File
import java.io.InputStreamReader
import java.util.concurrent.ConcurrentHashMap

/**
 * Loads a CloudStream .cs3 plugin bundled inside this .hiki and adapts it to
 * [HikariProvider], so any .cs3 extension works fully inside Hikari without a
 * native port. The .cs3 is extracted from this archive and loaded through the
 * app's real CloudStream runtime exactly like the app's own Cs3PluginManager
 * does — the plugin's extractors, WebView captures and signed-URL logic all
 * keep working because they run against the app's bundled cloudstream3.jar.
 *
 * Each bundled .cs3 gets one subclass (one HikariProvider), and the manifest
 * registers them all, so a single .hiki install turns every bundled plugin
 * into its own provider on the Home screen.
 */
abstract class Cs3BridgeProvider(
    private val cs3Resource: String,
    private val apiIndex: Int,
    override val id: String,
    override val name: String,
) : HikariProvider {

    override val description: String get() = "CloudStream plugin via Hikari's .cs3 bridge."

    override val version: Int get() = 1

    override val iconUrl: String? get() = null

    override val tvTypes: Set<HikariMediaType>
        get() = setOf(HikariMediaType.MOVIE, HikariMediaType.SERIES)

    override val mainUrl: String
        get() {
            // The Home screen reads mainUrl on the UI thread. Never load a
            // plugin there (dex + load() can block for seconds → ANR) — and
            // never cache the null result, or the provider dies permanently.
            if (android.os.Looper.myLooper() == android.os.Looper.getMainLooper()) return ""
            return api()?.mainUrl ?: ""
        }

    // The plugin's load() can take a long time on cold start — CNC Verse live
    // providers (e.g. PlayZTV) do a full network fetch inside load() with 30s
    // OkHttp timeouts, which regularly outlives any single join cap. Unlike
    // `by lazy`, a timed-out/failed load is NOT cached forever: we remember it
    // with a short retry cooldown so one slow boot doesn't kill the provider
    // for the rest of the session (it reloads on the next access / retry tap).
    private val apiLock = Any()
    @Volatile private var api: MainAPI? = null
    @Volatile private var apiFailedAt = 0L

    private fun api(): MainAPI? {
        if (android.os.Looper.myLooper() == android.os.Looper.getMainLooper()) return api
        api?.let { return it }
        synchronized(apiLock) {
            api?.let { return it }
            if (apiFailedAt != 0L && android.os.SystemClock.elapsedRealtime() - apiFailedAt < 30_000) {
                // Don't hot-reload a just-failed plugin, but DO pick up a slow
                // load that finished in the background since we gave up on it.
                return cachedApis()?.also { api = it }
            }
            val loaded = loadApi()
            if (loaded != null) {
                api = loaded
                apiFailedAt = 0L
            } else {
                apiFailedAt = android.os.SystemClock.elapsedRealtime()
            }
            return loaded
        }
    }

    /** Non-blocking peek at the shared plugin cache (null = still nothing). */
    private fun cachedApis(): MainAPI? {
        val ctx = bridgeContext() ?: return null
        val file = extract(ctx) ?: return null
        return loadedPlugins[file.absolutePath]?.getOrNull(apiIndex)
    }

    private val loadCache = ConcurrentHashMap<String, LoadResponse>()

    // ------------------------------------------------------------------ catalogs

    override fun catalogs(): List<HikariCatalog> {
        val a = api() ?: return emptyList()
        return a.mainPage?.map { page ->
            val raw = page.data
            // Newer SDKs give new-style plugins a single blank MainPageData
            // catalog (the real rows only come from getMainPage), so fall back
            // to a friendly row title instead of an empty one.
            val title = page.name.ifBlank { raw }.ifBlank { "Home" }
            HikariCatalog(raw, title, catalogType(), raw)
        } ?: emptyList()
    }

    private fun catalogType(): HikariMediaType {
        val types = api()?.supportedTypes ?: return HikariMediaType.SERIES
        val movieOnly = types.isNotEmpty() && types.all {
            it == TvType.Movie || it == TvType.AnimeMovie || it == TvType.NSFW
        }
        return if (movieOnly) HikariMediaType.MOVIE else HikariMediaType.SERIES
    }

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> =
        withContext(Dispatchers.IO) {
            val a = api() ?: return@withContext emptyList()
            val resp = try {
                a.getMainPage(page, MainPageRequest(catalog.name, catalog.id, false))
            } catch (t: Throwable) {
                if (t is CancellationException) throw t
                null
            } ?: return@withContext emptyList()
            resp.items.orEmpty()
                .flatMap { row -> row.list.orEmpty().mapNotNull { it.toMedia() } }
        }

    override suspend fun search(query: String, page: Int): List<HikariMedia> =
        withContext(Dispatchers.IO) {
            val a = api() ?: return@withContext emptyList()
            try {
                a.search(query).orEmpty().mapNotNull { it.toMedia() }
            } catch (t: Throwable) {
                if (t is CancellationException) throw t
                emptyList()
            }
        }

    // --------------------------------------------------------------------- meta

    override suspend fun getMeta(media: HikariMedia): HikariMedia {
        val resp = loadResponse(media.id) ?: return media
        // Providers rewrite the URL during load(); the response's final url is
        // what loadLinks must be called with (same as CloudStream does).
        val canonicalUrl = resp.url.takeIf { it.isNotBlank() } ?: media.id
        if (canonicalUrl != media.id) loadCache[canonicalUrl] = resp
        return when (resp) {
            is MovieLoadResponse -> media.copy(
                id = canonicalUrl,
                type = HikariMediaType.MOVIE,
                overview = resp.plot ?: media.overview,
                genres = resp.tags ?: media.genres,
                year = resp.year ?: media.year,
                posterUrl = resp.posterUrl ?: media.posterUrl,
                backdropUrl = resp.backgroundPosterUrl ?: media.backdropUrl,
            )
            is AnimeLoadResponse -> media.copy(
                id = canonicalUrl,
                type = HikariMediaType.SERIES,
                title = resp.engName?.takeIf { it.isNotBlank() } ?: media.title,
                overview = resp.plot ?: media.overview,
                genres = resp.tags ?: media.genres,
                year = resp.year ?: media.year,
                posterUrl = resp.posterUrl ?: media.posterUrl,
                backdropUrl = resp.backgroundPosterUrl ?: media.backdropUrl,
            )
            is TvSeriesLoadResponse -> media.copy(
                id = canonicalUrl,
                type = HikariMediaType.SERIES,
                overview = resp.plot ?: media.overview,
                genres = resp.tags ?: media.genres,
                year = resp.year ?: media.year,
                posterUrl = resp.posterUrl ?: media.posterUrl,
                backdropUrl = resp.backgroundPosterUrl ?: media.backdropUrl,
            )
            else -> media
        }
    }

    override suspend fun getEpisodes(media: HikariMedia): List<HikariEpisode>? =
        withContext(Dispatchers.IO) {
            val resp = loadResponse(media.id) ?: return@withContext null
            when (resp) {
                is AnimeLoadResponse -> {
                    val eps = resp.episodes.values.flatten()
                    if (eps.isEmpty()) null
                    else eps
                        .sortedBy { it.episode ?: Int.MAX_VALUE }
                        .distinctBy { it.data ?: it.episode ?: 0 }
                        .map { it.toEp() }
                }
                is TvSeriesLoadResponse -> {
                    if (resp.episodes.isEmpty()) null
                    else resp.episodes
                        .distinctBy { it.data ?: it.episode ?: 0 }
                        .map { it.toEp() }
                }
                else -> null
            }
        }

    // ------------------------------------------------------------------- streams

    override suspend fun getStreams(media: HikariMedia, episode: HikariEpisode?): List<HikariStream> =
        withContext(Dispatchers.IO) {
            val a = api() ?: return@withContext emptyList()
            // For SERIES, the plugin's per-episode data string lives on the
            // episode and is already in episode.id. For MOVIES the provider
            // serialized its source list into MovieLoadResponse.dataUrl during
            // load() (MoviesMod/VegaMovies: `[{"source":"…"}]`, re-parsed by
            // loadLinks via parseJson). Handing loadLinks the plain page URL
            // makes those providers throw Jackson's "Unrecognized token
            // 'https'" — pass the response's data string like CloudStream does.
            val movieData = if (episode == null) {
                (loadResponse(media.id) as? MovieLoadResponse)
                    ?.dataUrl
                    ?.takeIf { it.isNotBlank() }
            } else null
            val data = if (episode != null) episode.id else movieData ?: media.id
            val subs = mutableListOf<SubtitleFile>()
            val links = mutableListOf<ExtractorLink>()

            // Respect the plugin's own loadLinks budget (CloudStream default
            // 30s) — providers that sign requests / walk several API pages
            // routinely need more than a short fixed cap.
            val rawTimeout = a.loadLinksTimeoutMs
            val budget = if (rawTimeout != null && rawTimeout in 1..120_000L) rawTimeout else 30_000L

            val started = System.currentTimeMillis()
            var completed: Boolean? = null

            // loadLinks is a suspend function running plugin code; run it on
            // this IO thread inside a bounded coroutine — withTimeoutOrNull
            // cancels a hung provider instead of leaking a thread. (The app's
            // native CS3 path uses a detached scope to return early; here the
            // provider is the ONLY source engine, so we just wait it out.)
            fun runOnce(budgetMs: Long) {
                links.clear()
                subs.clear()
                kotlinx.coroutines.runBlocking {
                    completed = try {
                        withTimeoutOrNull(budgetMs) {
                            a.loadLinks(data, false, { subs.add(it) }, { links.add(it) })
                        }
                    } catch (t: Throwable) {
                        false
                    }
                }
            }

            runOnce(budget)
            val elapsed = System.currentTimeMillis() - started
            // A "success" that took suspiciously little time and yielded zero
            // links is usually the provider's first network lookup failing on
            // a cold start — give it ONE bounded retry.
            if ((completed != true || links.isEmpty()) && elapsed < 15_000) {
                runOnce(minOf(budget, 20_000L))
            }

            toStreams(links.toList(), subs.toList())
        }

    private fun toStreams(rawLinks: List<ExtractorLink>, rawSubs: List<SubtitleFile>): List<HikariStream> {
        val a = api()
        return rawLinks
            .filter { it.url.isNotBlank() && it.url != a?.mainUrl && it.type.name != "ERROR" }
            .map { l ->
                // CloudStream keeps the Referer OUT of ExtractorLink.headers —
                // without it most CDNs answer with an anti-hotlink HTML page
                // and the player reports PARSING_CONTAINER_UNSUPPORTED. Merge
                // the referer in, sanitizing header values to ASCII (OkHttp
                // rejects non-ASCII header values).
                val headers = LinkedHashMap<String, String>()
                l.headers?.forEach { (k, v) ->
                    val c = v.filter { it.code < 128 }
                    if (c.isNotBlank()) headers[k] = c
                }
                val ref = l.referer
                if (!ref.isNullOrBlank()) {
                    val c = ref.filter { it.code < 128 }
                    if (c.isNotBlank()) headers.putIfAbsent("Referer", c)
                }
                val isTorrent = l.type.name == "MAGNET" || l.type.name == "TORRENT" ||
                    l.url.startsWith("magnet:", true) || l.url.startsWith("torrent:", true)
                val qualityLabel = Qualities.getStringByInt(l.quality)
                val baseName = l.name.ifBlank { "Stream" }
                HikariStream(
                    name = if (qualityLabel.isNotBlank() && !baseName.contains(qualityLabel, ignoreCase = true)) {
                        "$baseName $qualityLabel"
                    } else {
                        baseName
                    },
                    url = l.url,
                    headers = headers,
                    subtitles = rawSubs.map { HikariSubtitle(it.lang.ifBlank { "Sub" }, it.url) },
                    isM3u8 = l.isM3u8,
                    isMpd = l.isDash,
                    isTorrent = isTorrent,
                    infoHash = infoHashOf(l.url),
                    fileIdx = magnetIndex(l.url),
                    trackers = magnetTrackers(l.url),
                )
            }
            .distinctBy { it.url }
    }

    // ------------------------------------------------------------------ loading

    private fun loadApi(): MainAPI? {
        // The `api()` accessor is normally only touched from Dispatchers.IO contexts
        // (catalogs/search/meta/streams). Guard against any accidental main
        // thread access — returning null is safe because the caller is always
        // a withContext(IO) wrapper that can retry the next call, and the
        // plugin gets loaded by the loadedPlugins cache on that IO access.
        if (android.os.Looper.myLooper() == android.os.Looper.getMainLooper()) return null
        val ctx = bridgeContext() ?: return null
        val file = extract(ctx) ?: return null
        // Plugins run real (sometimes network) code in load() — never let that
        // happen on the caller's thread. Load on a daemon thread with a cap;
        // a hung load yields an empty provider instead of an ANR.
        var result: List<MainAPI>? = null
        val worker = Thread {
            try {
                result = loadedCs3(ctx, file)
            } catch (t: Throwable) {
                result = emptyList()
            }
        }.apply { isDaemon = true }
        worker.start()
        try {
            worker.join(120_000)
        } catch (e: InterruptedException) {
            // ignore
        }
        return result?.getOrNull(apiIndex)
    }

    companion object {
        // One plugin archive (several bundled providers may share a .cs3) is
        // only ever loaded once per install; every wrapper indexes into the
        // same loaded API list. Keyed by the extracted file's absolute path.
        private val loadedPlugins = java.util.concurrent.ConcurrentHashMap<String, List<MainAPI>>()

        // Only SUCCESSFUL loads are cached — an empty result (load() threw or
        // was still running when the join cap hit) must never be remembered,
        // or every retry would replay the same empty catalog until the app is
        // force-stopped. Serialized so concurrent retries can't double-load.
        private val loadLock = java.util.concurrent.locks.ReentrantLock()

        private fun loadedCs3(ctx: Context, file: File): List<MainAPI> {
            val path = file.absolutePath
            loadedPlugins[path]?.let { return it }
            loadLock.lock()
            try {
                loadedPlugins[path]?.let { return it }
                val apis = loadCs3(ctx, file)
                if (apis.isNotEmpty()) loadedPlugins[path] = apis
                return apis
            } finally {
                loadLock.unlock()
            }
        }

        /**
         * Loads a .cs3 archive the same way the app's Cs3PluginManager does:
         * read-only file (Android 14+ refuses writable dex), PathClassLoader on
         * the archive, manifest.json -> plugin class, instantiate + load(), then
         * collect the MainAPIs the plugin registered in the shared APIHolder.
         */
        private fun loadCs3(ctx: Context, file: File): List<MainAPI> {
            val path = file.absolutePath
            return try {
                try {
                    file.setReadOnly()
                } catch (t: Throwable) {
                    // not fatal
                }
                val classLoader = dalvik.system.PathClassLoader(path, ctx.classLoader)
                val manifestText = classLoader.getResourceAsStream("manifest.json")?.use {
                    InputStreamReader(it).readText()
                } ?: return emptyList()
                val root = org.json.JSONObject(manifestText)
                val pluginClassName = root.optString("pluginClassName").takeIf { it.isNotBlank() }
                    ?: root.optString("pluginClass").takeIf { it.isNotBlank() }
                    ?: return emptyList()
                val requiresResources = root.optBoolean("requiresResources", false)
                @Suppress("UNCHECKED_CAST")
                val instance = (classLoader.loadClass(pluginClassName) as Class<out BasePlugin>)
                    .getDeclaredConstructor().newInstance()
                try {
                    APIHolder.allProviders.removeAll { it.sourcePlugin == path }
                } catch (t: Throwable) {
                    // not fatal
                }
                instance.filename = path
                if (requiresResources) {
                    try {
                        val assets = AssetManager::class.java.getDeclaredConstructor().newInstance()
                        val addPath = AssetManager::class.java.getMethod("addAssetPath", String::class.java)
                        addPath.invoke(assets, path)
                        @Suppress("DEPRECATION")
                        (instance as? Plugin)?.resources = Resources(
                            assets as AssetManager,
                            ctx.resources.displayMetrics,
                            ctx.resources.configuration
                        )
                    } catch (t: Throwable) {
                        // not fatal
                    }
                }
                if (instance is Plugin) {
                    instance.load(HikariApp.mainActivity ?: ctx)
                } else {
                    instance.load()
                }
                val apis = try {
                    APIHolder.allProviders.filter { it.sourcePlugin == path }
                } catch (t: Throwable) {
                    emptyList()
                }
                // Some plugins read the app off their providers (e.g. `MainAPI.app`)
                // after load. The real CloudStream host sets it to the activity —
                // mirror that, locating the field wherever the jar puts it (instance
                // member, companion, or a provider subclass override).
                HikariApp.mainActivity?.let { activity ->
                    apis.forEach { api ->
                        runCatching {
                            var done = false
                            var c: Class<*>? = api.javaClass
                            while (c != null && !done) {
                                runCatching { c.getField("app").set(api, activity); done = true }
                                if (!done) runCatching {
                                    c.getDeclaredField("app").apply { isAccessible = true }
                                        .set(api, activity); done = true
                                }
                                c = c.superclass
                            }
                            if (!done) {
                                runCatching {
                                    val holder = api.javaClass.getField("Companion").get(null)
                                    holder.javaClass.getField("app").set(holder, activity)
                                }
                            }
                        }
                    }
                }
                apis
            } catch (t: Throwable) {
                emptyList()
            }
        }
    }

    /** The app's MainActivity when present (plugins cast it as AppCompatActivity),
     *  else the Application context. Falls back to ActivityThread reflection. */
    private fun bridgeContext(): Context? = try {
        HikariApp.mainActivity ?: HikariApp.instance
    } catch (t: Throwable) {
        try {
            Class.forName("android.app.ActivityThread")
                .getMethod("currentApplication").invoke(null) as? Context
        } catch (t2: Throwable) {
            null
        }
    }

    private fun extract(ctx: Context): File? = try {
        val dir = File(ctx.cacheDir, "cs3bridge").apply { mkdirs() }
        val target = File(dir, cs3Resource)
        if (!target.exists() || target.length() == 0L) {
            target.parentFile?.mkdirs()
            val input = javaClass.classLoader?.getResourceAsStream(cs3Resource) ?: return null
            input.use { ins ->
                target.outputStream().use { out -> ins.copyTo(out) }
            }
        }
        target
    } catch (t: Throwable) {
        null
    }

    // ------------------------------------------------------------------- helpers

    private suspend fun loadResponse(id: String): LoadResponse? {
        loadCache[id]?.let { return it }
        val a = api() ?: return null
        val r = try {
            withTimeoutOrNull(45_000) {
                val first = tryLoad(a, id)
                // A blank first response is usually the provider's very first
                // network lookup failing on a cold start — retry once before
                // caching a dead response forever.
                val hollow = first == null || first.url.isBlank()
                if (!hollow) first else tryLoad(a, id)
            }
        } catch (t: Throwable) {
            if (t is CancellationException) throw t
            null
        } ?: return null
        loadCache[id] = r
        return r
    }

    private suspend fun tryLoad(a: MainAPI, id: String): LoadResponse? = try {
        a.load(id)
    } catch (t: Throwable) {
        null
    }

    private fun SearchResponse.toMedia(): HikariMedia? {
        if (url.isBlank() || name.isBlank()) return null
        val mt = when (type) {
            TvType.Movie, TvType.AnimeMovie, TvType.NSFW -> HikariMediaType.MOVIE
            TvType.TvSeries, TvType.Anime, TvType.Cartoon, TvType.OVA, TvType.AsianDrama -> HikariMediaType.SERIES
            else -> HikariMediaType.UNKNOWN
        }
        val year = when (this) {
            is MovieSearchResponse -> this.year
            is AnimeSearchResponse -> this.year
            is TvSeriesSearchResponse -> this.year
            else -> null
        }
        return HikariMedia(id = url, title = name, type = mt, posterUrl = posterUrl, year = year)
    }

    private fun Episode.toEp(): HikariEpisode {
        val num = episode ?: data?.substringAfterLast("|")?.toIntOrNull() ?: 1
        return HikariEpisode(
            number = num,
            id = data ?: num.toString(),
            name = name ?: "Episode $num",
            image = posterUrl,
        )
    }

    private fun infoHashOf(url: String): String? {
        Regex("[?&]xt=urn:btih:([a-zA-Z0-9]{32,40})").find(url)?.let { return it.groupValues[1] }
        Regex("urn:btih:([a-zA-Z0-9]{32,40})").find(url)?.let { return it.groupValues[1] }
        return null
    }

    private fun magnetIndex(url: String): Int? =
        Regex("[?&]index=(\\d+)").find(url)?.groupValues?.get(1)?.toIntOrNull()

    private fun magnetTrackers(url: String): List<String> =
        Regex("[?&]tr=([^&]+)").findAll(url)
            .mapNotNull { m ->
                runCatching { java.net.URLDecoder.decode(m.groupValues[1], "UTF-8") }.getOrNull()
            }
            .toList()
}
