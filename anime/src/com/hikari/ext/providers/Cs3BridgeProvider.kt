@file:OptIn(com.lagradost.cloudstream3.InternalAPI::class)

package com.hikari.ext.providers

import android.content.Context
import android.content.res.AssetManager
import android.content.res.Resources
import android.os.SystemClock
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
import com.lagradost.cloudstream3.HomePageList
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
import com.lagradost.cloudstream3.utils.extractorApis
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import java.io.File
import java.io.InputStreamReader
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * Loads a CloudStream `.cs3` plugin bundled inside this `.hiki` and adapts it to
 * [HikariProvider], so any `.cs3` extension works fully inside Hikari without a
 * native port. The `.cs3` is extracted from this archive and loaded through the
 * app's real CloudStream runtime exactly like the app's own Cs3PluginManager
 * does — the plugin's extractors, WebView captures and signed-URL logic all
 * keep working because they run against the app's bundled cloudstream3.jar.
 *
 * Each bundled `.cs3` gets one subclass (one HikariProvider), and the manifest
 * registers them all, so a single `.hiki` install turns every bundled plugin
 * into its own provider on the Home screen.
 *
 * ## Nothing here fails silently
 *
 * A bridge provider that could not be loaded used to answer `emptyList()` from
 * `catalogs()`, which Hikari's Home renders as "Couldn't load <name>" plus its
 * generic "check the site in the WebView — it may be behind a verification
 * page" line. That text was a lie: the real reasons are all local and all
 * knowable (the extension archive has no bundled plugin, the plugin's `load()`
 * threw, the plugin registered nothing, the home page came back empty), and
 * they were being thrown away one line before they could reach the UI. Every
 * failure path below now raises an [IllegalStateException] naming the actual
 * cause; the app's HikariProviderAdapter puts that message in its
 * `catalogErrors` map and Home shows it instead of the boilerplate. The reason
 * is also mirrored to `lastFailure` (on the instance) and to logcat under the
 * `HikariBridge` tag.
 *
 * ## The catalog protocol
 *
 * `catalogs()` does what the app's NATIVE `.cs3` host
 * (`com.hikari.app.cs3.Cs3MainApiProvider`) does, not the naive thing: it asks
 * the plugin for its first home page and exposes ONE catalog per
 * [HomePageList] row the plugin actually answers with, carrying a synthetic
 * `row:<pageIndex>:<rowIndex>` id. A plugin whose home page is a single flat
 * page (or which ignores the request) falls back to one catalog per
 * `mainPage` entry, identified by that entry's own `data` — never by an empty
 * string, because an empty id is not a catalogue identity. `getCatalog()`
 * decodes those ids and returns only the row that catalog stands for.
 *
 * The previous one-catalog-per-mainPage shape passed the wrong thing twice:
 * the request name was the placeholder title ("Home") rather than the plugin's
 * own page name, and every row of the home page was flattened into one list.
 * Plugins that route `getMainPage` on the request (most modern ones do) then
 * answered nothing at all, and the ones that ignored it showed "only one
 * category" where CloudStream shows many.
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

    /** Why the last call to this provider failed (null when it worked). Handy
     *  from a debugger/logcat capture; the text the user sees is the
     *  IllegalStateException's message, which carries the same string. */
    @Volatile
    var lastFailure: String? = null
        private set

    override val mainUrl: String
        get() {
            // The Home screen reads mainUrl on the UI thread. Never load a
            // plugin there (dex + load() can block for seconds -> ANR) — and
            // never cache the null result, or the provider dies permanently.
            if (android.os.Looper.myLooper() == android.os.Looper.getMainLooper()) return ""
            return apiOrNull()?.mainUrl ?: ""
        }

    // --------------------------------------------------------------- loading

    private val apiLock = Any()
    @Volatile private var api: MainAPI? = null

    /**
     * The plugin instance, loading it if needed. Never throws: it returns null
     * and records the reason on [lastFailure] / the shared failure map, so
     * callers that only want a value (mainUrl, search, streams) keep working
     * while the catalog paths raise the reason properly.
     */
    private fun apiOrNull(): MainAPI? {
        if (android.os.Looper.myLooper() == android.os.Looper.getMainLooper()) return api
        api?.let { return it }
        synchronized(apiLock) {
            api?.let { return it }
            val ctx = bridgeContext()
            if (ctx == null) {
                noteFailure("no Android context is available yet")
                return null
            }
            val file = extract(ctx)
            if (file == null) {
                noteFailure(
                    "this extension is missing its bundled CloudStream plugin " +
                        "(\"$cs3Resource\") — reinstall the extension from its repo"
                )
                return null
            }
            val path = file.absolutePath
            loadedPlugins[path]?.let { list ->
                val chosen = list.getOrNull(apiIndex)
                if (chosen == null) {
                    noteFailure(
                        "the plugin loaded but registered no provider at index $apiIndex " +
                            "(it has ${list.size})"
                    )
                    return null
                }
                api = chosen
                lastFailure = null
                return chosen
            }
            val failedAt = loadFailureAt[path]
            if (failedAt != null && SystemClock.elapsedRealtime() - failedAt < FAIL_RETRY_MS) {
                noteFailure(loadFailures[path] ?: "the plugin failed to load a moment ago")
                return null
            }
            val list = loadOnWorker(ctx, file, path)
            val chosen = list?.getOrNull(apiIndex)
            if (chosen == null) {
                val why = loadFailures[path]
                    ?: if (list == null) {
                        "its plugin did not finish loading"
                    } else {
                        "the plugin loaded but registered no provider at index $apiIndex " +
                            "(it has ${list.size})"
                    }
                loadFailures[path] = why
                loadFailureAt[path] = SystemClock.elapsedRealtime()
                noteFailure(why)
                return null
            }
            api = chosen
            lastFailure = null
            return chosen
        }
    }

    /** The plugin, or an exception naming exactly why it is not available. */
    private fun requireApi(): MainAPI {
        apiOrNull()?.let { return it }
        throw failed(lastFailure ?: "this extension's CloudStream plugin could not be loaded")
    }

    private fun noteFailure(why: String) {
        lastFailure = why
        android.util.Log.e(TAG, "$name ($id): $why")
    }

    private fun failed(why: String): IllegalStateException {
        noteFailure(why)
        return IllegalStateException("$name: $why")
    }

    // -------------------------------------------------------------- catalogs

    /** `row:<page>:<row>` for one row of a home page, `page:<page>` for a home
     *  page the plugin answers with a single flat list. */
    private fun rowRefId(pageIndex: Int, rowIndex: Int): String = "row:$pageIndex:$rowIndex"

    private fun pageRefId(pageIndex: Int): String = "page:$pageIndex"

    private fun indexAfter(id: String, prefix: String): Int? =
        id.removePrefix(prefix).substringBefore(':').takeIf { id.startsWith(prefix) }?.toIntOrNull()

    override suspend fun catalogs(): List<HikariCatalog> = withContext(Dispatchers.IO) {
        val a = requireApi()
        val pages = try {
            a.mainPage
        } catch (t: Throwable) {
            if (t is CancellationException) throw t
            throw failed("its plugin threw ${describe(t)}")
        }
        if (pages.isEmpty()) throw failed("its plugin exposes no home page")
        val type = catalogType(a)
        val out = ArrayList<HikariCatalog>()
        pages.forEachIndexed { pageIndex, page ->
            val rows = if (pageIndex == 0) {
                // One row per HomePageList the plugin actually answers with —
                // the same shape the app's native CS3 host uses, and the only
                // one that works for a plugin which routes getMainPage on the
                // request it is given.
                fetchRowsWithRetry(a, page.name, page.data, 1).orEmpty()
            } else {
                // Later mainPage entries are already distinct catalogs; asking
                // for each one's rows here would multiply the network calls a
                // Home load makes, so they keep the one-catalog-per-entry
                // shape (their own data identifies them).
                emptyList()
            }
            if (rows.isEmpty()) {
                out += HikariCatalog(
                    pageRefId(pageIndex),
                    page.name.ifBlank { if (pageIndex == 0) "Home" else "Catalog ${pageIndex + 1}" },
                    type,
                    page.data,
                )
            } else {
                rows.forEachIndexed { rowIndex, row ->
                    out += HikariCatalog(
                        rowRefId(pageIndex, rowIndex),
                        row.name.ifBlank { page.name.ifBlank { "Home" } },
                        type,
                        page.data,
                    )
                }
            }
        }
        val distinct = out.distinctBy { it.type to it.id }
        if (distinct.isEmpty()) throw failed("its plugin returned no home page")
        lastFailure = null
        distinct
    }

    private suspend fun fetchRows(a: MainAPI, name: String, data: String, page: Int): List<HomePageList> {
        val resp = a.getMainPage(page, MainPageRequest(name, data, false)) ?: return emptyList()
        return resp.items.orEmpty()
    }

    /** A brand-new plugin instance can fail its very first network call while
     *  the runtime initializes — retry exactly once, like the app's native CS3
     *  host does. */
    private suspend fun fetchRowsWithRetry(
        a: MainAPI,
        name: String,
        data: String,
        page: Int,
    ): List<HomePageList>? = try {
        fetchRows(a, name, data, page)
    } catch (t: Throwable) {
        if (t is CancellationException) throw t
        try {
            fetchRows(a, name, data, page)
        } catch (t2: Throwable) {
            if (t2 is CancellationException) throw t2
            null
        }
    }

    private fun catalogType(a: MainAPI): HikariMediaType {
        val types = try {
            a.supportedTypes
        } catch (t: Throwable) {
            emptySet()
        }
        if (types.isEmpty()) return HikariMediaType.SERIES
        val movieOnly = types.all {
            it == TvType.Movie || it == TvType.AnimeMovie || it == TvType.NSFW
        }
        return if (movieOnly) HikariMediaType.MOVIE else HikariMediaType.SERIES
    }

    override suspend fun getCatalog(catalog: HikariCatalog, page: Int): List<HikariMedia> =
        withContext(Dispatchers.IO) {
            val a = requireApi()
            val rows: List<HomePageList>
            val wanted: Int
            val pageIndex = indexAfter(catalog.id, "row:") ?: indexAfter(catalog.id, "page:")
            if (pageIndex != null) {
                val mainPage = try {
                    a.mainPage.getOrNull(pageIndex)
                } catch (t: Throwable) {
                    if (t is CancellationException) throw t
                    null
                } ?: throw failed("\"${catalog.name}\" is no longer one of its home pages — refresh Home")
                val fetched = try {
                    fetchRows(a, mainPage.name, mainPage.data, page)
                } catch (t: Throwable) {
                    if (t is CancellationException) throw t
                    throw failed("its home page threw ${describe(t)}")
                }
                rows = fetched
                wanted = indexAfter(catalog.id, "row:") ?: -1
                if (rows.isEmpty() && wanted >= 0) {
                    throw failed("\"${catalog.name}\" came back empty")
                }
            } else {
                // Back-compat: an older Hikari sent the page's own data as the
                // catalog id. The first home page is what that meant.
                val mainPage = try {
                    a.mainPage.firstOrNull()
                } catch (t: Throwable) {
                    if (t is CancellationException) throw t
                    null
                } ?: throw failed("its plugin exposes no home page")
                rows = try {
                    fetchRows(a, catalog.name, catalog.rawType.ifBlank { mainPage.data }, page)
                } catch (t: Throwable) {
                    if (t is CancellationException) throw t
                    throw failed("its home page threw ${describe(t)}")
                }
                wanted = -1
            }
            val items = if (wanted >= 0) {
                val row = rows.getOrNull(wanted) ?: rows.firstOrNull()
                row?.list.orEmpty().mapNotNull { it.toMedia() }
            } else {
                rows.flatMap { row -> row.list.orEmpty().mapNotNull { it.toMedia() } }
            }
            if (items.isEmpty()) {
                throw failed(if (wanted >= 0) "\"${catalog.name}\" is empty" else "its home page is empty")
            }
            lastFailure = null
            items
        }

    override suspend fun search(query: String, page: Int): List<HikariMedia> =
        withContext(Dispatchers.IO) {
            val a = requireApi()
            val found = try {
                searchItems(a, query, page)
            } catch (t: Throwable) {
                if (t is CancellationException) throw t
                // A brand-new plugin instance can fail its very first network
                // call while the runtime initializes — retry once.
                try {
                    searchItems(a, query, page)
                } catch (t2: Throwable) {
                    if (t2 is CancellationException) throw t2
                    throw failed("its search threw ${describe(t2)}")
                }
            }
            found.mapNotNull { it.toMedia() }
        }

    /** Modern plugins override the paginated `search(query, page)` — the plain
     *  `search(query)` overload throws NotImplementedError for them, which made
     *  Hikari search return nothing for those providers while CloudStream
     *  worked. Old-style plugins override `search(query)` and the base
     *  `search(query, page)` delegates to it, so the paginated form is correct
     *  for BOTH generations. */
    private suspend fun searchItems(a: MainAPI, query: String, page: Int): List<SearchResponse> =
        try {
            a.search(query, page)?.items.orEmpty()
        } catch (t: Throwable) {
            if (t is CancellationException) throw t
            a.search(query).orEmpty()
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
            // Deliberately NOT throwing here: the app's stream path has no
            // error map of its own, so an empty list plus the reason on
            // `lastFailure` is the safe answer (search already reports the
            // reason for this provider through its own error channel).
            val a = apiOrNull() ?: return@withContext emptyList()
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
        val a = apiOrNull()
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

    // ------------------------------------------------------- plugin runtime

    /** Loads the plugin on a shared daemon worker with a hard budget. A plugin
     *  whose `load()` does network work can take a while — but it must never
     *  hold the caller forever, and it must never pin a thread after the
     *  budget expires (the app's Home gives a provider 80s in total). */
    private fun loadOnWorker(ctx: Context, file: File, path: String): List<MainAPI>? {
        var result: List<MainAPI>? = null
        var error: String? = null
        val future = loadExecutor.submit {
            try {
                result = loadedCs3(ctx, file, path)
            } catch (t: Throwable) {
                error = "its plugin failed to load: ${describe(t)}"
            }
        }
        try {
            future.get(LOAD_BUDGET_MS, TimeUnit.MILLISECONDS)
        } catch (t: java.util.concurrent.TimeoutException) {
            future.cancel(true)
            loadFailures[path] = "its plugin's load() did not finish within ${LOAD_BUDGET_MS / 1000}s"
            return null
        } catch (t: Throwable) {
            error = "its plugin failed to load: ${describe(t)}"
        }
        error?.let { loadFailures[path] = it; return null }
        return result
    }

    private fun loadedCs3(ctx: Context, file: File, path: String): List<MainAPI> {
        loadedPlugins[path]?.let { return it }
        loadLock.lock()
        try {
            loadedPlugins[path]?.let { return it }
            val apis = loadCs3(ctx, file, path)
            if (apis.isNotEmpty()) loadedPlugins[path] = apis
            return apis
        } finally {
            loadLock.unlock()
        }
    }

    /**
     * Loads a .cs3 archive the same way the app's Cs3PluginManager does:
     * read-only file (Android 14+ refuses writable dex), PathClassLoader on the
     * archive, manifest.json -> plugin class, instantiate + load(), then collect
     * the MainAPIs the plugin registered in the shared APIHolder.
     */
    private fun loadCs3(ctx: Context, file: File, path: String): List<MainAPI> {
        try {
            file.setReadOnly()
        } catch (t: Throwable) {
            // not fatal
        }
        val classLoader = dalvik.system.PathClassLoader(path, ctx.classLoader)
        val manifestText = classLoader.getResourceAsStream("manifest.json")?.use {
            InputStreamReader(it).readText()
        } ?: return failLoad(path, "its plugin archive has no manifest.json")
        val root = try {
            org.json.JSONObject(manifestText)
        } catch (t: Throwable) {
            return failLoad(path, "its plugin manifest.json is not valid JSON: ${describe(t)}")
        }
        val pluginClassName = root.optString("pluginClassName").takeIf { it.isNotBlank() }
            ?: root.optString("pluginClass").takeIf { it.isNotBlank() }
            ?: return failLoad(path, "its plugin manifest names no plugin class")
        val requiresResources = root.optBoolean("requiresResources", false)
        val instance = try {
            @Suppress("UNCHECKED_CAST")
            (classLoader.loadClass(pluginClassName) as Class<out BasePlugin>)
                .getDeclaredConstructor().newInstance()
        } catch (t: Throwable) {
            return failLoad(path, "its plugin class $pluginClassName could not be created: ${describe(t)}")
        }

        // Drop this file's earlier registrations (reinstall) from BOTH
        // registries. The MainAPIs are the visible half; the extractors are what
        // make its streams resolve at all, and leaving stale ones behind on a
        // reinstall made a plugin resolve links through a previous build's code.
        try {
            APIHolder.allProviders.removeAll { it.sourcePlugin == path }
        } catch (t: Throwable) {
            // not fatal
        }
        try {
            extractorApis.removeAll { it.sourcePlugin == path }
        } catch (t: Throwable) {
            // not fatal
        }
        // What the runtime held before this load, so its providers can be found
        // by diff when they do not carry our path in `sourcePlugin`.
        val before = try {
            APIHolder.allProviders.toList()
        } catch (t: Throwable) {
            emptyList()
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
        // CloudStream plugins cast the context they are handed to Activity /
        // AppCompatActivity, and read CommonActivity for dialogs and WebView
        // work — so hand them a LIVE activity when one exists, waiting briefly
        // for one at startup, exactly like the app's native CS3 host does.
        val host = resolveHostActivity(ctx)
        try {
            if (instance is Plugin) {
                instance.load(host)
            } else {
                instance.load()
            }
        } catch (t: Throwable) {
            return failLoad(path, "its load() threw ${describe(t)}")
        }

        var apis = collectProviders(path, before)
        if (apis.isEmpty()) {
            // A plugin that fetches its domains/provider list in a coroutine
            // started from load() registers them just after load() returns.
            val deadline = System.currentTimeMillis() + REGISTER_WAIT_MS
            while (apis.isEmpty() && System.currentTimeMillis() < deadline) {
                try {
                    Thread.sleep(100)
                } catch (t: InterruptedException) {
                    Thread.currentThread().interrupt()
                    break
                }
                apis = collectProviders(path, before)
            }
        }
        if (apis.isEmpty()) return failLoad(path, "its load() registered no providers")

        // Some plugins read the app off their providers (e.g. `MainAPI.app`)
        // after load. The real CloudStream host sets it to the activity —
        // mirror that, locating the field wherever the jar puts it (instance
        // member, companion, or a provider subclass override).
        val activity = host as? android.app.Activity
        if (activity != null) {
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
        loadFailures.remove(path)
        return apis
    }

    private fun collectProviders(path: String, before: List<MainAPI>): List<MainAPI> {
        val bySource = try {
            APIHolder.allProviders.filter { it.sourcePlugin == path }
        } catch (t: Throwable) {
            emptyList()
        }
        if (bySource.isNotEmpty()) return bySource
        return try {
            APIHolder.allProviders.toList().filter { it !in before }
        } catch (t: Throwable) {
            emptyList()
        }
    }

    private fun failLoad(path: String, why: String): List<MainAPI> {
        loadFailures[path] = why
        android.util.Log.e(TAG, "$name ($id): $why")
        return emptyList()
    }

    // --------------------------------------------------------------- helpers

    private fun describe(t: Throwable): String {
        var root = t
        var guard = 0
        while (guard++ < 6) {
            val c = root.cause ?: break
            if (c === root) break
            root = c
        }
        val base = "${t.javaClass.simpleName}: ${t.message ?: "no message"}"
        return if (root === t) base else "$base (caused by ${root.javaClass.simpleName}: ${root.message ?: "no message"})"
    }

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

    /** The current activity when there is a live one, else [ctx]. Never called
     *  on the main thread (the caller is always a load worker). */
    private fun resolveHostActivity(ctx: Context): Context {
        fun usable(c: Context?): Boolean {
            val a = c as? android.app.Activity ?: return false
            return !a.isFinishing && !a.isDestroyed
        }
        fun current(): Context? {
            HikariApp.mainActivity?.let { if (usable(it)) return it }
            val common = runCatching { com.lagradost.cloudstream3.CommonActivity.activity }
                .getOrNull()
            if (usable(common)) return common
            if (usable(ctx)) return ctx
            return null
        }
        current()?.let { return it }
        if (android.os.Looper.myLooper() == android.os.Looper.getMainLooper()) return ctx
        val deadline = System.currentTimeMillis() + ACTIVITY_WAIT_MS
        while (System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(120)
            } catch (t: InterruptedException) {
                Thread.currentThread().interrupt()
                break
            }
            current()?.let { return it }
        }
        return ctx
    }

    /**
     * The bundled .cs3, copied out of this archive into the cache directory.
     *
     * The copy carries a digest of the payload in its NAME, so an extension
     * update can never be served a stale plugin from this cache (the old shape
     * reused whatever bytes were already there for as long as the file existed,
     * which pinned a device to the first build it ever extracted). The digest is
     * computed once per process per payload, so this stays cheap.
     */
    private fun extract(ctx: Context): File? = try {
        val dir = File(ctx.cacheDir, "cs3bridge").apply { mkdirs() }
        val base = File(cs3Resource).name
        val known = fingerprints[cs3Resource]
        if (known != null) {
            val target = File(dir, "$known-$base")
            // The digest is known, so the name is known — the only reason the
            // file can be gone is that the system reclaimed the cache, and the
            // payload can simply be copied out again.
            if (target.exists() && target.length() > 0L) target else copyOut(dir, base, known)
        } else {
            val bytes = javaClass.classLoader?.getResourceAsStream(cs3Resource)?.use { it.readBytes() }
            if (bytes == null || bytes.isEmpty()) {
                null
            } else {
                val hex = digestPrefix(bytes)
                fingerprints[cs3Resource] = hex
                val target = File(dir, "$hex-$base")
                target.outputStream().use { out -> out.write(bytes) }
                target
            }
        }
    } catch (t: Throwable) {
        null
    }

    private fun copyOut(dir: File, base: String, hex: String): File? = try {
        val bytes = javaClass.classLoader?.getResourceAsStream(cs3Resource)?.use { it.readBytes() }
        if (bytes == null || bytes.isEmpty()) {
            null
        } else {
            val target = File(dir, "$hex-$base")
            target.outputStream().use { out -> out.write(bytes) }
            target
        }
    } catch (t: Throwable) {
        null
    }

    private fun digestPrefix(bytes: ByteArray): String =
        java.security.MessageDigest.getInstance("SHA-256")
            .digest(bytes)
            .take(8)
            .joinToString("") { "%02x".format(it) }

    // ------------------------------------------------------------------ load cache

    private val loadCache = ConcurrentHashMap<String, LoadResponse>()

    private suspend fun loadResponse(id: String): LoadResponse? {
        loadCache[id]?.let { return it }
        val a = apiOrNull() ?: return null
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

    // ------------------------------------------------------------------- mapping

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

    companion object {
        private const val TAG = "HikariBridge"

        /** One plugin archive (several bundled providers may share a .cs3) is
         *  only ever loaded once per install; every wrapper indexes into the
         *  same loaded API list. Keyed by the extracted file's absolute path. */
        private val loadedPlugins = ConcurrentHashMap<String, List<MainAPI>>()

        /** Why a given extracted plugin failed to load, and when — read by the
         *  wrapper that needs to explain itself to the user. */
        private val loadFailures = ConcurrentHashMap<String, String>()
        private val loadFailureAt = ConcurrentHashMap<String, Long>()

        /** cs3 resource -> the digest prefix its extracted copy is named after. */
        private val fingerprints = ConcurrentHashMap<String, String>()

        /** Only SUCCESSFUL loads are cached — an empty result (load() threw or
         *  was still running when the budget hit) must never be remembered, or
         *  every retry would replay the same empty catalog. Serialized so
         *  concurrent retries can't double-load. */
        private val loadLock = java.util.concurrent.locks.ReentrantLock()

        private val loadExecutor = Executors.newCachedThreadPool { r ->
            Thread(r, "cs3bridge-load").apply { isDaemon = true }
        }

        private const val LOAD_BUDGET_MS = 45_000L
        private const val FAIL_RETRY_MS = 60_000L
        private const val ACTIVITY_WAIT_MS = 12_000L
        private const val REGISTER_WAIT_MS = 3_000L
    }
}
