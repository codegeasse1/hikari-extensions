package com.hikari.ext.providers

import com.hikari.ext.HikariNet
import org.json.JSONObject

/**
 * Shared domain table (the source's FirebaseDomainHelper) plus the github
 * registry files some sites use. Cached for 5 minutes; keys support the
 * plain, _url and _domain suffixes.
 */
internal object JpDomains {
    private const val URL = "https://cloudstreampluginhelper-default-rtdb.firebaseio.com/.json"
    private const val CACHE_TTL_MS = 5 * 60 * 1000L

    @Volatile private var domains: Map<String, String> = emptyMap()
    @Volatile private var lastLoadTime: Long = 0L
    @Volatile private var everLoaded = false

    private suspend fun load(force: Boolean = false) {
        val now = System.currentTimeMillis()
        if (!force && everLoaded && now - lastLoadTime < CACHE_TTL_MS) return
        try {
            val txt = HikariNet.getString(URL) ?: return
            val obj = JSONObject(txt)
            val map = HashMap<String, String>()
            for (k in obj.keys()) {
                val v = obj.optString(k).trim().removeSuffix("/")
                if (v.isNotBlank()) map[k] = v
            }
            domains = map
            lastLoadTime = now
            everLoaded = true
        } catch (_: Exception) {
            lastLoadTime = now
        }
    }

    suspend fun get(key: String): String? {
        load()
        return domains[key] ?: domains["${key}_url"] ?: domains["${key}_domain"]
    }

    /** github registry files some sites publish (key -> https URL or null). */
    suspend fun registry(url: String, key: String): String? {
        return try {
            val txt = HikariNet.getString(url) ?: return null
            JSONObject(txt).optString(key).takeIf { it.startsWith("http") }
        } catch (_: Exception) {
            null
        }
    }
}
