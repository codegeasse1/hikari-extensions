// Compile-only stubs for app-internal types that the SDK sources (sdk/*.kt)
// reference. The REAL implementations ship inside the Hikari APK and win at
// runtime (PathClassLoader is parent-first), so these stubs are never loaded —
// they exist only so this repo can compile the SDK to build extensions.
package com.hikari.app.net

import okhttp3.Response

object Http {
    const val UA = "Mozilla/5.0 (compile stub)"
    fun get(url: String, headers: Map<String, String> = emptyMap()): Response =
        throw UnsupportedOperationException("stub")
    fun getString(url: String, headers: Map<String, String> = emptyMap()): String? = null
    fun postString(url: String, body: String, headers: Map<String, String> = emptyMap(), contentType: String = "application/json; charset=utf-8"): String? = null
    fun getStringSmart(url: String, headers: Map<String, String> = emptyMap()): String? = null
    fun getStringRendered(url: String, timeoutMs: Long = 25_000): String? = null
    fun getBytes(url: String, headers: Map<String, String> = emptyMap()): ByteArray? = null
}
