// Compile-only stub for CloudStream's WebViewResolver (see HttpStub.kt — never
// loaded at runtime; the app's own WebViewResolver wins).
package com.lagradost.cloudstream3.network

import okhttp3.Interceptor
import okhttp3.Request
import okhttp3.Response

class WebViewResolver(
    val interceptUrl: Regex,
    val additionalUrls: List<Regex> = emptyList(),
    val userAgent: String? = null,
    val useOkhttp: Boolean = true,
    val script: String? = null,
    val scriptCallback: ((String) -> Unit)? = null,
    val timeout: Long = 60_000L,
) : Interceptor {
    override fun intercept(chain: Interceptor.Chain): Response =
        chain.proceed(chain.request())

    suspend fun resolveUsingWebView(
        url: String,
        referer: String? = null,
        method: String = "GET",
        requestCallBack: (Request) -> Boolean = { false },
    ): Pair<Request?, List<Request>> = null to emptyList()
}
