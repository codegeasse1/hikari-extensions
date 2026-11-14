package com.hikari.ext.providers

/** Port of the source's PlaySourceFilter: download-only names are tagged (and skipped by default). */
internal object JpFilter {

    private val downloadTokens = listOf(
        "10gbps",
        "gdflix instant download",
        "instant download",
        "download",
        "g-direct",
        "g direct",
    )

    fun isDownloadOnlyName(name: String): Boolean {
        val n = name.lowercase()
        return downloadTokens.any { n.contains(it) }
    }

    fun taggedDownloadOnly(name: String): String =
        if (name.contains("(DOWNLOAD ONLY)", ignoreCase = true)) name
        else "${name.trimEnd()} (DOWNLOAD ONLY)"
}
