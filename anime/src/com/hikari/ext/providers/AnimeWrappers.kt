package com.hikari.ext.providers

/**
 * The five anime providers the user asked for, bridged from their upstream
 * CloudStream .cs3 plugins (phisher98/cloudstream-extensions-phisher, fetched
 * at build time via bridge-cs3.conf + bridge-sources.txt). Each .cs3 exposes
 * one MainAPI here, wrapped as its own HikariProvider.
 */
class AnimeAniKoto : Cs3BridgeProvider(
    cs3Resource = "cs3/anime/AniKoto.cs3",
    apiIndex = 0,
    id = "anime|anikoto",
    name = "AniKoto",
)

class AnimeAnikage : Cs3BridgeProvider(
    cs3Resource = "cs3/anime/Anikage.cs3",
    apiIndex = 0,
    id = "anime|anikage",
    name = "Anikage",
)

class AnimeAnimekhor : Cs3BridgeProvider(
    cs3Resource = "cs3/anime/Animekhor.cs3",
    apiIndex = 0,
    id = "anime|animekhor",
    name = "Animekhor",
)

class AnimeAnimexin : Cs3BridgeProvider(
    cs3Resource = "cs3/anime/Animexin.cs3",
    apiIndex = 0,
    id = "anime|animexin",
    name = "Animexin",
)

class AnimeAnineko : Cs3BridgeProvider(
    cs3Resource = "cs3/anime/Anineko.cs3",
    apiIndex = 0,
    id = "anime|anineko",
    name = "Anineko",
)
