# Hikari Extensions

Hikari's own extension format — **`.hiki`** files that plug straight into the
[Hikari](https://github.com/codegeasse1/hikari) app (Home rows, search, detail,
player) with no CloudStream or Stremio dependencies.

Extensions are compiled against the `com.hikari.ext` SDK that ships inside the
Hikari APK, so they are tiny, load with the app's hardened networking
(`HikariNet`), and get every SDK fix for free.

## Installing

### Easiest: add this repo in the app

In Hikari (v0.3.21+): **Extensions → Add Hikari repo** and paste:

```
https://raw.githubusercontent.com/codegeasse1/hikari-extensions/builds/repo.json
```

The repo (**Hikari Extensions**) then appears in the *Extension repos* list —
tap it to see every extension it contains, and **Install / Uninstall** each one
from there. `repo.json` is regenerated on every build and pushed to the
`builds` branch (and to the continuous release), so new extensions show up
automatically — no need to remove and re-add the repo; just reopen the
Extensions screen or pull to refresh.

### Or install a single extension by URL

In Hikari (v0.3.19+): **Extensions → Install .hiki from URL** and paste:

```
https://github.com/codegeasse1/hikari-extensions/releases/download/continuous/chaturbate.hiki
```

Or download `chaturbate.hiki` and use **Pick .hiki file**.

## Extensions

| Extension | Description |
|---|---|
| `chaturbate.hiki` | Live cam rooms from chaturbate.com — public broadcasts play in the built-in player (signed LL-HLS). Uses only public, unauthenticated endpoints. |
| `missav.hiki` | Free JAV from missav.ws — 100k+ videos across 12 home rows (new releases, hot today, uncensored leak, big breasts, mature woman, creampie, wife, …), genre browsing and search, LL-HLS in the built-in player. |
| `oppai.hiki` | Hentai anime from oppai.stream in HD/4K — 16 home rows (orders + genres), direct MP4 plus DASH 720/1080/4k streams, no WebView needed. |
| `hanime1.hiki` | The full hanime1.me feed — 12 home rows (new releases, recent uploads, 裏番, 泡麵番, Motion, 3DCG, 2.5D, 2D, AI, MMD, Cosplay, popular). Streams are captured in a real WebView (Cloudflare). |
| `watchhentai.hiki` | 1,400+ series & episodes from watchhentai.net — series/episode/genre listings and trending, 720p/1080p direct MP4 streams. |
| `hanimetv.hiki` | Curated 720p/1080p hentai from hanime.tv — Recent Uploads, New Releases, Trending and Random home rows. Streams are captured in a real WebView (Cloudflare). |
| `hstream.hiki` | English-subbed hentai in HD/FHD/4K from hstream.moe — 7 order-based browse rows and working search, DASH streams captured in a real WebView. |

## Building an extension

The repo builds `.hiki` files automatically on every push (`.github/workflows/build.yml`):
compile the SDK + provider with `kotlinc`, dex with `d8`, package `classes.dex` +
`manifest.json` into the `.hiki` jar.

```
repo layout
  sdk/        com.hikari.ext SDK sources (vendored from the Hikari app — keep in sync)
  stubs/      compile-only stubs for app-internal types (never shipped/loaded)
  <name>/     one extension per folder: manifest.json + src/com/hikari/ext/providers/*
```

Local build:

```bash
./build.sh          # needs java, kotlinc, jar, and deps/ (see build.sh)
```

The API reference and full guide live in the Hikari repo:
[docs/HIKARI_EXTENSIONS.md](https://github.com/codegeasse1/hikari/blob/main/docs/HIKARI_EXTENSIONS.md)
