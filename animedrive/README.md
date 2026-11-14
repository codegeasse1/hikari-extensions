# AnimeDrive extension for hikari-extensions

Drop-in extension folder. Copy this whole `animedrive/` directory into the
root of https://github.com/codegeasse1/hikari-extensions (next to
`animeworldindia/`, `rareanimesindia/`, …), commit, and push.
The repo's `build.sh` / CI picks up every folder with a `manifest.json`
automatically: it compiles `src/com/hikari/ext/providers/*.kt`,
packages `animedrive.hiki`, and regenerates `repo.json`.
No workflow changes needed.

## Contents

- `manifest.json` — name `AnimeDrive`, version 1, mainClass
  `com.hikari.ext.providers.AnimeDriveProvider`, tvTypes series+movie.
- `src/com/hikari/ext/providers/AnimeDriveProvider.kt` — the provider.

## How it works

- Catalogs/search: plain WordPress parsing of animedrive.cc
  (`article.ast-archive-post` cards, `h2.entry-title` links) via
  `getStringSmart` (rendered fetch, survives Cloudflare).
- Detail pages: reads the `a.anime-dl-btn` "Download / Watch" button to get
  the `link.animedrive.cc` landing page.
- Episodes: the landing page's `.adc-ep` blocks (EP badge + `.adc-epname`)
  become episodes; each block's FilePress buttons
  (`filepress.*/file/<id>`, quality label kept as stream name) become
  that episode's files. HubCloud `/dl/` buttons are deliberately skipped —
  they sit behind a Cloudflare challenge for plain HTTP.
- Playback: FilePress is a FileBee SPA. The provider calls
  `GET {origin}/api/file/get/<id>` then
  `POST {origin}/api/file/downlaod[/2]` with
  `{"id","method","captchaValue":null}` as `application/json`
  (methods tried: `dotFlixDownlaod` = Instant Download, `indexDownlaod`,
  `publicDownlaod`, `cloudR2Downlaod`; captcha is not required on these
  files). If the API path fails, it falls back to WebView m3u8/mp4 capture
  on the file page.

## After pushing

1. Wait for the `continuous` release build to finish.
2. In Hikari: Extensions → the `hikari-extensions` repo (already a default)
   → install/update **AnimeDrive** → open it, search e.g. "Psyren",
   open a title, pick an episode, play.
3. If a title shows no streams, check `link.animedrive.cc` in a browser:
   episodes with only HubCloud buttons have no playable source by design.
