#!/usr/bin/env bash
# Builds every extension in the repo (<name>/manifest.json) → <name>.hiki,
# then regenerates repo.json listing them all.
# Needs: java 17+, jar (JDK), curl, unzip. Downloads kotlinc + deps on first run.
set -euo pipefail
cd "$(dirname "$0")"

KOTLINC_DIR="${KOTLINC_DIR:-./kotlinc}"
DEPS="deps"
mkdir -p "$DEPS"

fetch() {
  local url="$1" file="$2"
  [ -f "$DEPS/$file" ] || curl -fsSL -o "$DEPS/$file" "$url"
}

fetch https://repo1.maven.org/maven2/com/google/android/android/4.1.1.4/android-4.1.1.4.jar android-4.1.1.4.jar
fetch https://repo1.maven.org/maven2/org/json/json/20231013/json-20231013.jar json-20231013.jar
fetch https://repo1.maven.org/maven2/org/jetbrains/kotlinx/kotlinx-coroutines-core-jvm/1.9.0/kotlinx-coroutines-core-jvm-1.9.0.jar kotlinx-coroutines-core-jvm-1.9.0.jar
fetch https://repo1.maven.org/maven2/com/squareup/okhttp3/okhttp/4.12.0/okhttp-4.12.0.jar okhttp-4.12.0.jar
fetch https://repo1.maven.org/maven2/com/squareup/okio/okio-jvm/3.6.0/okio-jvm-3.6.0.jar okio-jvm-3.6.0.jar
fetch https://dl.google.com/dl/android/maven2/com/android/tools/r8/8.3.37/r8-8.3.37.jar r8-8.3.37.jar
fetch https://raw.githubusercontent.com/codegeasse1/hikari/main/app/libs/cloudstream3.jar cloudstream3.jar
fetch https://repo1.maven.org/maven2/org/jetbrains/kotlinx/kotlinx-serialization-core-jvm/1.7.1/kotlinx-serialization-core-jvm-1.7.1.jar kotlinx-serialization-core-jvm-1.7.1.jar
fetch https://repo1.maven.org/maven2/org/jetbrains/kotlinx/kotlinx-serialization-json-jvm/1.7.1/kotlinx-serialization-json-jvm-1.7.1.jar kotlinx-serialization-json-jvm-1.7.1.jar

if [ ! -x "$KOTLINC_DIR/bin/kotlinc" ]; then
  curl -fsSL -o kotlinc.zip https://github.com/JetBrains/kotlin/releases/download/v2.4.10/kotlin-compiler-2.4.10.zip
  unzip -q kotlinc.zip
fi

CP="deps/json-20231013.jar:deps/kotlinx-coroutines-core-jvm-1.9.0.jar:deps/okhttp-4.12.0.jar:deps/okio-jvm-3.6.0.jar:deps/kotlinx-serialization-core-jvm-1.7.1.jar:deps/kotlinx-serialization-json-jvm-1.7.1.jar"
# CloudStream runtime classes (bridge extensions) + android.jar (context/app
# classes). The app ships both, so they stay EXTERNAL references — never dexed in.
EXTRA_CP="deps/cloudstream3.jar:deps/android-4.1.1.4.jar"

rm -rf build
mkdir -p build/sdk-out build/ext-out build/dex-out build/pkg

# 1) compile the SDK (interface + net helpers) against stubs → sdk.jar
"$KOTLINC_DIR/bin/kotlinc" -cp "$CP:deps/android-4.1.1.4.jar" -d build/sdk-out \
  sdk/HikariProvider.kt sdk/HikariNet.kt stubs/HttpStub.kt stubs/WebViewResolverStub.kt stubs/HikariAppStub.kt
jar cf build/sdk.jar -C build/sdk-out .

# 2..4) build every extension folder that has a manifest.json
BUILT=""
for dir in */; do
  [ -f "$dir/manifest.json" ] || continue
  name="${dir%/}"
  echo "building $name"
  rm -rf build/ext-out build/dex-out build/pkg
  mkdir -p build/ext-out build/dex-out build/pkg

  # compile against sdk.jar (coroutines on classpath so suspend helpers +
  # kotlinx.coroutines.sync.Mutex resolve; at runtime the app's classloader
  # provides kotlinx-coroutines)
  "$KOTLINC_DIR/bin/kotlinc" -cp "build/sdk.jar:$EXTRA_CP:$CP" -d build/ext-out \
    "$dir"src/com/hikari/ext/providers/*.kt
  jar cf "build/$name.jar" -C build/ext-out .

  # dex the extension (SDK + CloudStream + android classes stay external refs)
  cp_args=(--classpath build/sdk.jar)
  IFS=':' read -ra _cps <<< "$EXTRA_CP:$CP"
  for c in "${_cps[@]}"; do cp_args+=(--classpath "$c"); done
  java -cp deps/r8-8.3.37.jar com.android.tools.r8.D8 --release \
    --lib deps/android-4.1.1.4.jar \
    "${cp_args[@]}" \
    --output build/dex-out "build/$name.jar"

  # package .hiki = classes.dex + manifest.json + any bundled resources
  cp build/dex-out/classes.dex build/pkg/
  cp "$name/manifest.json" build/pkg/
  if [ -d "$name/bundle" ]; then
    cp -r "$name/bundle/." build/pkg/
  fi

  # Bridge extensions (cncverse, phisher) fetch their .cs3 files from the
  # upstream repos' builds branches at build time — they're third-party
  # artifacts that change often, and pinning them in git would bloat the repo.
  # bridge-cs3.conf names the source repo + packaged subdir; bridge-sources.txt
  # lists "<upstream path>\t<packaged filename>" lines. A failed fetch aborts
  # the build (a missing .cs3 means broken providers).
  if [ -f "$name/bridge-cs3.conf" ] && [ -f "$name/bridge-sources.txt" ]; then
    . "$name/bridge-cs3.conf"
    mkdir -p "build/pkg/cs3/$bridge_subdir"
    while IFS=$'\t' read -r upstream packaged; do
      [ -z "$upstream" ] && continue
      packaged="${packaged:-$upstream}"
      curl -fsSL -o "build/pkg/cs3/$bridge_subdir/$packaged" \
        "https://raw.githubusercontent.com/$bridge_repo/builds/$upstream"
    done < "$name/bridge-sources.txt"
  fi

  jar cf "$name.hiki" -C build/pkg .

  BUILT="$BUILT $name"
  echo "built $name.hiki"
done

if [ -z "$BUILT" ]; then
  echo "no extensions found" >&2
  exit 1
fi

# 5) regenerate repo.json from the built .hiki files + their manifests
generate_repo_json() {
  echo "{"
  echo "  \"name\": \"Hikari Extensions\","
  echo "  \"description\": \"Official .hiki extensions for Hikari.\","
  echo "  \"plugins\": ["
  first=1
  for name in $BUILT; do
    [ $first -eq 0 ] && echo ","
    first=0
    local ver
    ver=$(sed -n 's/.*"version"[^0-9]*\([0-9][0-9]*\).*/\1/p' "$name/manifest.json" | head -1)
    ver="${ver:-1}"
    local tvtypes
    tvtypes=$(sed -n 's/.*"tvTypes"[^[]*\[\([^]]*\)\].*/\1/p' "$name/manifest.json" | head -1)
    [ -n "$tvtypes" ] || tvtypes='"movie"'
    printf '    {\n'
    printf '      "name": "%s",\n' "$(sed -n 's/.*"name"[^"]*"\([^"]*\)".*/\1/p' "$name/manifest.json" | head -1)"
    printf '      "description": "%s",\n' "Auto-built Hikari extension from this repo."
    printf '      "url": "https://github.com/codegeasse1/hikari-extensions/releases/download/continuous/%s.hiki",\n' "$name"
    printf '      "version": %s,\n' "$ver"
    printf '      "tvTypes": [%s]\n' "$tvtypes"
    printf '    }'
  done
  echo ""
  echo "  ]"
  echo "}"
}
generate_repo_json > repo.json
echo "repo.json updated with:$BUILT"
