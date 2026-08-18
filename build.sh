#!/usr/bin/env bash
# Local build of all extensions in this repo → *.hiki
# Needs: java 17+, jar (JDK), and a downloaded deps/ set (or run the workflow).
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
fetch https://repo1.maven.org/maven2/org/jetbrains/kotlinx/kotlinx-coroutines-core-jvm/1.9.0/kotlinx-coroutines-core-jvm-1.9.0.jar kotlinx-coroutines-core-jvm-1.9.0.jar
fetch https://repo1.maven.org/maven2/com/squareup/okhttp3/okhttp/4.12.0/okhttp-4.12.0.jar okhttp-4.12.0.jar
fetch https://repo1.maven.org/maven2/com/squareup/okio/okio-jvm/3.6.0/okio-jvm-3.6.0.jar okio-jvm-3.6.0.jar
fetch https://dl.google.com/dl/android/maven2/com/android/tools/r8/8.3.37/r8-8.3.37.jar r8-8.3.37.jar

if [ ! -x "$KOTLINC_DIR/bin/kotlinc" ]; then
  curl -fsSL -o kotlinc.zip https://github.com/JetBrains/kotlin/releases/download/v2.1.20/kotlin-compiler-2.1.20.zip
  unzip -q kotlinc.zip
fi

CP="deps/android-4.1.1.4.jar:deps/kotlinx-coroutines-core-jvm-1.9.0.jar:deps/okhttp-4.12.0.jar:deps/okio-jvm-3.6.0.jar"

rm -rf build
mkdir -p build/sdk-out build/ext-out build/dex-out build/pkg

# 1) compile the SDK (interface + net helpers) against stubs → sdk.jar
"$KOTLINC_DIR/bin/kotlinc" -nowarn -cp "$CP" -d build/sdk-out sdk/HikariProvider.kt sdk/HikariNet.kt stubs/*.kt
jar cf build/sdk.jar -C build/sdk-out .

# 2) compile every extension against sdk.jar → one jar per folder
for ext in */manifest.json; do
  dir=$(dirname "$ext")
  [ "$dir" = "chaturbate" ] || continue   # only known extensions here
  name=$(basename "$dir")
  rm -rf build/ext-out
  "$KOTLINC_DIR/bin/kotlinc" -nowarn -cp "build/sdk.jar:deps/android-4.1.1.4.jar" -d build/ext-out "$dir"/src/**/*.kt
  jar cf "build/$name.jar" -C build/ext-out .
  # 3) dex
  rm -rf build/dex-out
  java -cp deps/r8-8.3.37.jar com.android.tools.r8.D8 --release \
    --lib deps/android-4.1.1.4.jar --classpath build/sdk.jar \
    --output build/dex-out "build/$name.jar"
  # 4) package .hiki = classes.dex + manifest.json
  rm -rf build/pkg
  mkdir -p build/pkg
  cp build/dex-out/classes.dex build/pkg/
  cp "$dir/manifest.json" build/pkg/
  jar cf "$name.hiki" -C build/pkg .
  echo "built $name.hiki"
done
