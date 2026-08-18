#!/usr/bin/env bash
# Builds an extension (default: chaturbate) → <name>.hiki
# Needs: java 17+, jar (JDK), curl, unzip. Downloads kotlinc + deps on first run.
set -euo pipefail
cd "$(dirname "$0")"

KOTLINC_DIR="${KOTLINC_DIR:-./kotlinc}"
DEPS="deps"
EXT_NAME="${EXT_NAME:-chaturbate}"
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

if [ ! -x "$KOTLINC_DIR/bin/kotlinc" ]; then
  curl -fsSL -o kotlinc.zip https://github.com/JetBrains/kotlin/releases/download/v2.1.20/kotlin-compiler-2.1.20.zip
  unzip -q kotlinc.zip
fi

CP="deps/json-20231013.jar:deps/kotlinx-coroutines-core-jvm-1.9.0.jar:deps/okhttp-4.12.0.jar:deps/okio-jvm-3.6.0.jar"

rm -rf build
mkdir -p build/sdk-out build/ext-out build/dex-out build/pkg

# 1) compile the SDK (interface + net helpers) against stubs → sdk.jar
"$KOTLINC_DIR/bin/kotlinc" -cp "$CP" -d build/sdk-out \
  sdk/HikariProvider.kt sdk/HikariNet.kt stubs/HttpStub.kt stubs/WebViewResolverStub.kt
jar cf build/sdk.jar -C build/sdk-out .

# 2) compile the extension against sdk.jar (coroutines on classpath so
#    suspend helpers + kotlinx.coroutines.sync.Mutex resolve; at runtime the
#    app's classloader provides kotlinx-coroutines)
"$KOTLINC_DIR/bin/kotlinc" -cp "build/sdk.jar:deps/json-20231013.jar:deps/kotlinx-coroutines-core-jvm-1.9.0.jar" -d build/ext-out \
  "$EXT_NAME"/src/com/hikari/ext/providers/*.kt
jar cf "build/$EXT_NAME.jar" -C build/ext-out .

# 3) dex the extension (SDK classes stay as external references)
java -cp deps/r8-8.3.37.jar com.android.tools.r8.D8 --release \
  --lib deps/android-4.1.1.4.jar \
  --classpath build/sdk.jar \
  --classpath deps/json-20231013.jar \
  --classpath deps/kotlinx-coroutines-core-jvm-1.9.0.jar \
  --output build/dex-out "build/$EXT_NAME.jar"

# 4) package .hiki = classes.dex + manifest.json
cp build/dex-out/classes.dex build/pkg/
cp "$EXT_NAME/manifest.json" build/pkg/
jar cf "$EXT_NAME.hiki" -C build/pkg .

echo "built $EXT_NAME.hiki"
