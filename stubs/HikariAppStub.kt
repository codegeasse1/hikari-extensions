// Compile-only stub for the app's HikariApp + MainActivity (see HttpStub.kt —
// never loaded at runtime; the real classes in the Hikari APK win). The bridge
// provider uses HikariApp.mainActivity to give CloudStream plugins a real
// Activity to load() with (some cast it `as AppCompatActivity`).
package com.hikari.app

class MainActivity : android.app.Activity()

class HikariApp : android.app.Application() {
    companion object {
        lateinit var instance: HikariApp
        @Volatile
        var mainActivity: MainActivity? = null
    }
}
