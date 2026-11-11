plugins {
    alias(libs.plugins.android.library)
    alias(libs.plugins.kotlin.serialization)
    alias(libs.plugins.vanniktech.maven.publish)
    alias(libs.plugins.binary.compatibility.validator)
}

val libraryVersion = (project.findProperty("VERSION_NAME") as? String) ?: "1.0.0"

android {
    namespace = "io.github.cristeaion.signalr"
    compileSdk {
        version = release(37)
    }

    defaultConfig {
        minSdk = 24
        // Sent in the default User-Agent.
        buildConfigField("String", "LIBRARY_VERSION", "\"$libraryVersion\"")

        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
    }
    buildFeatures {
        buildConfig = true
    }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
    }
}

mavenPublishing {
    publishToMavenCentral()
    signAllPublications()

    coordinates(
        groupId = (project.findProperty("GROUP") as? String) ?: "io.github.cristeaion",
        artifactId = (project.findProperty("POM_ARTIFACT_ID") as? String) ?: "signalr",
        version = libraryVersion,
    )

    pom {
        name.set("SignalR Kotlin")
        description.set("Lightweight SignalR Client library for Android and Kotlin")
        inceptionYear.set("2026")
        url.set("https://github.com/CristeaIon/signalr.git")
        licenses {
            license {
                name.set("MIT License")
                url.set("https://opensource.org/licenses/MIT")
                distribution.set("repo")
            }
        }
        developers {
            developer {
                id.set("CristeaIon")
                name.set("Ion Cristea")
                url.set("https://github.com/CristeaIon")
            }
        }
        scm {
            connection.set("scm:git:git://github.com/CristeaIon/signalr.git")
            developerConnection.set("scm:git:ssh://git@github.com/CristeaIon/signalr.git")
            url.set("https://github.com/CristeaIon/signalr")
        }
    }
}

dependencies {
    implementation(libs.ktor.client.core)
    implementation(libs.ktor.client.okhttp)
    implementation(libs.ktor.client.websockets)
    implementation(libs.ktor.client.content.negotiation)
    implementation(libs.ktor.serialization.kotlinx.json)
    implementation(libs.kotlinx.serialization.json)
    implementation(libs.kotlinx.coroutines.core)
    
    testImplementation(libs.junit)
    testImplementation(libs.ktor.server.cio)
    testImplementation(libs.ktor.server.websockets)
    androidTestImplementation(libs.androidx.espresso.core)
    androidTestImplementation(libs.androidx.junit)
}
kotlin {
    // Every public declaration must state its visibility, so nothing becomes API by accident.
    explicitApi()
}

// Binary-compatibility validation. The validator plugin only wires itself up for the
// `kotlin-android` plugin, which AGP's built-in Kotlin doesn't apply, so its tasks are
// registered here against the release variant's compiled classes.
//   ./gradlew :signalr:apiCheck   fails when the public API differs from signalr/api/signalr.api
//   ./gradlew :signalr:apiDump    records the current API; review the diff, note breaks in CHANGELOG.md
dependencies {
    // What the validator would add itself if it detected the Kotlin plugin.
    "bcv-rt-jvm-cp"(libs.kotlin.metadata.jvm)
    "bcv-rt-jvm-cp"(libs.asm)
    "bcv-rt-jvm-cp"(libs.asm.tree)
}
val apiFile = layout.projectDirectory.file("api/signalr.api")
val apiBuild = tasks.register<kotlinx.validation.KotlinApiBuildTask>("apiBuild") {
    inputClassesDirs.from(tasks.named("compileReleaseKotlin").map { it.outputs.files.asFileTree.matching { include("**/*.class") } })
    outputApiFile.set(layout.buildDirectory.file("api/signalr.api"))
    runtimeClasspath.from(configurations.named("bcv-rt-jvm-cp-resolver"))
}
tasks.register<Copy>("apiDump") {
    group = "verification"
    description = "Records the library's public API in signalr/api/signalr.api."
    from(apiBuild.flatMap { it.outputApiFile })
    into(apiFile.asFile.parentFile)
}
val apiCheck = tasks.register<kotlinx.validation.KotlinApiCompareTask>("apiCheck") {
    group = "verification"
    description = "Fails when the library's public API differs from signalr/api/signalr.api."
    projectApiFile.set(apiFile)
    generatedApiFile.set(apiBuild.flatMap { it.outputApiFile })
}
tasks.named("check") { dependsOn(apiCheck) }
