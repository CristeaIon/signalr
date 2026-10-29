plugins {
    alias(libs.plugins.android.library)
    id("org.jetbrains.kotlin.plugin.serialization") version "2.0.21"
    alias(libs.plugins.vanniktech.maven.publish)
}

android {
    namespace = "io.github.cristeaion.signalr"
    compileSdk {
        version = release(37)
    }

    defaultConfig {
        minSdk = 24

        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
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
        version = (project.findProperty("VERSION_NAME") as? String) ?: "1.0.0",
    )

    pom {
        name.set("SignalR Kotlin")
        description.set("Lightweight SignalR Client library for Android and Kotlin")
        inceptionYear.set("2026")
        url.set("https://github.com/CristeaIon/signalR")
        licenses {
            license {
                name.set("The Apache License, Version 2.0")
                url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
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
            connection.set("scm:git:git://github.com/CristeaIon/signalR.git")
            developerConnection.set("scm:git:ssh://git@github.com/CristeaIon/signalR.git")
            url.set("https://github.com/CristeaIon/signalR")
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
    
    implementation(libs.androidx.appcompat)
    implementation(libs.androidx.core.ktx)
    implementation(libs.material)
    testImplementation(libs.junit)
    androidTestImplementation(libs.androidx.espresso.core)
    androidTestImplementation(libs.androidx.junit)
}