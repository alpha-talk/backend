plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.8.0"
}

rootProject.name = "alphatalk"

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}

include("contracts", "auth-jwt", "db-migrations", "ws", "worker-ingest", "worker-llm")
