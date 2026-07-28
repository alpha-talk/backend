plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.8.0"
}

rootProject.name = "alphatalk"

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}

include(
    "contracts",
    "auth-jwt",
    "db-migrations",
    "kis-client",
    "ws",
    "worker-price",
    "worker-batch",
    "worker-ingest",
    "worker-llm",
)
