plugins {
    alias(libs.plugins.kotlin.jvm)
    `java-library`
    `java-test-fixtures`
}

kotlin {
    jvmToolchain(21)
    compilerOptions {
        freeCompilerArgs.add("-Xjsr305=strict")
    }
}

dependencies {
    implementation(libs.jackson.databind)
    implementation(libs.jackson.kotlin)
    implementation(libs.resilience4j.ratelimiter)

    testFixturesApi(libs.java.websocket)

    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}
