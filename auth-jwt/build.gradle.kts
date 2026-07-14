plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.spring)
    `java-library`
}

kotlin {
    jvmToolchain(21)
}

dependencies {
    implementation(libs.jjwt.api)
    runtimeOnly(libs.jjwt.impl)
    runtimeOnly(libs.jjwt.jackson)

    implementation(libs.spring.boot.autoconfigure)

    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}
