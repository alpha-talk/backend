plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.spring.dependency.management)
    `java-library`
}

kotlin {
    jvmToolchain(21)
}

extra["kotlin.version"] = libs.versions.kotlin.get()

dependencyManagement {
    imports {
        mavenBom("org.springframework.boot:spring-boot-dependencies:${libs.versions.spring.boot.get()}")
    }
}

dependencies {
    api(project(":kis-client"))
    implementation(project(":contracts"))
    api("org.springframework.data:spring-data-redis")

    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation("io.lettuce:lettuce-core")
}

tasks.test {
    useJUnitPlatform()
}
