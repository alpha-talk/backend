plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.spring)
    alias(libs.plugins.spring.boot)
    alias(libs.plugins.spring.dependency.management)
}

kotlin {
    jvmToolchain(21)
    compilerOptions {
        freeCompilerArgs.add("-Xjsr305=strict")
    }
}

extra["kotlin.version"] = libs.versions.kotlin.get()

dependencyManagement {
    imports {
        mavenBom(libs.spring.cloud.bom.get().toString())
    }
}

dependencies {
    implementation(project(":contracts"))
    implementation(project(":auth-jwt"))

    implementation(libs.spring.cloud.openfeign)
    implementation("org.springframework.boot:spring-boot-starter-websocket")
    implementation("org.springframework.boot:spring-boot-starter-data-redis")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    runtimeOnly("io.micrometer:micrometer-registry-prometheus")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("org.jetbrains.kotlin:kotlin-reflect")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.boot:spring-boot-testcontainers")
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation("org.awaitility:awaitility")
}

tasks.test {
    useJUnitPlatform {
        excludeTags("load")
    }
}

tasks.register<Test>("loadTest") {
    description = "Runs the opt-in single-instance WebSocket load smoke"
    group = "verification"
    testClassesDirs = sourceSets["test"].output.classesDirs
    classpath = sourceSets["test"].runtimeClasspath
    maxHeapSize = providers.gradleProperty("wsLoadHeap").getOrElse("2g")
    useJUnitPlatform {
        includeTags("load")
    }
    systemProperty("ws.load.clients", providers.gradleProperty("wsLoadClients").getOrElse("500"))
    systemProperty("ws.load.rounds", providers.gradleProperty("wsLoadRounds").getOrElse("5"))
    systemProperty("ws.load.fanout.seconds", providers.gradleProperty("wsLoadFanoutSeconds").getOrElse("30"))
    systemProperty("ws.load.connect.batch", providers.gradleProperty("wsLoadConnectBatch").getOrElse("50"))
    systemProperty("ws.load.connect.pause.millis", providers.gradleProperty("wsLoadConnectPauseMillis").getOrElse("0"))
    systemProperty("ws.load.connect.timeout.seconds", providers.gradleProperty("wsLoadConnectTimeoutSeconds").getOrElse("30"))
}
