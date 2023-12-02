import com.google.cloud.tools.jib.gradle.BuildImageTask

plugins {
    kotlin("jvm") version "1.9.0"
    kotlin("plugin.serialization") version "1.9.0"
    id("io.ktor.plugin") version "2.3.5"
    application
}

group = "com.github.aayushjn"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("io.ktor:ktor-server-core")
    implementation("io.ktor:ktor-server-netty")
    implementation("io.ktor:ktor-server-content-negotiation")
    implementation("io.ktor:ktor-serialization-kotlinx-json")
    implementation("io.ktor:ktor-server-compression")
    implementation("io.ktor:ktor-server-call-logging")
    implementation("io.ktor:ktor-server-call-id")
    implementation("io.ktor:ktor-server-default-headers")
    implementation("ch.qos.logback:logback-classic:1.4.9")
    implementation("io.ktor:ktor-client-core")
    implementation("io.ktor:ktor-client-java")
    implementation("io.ktor:ktor-client-logging")
    implementation("io.ktor:ktor-client-content-negotiation")
    implementation("io.ktor:ktor-serialization-kotlinx-json")
    implementation("io.ktor:ktor-client-encoding")
    implementation("com.github.ajalt.clikt:clikt:4.2.1")
    implementation("com.google.guava:guava:32.1.3-jre")
    testImplementation(kotlin("test"))
}

application {
    mainClass = "com.github.aayushjn.kvstore.AppKt"
}

tasks {
    test {
        useJUnitPlatform()
    }

    withType<Jar> {
        from(sourceSets.main.get().output)
        dependsOn(configurations.runtimeClasspath)
        from({ configurations.runtimeClasspath.get().filter { it.name.endsWith("jar") }.map { zipTree(it) } })

        duplicatesStrategy = DuplicatesStrategy.INCLUDE

        manifest {
            attributes(
                "Main-Class" to application.mainClass,
            )
        }
    }
}

kotlin {
    jvmToolchain(17)
}

ktor {
    docker {
        localImageName = "aayushjn/distributed-kvstore"
        imageTag = project.version.toString()
        jreVersion = JavaVersion.VERSION_17
    }
}