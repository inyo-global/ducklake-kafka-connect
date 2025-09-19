plugins {
    id("java")
    id("distribution")
    id("com.gradleup.shadow") version "9.1.0"
    id("com.github.spotbugs") version "6.0.18"
    id("com.diffplug.spotless") version "6.25.0"
}

group = "com.inyo"
version = "1.0-SNAPSHOT"

repositories { mavenCentral() }

tasks.jar {
    archiveBaseName.set("ducklake-connector")
}

tasks.shadowJar {
    archiveBaseName.set("ducklake-connector")
    archiveClassifier.set("all") // usar classifier para não sobrescrever o jar padrão
    mergeServiceFiles()
}

tasks.withType<com.github.spotbugs.snom.SpotBugsTask> {
    reports.create("html") {
        required.set(true)
        outputLocation.set(file("build/reports/spotbugs/${name}.html"))
        setStylesheet("fancy-hist.xsl")
    }
    reports.create("sarif") {
        required.set(true)
        outputLocation.set(file("build/reports/spotbugs/${name}.sarif"))
    }
}

tasks.test {
    useJUnitPlatform()
    jvmArgs("--add-opens=java.base/java.nio=ALL-UNNAMED")
}

tasks.check {
    dependsOn("spotbugsMain", "spotbugsTest", "spotlessCheck", integrationTest)
}

val integrationTest by tasks.registering(Test::class) {
    description = "Runs integration tests"
    group = "verification"

    testClassesDirs = sourceSets["integrationTest"].output.classesDirs
    classpath = sourceSets["integrationTest"].runtimeClasspath

    dependsOn(tasks.installDist)

    systemProperty("distribution.path", layout.buildDirectory.dir("install").get().asFile.absolutePath)

    useJUnitPlatform()
}

val processManifest by tasks.registering(Copy::class) {
    val projectVersion = project.provider { version.toString() }
    inputs.property("version", projectVersion)

    from(rootProject.file("manifest.json"))
    into(layout.buildDirectory.dir("processed"))
    expand("version" to projectVersion.get())
}

sourceSets {
    create("integrationTest") {
        java {
            srcDir("src/integrationTest/java")
        }
        resources {
            srcDir("src/integrationTest/resources")
        }
        compileClasspath += sourceSets.main.get().output + configurations.testRuntimeClasspath.get()
        runtimeClasspath += output + compileClasspath
    }
}

configurations {
    getByName("integrationTestImplementation") {
        extendsFrom(configurations.testImplementation.get())
    }
    getByName("integrationTestRuntimeOnly") {
        extendsFrom(configurations.testRuntimeOnly.get())
    }
    getByName("runtimeClasspath") {
        extendsFrom(configurations.implementation.get())
        exclude("org.slf4j")
    }
}

distributions {
    main {
        contents {
            into("lib/") {
                from(tasks.shadowJar)
            }
            into("") {
                from(processManifest)
                from(rootProject.file("LICENSE.md"))
                from(rootProject.file("README.md"))
            }
        }
    }
}

spotless {
    java {
        target("src/**/*.java")
        licenseHeaderFile("config/spotless.license.java", "package ")
        googleJavaFormat()
        removeUnusedImports()
        trimTrailingWhitespace()
        endWithNewline()
        custom("no-wildcard-imports") { content ->
            if (content.contains("import .*\\*;".toRegex())) {
                throw RuntimeException("Wildcard imports are not allowed. Use specific imports instead.")
            }
            content
        }
        custom("line-length-check") { content ->
            val lines = content.split("\n")
            lines.forEachIndexed { index, line ->
                if (line.length > 120 && !line.trim().startsWith("//") && !line.contains("http")) {
                    throw RuntimeException("Line ${index + 1} exceeds 120 characters: ${line.length} chars")
                }
            }
            content
        }
    }
}

dependencies {
    implementation("org.duckdb:duckdb_jdbc:1.4.0.0")
    implementation("org.apache.arrow:arrow-vector:18.3.0") {
        exclude(group = "org.slf4j")
    }
    implementation("org.apache.arrow:arrow-c-data:18.3.0") {
        exclude(group = "org.slf4j")
    }
    implementation("org.apache.arrow:arrow-memory-unsafe:18.3.0")

    compileOnly("org.apache.kafka:kafka-clients:4.0.0")
    compileOnly("org.apache.kafka:connect-api:4.0.0")
    compileOnly("org.apache.kafka:connect-json:4.0.0")
    compileOnly("com.github.spotbugs:spotbugs-annotations:4.8.4")

    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.apache.kafka:connect-api:4.0.0")
    testImplementation("org.postgresql:postgresql:42.7.7")
    testImplementation("io.minio:minio:8.5.17")

    add("integrationTestImplementation", "org.testcontainers:kafka:1.21.3")
    add("integrationTestImplementation", "org.testcontainers:postgresql:1.21.3")
    add("integrationTestImplementation", "org.testcontainers:minio:1.21.3")
    add("integrationTestImplementation", "org.testcontainers:junit-jupiter:1.21.3")
}
