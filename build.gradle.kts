plugins {
    id("java")
    id("com.github.spotbugs") version "6.0.18"
    id("com.diffplug.spotless") version "6.25.0"
}

group = "com.inyo"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {

    implementation("org.duckdb:duckdb_jdbc:1.3.2.1")
    implementation("org.apache.arrow:arrow-vector:18.3.0")
    implementation("org.apache.arrow:arrow-memory-unsafe:18.3.0")

    compileOnly("org.apache.kafka:kafka-clients:4.0.0")
    compileOnly("org.apache.kafka:connect-api:4.0.0")
    compileOnly("org.apache.kafka:connect-json:4.0.0")
    compileOnly("com.github.spotbugs:spotbugs-annotations:4.8.4")

    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.apache.kafka:connect-api:4.0.0")
}

spotbugs {
    toolVersion.set("4.8.4")
    effort.set(com.github.spotbugs.snom.Effort.MAX)
    reportLevel.set(com.github.spotbugs.snom.Confidence.LOW)
    ignoreFailures.set(false) // Fail build on any SpotBugs violations
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
    dependsOn("spotbugsMain", "spotbugsTest", "spotlessCheck")
}

spotless {
    java {
        target("src/**/*.java")

        // Google Java Format for consistent formatting
        googleJavaFormat()

        // Import organization - no wildcard imports
        removeUnusedImports()

        // Whitespace and line ending fixes
        trimTrailingWhitespace()
        endWithNewline()

        // Custom rules that replace some Checkstyle functionality
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
