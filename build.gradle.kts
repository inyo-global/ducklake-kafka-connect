plugins {
    id("java")
    id("checkstyle")
    id("com.github.spotbugs") version "6.0.18"
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

    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.apache.kafka:connect-api:4.0.0")
}

checkstyle {
    toolVersion = "10.12.4"
    configFile = file("config/checkstyle/checkstyle.xml")
    isIgnoreFailures = false
}

spotbugs {
    toolVersion.set("4.8.4")
    effort.set(com.github.spotbugs.snom.Effort.MAX)
    reportLevel.set(com.github.spotbugs.snom.Confidence.LOW)
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
    dependsOn("spotbugsMain", "spotbugsTest")
}
