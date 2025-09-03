plugins {
    id("java")
}

group = "com.inyo"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {

    implementation("org.duckdb:duckdb_jdbc:1.3.2.1")
    implementation("org.apache.arrow:arrow-vector:18.3.0")

    compileOnly("org.apache.kafka:kafka-clients:4.0.0")
    compileOnly("org.apache.kafka:connect-api:4.0.0")
    compileOnly("org.apache.kafka:connect-json:4.0.0")

    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.apache.kafka:connect-api:4.0.0")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
}