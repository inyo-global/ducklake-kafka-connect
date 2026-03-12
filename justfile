# DuckLake Kafka Connect — Kafka Connect sink for DuckLake (DuckDB + object storage)

# Default recipe: list all available recipes
default:
    @just --list

# === Build ===

# Build the connector (skip tests)
[group('build')]
build:
    ./gradlew build -x test -x integrationTest

# Build shadow JAR
[group('build')]
shadow:
    ./gradlew shadowJar

# Clean build artifacts
[group('build')]
clean:
    ./gradlew clean

# === Dev ===

# Format code (Spotless)
[group('dev')]
format:
    ./gradlew spotlessApply

# Check formatting
[group('dev')]
format-check:
    ./gradlew spotlessCheck

# === Test ===

# Run unit tests
[group('test')]
test:
    ./gradlew test

# Run integration tests (requires Docker)
[group('test')]
test-integration:
    ./gradlew integrationTest

# Run SpotBugs static analysis
[group('test')]
spotbugs:
    ./gradlew spotbugsMain spotbugsTest

# Full build with all checks
[group('test')]
ci:
    ./gradlew build
