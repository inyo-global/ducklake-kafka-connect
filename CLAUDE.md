# CLAUDE.md - Project Context for Claude Code

## Project Overview
This is a Kafka Connect sink connector that writes data to DuckLake (DuckDB + object storage like S3). It converts Kafka records to Apache Arrow format and writes them as Parquet files.

## Build & Test Commands

```bash
# Build (skipping tests)
./gradlew build -x test -x integrationTest

# Run unit tests
./gradlew test

# Run integration tests (requires Docker)
./gradlew integrationTest

# Format code (Spotless)
./gradlew spotlessApply

# Check formatting
./gradlew spotlessCheck

# Run SpotBugs static analysis
./gradlew spotbugsMain spotbugsTest

# Full build with all checks
./gradlew build
```

## Key Architecture

### Core Components
- **DucklakeSinkTask** (`src/main/java/.../DucklakeSinkTask.java`): Main sink task that buffers records and flushes to DuckLake
- **DucklakeWriter** (`src/main/java/.../ingestor/DucklakeWriter.java`): Writes Arrow data to DuckLake tables
- **DucklakeTableManager** (`src/main/java/.../ingestor/DucklakeTableManager.java`): Manages table creation/schema with per-table locking
- **SinkRecordToArrowConverter**: Converts Kafka Connect records to Arrow VectorSchemaRoot

### Buffering Configuration
The sink buffers data before writing based on three thresholds:
- `flush.size`: Number of records (default: 10000)
- `flush.interval.ms`: Time interval in ms (default: 60000)
- `file.size.bytes`: Estimated file size in bytes (default: 256MB)

### Thread Safety
- Uses `ReentrantLock` for coordinating between `put()` and scheduled flush
- `checkTimeBasedFlush()` uses `tryLock` with 100ms timeout to avoid blocking
- `AtomicInteger` tracks consecutive flush skips for monitoring

## CI/CD Pipeline

### GitHub Actions Workflow (`.github/workflows/ci.yml`)
Jobs run in this order:
1. **test**: Unit tests
2. **integration-test**: Integration tests with Testcontainers (depends on test)
3. **static-analysis**: Spotless + SpotBugs (depends on test)
4. **security-scan**: Dependency check + CodeQL (depends on test)
5. **build**: Build artifacts (depends on test, integration-test, static-analysis)
6. **release**: Create GitHub release (depends on build, static-analysis, security-scan)

### Release Process
Releases are triggered automatically when:
1. Push to `main` branch
2. Commit message starts with `release:`

The release creates a tag `v0.0.<run_number>` where `<run_number>` is the GitHub Actions run number.

**To create a release:**
1. Create a PR with commit message starting with `release:`
2. Merge to main
3. CI automatically builds and creates GitHub release with JAR artifacts

## Integration Tests

### Test Infrastructure
Uses Testcontainers with:
- `apache/kafka-native:4.0.0` - Kafka broker
- `postgres:15` - PostgreSQL for DuckLake metadata
- `minio/minio` - S3-compatible object storage

### Common Test Flakiness
The Kafka container can be flaky with timeout errors:
```
Timed out waiting for log output matching '.*Transitioning from RECOVERY to RUNNING.*'
```
This is infrastructure flakiness, not a code issue. Re-running the CI job usually fixes it.

### Test Configuration
Integration tests use low flush thresholds to avoid waiting:
```java
cfg.put(DucklakeSinkConfig.FLUSH_SIZE, "1");
cfg.put(DucklakeSinkConfig.FLUSH_INTERVAL_MS, "1000");
cfg.put(DucklakeSinkConfig.FILE_SIZE_BYTES, "1024");
```

## Data Path Configuration
The `data.path` config must use proper URI schemes:
- S3: `s3://bucket-name/path`
- Local files: `file:///path/to/dir`

Validation is in `DucklakeSinkConfig.validateDataPath()`.

## Code Style
- Uses Spotless with Google Java Format
- Run `./gradlew spotlessApply` before committing
- SpotBugs for static analysis (avoid patterns like volatile increment - use AtomicInteger)

## Logging
All classes use SLF4J logging:
```java
private static final Logger LOG = LoggerFactory.getLogger(ClassName.class);
```

## GitHub Repository

This is a fork. Always create PRs on the **PostHog** repo, not upstream:
- **origin**: `PostHog/ducklake-kafka-connect` (CREATE PRs HERE)
- **upstream**: `inyo-global/ducklake-kafka-connect` (DO NOT create PRs here)

When creating PRs, explicitly specify the repo:
```bash
gh pr create --repo PostHog/ducklake-kafka-connect
```
