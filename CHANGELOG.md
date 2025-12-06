# Changelog

All notable changes to the DuckLake Kafka Connect Sink Connector will be documented in this file.

## [0.0.63]

### Added
- `CLAUDE.md` for project context and documentation

### Changed
- Thread safety improvements in `checkTimeBasedFlush()` using `tryLock` with timeout (#11)
- Memory tracking now uses allocator-level monitoring for better accuracy (#12)
- `DucklakeTableManager` uses per-table locking to reduce contention (#13)
- Standardized all logging to SLF4J (#15)

### Fixed
- Data path validation now requires proper URI schemes (`s3://` or `file://`) (#14)
- Integration tests use appropriate flush thresholds for faster execution
- Removed CodeQL advanced setup conflict with GitHub default setup

## [0.0.62] - Previous Release

### Added
- Buffering support for larger file sizes (#1)
- Configurable flush thresholds: `flush.size`, `flush.interval.ms`, `file.size.bytes`
- Time-based flush with background scheduler
- Memory pressure detection triggers early flush

### Changed
- Enhanced `DucklakeWriter` to use explicit target column list in MERGE statements
- Improved error logging with concise metadata sample (topic:partition@offset)

## [0.0.50-0.0.61] - Schema & Error Handling Improvements

### Added
- Arrow IPC support for Ducklake connector
- Consumer poll record limit configuration
- Sample value collection for improved error messaging

### Changed
- Enhanced schema inference to handle null values
- `ArrowSchemaMerge` handles structurally identical fields
- Schema unification collects sample values for better error messages

### Fixed
- Memory management improvements in vector allocation
- Use `setSafe` for VarCharVector and VarBinaryVector to prevent data loss
- Handle ArithmeticException during vector allocation
- Ignore fields where all values are empty structures

## [0.0.33-0.0.49] - Partitioning & Expression Support

### Changed
- Updated partitioning functionality to support flexible expressions
- Dropped unsupported DuckDB expressions

### Fixed
- Various logging and error message improvements

## [0.0.26-0.0.32] - Initial Stabilization

### Added
- End-to-end integration tests with Testcontainers (Kafka, PostgreSQL, MinIO)
- CI/CD pipeline with GitHub Actions

### Changed
- Refactored to use MERGE INTO for upserts
- Enhanced timestamp handling and validation

### Fixed
- S3 secret access key naming
- SpotBugs issues (P1, P2)
- Spotless formatting

## [0.0.1] - Initial Release

### Added
- DuckLake Kafka Connect Sink Connector
- Apache Arrow integration for efficient data handling
- Support for S3 and local file storage
- Schema evolution support
- Configurable table and schema management
