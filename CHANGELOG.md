# Changelog

All notable changes to `filock` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0-alpha.1] - 2026-08-21

### Added

- Closure-based file locking API (`with_read`, `with_write`)
- Non-blocking try-locks (`try_with_read`, `try_with_write`)
- Timeout-based locks (`with_read_timeout`, `with_write_timeout`)
- Read-modify-write convenience method (`modify`)
- Introspection methods (`is_locked`, `holder_count`)
- Thread-safe `FileStore` with `Send + Sync`
- Deadlock-free by construction within same process
- Condvar-based blocking (no spin-waiting)
- Advisory `flock(2)` locking for cross-process coordination
- Panic-safe closure execution (`catch_unwind`)

### Fixed

- HolderId thread reuse bug — now uses thread-local atomic counter
- Thundering herd — `notify_one` for shared lock releases
- Missing `Drop` impl — blocked threads wake on store drop

### Security

- Unique temp file names prevent metadata corruption under concurrent writes

[Unreleased]: https://github.com/Ankk98/filock/compare/v0.1.0-alpha.1...HEAD
[0.1.0-alpha.1]: https://github.com/Ankk98/filock/releases/tag/v0.1.0-alpha.1
