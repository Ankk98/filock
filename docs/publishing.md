# Publishing to crates.io

Step-by-step guide for publishing `filock` to crates.io.

## Prerequisites

1. Create an account at [crates.io](https://crates.io) (sign in with GitHub)
2. Go to [Account Settings → API Tokens](https://crates.io/settings/tokens)
3. Generate a new token and save it

## First-Time Setup

```bash
# Login to crates.io (stores token in ~/.cargo/credentials)
cargo login <your-api-token>
```

## Publishing a New Version

### 1. Update version in Cargo.toml

```toml
[package]
version = "0.1.0"  # ← bump this
```

Follow [Semantic Versioning](https://semver.org/):
- `0.0.x` — initial development, API may change
- `0.x.0` — public API may change
- `x.0.0` — stable, backwards-compatible

### 2. Update CHANGELOG.md (if you have one)

```markdown
## [0.1.0] - 2026-08-21

### Added
- Closure-based file locking with flock(2)
- Shared and exclusive locks
- Non-blocking try-locks
- Timeout support

### Fixed
- HolderId thread reuse bug
- Thundering herd on lock release
```

### 3. Commit and tag

```bash
git add -A
git commit -m "release: v0.1.0"
git tag v0.1.0
git push origin main --tags
```

### 4. Dry run (verify before publishing)

```bash
cargo publish --dry-run
```

Check for warnings or errors. Fix any issues before proceeding.

### 5. Publish

```bash
cargo publish
```

That's it! Your crate is live at [crates.io/filock](https://crates.io/crates/filock).

### 6. Verify

```bash
# Check docs rendered correctly
cargo doc --open

# Or visit https://docs.rs/filock
```

## Version Bump Cheat Sheet

| Change Type | Version Bump | Example |
|-------------|--------------|---------|
| Fix a bug | patch (x.y.Z) | 0.1.0 → 0.1.1 |
| Add feature (backward-compatible) | minor (x.Y.0) | 0.1.1 → 0.2.0 |
| Breaking API change | major (X.0.0) | 0.2.0 → 1.0.0 |

## Troubleshooting

### "crate name is already used"
Crate names are permanent. You cannot reuse a name after yanking.

### "version already exists"
Bump the version number in Cargo.toml.

### "missing required field"
Ensure all required fields are in Cargo.toml:
```toml
[package]
name = "filock"
version = "X.Y.Z"
edition = "2024"
license = "MIT"          # or license-file = "LICENSE"
description = "..."      # required by crates.io
```

### Yanking a version
If you published a broken version:
```bash
cargo yank --version 0.1.0
```
Users won't get it by default, but it's still downloadable.

### Unyanking
```bash
cargo yank --version 0.1.0 --undo
```

## Release Checklist

- [ ] Version bumped in Cargo.toml
- [ ] CHANGELOG.md updated
- [ ] All tests pass (`cargo test`)
- [ ] No warnings (`cargo build 2>&1 | grep warning`)
- [ ] Doc test passes (`cargo test --doc`)
- [ ] Dry run passes (`cargo publish --dry-run`)
- [ ] Committed and tagged
- [ ] Published (`cargo publish`)
- [ ] Verified on crates.io
- [ ] Docs verified on docs.rs
