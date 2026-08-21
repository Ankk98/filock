# filock

Thread-safe, closure-based file locking for Rust using `flock(2)`.

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.85+-orange.svg)](https://www.rust-lang.org)

## Features

- **Closure-based API** — locks are held for exactly the closure's duration, preventing leaks
- **Thread-safe** — `FileStore` is `Send + Sync`
- **Deadlock-free** — by construction within the same process
- **Condvar-based waiting** — no spin-waiting, efficient blocking
- **Non-blocking try-locks** — `try_with_read` / `try_with_write`
- **Timeout support** — `with_read_timeout` / `with_write_timeout`
- **Minimal dependencies** — only `libc`
- **Advisory locking** — works across processes using `flock(2)`

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
filock = "0.1"
```

```rust
use filock::FileStore;
use std::io::{Read, Write};

fn main() -> std::io::Result<()> {
    let store = FileStore::new("/tmp/myapp")?;

    // Write data
    store.with_write("data.json", |file| {
        writeln!(file, "{{\"key\": \"value\"}}")?;
        Ok(())
    })?;

    // Read data
    let content: String = store.with_read("data.json", |file| {
        let mut buf = String::new();
        file.read_to_string(&mut buf)?;
        Ok(buf)
    })?;
    println!("{}", content);

    Ok(())
}
```

## API

### Blocking locks

```rust
// Shared (read) — multiple threads can hold simultaneously
store.with_read("file.txt", |file| { /* ... */ })?;

// Exclusive (write) — only one holder at a time
store.with_write("file.txt", |file| { /* ... */ })?;
```

### Non-blocking try-locks

```rust
// Returns Some(result) if lock acquired, None if contended
let result = store.try_with_write("file.txt", |file| { /* ... */ })?;

match result {
    Some(val) => println!("Got lock: {:?}", val),
    None      => println!("File is locked by another thread"),
}
```

### Timeout locks

```rust
use std::time::Duration;

// Returns Err(FileLockError::Timeout) if deadline exceeded
store.with_write_timeout(
    "file.txt",
    Duration::from_secs(5),
    |file| { /* ... */ },
)?;
```

### Read-modify-write

```rust
store.modify("counter.txt", |file| {
    let mut buf = String::new();
    file.read_to_string(&mut buf)?;
    let val: u64 = buf.trim().parse().unwrap_or(0);

    file.seek(std::io::SeekFrom::Start(0))?;
    file.set_len(0)?;
    writeln!(file, "{}", val + 1)?;
    Ok(())
})?;
```

### Introspection

```rust
if store.is_locked("file.txt")? {
    let holders = store.holder_count("file.txt")?;
    println!("{} threads holding lock", holders);
}
```

## Lock Compatibility

| Held \ Requested | Shared | Exclusive |
|------------------|--------|-----------|
| None             | ✅ Grant | ✅ Grant |
| Shared (other)   | ✅ Grant | ❌ Block |
| Shared (self)    | ✅ Grant | ❌ No upgrade |
| Exclusive (other)| ❌ Block | ❌ Block |
| Exclusive (self) | ❌ No downgrade | ❌ No re-entrant |

## Thread Safety

- Within the same process: full thread safety with Condvar-based blocking
- Across processes: advisory `flock(2)` locking (cooperative processes only)

## License

MIT — see [LICENSE](LICENSE) for details.
