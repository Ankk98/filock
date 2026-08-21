//! # filock
//!
//! Thread-safe, closure-based file locking for Rust using `flock(2)`.
//!
//! `filock` provides a simple API for concurrent file access where locks are
//! held for exactly the duration of a closure. This eliminates lock leaks and
//! makes concurrent file access safe and predictable.
//!
//! ## Features
//!
//! - **Closure-based API** — locks are held for exactly the closure's duration
//! - **Thread-safe** — `FileStore` is `Send + Sync`
//! - **Deadlock-free** — by construction within the same process
//! - **Condvar-based waiting** — no spin-waiting, efficient blocking
//! - **Non-blocking try-locks** — `try_with_read` / `try_with_write`
//! - **Timeout support** — `with_read_timeout` / `with_write_timeout`
//! - **Minimal dependencies** — only `libc`
//! - **Advisory locking** — works with other processes using `flock(2)`
//!
//! ## Quick Start
//!
//! ```no_run
//! use filock::FileStore;
//!
//! let store = FileStore::new("/tmp/myapp").unwrap();
//!
//! // Write data
//! store.with_write("data.json", |file| {
//!     use std::io::Write;
//!     writeln!(file, "{{\"key\": \"value\"}}")?;
//!     Ok(())
//! }).unwrap();
//!
//! // Read data
//! let content: String = store.with_read("data.json", |file| {
//!     use std::io::Read;
//!     let mut buf = String::new();
//!     let mut reader = std::io::BufReader::new(file);
//!     reader.read_to_string(&mut buf)?;
//!     Ok(buf)
//! }).unwrap();
//! ```

mod error;
mod fd;
mod store;

pub use error::{FileLockError, LockType};
pub use store::FileStore;

/// Version of this crate.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
