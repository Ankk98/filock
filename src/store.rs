//! The core `FileStore` — a lock manager for concurrent file access.
//!
//! `FileStore` provides closure-based locking where the lock is held for
//! exactly the duration of the closure. This prevents lock leaks and
//! makes the API simple and safe.
//!
//! # Threading Model
//!
//! - **Within the same process:** Thread-safe with Condvar-based blocking.
//!   No spin-waiting. Deadlock-free by construction (lock upgrades are
//!   blocked, not silently allowed).
//! - **Across processes:** Advisory `flock(2)` locking. Other processes
//!   using `fcntl` locks or ignoring locks entirely will not be blocked.
//!
//! # Lock Compatibility
//!
//! | Held \\ Requested | Shared    | Exclusive     |
//! |-------------------|-----------|---------------|
//! | None              | Grant     | Grant         |
//! | Shared (other)    | Grant     | Block         |
//! | Shared (self)     | Grant     | No upgrade    |
//! | Exclusive (other) | Block     | Block         |
//! | Exclusive (self)  | No down   | No re-entrant |

use std::collections::HashMap;
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::error::{FileLockError, LockType};
use crate::fd;

/// Global counter for generating unique HolderIds.
static HOLDER_COUNTER: AtomicU64 = AtomicU64::new(1);

// ============================================================================
// Public API
// ============================================================================

/// The central lock manager for file access.
///
/// `FileStore` is `Send + Sync` — it can be shared across threads safely.
/// Create one per application (or per logical "directory of coordinated files").
///
/// When a `FileStore` is dropped, all threads blocked on lock acquisition
/// will be woken and receive an `io::Error` of kind `Other`.
pub struct FileStore {
    root: PathBuf,
    state: Mutex<StoreState>,
    cv: std::sync::Condvar,
    /// Shared shutdown flag — set to true on Drop to wake blocked threads.
    shutdown: Arc<AtomicBool>,
}

struct StoreState {
    locks: HashMap<PathBuf, FileLockInfo>,
}

/// Per-file lock state.
struct FileLockInfo {
    /// Map from thread holder to the lock type they hold.
    holders: HashMap<HolderId, LockType>,
    /// The effective lock type: Exclusive if any holder has Exclusive,
    /// Shared otherwise. None if no holders.
    effective: Option<LockType>,
}

/// Opaque identifier for the current thread.
///
/// Uses a thread-local atomic counter to guarantee uniqueness.
/// Unlike hashing `ThreadId`, this never collides and is never reused
/// across thread lifetimes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct HolderId(u64);

impl HolderId {
    fn current() -> Self {
        thread_local! {
            static ID: u64 = HOLDER_COUNTER.fetch_add(1, Ordering::Relaxed);
        }
        ID.with(|&id| Self(id))
    }
}

// ============================================================================
// FileLockInfo
// ============================================================================

impl FileLockInfo {
    fn new() -> Self {
        Self {
            holders: HashMap::new(),
            effective: None,
        }
    }

    /// Check whether the requested lock type can be granted to `requester`
    /// given the current holders.
    fn can_grant(&self, requested: LockType, requester: HolderId) -> bool {
        if self.holders.is_empty() {
            return true;
        }

        for (holder, &held_type) in &self.holders {
            if *holder == requester {
                // Same thread re-entrancy:
                //   Shared → Shared: allowed (multiple read fds, no deadlock)
                //   Everything else: blocked (no upgrade, no downgrade, no re-entrant exclusive)
                return matches!((held_type, requested), (LockType::Shared, LockType::Shared));
            }
            // Different thread:
            //   Shared → Shared: compatible
            //   Anything else: incompatible
            if !matches!(
                (held_type, requested),
                (LockType::Shared, LockType::Shared)
            ) {
                return false;
            }
        }
        true
    }

    fn add_holder(&mut self, holder: HolderId, lock_type: LockType) {
        self.holders.insert(holder, lock_type);
        self.effective = Some(match self.effective {
            Some(LockType::Exclusive) => LockType::Exclusive,
            _ => lock_type,
        });
    }

    fn remove_holder(&mut self, holder: &HolderId) {
        self.holders.remove(holder);
        self.effective = if self.holders.is_empty() {
            None
        } else if self.holders.values().all(|lt| *lt == LockType::Shared) {
            Some(LockType::Shared)
        } else {
            Some(LockType::Exclusive)
        };
    }

    fn is_empty(&self) -> bool {
        self.holders.is_empty()
    }
}

// ============================================================================
// FileStore implementation
// ============================================================================

impl FileStore {
    /// Create a new `FileStore` rooted at the given directory.
    ///
    /// The directory is created if it doesn't exist and canonicalized.
    pub fn new(root: impl AsRef<Path>) -> io::Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        let root = fs::canonicalize(&root)?;

        Ok(Self {
            root,
            state: Mutex::new(StoreState {
                locks: HashMap::new(),
            }),
            cv: std::sync::Condvar::new(),
            shutdown: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Get the root directory of this store.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Resolve a relative path to an absolute path under the store root.
    /// Creates parent directories if needed.
    fn resolve_path(&self, path: &Path) -> io::Result<PathBuf> {
        let abs = self.root.join(path);
        if let Some(parent) = abs.parent() {
            fs::create_dir_all(parent)?;
        }
        if abs.exists() {
            fs::canonicalize(&abs)
        } else {
            Ok(abs)
        }
    }

    // ------------------------------------------------------------------
    // Blocking locks
    // ------------------------------------------------------------------

    /// Execute a closure with shared (read) access to a file.
    ///
    /// Multiple threads can hold shared locks simultaneously.
    /// Blocks if any thread holds an exclusive lock.
    pub fn with_read<R>(
        &self,
        path: impl AsRef<Path>,
        f: impl FnOnce(&mut File) -> io::Result<R>,
    ) -> io::Result<R> {
        self.acquire_and_execute(path.as_ref(), LockType::Shared, f, None)
            .map_err(io_error_from_lock_error)
    }

    /// Execute a closure with exclusive (write) access to a file.
    ///
    /// Only one thread can hold an exclusive lock. All other requests
    /// (shared or exclusive) are blocked.
    pub fn with_write<R>(
        &self,
        path: impl AsRef<Path>,
        f: impl FnOnce(&mut File) -> io::Result<R>,
    ) -> io::Result<R> {
        self.acquire_and_execute(path.as_ref(), LockType::Exclusive, f, None)
            .map_err(io_error_from_lock_error)
    }

    // ------------------------------------------------------------------
    // Non-blocking try-locks
    // ------------------------------------------------------------------

    /// Try to acquire a shared (read) lock without blocking.
    ///
    /// Returns `Some(result)` if the lock was acquired and the closure ran,
    /// or `None` if the lock is currently held exclusively by another thread.
    pub fn try_with_read<R>(
        &self,
        path: impl AsRef<Path>,
        f: impl FnOnce(&mut File) -> io::Result<R>,
    ) -> io::Result<Option<R>> {
        self.try_acquire_and_execute(path.as_ref(), LockType::Shared, f)
    }

    /// Try to acquire an exclusive (write) lock without blocking.
    ///
    /// Returns `Some(result)` if the lock was acquired and the closure ran,
    /// or `None` if the lock is currently held by any other thread.
    pub fn try_with_write<R>(
        &self,
        path: impl AsRef<Path>,
        f: impl FnOnce(&mut File) -> io::Result<R>,
    ) -> io::Result<Option<R>> {
        self.try_acquire_and_execute(path.as_ref(), LockType::Exclusive, f)
    }

    // ------------------------------------------------------------------
    // Timeout locks
    // ------------------------------------------------------------------

    /// Acquire a shared (read) lock with a timeout.
    pub fn with_read_timeout<R>(
        &self,
        path: impl AsRef<Path>,
        timeout: Duration,
        f: impl FnOnce(&mut File) -> io::Result<R>,
    ) -> Result<R, FileLockError> {
        self.acquire_and_execute(path.as_ref(), LockType::Shared, f, Some(timeout))
    }

    /// Acquire an exclusive (write) lock with a timeout.
    pub fn with_write_timeout<R>(
        &self,
        path: impl AsRef<Path>,
        timeout: Duration,
        f: impl FnOnce(&mut File) -> io::Result<R>,
    ) -> Result<R, FileLockError> {
        self.acquire_and_execute(path.as_ref(), LockType::Exclusive, f, Some(timeout))
    }

    // ------------------------------------------------------------------
    // Convenience
    // ------------------------------------------------------------------

    /// Read-modify-write pattern with exclusive lock.
    ///
    /// Identical to `with_write` — provided for semantic clarity when
    /// performing read-modify-write operations.
    pub fn modify<R>(
        &self,
        path: impl AsRef<Path>,
        f: impl FnOnce(&mut File) -> io::Result<R>,
    ) -> io::Result<R> {
        self.with_write(path, f)
    }

    // ------------------------------------------------------------------
    // Introspection
    // ------------------------------------------------------------------

    /// Check if a file currently has any lock holders.
    pub fn is_locked(&self, path: impl AsRef<Path>) -> io::Result<bool> {
        let abs_path = self.resolve_path(path.as_ref())?;
        let state = self.state.lock().unwrap();
        Ok(state
            .locks
            .get(&abs_path)
            .map(|info| !info.is_empty())
            .unwrap_or(false))
    }

    /// Get the number of current lock holders for a file.
    pub fn holder_count(&self, path: impl AsRef<Path>) -> io::Result<usize> {
        let abs_path = self.resolve_path(path.as_ref())?;
        let state = self.state.lock().unwrap();
        Ok(state
            .locks
            .get(&abs_path)
            .map(|info| info.holders.len())
            .unwrap_or(0))
    }

    // ====================================================================
    // Internal implementation
    // ====================================================================

    /// Core blocking lock acquisition with Condvar-based waiting.
    ///
    /// Uses the standard Condvar pattern:
    /// 1. Lock the state mutex
    /// 2. Check if the lock can be granted
    /// 3. If not, `cv.wait()` atomically releases the mutex and sleeps
    /// 4. On wake, re-check (re-acquires mutex automatically)
    /// 5. When done, update state and notify all waiters
    fn acquire_and_execute<R>(
        &self,
        path: &Path,
        lock_type: LockType,
        f: impl FnOnce(&mut File) -> io::Result<R>,
        timeout: Option<Duration>,
    ) -> Result<R, FileLockError> {
        let abs_path = self.resolve_path(path)?;
        let holder = HolderId::current();
        let deadline = timeout.map(|d| Instant::now() + d);

        // Phase 1: Wait for in-process lock availability.
        //
        // Lock ordering: state is the only mutex acquired. The Condvar
        // atomically releases it during wait and re-acquires on wake.
        // The notifier also only acquires state → releases → notifies.
        // Single mutex = no deadlock possible.
        {
            let mut state = self.state.lock().unwrap();

            loop {
                let can_grant = state
                    .locks
                    .get(&abs_path)
                    .map(|info| info.can_grant(lock_type, holder))
                    .unwrap_or(true);

                if can_grant {
                    // Grant the in-process lock
                    let lock_info = state
                        .locks
                        .entry(abs_path.clone())
                        .or_insert_with(FileLockInfo::new);
                    lock_info.add_holder(holder, lock_type);
                    break;
                }

                // Must wait — use timeout or infinite wait
                // Check shutdown flag
                if self.shutdown.load(Ordering::SeqCst) {
                    return Err(FileLockError::Io(io::Error::new(
                        io::ErrorKind::Other,
                        "FileStore dropped while lock was pending",
                    )));
                }

                match deadline {
                    Some(deadline) => {
                        let now = Instant::now();
                        if now >= deadline {
                            return Err(FileLockError::Timeout {
                                path: abs_path,
                                lock_type,
                                timeout: timeout.unwrap(),
                            });
                        }
                        let remaining = deadline - now;
                        let result = self.cv.wait_timeout(state, remaining).unwrap();
                        state = result.0;

                        // Check shutdown after waking
                        if self.shutdown.load(Ordering::SeqCst) {
                            return Err(FileLockError::Io(io::Error::new(
                                io::ErrorKind::Other,
                                "FileStore dropped while lock was pending",
                            )));
                        }

                        if result.1.timed_out() {
                            // Double-check after re-acquiring the mutex
                            let can = state
                                .locks
                                .get(&abs_path)
                                .map(|info| info.can_grant(lock_type, holder))
                                .unwrap_or(true);
                            if can {
                                let lock_info = state
                                    .locks
                                    .entry(abs_path.clone())
                                    .or_insert_with(FileLockInfo::new);
                                lock_info.add_holder(holder, lock_type);
                                break;
                            }
                            return Err(FileLockError::Timeout {
                                path: abs_path,
                                lock_type,
                                timeout: timeout.unwrap(),
                            });
                        }
                        // Spurious wakeup or genuine notification — loop back
                    }
                    None => {
                        state = self.cv.wait(state).unwrap();
                        // Check shutdown after waking
                        if self.shutdown.load(Ordering::SeqCst) {
                            return Err(FileLockError::Io(io::Error::new(
                                io::ErrorKind::Other,
                                "FileStore dropped while lock was pending",
                            )));
                        }
                        // Re-check condition after wake
                    }
                }
            }
        }
        // Mutex released here

        // Phase 2: Get fd and apply kernel-level flock
        let create = lock_type == LockType::Exclusive;
        let mut file = fd::open_for_lock(&abs_path, create).map_err(|e| {
            self.release_lock(&abs_path, holder);
            FileLockError::Io(e)
        })?;

        let raw_fd = std::os::unix::io::AsRawFd::as_raw_fd(&file);

        let flock_op = match lock_type {
            LockType::Shared => libc::LOCK_SH,
            LockType::Exclusive => libc::LOCK_EX,
        };

        let result = unsafe { libc::flock(raw_fd, flock_op) };
        if result != 0 {
            let err = io::Error::last_os_error();
            drop(file);
            self.release_lock(&abs_path, holder);
            return Err(FileLockError::Io(err));
        }

        // Phase 3: Execute the closure with panic safety
        let closure_result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(&mut file)));

        // Phase 4: Release kernel lock and in-process lock
        //
        // Order matters for correctness:
        // 1. Drop the file (closes fd, implicit unlock on close)
        // 2. Release in-process lock and notify waiters
        // This way, no other thread can acquire the lock before we've
        // actually released the kernel-level lock.
        drop(file);
        self.release_lock(&abs_path, holder);

        // Phase 5: Return result
        match closure_result {
            Ok(inner) => inner.map_err(FileLockError::Io),
            Err(panic_err) => std::panic::resume_unwind(panic_err),
        }
    }

    /// Non-blocking lock acquisition.
    ///
    /// Checks the in-process state, and if compatible, attempts a
    /// non-blocking `flock(LOCK_NB)`. Returns `None` if either check fails.
    fn try_acquire_and_execute<R>(
        &self,
        path: &Path,
        lock_type: LockType,
        f: impl FnOnce(&mut File) -> io::Result<R>,
    ) -> io::Result<Option<R>> {
        let abs_path = self.resolve_path(path)?;
        let holder = HolderId::current();

        // Phase 1: Non-blocking in-process check
        {
            let state = self.state.lock().unwrap();
            if let Some(info) = state.locks.get(&abs_path) {
                if !info.can_grant(lock_type, holder) {
                    return Ok(None);
                }
            }
        }

        // Phase 2: Grant in-process lock
        {
            let mut state = self.state.lock().unwrap();
            let lock_info = state
                .locks
                .entry(abs_path.clone())
                .or_insert_with(FileLockInfo::new);
            lock_info.add_holder(holder, lock_type);
        }

        // Phase 3: Non-blocking kernel flock
        let create = lock_type == LockType::Exclusive;
        let mut file = match fd::open_for_lock(&abs_path, create) {
            Ok(f) => f,
            Err(e) => {
                self.release_lock(&abs_path, holder);
                return Err(e);
            }
        };

        let raw_fd = std::os::unix::io::AsRawFd::as_raw_fd(&file);

        let flock_op = match lock_type {
            LockType::Shared => libc::LOCK_SH | libc::LOCK_NB,
            LockType::Exclusive => libc::LOCK_EX | libc::LOCK_NB,
        };

        let result = unsafe { libc::flock(raw_fd, flock_op) };
        if result != 0 {
            let err = io::Error::last_os_error();
            drop(file);
            self.release_lock(&abs_path, holder);

            if err.raw_os_error() == Some(libc::EWOULDBLOCK)
                || err.raw_os_error() == Some(libc::EAGAIN)
            {
                return Ok(None);
            }
            return Err(err);
        }

        // Phase 4: Execute closure
        let closure_result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(&mut file)));

        // Phase 5: Release
        drop(file);
        self.release_lock(&abs_path, holder);

        match closure_result {
            Ok(inner) => inner.map(Some),
            Err(panic_err) => std::panic::resume_unwind(panic_err),
        }
    }

    /// Release the in-process lock and notify waiters.
    ///
    /// Uses smart notification to reduce thundering herd:
    /// - When releasing an exclusive lock → `notify_all` (multiple shared
    ///   waiters may now be able to proceed)
    /// - When releasing a shared lock and others remain → `notify_one`
    ///   (only an exclusive waiter could be unblocked)
    fn release_lock(&self, path: &Path, holder: HolderId) {
        // Acquire state, modify, release, then notify.
        // The notifier MUST NOT hold state while notifying, otherwise
        // a waiter could wake up, try to acquire state, and deadlock
        // with another notifier.
        let notify_all;
        {
            let mut state = self.state.lock().unwrap();
            if let Some(info) = state.locks.get_mut(path) {
                let was_exclusive = info.effective == Some(LockType::Exclusive);
                info.remove_holder(&holder);
                if info.is_empty() {
                    state.locks.remove(path);
                }
                // Released exclusive → multiple shared waiters can now proceed
                // Released shared with others remaining → only exclusive waiter benefits
                notify_all = was_exclusive;
            } else {
                notify_all = true;
            }
        }
        // Mutex released — now safe to notify
        if notify_all {
            self.cv.notify_all();
        } else {
            self.cv.notify_one();
        }
    }
}

// ============================================================================
// Drop — wake blocked threads on shutdown
// ============================================================================

impl Drop for FileStore {
    fn drop(&mut self) {
        // Signal all blocked threads to wake up and exit
        self.shutdown.store(true, Ordering::SeqCst);
        self.cv.notify_all();
    }
}

fn io_error_from_lock_error(e: FileLockError) -> io::Error {
    match e {
        FileLockError::Io(io_err) => io_err,
        other => io::Error::new(io::ErrorKind::Other, other.to_string()),
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;

    static COUNTER: AtomicUsize = AtomicUsize::new(0);

    fn temp_dir() -> PathBuf {
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("filock_store_{}_{}", std::process::id(), id));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn new_store_creates_directory() {
        let dir = temp_dir();
        let store_dir = dir.join("new_store");
        let _store = FileStore::new(&store_dir).unwrap();
        assert!(store_dir.exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn write_and_read_basic() {
        let dir = temp_dir();
        let store = FileStore::new(&dir).unwrap();

        store
            .with_write("test.txt", |file: &mut File| {
                use std::io::Write;
                file.write_all(b"hello world")?;
                Ok(())
            })
            .unwrap();

        let content: Vec<u8> = store
            .with_read("test.txt", |file: &mut File| {
                use std::io::Read;
                let mut buf = Vec::new();
                file.read_to_end(&mut buf)?;
                Ok(buf)
            })
            .unwrap();

        assert_eq!(content, b"hello world");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn concurrent_readers_allowed() {
        let dir = temp_dir();
        let store = Arc::new(FileStore::new(&dir).unwrap());

        store
            .with_write("shared.txt", |file: &mut File| {
                use std::io::Write;
                file.write_all(b"shared data")?;
                Ok(())
            })
            .unwrap();

        let mut handles = vec![];
        for _ in 0..10 {
            let store = store.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    let content: Vec<u8> = store
                        .with_read("shared.txt", |file: &mut File| {
                            use std::io::Read;
                            let mut buf = Vec::new();
                            file.read_to_end(&mut buf)?;
                            Ok(buf)
                        })
                        .unwrap();
                    assert_eq!(content, b"shared data");
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn exclusive_writer_blocks_readers() {
        let dir = temp_dir();
        let store = Arc::new(FileStore::new(&dir).unwrap());

        store
            .with_write("contested.txt", |file: &mut File| {
                use std::io::Write;
                writeln!(file, "original")?;
                Ok::<(), io::Error>(())
            })
            .unwrap();

        let reader_started = Arc::new(AtomicBool::new(false));
        let reader_done = Arc::new(AtomicBool::new(false));
        let writer_done = Arc::new(AtomicBool::new(false));

        let rs = reader_started.clone();
        let rd = reader_done.clone();
        let s = store.clone();

        let reader_handle = thread::spawn(move || {
            s.with_read("contested.txt", |file: &mut File| {
                rs.store(true, Ordering::SeqCst);
                thread::sleep(Duration::from_millis(50));
                use std::io::Read;
                let mut buf = Vec::new();
                file.read_to_end(&mut buf)?;
                Ok(buf)
            })
            .unwrap();
            rd.store(true, Ordering::SeqCst);
        });

        // Wait for reader to start holding the lock
        while !reader_started.load(Ordering::SeqCst) {
            thread::yield_now();
        }
        thread::sleep(Duration::from_millis(10));

        let wd = writer_done.clone();
        let s = store.clone();
        let writer_handle = thread::spawn(move || {
            s.with_write("contested.txt", |file: &mut File| {
                use std::io::Write;
                writeln!(file, "updated")?;
                Ok::<(), io::Error>(())
            })
            .unwrap();
            wd.store(true, Ordering::SeqCst);
        });

        // Writer should NOT be done yet (reader still holds lock)
        thread::sleep(Duration::from_millis(20));
        assert!(!writer_done.load(Ordering::SeqCst));

        reader_handle.join().unwrap();
        writer_handle.join().unwrap();

        assert!(reader_done.load(Ordering::SeqCst));
        assert!(writer_done.load(Ordering::SeqCst));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn try_with_write_returns_none_when_contended() {
        let dir = temp_dir();
        let store = Arc::new(FileStore::new(&dir).unwrap());

        store
            .with_write("try.txt", |file: &mut File| {
                use std::io::Write;
                writeln!(file, "data")?;
                Ok::<(), io::Error>(())
            })
            .unwrap();

        let started = Arc::new(AtomicBool::new(false));
        let sc = started.clone();
        let s = store.clone();

        let handle = thread::spawn(move || {
            s.with_write("try.txt", |file: &mut File| {
                sc.store(true, Ordering::SeqCst);
                thread::sleep(Duration::from_millis(100));
                use std::io::Read;
                let mut buf = Vec::new();
                file.read_to_end(&mut buf)?;
                Ok(buf)
            })
            .unwrap();
        });

        while !started.load(Ordering::SeqCst) {
            thread::yield_now();
        }

        let result = store.try_with_write("try.txt", |_file: &mut File| Ok(())).unwrap();
        assert!(result.is_none());

        handle.join().unwrap();
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn try_with_read_returns_some_when_shared_held() {
        let dir = temp_dir();
        let store = Arc::new(FileStore::new(&dir).unwrap());

        store
            .with_write("tryread.txt", |file: &mut File| {
                use std::io::Write;
                writeln!(file, "data")?;
                Ok::<(), io::Error>(())
            })
            .unwrap();

        let started = Arc::new(AtomicBool::new(false));
        let sc = started.clone();
        let s = store.clone();

        let handle = thread::spawn(move || {
            s.with_read("tryread.txt", |file: &mut File| {
                sc.store(true, Ordering::SeqCst);
                thread::sleep(Duration::from_millis(100));
                use std::io::Read;
                let mut buf = Vec::new();
                file.read_to_end(&mut buf)?;
                Ok(buf)
            })
            .unwrap();
        });

        while !started.load(Ordering::SeqCst) {
            thread::yield_now();
        }

        // Shared lock held — another reader should succeed
        let result = store.try_with_read("tryread.txt", |_file: &mut File| Ok(42)).unwrap();
        assert_eq!(result, Some(42));

        handle.join().unwrap();
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn with_write_timeout_expires() {
        let dir = temp_dir();
        let store = Arc::new(FileStore::new(&dir).unwrap());

        store
            .with_write("timeout.txt", |file: &mut File| {
                use std::io::Write;
                writeln!(file, "data")?;
                Ok::<(), io::Error>(())
            })
            .unwrap();

        let started = Arc::new(AtomicBool::new(false));
        let sc = started.clone();
        let s = store.clone();

        let handle = thread::spawn(move || {
            s.with_write("timeout.txt", |file: &mut File| {
                sc.store(true, Ordering::SeqCst);
                thread::sleep(Duration::from_millis(200));
                use std::io::Write;
                writeln!(file, "more")?;
                Ok::<(), io::Error>(())
            })
            .unwrap();
        });

        while !started.load(Ordering::SeqCst) {
            thread::yield_now();
        }

        let result = store.with_write_timeout(
            "timeout.txt",
            Duration::from_millis(50),
            |_file: &mut File| Ok(()),
        );
        assert!(result.is_err());
        match result.unwrap_err() {
            FileLockError::Timeout { .. } => {}
            other => panic!("expected Timeout, got {:?}", other),
        }

        handle.join().unwrap();
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn multiple_files_independent() {
        let dir = temp_dir();
        let store = Arc::new(FileStore::new(&dir).unwrap());

        let mut handles = vec![];

        for i in 0..5 {
            let store = store.clone();
            let name = format!("file_{}.txt", i);
            handles.push(thread::spawn(move || {
                for j in 0..50 {
                    store
                        .with_write(&name, |file: &mut File| {
                            use std::io::Write;
                            writeln!(file, "{}-{}", i, j)?;
                            Ok::<(), io::Error>(())
                        })
                        .unwrap();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn holder_count_tracks_correctly() {
        let dir = temp_dir();
        let store = Arc::new(FileStore::new(&dir).unwrap());

        // Create the file first
        store.with_write("x.txt", |file: &mut File| {
            use std::io::Write;
            file.write_all(b"test")?;
            Ok(())
        }).unwrap();

        assert_eq!(store.holder_count("x.txt").unwrap(), 0);
        assert!(!store.is_locked("x.txt").unwrap());

        let started = Arc::new(AtomicBool::new(false));
        let sc = started.clone();
        let s = store.clone();

        let handle = thread::spawn(move || {
            s.with_read("x.txt", |_file: &mut File| {
                sc.store(true, Ordering::SeqCst);
                thread::sleep(Duration::from_millis(50));
                Ok(())
            })
            .unwrap();
        });

        while !started.load(Ordering::SeqCst) {
            thread::yield_now();
        }
        thread::sleep(Duration::from_millis(5));

        // Verify the lock is released after the thread finishes
        handle.join().unwrap();

        assert_eq!(store.holder_count("x.txt").unwrap(), 0);
        assert!(!store.is_locked("x.txt").unwrap());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn modify_works_like_with_write() {
        let dir = temp_dir();
        let store = FileStore::new(&dir).unwrap();

        store
            .modify("data.txt", |file: &mut File| {
                use std::io::Write;
                writeln!(file, "modified")?;
                Ok::<(), io::Error>(())
            })
            .unwrap();

        let content: Vec<u8> = store
            .with_read("data.txt", |file: &mut File| {
                use std::io::Read;
                let mut buf = Vec::new();
                file.read_to_end(&mut buf)?;
                Ok(buf)
            })
            .unwrap();

        assert_eq!(content, b"modified\n");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn writer_excludes_other_writers() {
        let dir = temp_dir();
        let store = Arc::new(FileStore::new(&dir).unwrap());

        let counter = Arc::new(AtomicUsize::new(0));

        let mut handles = vec![];
        for _ in 0..5 {
            let store = store.clone();
            let counter = counter.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..20 {
                    store
                        .with_write("counter.txt", |_file: &mut File| {
                            let val = counter.load(Ordering::SeqCst);
                            // Small delay to increase chance of race
                            thread::yield_now();
                            counter.store(val + 1, Ordering::SeqCst);
                            Ok(())
                        })
                        .unwrap();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // With proper exclusive locking, no increments should be lost
        assert_eq!(counter.load(Ordering::SeqCst), 100);

        let _ = fs::remove_dir_all(&dir);
    }
}
