//! Multi-threaded concurrency tests for filock.
//!
//! These tests verify that FileStore correctly coordinates concurrent access
//! from multiple threads, including race conditions, lock contention, and
//! stress scenarios.

use filock::{FileStore, FileLockError};
use std::fs::{self, File};
use std::io::{self, Read, Seek, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

static COUNTER: AtomicUsize = AtomicUsize::new(0);

fn temp_dir() -> PathBuf {
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("filock_mt_{}_{}", std::process::id(), id));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

// ---------------------------------------------------------------------------
// Lock contention tests
// ---------------------------------------------------------------------------

/// Multiple writers must not corrupt data — the counter increment test.
#[test]
fn concurrent_writers_no_data_corruption() {
    let dir = temp_dir();
    let store = Arc::new(FileStore::new(&dir).unwrap());

    // Create file with initial content
    store
        .with_write("data.txt", |file: &mut File| {
            writeln!(file, "0")?;
            Ok(())
        })
        .unwrap();

    let num_writers = 8;
    let writes_per_thread = 50;
    let barrier = Arc::new(Barrier::new(num_writers));

    let mut handles = vec![];
    for _ in 0..num_writers {
        let store = store.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            for _ in 0..writes_per_thread {
                store
                    .with_write("data.txt", |file: &mut File| {
                        // Read current value, increment, write back
                        file.seek(io::SeekFrom::Start(0))?;
                        let mut content = String::new();
                        file.read_to_string(&mut content)?;
                        let val: u64 = content.trim().parse().unwrap_or(0);

                        // Overwrite the file with new value
                        file.seek(io::SeekFrom::Start(0))?;
                        file.set_len(0)?;
                        writeln!(file, "{}", val + 1)?;
                        Ok(())
                    })
                    .unwrap();
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify final count
    let content: String = store
        .with_read("data.txt", |file: &mut File| {
            let mut buf = String::new();
            file.read_to_string(&mut buf)?;
            Ok(buf)
        })
        .unwrap();

    let final_val: u64 = content.trim().parse().unwrap();
    assert_eq!(
        final_val,
        (num_writers * writes_per_thread) as u64,
        "Lost writes detected: expected {} but got {}",
        num_writers * writes_per_thread,
        final_val
    );

    let _ = fs::remove_dir_all(&dir);
}

/// Readers don't block each other — concurrent shared locks.
#[test]
fn many_concurrent_readers() {
    let dir = temp_dir();
    let store = Arc::new(FileStore::new(&dir).unwrap());

    store
        .with_write("readme.txt", |file: &mut File| {
            file.write_all(b"constant content for readers")?;
            Ok(())
        })
        .unwrap();

    let num_readers = 20;
    let barrier = Arc::new(Barrier::new(num_readers));
    let error_count = Arc::new(AtomicUsize::new(0));

    let mut handles = vec![];
    for _ in 0..num_readers {
        let store = store.clone();
        let barrier = barrier.clone();
        let error_count = error_count.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            for _ in 0..100 {
                let result = store.with_read("readme.txt", |file: &mut File| {
                    let mut buf = Vec::new();
                    file.read_to_end(&mut buf)?;
                    Ok(buf)
                });
                match result {
                    Ok(content) => assert_eq!(content, b"constant content for readers"),
                    Err(_) => {
                        error_count.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }
    assert_eq!(error_count.load(Ordering::SeqCst), 0);

    let _ = fs::remove_dir_all(&dir);
}

/// Writer blocks all readers until released.
#[test]
fn writer_excludes_readers_until_release() {
    let dir = temp_dir();
    let store = Arc::new(FileStore::new(&dir).unwrap());

    // Initialize file
    store
        .with_write("block.txt", |file: &mut File| {
            writeln!(file, "original")?;
            Ok(())
        })
        .unwrap();

    let writer_started = Arc::new(AtomicBool::new(false));
    let writer_can_finish = Arc::new(AtomicBool::new(false));
    let readers_passed = Arc::new(AtomicUsize::new(0));

    let ws = writer_started.clone();
    let wcf = writer_can_finish.clone();
    let s = store.clone();

    // Writer thread: holds exclusive lock for 100ms
    let writer_handle = thread::spawn(move || {
        s.with_write("block.txt", |file: &mut File| {
            ws.store(true, Ordering::SeqCst);
            // Wait until we're told we can finish
            while !wcf.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(5));
            }
            writeln!(file, "updated")?;
            Ok(())
        })
        .unwrap();
    });

    // Wait for writer to hold the lock
    while !writer_started.load(Ordering::SeqCst) {
        thread::yield_now();
    }
    thread::sleep(Duration::from_millis(10));

    // Spawn readers that should block
    let mut reader_handles = vec![];
    for _ in 0..5 {
        let store = store.clone();
        let rp = readers_passed.clone();
        reader_handles.push(thread::spawn(move || {
            let result = store.with_read("block.txt", |file: &mut File| {
                let mut buf = String::new();
                file.read_to_string(&mut buf)?;
                Ok(buf)
            });
            if result.is_ok() {
                rp.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }

    // Give readers time to start and block
    thread::sleep(Duration::from_millis(20));

    // No readers should have passed yet
    assert_eq!(
        readers_passed.load(Ordering::SeqCst),
        0,
        "readers should be blocked while writer holds lock"
    );

    // Release the writer
    writer_can_finish.store(true, Ordering::SeqCst);
    writer_handle.join().unwrap();

    // Now all readers should complete
    for h in reader_handles {
        h.join().unwrap();
    }
    assert_eq!(readers_passed.load(Ordering::SeqCst), 5);

    let _ = fs::remove_dir_all(&dir);
}

/// try_with_write returns None under contention.
#[test]
fn try_write_returns_none_under_contention() {
    let dir = temp_dir();
    let store = Arc::new(FileStore::new(&dir).unwrap());

    let started = Arc::new(AtomicBool::new(false));
    let sc = started.clone();
    let s = store.clone();

    let writer_handle = thread::spawn(move || {
        s.with_write("try_contention.txt", |file: &mut File| {
            sc.store(true, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(100));
            writeln!(file, "done")?;
            Ok(())
        })
        .unwrap();
    });

    while !started.load(Ordering::SeqCst) {
        thread::yield_now();
    }

    // try_with_write should return None
    let result = store.try_with_write("try_contention.txt", |_file: &mut File| Ok(())).unwrap();
    assert!(result.is_none(), "try_with_write should return None when write-locked");

    // try_with_read should also return None (exclusive held)
    let result = store.try_with_read("try_contention.txt", |_file: &mut File| Ok(())).unwrap();
    assert!(result.is_none(), "try_with_read should return None when write-locked");

    writer_handle.join().unwrap();
    let _ = fs::remove_dir_all(&dir);
}

/// try_with_read succeeds when only shared locks are held.
#[test]
fn try_read_succeeds_under_shared_contention() {
    let dir = temp_dir();
    let store = Arc::new(FileStore::new(&dir).unwrap());

    store
        .with_write("try_shared.txt", |file: &mut File| {
            writeln!(file, "shared data")?;
            Ok(())
        })
        .unwrap();

    let started = Arc::new(AtomicBool::new(false));
    let sc = started.clone();
    let s = store.clone();

    let reader_handle = thread::spawn(move || {
        s.with_read("try_shared.txt", |file: &mut File| {
            sc.store(true, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(100));
            let mut buf = String::new();
            file.read_to_string(&mut buf)?;
            Ok(buf)
        })
        .unwrap();
    });

    while !started.load(Ordering::SeqCst) {
        thread::yield_now();
    }

    // try_with_read should succeed (shared is compatible with shared)
    let result = store.try_with_read("try_shared.txt", |_file: &mut File| Ok(42)).unwrap();
    assert_eq!(result, Some(42));

    reader_handle.join().unwrap();
    let _ = fs::remove_dir_all(&dir);
}

/// Timeout fires correctly when lock is held.
#[test]
fn timeout_fires_when_lock_held() {
    let dir = temp_dir();
    let store = Arc::new(FileStore::new(&dir).unwrap());

    let started = Arc::new(AtomicBool::new(false));
    let sc = started.clone();
    let s = store.clone();

    let writer_handle = thread::spawn(move || {
        s.with_write("timeout_test.txt", |file: &mut File| {
            sc.store(true, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(200));
            writeln!(file, "done")?;
            Ok(())
        })
        .unwrap();
    });

    while !started.load(Ordering::SeqCst) {
        thread::yield_now();
    }

    let start = Instant::now();
    let result = store.with_write_timeout(
        "timeout_test.txt",
        Duration::from_millis(50),
        |_file: &mut File| Ok(()),
    );
    let elapsed = start.elapsed();

    assert!(result.is_err());
    match result.unwrap_err() {
        FileLockError::Timeout { .. } => {}
        other => panic!("expected Timeout, got {:?}", other),
    }

    // Should have taken approximately 50ms, not 200ms
    assert!(
        elapsed < Duration::from_millis(150),
        "timeout took too long: {:?}",
        elapsed
    );

    writer_handle.join().unwrap();
    let _ = fs::remove_dir_all(&dir);
}

/// Timeout succeeds immediately if lock is available.
#[test]
fn timeout_succeeds_immediately_when_available() {
    let dir = temp_dir();
    let store = FileStore::new(&dir).unwrap();

    let result = store.with_write_timeout(
        "timeout_avail.txt",
        Duration::from_millis(100),
        |file: &mut File| {
            writeln!(file, "immediate")?;
            Ok(())
        },
    );

    assert!(result.is_ok());

    let content: String = store
        .with_read("timeout_avail.txt", |file: &mut File| {
            let mut buf = String::new();
            file.read_to_string(&mut buf)?;
            Ok(buf)
        })
        .unwrap();
    assert_eq!(content, "immediate\n");

    let _ = fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// Stress tests
// ---------------------------------------------------------------------------

/// High-contention stress test: many threads, many operations.
#[test]
fn stress_many_threads_many_files() {
    let dir = temp_dir();
    let store = Arc::new(FileStore::new(&dir).unwrap());

    let num_threads = 16;
    let files_per_thread = 5;
    let ops_per_file = 20;
    let barrier = Arc::new(Barrier::new(num_threads));

    let mut handles = vec![];
    for t in 0..num_threads {
        let store = store.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            for f in 0..files_per_thread {
                let name = format!("stress_t{}_f{}.txt", t, f);
                for op in 0..ops_per_file {
                    store
                        .with_write(&name, |file: &mut File| {
                            writeln!(file, "t{}-f{}-op{}", t, f, op)?;
                            Ok(())
                        })
                        .unwrap();
                }
                // Read it back
                let content: String = store
                    .with_read(&name, |file: &mut File| {
                        let mut buf = String::new();
                        file.read_to_string(&mut buf)?;
                        Ok(buf)
                    })
                    .unwrap();
                assert!(content.contains(&format!("t{}-f{}-op{}", t, f, ops_per_file - 1)));
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let _ = fs::remove_dir_all(&dir);
}

/// Rapid lock acquisition and release across threads.
#[test]
fn stress_rapid_lock_churn() {
    let dir = temp_dir();
    let store = Arc::new(FileStore::new(&dir).unwrap());

    store
        .with_write("churn.txt", |file: &mut File| {
            writeln!(file, "0")?;
            Ok(())
        })
        .unwrap();

    let num_threads = 10;
    let ops = 100;
    let barrier = Arc::new(Barrier::new(num_threads));

    let mut handles = vec![];
    for _ in 0..num_threads {
        let store = store.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            for _ in 0..ops {
                store
                    .with_write("churn.txt", |file: &mut File| {
                        file.seek(io::SeekFrom::Start(0))?;
                        let mut buf = String::new();
                        file.read_to_string(&mut buf)?;
                        let val: u64 = buf.trim().parse().unwrap_or(0);
                        file.seek(io::SeekFrom::Start(0))?;
                        file.set_len(0)?;
                        writeln!(file, "{}", val + 1)?;
                        Ok(())
                    })
                    .unwrap();
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let content: String = store
        .with_read("churn.txt", |file: &mut File| {
            let mut buf = String::new();
            file.read_to_string(&mut buf)?;
            Ok(buf)
        })
        .unwrap();
    let final_val: u64 = content.trim().parse().unwrap();
    assert_eq!(final_val, (num_threads * ops) as u64);

    let _ = fs::remove_dir_all(&dir);
}

/// Mixed read/write workload with varying access patterns.
#[test]
fn stress_mixed_read_write_workload() {
    let dir = temp_dir();
    let store = Arc::new(FileStore::new(&dir).unwrap());

    // Create initial files
    for i in 0..10 {
        store
            .with_write(format!("mixed_{}.txt", i), |file: &mut File| {
                writeln!(file, "initial_{}", i)?;
                Ok(())
            })
            .unwrap();
    }

    let num_threads = 12;
    let barrier = Arc::new(Barrier::new(num_threads));

    let mut handles = vec![];
    for t in 0..num_threads {
        let store = store.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            for op in 0..50 {
                let file_idx = (t + op) % 10;
                let name = format!("mixed_{}.txt", file_idx);

                if t % 3 == 0 {
                    // Writer
                    store
                        .with_write(&name, |file: &mut File| {
                            file.seek(io::SeekFrom::Start(0))?;
                            file.set_len(0)?;
                            writeln!(file, "t{}_op{}", t, op)?;
                            Ok(())
                        })
                        .unwrap();
                } else {
                    // Reader
                    let _ = store.with_read(&name, |file: &mut File| {
                        let mut buf = String::new();
                        file.read_to_string(&mut buf)?;
                        // Just verify it's valid UTF-8 and not empty
                        assert!(!buf.is_empty());
                        Ok(())
                    });
                }
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let _ = fs::remove_dir_all(&dir);
}

/// verify panic in closure doesn't poison the store.
#[test]
fn panic_in_closure_does_not_poison_store() {
    let dir = temp_dir();
    let store = Arc::new(FileStore::new(&dir).unwrap());

    // Panic inside closure
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        store.with_write("panic.txt", |_file: &mut File| -> io::Result<()> {
            panic!("intentional panic");
        })
    }));
    assert!(result.is_err());

    // Store should still be usable
    let result = store.with_write("panic.txt", |file: &mut File| {
        writeln!(file, "recovered")?;
        Ok(())
    });
    assert!(result.is_ok());

    let content: String = store
        .with_read("panic.txt", |file: &mut File| {
            let mut buf = String::new();
            file.read_to_string(&mut buf)?;
            Ok(buf)
        })
        .unwrap();
    assert_eq!(content, "recovered\n");

    let _ = fs::remove_dir_all(&dir);
}

/// Drop FileStore while threads are waiting — no hang.
#[test]
fn drop_store_while_waiters() {
    let dir = temp_dir();
    let store = Arc::new(FileStore::new(&dir).unwrap());

    // Hold exclusive lock from main thread via a helper thread
    let started = Arc::new(AtomicBool::new(false));
    let sc = started.clone();
    let s = store.clone();

    let holder = thread::spawn(move || {
        s.with_write("drop_test.txt", |file: &mut File| {
            sc.store(true, Ordering::SeqCst);
            // Hold the lock for a while
            thread::sleep(Duration::from_millis(200));
            writeln!(file, "held")?;
            Ok(())
        })
        .unwrap();
    });

    while !started.load(Ordering::SeqCst) {
        thread::yield_now();
    }

    // Drop our Arc reference — the waiter thread will hold the last one
    drop(store);

    // The holder thread should still finish cleanly
    holder.join().unwrap();

    let _ = fs::remove_dir_all(&dir);
}
