//! Multi-process tests for filock.
//!
//! These tests verify that flock-based locking works across process
//! boundaries. Since flock is advisory, we can only verify that
//! cooperative processes coordinate correctly.

use std::fs;
use std::io::{self, Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

static COUNTER: AtomicUsize = AtomicUsize::new(0);

fn temp_dir() -> PathBuf {
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("filock_proc_{}_{}", std::process::id(), id));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

// ---------------------------------------------------------------------------
// Cross-process lock coordination tests
// ---------------------------------------------------------------------------

/// Exclusive flock on one fd blocks exclusive flock on another fd.
#[test]
fn exclusive_flock_blocks_exclusive() {
    let dir = temp_dir();
    let file_path = dir.join("excl.txt");
    fs::write(&file_path, "data").unwrap();

    // Hold exclusive flock from one fd
    let file1 = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .unwrap();
    let fd1 = std::os::unix::io::AsRawFd::as_raw_fd(&file1);
    let result = unsafe { libc::flock(fd1, libc::LOCK_EX) };
    assert_eq!(result, 0);

    // Non-blocking exclusive flock on another fd should fail
    let file2 = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .unwrap();
    let fd2 = std::os::unix::io::AsRawFd::as_raw_fd(&file2);
    let result = unsafe { libc::flock(fd2, libc::LOCK_EX | libc::LOCK_NB) };
    assert_eq!(result, -1);
    let err = io::Error::last_os_error();
    assert!(
        err.raw_os_error() == Some(libc::EWOULDBLOCK) || err.raw_os_error() == Some(libc::EAGAIN),
        "expected EWOULDBLOCK or EAGAIN, got {:?}",
        err
    );

    // Release first lock, second should now succeed
    unsafe { libc::flock(fd1, libc::LOCK_UN) };
    drop(file1);

    let result = unsafe { libc::flock(fd2, libc::LOCK_EX | libc::LOCK_NB) };
    assert_eq!(result, 0);
    unsafe { libc::flock(fd2, libc::LOCK_UN) };

    let _ = fs::remove_dir_all(&dir);
}

/// Two processes reading concurrently should both succeed (shared locks).
#[test]
fn two_processes_shared_contention() {
    let dir = temp_dir();
    let file_path = dir.join("shared_read.txt");

    // Create the file first
    fs::write(&file_path, "shared content").unwrap();

    let barrier = Arc::new(Barrier::new(2));

    let b1 = barrier.clone();
    let fp1 = file_path.clone();
    let h1 = thread::spawn(move || {
        // Simulate a process: open file, flock SH, read, verify
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&fp1)
            .unwrap();
        let fd = std::os::unix::io::AsRawFd::as_raw_fd(&file);
        let result = unsafe { libc::flock(fd, libc::LOCK_SH) };
        assert_eq!(result, 0);

        b1.wait();

        let mut buf = String::new();
        let mut f = std::io::BufReader::new(&file);
        f.read_to_string(&mut buf).unwrap();
        assert_eq!(buf, "shared content");

        unsafe { libc::flock(fd, libc::LOCK_UN) };
    });

    let b2 = barrier.clone();
    let fp2 = file_path.clone();
    let h2 = thread::spawn(move || {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&fp2)
            .unwrap();
        let fd = std::os::unix::io::AsRawFd::as_raw_fd(&file);
        let result = unsafe { libc::flock(fd, libc::LOCK_SH) };
        assert_eq!(result, 0);

        b2.wait();

        let mut buf = String::new();
        let mut f = std::io::BufReader::new(&file);
        f.read_to_string(&mut buf).unwrap();
        assert_eq!(buf, "shared content");

        unsafe { libc::flock(fd, libc::LOCK_UN) };
    });

    h1.join().unwrap();
    h2.join().unwrap();
    let _ = fs::remove_dir_all(&dir);
}

/// Non-blocking flock (LOCK_NB) returns immediately with EWOULDBLOCK
/// when another process holds an exclusive lock.
#[test]
fn non_blocking_flock_returns_would_block() {
    let dir = temp_dir();
    let file_path = dir.join("nb_test.txt");
    fs::write(&file_path, "data").unwrap();

    // Hold exclusive lock from another thread (simulates another process)
    let file1 = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .unwrap();
    let fd1 = std::os::unix::io::AsRawFd::as_raw_fd(&file1);
    let result = unsafe { libc::flock(fd1, libc::LOCK_EX) };
    assert_eq!(result, 0);

    // Try non-blocking exclusive lock from current "process"
    let file2 = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .unwrap();
    let fd2 = std::os::unix::io::AsRawFd::as_raw_fd(&file2);
    let result = unsafe { libc::flock(fd2, libc::LOCK_EX | libc::LOCK_NB) };
    assert_eq!(result, -1);
    let err = io::Error::last_os_error();
    assert!(
        err.raw_os_error() == Some(libc::EWOULDBLOCK) || err.raw_os_error() == Some(libc::EAGAIN),
        "expected EWOULDBLOCK or EAGAIN, got {:?}",
        err
    );

    // Release and retry — should succeed
    unsafe { libc::flock(fd1, libc::LOCK_UN) };
    drop(file1);

    let result = unsafe { libc::flock(fd2, libc::LOCK_EX | libc::LOCK_NB) };
    assert_eq!(result, 0);

    unsafe { libc::flock(fd2, libc::LOCK_UN) };
    let _ = fs::remove_dir_all(&dir);
}

/// Advisory lock: fd without flock can still read/write the file.
#[test]
fn advisory_lock_bypassed_by_non_cooperating_fd() {
    let dir = temp_dir();
    let file_path = dir.join("advisory.txt");

    // Hold exclusive flock on one fd
    let file1 = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&file_path)
        .unwrap();
    let fd1 = std::os::unix::io::AsRawFd::as_raw_fd(&file1);
    let result = unsafe { libc::flock(fd1, libc::LOCK_EX) };
    assert_eq!(result, 0);

    // A non-cooperating fd (no flock) can still write — flock is advisory
    let mut raw_file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .unwrap();
    writeln!(raw_file, "bypassed").unwrap();
    raw_file.sync_all().unwrap();

    // Read back to confirm the write succeeded
    use std::io::Seek;
    raw_file.seek(std::io::SeekFrom::Start(0)).unwrap();
    let mut content = String::new();
    raw_file.read_to_string(&mut content).unwrap();
    assert!(content.contains("bypassed"), "got: {}", content);

    unsafe { libc::flock(fd1, libc::LOCK_UN) };
    let _ = fs::remove_dir_all(&dir);
}

/// Lock is automatically released when the file descriptor is closed
/// (e.g., when a process crashes or exits).
#[test]
fn lock_released_on_fd_close() {
    let dir = temp_dir();
    let file_path = dir.join("release_on_close.txt");
    fs::write(&file_path, "initial").unwrap();

    // Hold exclusive lock, then drop the fd
    {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file_path)
            .unwrap();
        let fd = std::os::unix::io::AsRawFd::as_raw_fd(&file);
        let result = unsafe { libc::flock(fd, libc::LOCK_EX) };
        assert_eq!(result, 0);
        // File dropped here — fd closed, lock released
    }

    // Should be able to acquire lock immediately now
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .unwrap();
    let fd = std::os::unix::io::AsRawFd::as_raw_fd(&file);
    let result = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };
    assert_eq!(result, 0, "lock should be available after fd close");

    unsafe { libc::flock(fd, libc::LOCK_UN) };
    let _ = fs::remove_dir_all(&dir);
}

