//! File descriptor management for flock operations.
//!
//! Each lock acquisition opens its own file descriptor. This avoids the
//! complexity of thread-local fd tables and ensures each flock operation
//! has an independent fd, which is correct behavior for `flock(2)`.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

/// Open a file suitable for flock operations.
///
/// Opens with read+write access. If `create` is true, creates the file
/// with mode 0o644 if it doesn't exist.
pub fn open_for_lock(path: &Path, create: bool) -> io::Result<File> {
    let mut opts = OpenOptions::new();
    opts.read(true).write(true);

    if create {
        opts.create(true).mode(0o644);
    }

    opts.open(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static FD_COUNTER: AtomicUsize = AtomicUsize::new(0);

    fn temp_dir() -> PathBuf {
        let id = FD_COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("filock_fd_test_{}_{}", std::process::id(), id));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn open_existing_file() {
        let dir = temp_dir();
        let path = dir.join("test.txt");
        fs::write(&path, "hello").unwrap();

        let file = open_for_lock(&path, false).unwrap();
        use std::os::unix::io::AsRawFd;
        assert!(file.as_raw_fd() >= 0);

        drop(file);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn open_creates_file_when_flagged() {
        let dir = temp_dir();
        let path = dir.join("new.txt");

        let _file = open_for_lock(&path, true).unwrap();
        assert!(path.exists());

        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn separate_threads_get_separate_fds() {
        let dir = temp_dir();
        let path = dir.join("thread.txt");
        fs::write(&path, "").unwrap();

        let mut fds = Vec::new();

        let path_clone = path.clone();
        let handle = std::thread::spawn(move || {
            let file = open_for_lock(&path_clone, false).unwrap();
            use std::os::unix::io::AsRawFd;
            file.as_raw_fd()
        });

        let file = open_for_lock(&path, false).unwrap();
        use std::os::unix::io::AsRawFd;
        fds.push(file.as_raw_fd());

        let other_fd = handle.join().unwrap();
        fds.push(other_fd);

        // Each thread gets its own fd (they're different file descriptors,
        // even if they refer to the same file)
        assert_ne!(fds[0], fds[1]);

        fs::remove_dir_all(&dir).unwrap();
    }
}
