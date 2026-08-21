use std::fmt;
use std::io;
use std::path::PathBuf;
use std::time::Duration;

/// The type of lock being requested.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockType {
    /// Shared lock — multiple readers allowed simultaneously.
    Shared,
    /// Exclusive lock — only one holder, blocks all others.
    Exclusive,
}

impl fmt::Display for LockType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LockType::Shared => write!(f, "shared"),
            LockType::Exclusive => write!(f, "exclusive"),
        }
    }
}

/// Errors that can occur when acquiring or holding a file lock.
#[derive(Debug)]
pub enum FileLockError {
    /// The lock could not be acquired within the specified timeout.
    Timeout {
        /// The path of the file.
        path: PathBuf,
        /// The type of lock that was requested.
        lock_type: LockType,
        /// The timeout duration that was exceeded.
        timeout: Duration,
    },

    /// An underlying I/O error occurred.
    Io(io::Error),

    /// The lock was poisoned because the previous holder panicked while
    /// the lock was held.
    Poisoned {
        /// The path of the file.
        path: PathBuf,
    },
}

impl fmt::Display for FileLockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FileLockError::Timeout {
                path,
                lock_type,
                timeout,
            } => write!(
                f,
                "failed to acquire {} lock on '{}' within {:?}",
                lock_type,
                path.display(),
                timeout,
            ),
            FileLockError::Io(e) => write!(f, "I/O error: {}", e),
            FileLockError::Poisoned { path } => write!(
                f,
                "lock on '{}' was poisoned by a panicking thread",
                path.display()
            ),
        }
    }
}

impl std::error::Error for FileLockError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            FileLockError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for FileLockError {
    fn from(e: io::Error) -> Self {
        FileLockError::Io(e)
    }
}
