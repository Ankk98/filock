//! Basic usage example for filock.
//!
//! Run with: cargo run --example basic

use filock::FileStore;
use std::fs::File;
use std::io::{Read, Seek, Write};
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = PathBuf::from("/tmp/filock_example");
    let store = FileStore::new(&dir)?;

    println!("filock v{} example", filock::VERSION);
    println!("Store root: {}", store.root().display());
    println!();

    // --- Write data ---
    println!("Writing configuration...");
    store.with_write("config.json", |file: &mut File| {
        writeln!(file, "{{")?;
        writeln!(file, "  \"name\": \"filock-demo\",")?;
        writeln!(file, "  \"version\": 1,")?;
        writeln!(file, "  \"features\": [\"locking\", \"concurrency\"]")?;
        write!(file, "}}")?;
        Ok(())
    })?;
    println!("  ✓ Config written\n");

    // --- Read data ---
    println!("Reading configuration...");
    let content: String = store.with_read("config.json", |file: &mut File| {
        let mut buf = String::new();
        file.read_to_string(&mut buf)?;
        Ok(buf)
    })?;
    println!("  Content: {}\n", content.replace('\n', " "));

    // --- Non-blocking try-lock ---
    println!("Trying non-blocking write...");
    match store.try_with_write("config.json", |file: &mut File| {
        writeln!(file, "\n  // updated")?;
        Ok(())
    })? {
        Some(()) => println!("  ✓ Lock acquired, config updated"),
        None => println!("  ✗ Could not acquire lock (contended)"),
    }
    println!();

    // --- Timeout example ---
    println!("Trying write with 100ms timeout...");
    match store.with_write_timeout(
        "config.json",
        std::time::Duration::from_millis(100),
        |file: &mut File| {
            writeln!(file, "\n  // timestamp: now")?;
            Ok(())
        },
    ) {
        Ok(()) => println!("  ✓ Lock acquired within timeout"),
        Err(e) => println!("  ✗ {}", e),
    }
    println!();

    // --- Modify pattern ---
    println!("Using modify() for read-modify-write...");
    store.modify("counter.txt", |file: &mut File| {
        // Read current value
        let mut buf = String::new();
        file.read_to_string(&mut buf)?;
        let count: u64 = buf.trim().parse().unwrap_or(0);

        // Write updated value
        file.seek(std::io::SeekFrom::Start(0))?;
        file.set_len(0)?;
        writeln!(file, "{}", count + 1)?;
        Ok(())
    })?;
    println!("  ✓ Counter incremented\n");

    // --- Concurrent access ---
    println!("Running 4 concurrent writers...");
    use std::sync::{Arc, Barrier};
    use std::thread;

    let store = Arc::new(store);
    let barrier = Arc::new(Barrier::new(4));

    let mut handles = vec![];
    for _i in 0..4 {
        let store = store.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            for _ in 0..100 {
                store.modify("concurrent_counter.txt", |file: &mut File| {
                    let mut buf = String::new();
                    file.read_to_string(&mut buf)?;
                    let val: u64 = buf.trim().parse().unwrap_or(0);
                    file.seek(std::io::SeekFrom::Start(0))?;
                    file.set_len(0)?;
                    writeln!(file, "{}", val + 1)?;
                    Ok(())
                })
                .unwrap();
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let final_count: String = store.with_read("concurrent_counter.txt", |file: &mut File| {
        let mut buf = String::new();
        file.read_to_string(&mut buf)?;
        Ok(buf)
    })?;
    println!(
        "  ✓ Final count: {} (expected 400)",
        final_count.trim()
    );

    // Cleanup
    let _ = std::fs::remove_dir_all(&dir);
    println!("\nDone! Cleaned up.");

    Ok(())
}
