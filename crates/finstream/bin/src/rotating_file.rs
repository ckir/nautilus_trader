use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::PathBuf,
    sync::mpsc,
    thread,
};
use chrono::{Datelike, Local};

/// Bounded channel capacity to prevent memory blowup during I/O stalls.
const CHANNEL_CAPACITY: usize = 16384;

/// A synchronized rotator that manages multiple log streams and ensures they rotate in lock-step.
/// This implementation is NON-BLOCKING for the caller; it uses a background worker thread.
pub(crate) struct SynchronizedRotator {
    worker_tx: mpsc::SyncSender<WorkerCmd>,
    _worker_handle: Option<thread::JoinHandle<()>>,
}

enum WorkerCmd {
    Write { stream_idx: usize, data: Vec<u8> },
    Flush,
    Shutdown,
}

struct RotatorInner {
    directory: PathBuf,
    max_size:  u64,
    current_date: (i32, u32, u32),
    index:     u32,
    streams:   Vec<StreamState>,
}

struct StreamState {
    name:   String,
    file:   Option<File>,
    bytes:  u64,
}

impl SynchronizedRotator {
    /// Creates a new synchronized rotator managing the specified directory.
    /// 
    /// * `directory`: The path where log files will be stored.
    /// * `max_size_mb`: The threshold for rotating files based on size.
    pub(crate) fn new(directory: impl Into<PathBuf>, max_size_mb: u64) -> io::Result<Self> {
        let directory = directory.into();
        // Ensure the log directory exists
        fs::create_dir_all(&directory)?;

        // Create a synchronous channel with a large buffer for the worker
        let (tx, rx) = mpsc::sync_channel(CHANNEL_CAPACITY);

        let mut inner = RotatorInner {
            directory,
            max_size: max_size_mb * 1024 * 1024,
            current_date: (0, 0, 0), // Forces initial open/date check
            index: 0,
            streams: Vec::new(),
        };

        // Initialize pre-defined log streams
        inner.streams.push(StreamState { name: "app".into(),    file: None, bytes: 0 });
        inner.streams.push(StreamState { name: "data".into(),   file: None, bytes: 0 });
        inner.streams.push(StreamState { name: "status".into(), file: None, bytes: 0 });

        // Perform initial file opening and date check
        inner.rotate_if_needed()?;

        // Spawn the dedicated background worker thread for disk I/O
        let handle = thread::spawn(move || {
            let mut inner = inner;
            while let Ok(cmd) = rx.recv() {
                match cmd {
                    WorkerCmd::Write { stream_idx, data } => {
                        // Check if files need rotation before every write
                        let _ = inner.rotate_if_needed();
                        if let Some(stream) = inner.streams.get_mut(stream_idx) {
                            if let Some(file) = &mut stream.file {
                                // Write to the specific file and track total bytes
                                if let Ok(written) = file.write(&data) {
                                    stream.bytes += written as u64;
                                }
                            }
                        }
                    }
                    WorkerCmd::Flush => {
                        // Flush all managed streams to disk
                        for stream in &mut inner.streams {
                            if let Some(file) = &mut stream.file {
                                let _ = file.flush();
                            }
                        }
                    }
                    WorkerCmd::Shutdown => break,
                }
            }
            // Final flush on worker shutdown to ensure all logs are persisted
            for stream in &mut inner.streams {
                if let Some(file) = &mut stream.file {
                    let _ = file.flush();
                }
            }
        });

        Ok(Self {
            worker_tx: tx,
            _worker_handle: Some(handle),
        })
    }

    /// Returns a non-blocking writer handle for a specific log stream.
    /// 
    /// Supported stream names: "app", "data", "status".
    pub(crate) fn writer(&self, stream_name: &str) -> Option<RotatorWriter> {
        // Map logical names to internal stream indices
        let stream_idx = match stream_name {
            "app"    => 0,
            "data"   => 1,
            "status" => 2,
            _        => return None,
        };

        Some(RotatorWriter {
            tx: self.worker_tx.clone(),
            stream_idx,
        })
    }
}

impl Drop for SynchronizedRotator {
    fn drop(&mut self) {
        // Signal the worker thread to shut down cleanly
        let _ = self.worker_tx.send(WorkerCmd::Shutdown);
    }
}

impl RotatorInner {
    /// Scans the directory to find the next available file index for the current date.
    fn find_next_index(&self) -> io::Result<u32> {
        let (y, m, d) = self.current_date;
        let date_str = format!("{:04}-{:02}-{:02}", y, m, d);
        let mut max_idx = 0;

        // Iterate through existing files to avoid overwriting logs from previous runs
        for entry in fs::read_dir(&self.directory)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            // Check if the filename matches today's date pattern
            if name.contains(&date_str) {
                let parts: Vec<&str> = name.split('.').collect();
                if parts.len() >= 3 {
                    // Extract the index from names like 'app.2024-01-01.0.log'
                    if let Ok(idx) = parts[parts.len() - 2].parse::<u32>() {
                        if idx >= max_idx {
                            max_idx = idx + 1;
                        }
                    }
                }
            }
        }
        Ok(max_idx)
    }

    /// Opens all log files for the current date and index in append mode.
    fn open_all(&mut self) -> io::Result<()> {
        let (y, m, d) = self.current_date;
        let date_str = format!("{:04}-{:02}-{:02}", y, m, d);

        for stream in &mut self.streams {
            // Construct unique filename: <name>.<date>.<index>.log
            let filename = format!("{}.{}.{}.log", stream.name, date_str, self.index);
            let path = self.directory.join(filename);
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)?;
            
            // Sync current byte count with existing file size
            stream.bytes = file.metadata()?.len();
            stream.file = Some(file);
        }
        Ok(())
    }

    /// Checks if a rotation is required due to date change or file size limit.
    fn rotate_if_needed(&mut self) -> io::Result<()> {
        let now = Local::now();
        let today = (now.year(), now.month(), now.day());

        let date_changed = today != self.current_date;
        // Rotation happens if ANY stream exceeds the max size to keep them in sync
        let size_exceeded = self.streams.iter().any(|s| s.bytes >= self.max_size);

        if date_changed || size_exceeded {
            // Close current files
            for stream in &mut self.streams {
                stream.file = None;
            }

            if date_changed {
                // New day: reset index to next available
                self.current_date = today;
                self.index = self.find_next_index().unwrap_or(0);
            } else {
                // Same day, size exceeded: increment index
                self.index += 1;
            }

            // Re-open all streams at the new index
            self.open_all()?;
        }
        Ok(())
    }
}

/// A non-blocking writer that sends data to the background worker.
pub(crate) struct RotatorWriter {
    tx: mpsc::SyncSender<WorkerCmd>,
    stream_idx: usize,
}

impl Write for RotatorWriter {
    /// Dispatches a write command to the background thread.
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // Use try_send to prevent I/O blocking from stalling the main data loop.
        // In high-performance market data scenarios, dropping a log line is 
        // preferable to causing latency in event delivery.
        match self.tx.try_send(WorkerCmd::Write {
            stream_idx: self.stream_idx,
            data: buf.to_vec(),
        }) {
            Ok(_) => Ok(buf.len()),
            Err(mpsc::TrySendError::Full(_)) => {
                // Channel is full; drop this log line to save the hot path
                Ok(buf.len()) 
            }
            Err(_) => Err(io::Error::new(io::ErrorKind::Other, "worker thread died")),
        }
    }

    /// Dispatches a flush command to the background thread.
    fn flush(&mut self) -> io::Result<()> {
        let _ = self.tx.send(WorkerCmd::Flush);
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for RotatorWriter {
    type Writer = Self;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

impl Clone for RotatorWriter {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            stream_idx: self.stream_idx,
        }
    }
}
