//! Shared temporary tile spool for streaming/spooled tile writing.
//!
//! Provides a file-based buffer that writes tiles to a temporary file
//! and cleans it up on drop.

use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

/// Returns a unique suffix for naming temporary files.
pub fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{}_{}", std::process::id(), nanos)
}

/// Temporary file spool for buffering tiles before final write.
pub struct TileSpool {
    file: File,
    path: PathBuf,
}

impl TileSpool {
    pub fn new() -> Result<Self, std::io::Error> {
        let suffix = unique_suffix();
        let path = std::env::temp_dir().join(format!("freestiler_spool_{}.tmp", suffix));
        let file = File::create(&path)?;
        Ok(Self { file, path })
    }

    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    pub fn write_tile(&mut self, data: &[u8], compress: flate2::Compression) -> Result<usize, std::io::Error> {
        let compressed = if compress.level() > 0 {
            let mut encoder = flate2::write::GzEncoder::new(Vec::new(), compress);
            encoder.write_all(data)?;
            encoder.finish().unwrap_or_else(|_| data.to_vec())
        } else {
            data.to_vec()
        };
        self.file.write_all(&compressed)?;
        Ok(compressed.len())
    }

    pub fn into_file(self) -> (File, PathBuf) {
        let this = std::mem::ManuallyDrop::new(self);
        let file = unsafe { std::ptr::read(&this.file) };
        let path = unsafe { std::ptr::read(&this.path) };
        (file, path)
    }
}

impl Drop for TileSpool {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}
