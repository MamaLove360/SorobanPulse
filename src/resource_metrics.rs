use std::fs;
use std::time::Duration;
use tokio::sync::watch;

/// Parse file descriptor count from /proc/self/fd
#[cfg(target_os = "linux")]
pub fn read_fd_count() -> Option<u64> {
    match fs::read_dir("/proc/self/fd") {
        Ok(entries) => {
            let count = entries.count() as u64;
            Some(count)
        }
        Err(_) => None,
    }
}

#[cfg(not(target_os = "linux"))]
pub fn read_fd_count() -> Option<u64> {
    None
}

/// Parse disk I/O metrics from /proc/self/io
/// Returns (read_bytes, write_bytes, syscr, syscw)
#[cfg(target_os = "linux")]
pub fn read_disk_io_stats() -> Option<(u64, u64, u64, u64)> {
    match fs::read_to_string("/proc/self/io") {
        Ok(content) => {
            let mut read_bytes = 0u64;
            let mut write_bytes = 0u64;
            let mut syscr = 0u64;
            let mut syscw = 0u64;

            for line in content.lines() {
                if let Some(val_str) = line.strip_prefix("read_bytes:") {
                    read_bytes = val_str.trim().parse().unwrap_or(0);
                } else if let Some(val_str) = line.strip_prefix("write_bytes:") {
                    write_bytes = val_str.trim().parse().unwrap_or(0);
                } else if let Some(val_str) = line.strip_prefix("syscr:") {
                    syscr = val_str.trim().parse().unwrap_or(0);
                } else if let Some(val_str) = line.strip_prefix("syscw:") {
                    syscw = val_str.trim().parse().unwrap_or(0);
                }
            }

            Some((read_bytes, write_bytes, syscr, syscw))
        }
        Err(_) => None,
    }
}

#[cfg(not(target_os = "linux"))]
pub fn read_disk_io_stats() -> Option<(u64, u64, u64, u64)> {
    None
}

/// Update file descriptor count gauge
pub fn update_fd_count() {
    if let Some(count) = read_fd_count() {
        crate::metrics::update_fd_count(count);
    }
}

/// Update disk I/O metrics
pub fn update_disk_io() {
    if let Some((read_bytes, write_bytes, syscr, syscw)) = read_disk_io_stats() {
        crate::metrics::update_disk_read_bytes(read_bytes);
        crate::metrics::update_disk_write_bytes(write_bytes);
        crate::metrics::update_disk_syscalls_read(syscr);
        crate::metrics::update_disk_syscalls_write(syscw);
    }
}

/// Update memory usage from /proc/self/status
#[cfg(target_os = "linux")]
pub fn update_memory_usage() {
    if let Ok(status) = fs::read_to_string("/proc/self/status") {
        let mut rss_bytes = 0u64;
        let mut vms_bytes = 0u64;

        for line in status.lines() {
            if let Some(rest) = line.strip_prefix("VmRSS:") {
                if let Some(kb_str) = rest.split_whitespace().next() {
                    if let Ok(kb) = kb_str.parse::<u64>() {
                        rss_bytes = kb * 1024;
                    }
                }
            } else if let Some(rest) = line.strip_prefix("VmSize:") {
                if let Some(kb_str) = rest.split_whitespace().next() {
                    if let Ok(kb) = kb_str.parse::<u64>() {
                        vms_bytes = kb * 1024;
                    }
                }
            }
        }

        if rss_bytes > 0 {
            crate::metrics::update_memory_rss_bytes(rss_bytes);
        }
        if vms_bytes > 0 {
            crate::metrics::update_memory_vms_bytes(vms_bytes);
        }
    }
}

#[cfg(not(target_os = "linux"))]
pub fn update_memory_usage() {
    // Memory collection not supported on non-Linux platforms
}

/// Spawn a background task that updates resource metrics every 30 seconds.
pub fn spawn_resource_collector(mut shutdown_rx: watch::Receiver<bool>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    update_memory_usage();
                    update_fd_count();
                    update_disk_io();
                }
                _ = shutdown_rx.changed() => {
                    tracing::debug!("Resource metrics collector shutting down");
                    break;
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_fd_count() {
        #[cfg(target_os = "linux")]
        {
            let count = read_fd_count();
            assert!(count.is_some());
            assert!(count.unwrap() > 0);
        }
    }

    #[test]
    fn test_read_disk_io_stats() {
        #[cfg(target_os = "linux")]
        {
            let stats = read_disk_io_stats();
            assert!(stats.is_some());
            let (read, write, syscr, syscw) = stats.unwrap();
            // All values should be >= 0
            assert!(read >= 0);
            assert!(write >= 0);
            assert!(syscr >= 0);
            assert!(syscw >= 0);
        }
    }

    #[test]
    fn test_update_fd_count() {
        // Should not panic
        update_fd_count();
    }

    #[test]
    fn test_update_disk_io() {
        // Should not panic
        update_disk_io();
    }

    #[test]
    fn test_update_memory_usage() {
        // Should not panic
        update_memory_usage();
    }
}
