//! Machine health sampled onto the heartbeat.
//!
//! Scope is deliberately narrow: enough to answer "is this box healthy and is
//! the GPU actually busy", not enough to graph. The heartbeat is a control
//! channel written every 15 seconds, and turning it into a metrics pipeline
//! would be the wrong shape — for time series, use the relay to deploy
//! node_exporter and let Prometheus do its job.
//!
//! Every reading is optional. A machine without `/proc`, without NVIDIA
//! drivers, or with a permission problem reports what it can and stays silent
//! about the rest; nothing here is allowed to fail a heartbeat.

use std::path::{Path, PathBuf};
use std::time::Duration;

use common::protocol::{GpuMetrics, Metrics};
use tracing::debug;

/// `nvidia-smi` spawns a process and talks to the driver. Bounded so a wedged
/// driver cannot stall heartbeats, which would make the agent look dead.
const GPU_QUERY_TIMEOUT: Duration = Duration::from_secs(5);

pub struct Collector {
    disk_path: PathBuf,
    /// Resolved once at startup: probing for a missing binary on every
    /// heartbeat is wasted work on the many hosts that have no GPU.
    nvidia_smi: Option<PathBuf>,
}

impl Collector {
    pub fn new(disk_path: PathBuf, gpu_enabled: bool) -> Self {
        let nvidia_smi = if gpu_enabled { find_nvidia_smi() } else { None };
        if let Some(path) = &nvidia_smi {
            tracing::info!(path = %path.display(), "GPU metrics enabled");
        }
        Self { disk_path, nvidia_smi }
    }

    pub async fn sample(&self) -> Metrics {
        let (load_1m, cpu_count) = load_and_cpus();
        let (mem_total_mb, mem_available_mb) = memory_mb();
        let (disk_total_mb, disk_free_mb) = disk_mb(&self.disk_path);
        let gpus = match &self.nvidia_smi {
            Some(path) => query_gpus(path).await,
            None => Vec::new(),
        };
        Metrics {
            load_1m,
            cpu_count,
            mem_total_mb,
            mem_available_mb,
            disk_total_mb,
            disk_free_mb,
            gpus,
        }
    }
}

fn find_nvidia_smi() -> Option<PathBuf> {
    for candidate in ["/usr/bin/nvidia-smi", "/usr/local/bin/nvidia-smi", "/bin/nvidia-smi"] {
        let path = PathBuf::from(candidate);
        if path.is_file() {
            return Some(path);
        }
    }
    None
}

fn load_and_cpus() -> (Option<f32>, Option<u32>) {
    let load = std::fs::read_to_string("/proc/loadavg")
        .ok()
        .and_then(|text| text.split_whitespace().next()?.parse::<f32>().ok());
    let cpus = std::thread::available_parallelism().ok().map(|n| n.get() as u32);
    (load, cpus)
}

fn memory_mb() -> (Option<u64>, Option<u64>) {
    let Ok(text) = std::fs::read_to_string("/proc/meminfo") else {
        return (None, None);
    };
    let mut total = None;
    let mut available = None;
    for line in text.lines() {
        // Lines look like "MemTotal:       16316536 kB".
        let Some((key, rest)) = line.split_once(':') else { continue };
        let kb = rest.split_whitespace().next().and_then(|v| v.parse::<u64>().ok());
        match key {
            "MemTotal" => total = kb.map(|kb| kb / 1024),
            "MemAvailable" => available = kb.map(|kb| kb / 1024),
            _ => {}
        }
        if total.is_some() && available.is_some() {
            break;
        }
    }
    (total, available)
}

#[cfg(unix)]
fn disk_mb(path: &Path) -> (Option<u64>, Option<u64>) {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let Ok(c_path) = CString::new(path.as_os_str().as_bytes()) else {
        return (None, None);
    };
    // SAFETY: c_path is a valid NUL-terminated string that outlives the call,
    // and stat is a plain POD struct the kernel fully initialises on success.
    let stat = unsafe {
        let mut stat: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(c_path.as_ptr(), &mut stat) != 0 {
            return (None, None);
        }
        stat
    };
    let block = stat.f_frsize as u64;
    let total = (stat.f_blocks as u64).saturating_mul(block) / (1024 * 1024);
    // f_bavail, not f_bfree: the latter counts blocks reserved for root that an
    // ordinary process cannot touch.
    let free = (stat.f_bavail as u64).saturating_mul(block) / (1024 * 1024);
    (Some(total), Some(free))
}

#[cfg(not(unix))]
fn disk_mb(_path: &Path) -> (Option<u64>, Option<u64>) {
    (None, None)
}

async fn query_gpus(nvidia_smi: &Path) -> Vec<GpuMetrics> {
    let query = "index,name,utilization.gpu,memory.used,memory.total,temperature.gpu";
    let child = tokio::process::Command::new(nvidia_smi)
        .arg(format!("--query-gpu={query}"))
        .arg("--format=csv,noheader,nounits")
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true)
        .output();

    let output = match tokio::time::timeout(GPU_QUERY_TIMEOUT, child).await {
        Ok(Ok(output)) if output.status.success() => output,
        Ok(Ok(output)) => {
            debug!(
                status = ?output.status,
                stderr = %String::from_utf8_lossy(&output.stderr).trim(),
                "nvidia-smi returned an error"
            );
            return Vec::new();
        }
        Ok(Err(error)) => {
            debug!(%error, "could not run nvidia-smi");
            return Vec::new();
        }
        Err(_) => {
            debug!("nvidia-smi timed out");
            return Vec::new();
        }
    };
    parse_gpu_csv(&String::from_utf8_lossy(&output.stdout))
}

fn parse_gpu_csv(text: &str) -> Vec<GpuMetrics> {
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .filter_map(|line| {
            let fields: Vec<&str> = line.split(',').map(str::trim).collect();
            if fields.len() < 6 {
                return None;
            }
            Some(GpuMetrics {
                index: fields[0].parse().ok()?,
                name: fields[1].to_owned(),
                // Any field can be "[N/A]" on a GPU that does not report it,
                // which must not discard the whole row.
                utilization_pct: fields[2].parse().ok(),
                memory_used_mb: fields[3].parse().ok(),
                memory_total_mb: fields[4].parse().ok(),
                temperature_c: fields[5].parse().ok(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_nvidia_smi_output() {
        let text = "0, NVIDIA A100-SXM4-40GB, 97, 38912, 40960, 71\n\
                    1, NVIDIA A100-SXM4-40GB, 0, 4, 40960, 33\n";
        let gpus = parse_gpu_csv(text);
        assert_eq!(gpus.len(), 2);
        assert_eq!(gpus[0].index, 0);
        assert_eq!(gpus[0].name, "NVIDIA A100-SXM4-40GB");
        assert_eq!(gpus[0].utilization_pct, Some(97));
        assert_eq!(gpus[0].memory_used_mb, Some(38912));
        assert_eq!(gpus[1].temperature_c, Some(33));
    }

    #[test]
    fn keeps_rows_with_unavailable_fields() {
        // Consumer cards report [N/A] for several of these.
        let gpus = parse_gpu_csv("0, GeForce RTX 3090, [N/A], 1024, 24576, [N/A]\n");
        assert_eq!(gpus.len(), 1);
        assert_eq!(gpus[0].utilization_pct, None);
        assert_eq!(gpus[0].memory_used_mb, Some(1024));
        assert_eq!(gpus[0].temperature_c, None);
    }

    #[test]
    fn ignores_malformed_lines() {
        assert!(parse_gpu_csv("garbage\n\n").is_empty());
    }
}
