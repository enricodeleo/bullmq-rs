use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Lifecycle state of a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobState {
    Waiting,
    Delayed,
    Active,
    Completed,
    Failed,
}

impl std::fmt::Display for JobState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobState::Waiting => write!(f, "waiting"),
            JobState::Delayed => write!(f, "delayed"),
            JobState::Active => write!(f, "active"),
            JobState::Completed => write!(f, "completed"),
            JobState::Failed => write!(f, "failed"),
        }
    }
}

impl std::str::FromStr for JobState {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "waiting" => Ok(JobState::Waiting),
            "delayed" => Ok(JobState::Delayed),
            "active" => Ok(JobState::Active),
            "completed" => Ok(JobState::Completed),
            "failed" => Ok(JobState::Failed),
            _ => Err(format!("Unknown job state: {}", s)),
        }
    }
}

/// Options for creating a job.
#[derive(Debug, Clone, Default)]
pub struct JobOptions {
    /// Job priority. Lower values = higher priority. Default is 0.
    pub priority: Option<i32>,
    /// Delay before the job becomes available for processing.
    pub delay: Option<Duration>,
    /// Maximum number of attempts (including the first). Default is 1 (no retry).
    pub attempts: Option<u32>,
    /// Backoff strategy for retries.
    pub backoff: Option<BackoffStrategy>,
    /// Time-to-live: job expires after this duration.
    pub ttl: Option<Duration>,
    /// Custom job ID. Auto-generated if not provided.
    pub job_id: Option<String>,
}

/// Backoff strategy for job retries.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum BackoffStrategy {
    /// Fixed delay between retries.
    Fixed {
        #[serde(with = "duration_millis")]
        delay: Duration,
    },
    /// Exponential backoff with a maximum delay cap.
    Exponential {
        #[serde(with = "duration_millis")]
        base: Duration,
        #[serde(with = "duration_millis")]
        max: Duration,
    },
}

impl BackoffStrategy {
    /// Calculate the delay for a given attempt number (0-indexed).
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        match self {
            BackoffStrategy::Fixed { delay } => *delay,
            BackoffStrategy::Exponential { base, max } => {
                let delay = base.as_millis() as u64 * 2u64.saturating_pow(attempt);
                let max_ms = max.as_millis() as u64;
                Duration::from_millis(delay.min(max_ms))
            }
        }
    }
}

/// Options for creating a worker.
#[derive(Debug, Clone)]
pub struct WorkerOptions {
    /// Number of jobs to process concurrently. Default is 1.
    pub concurrency: usize,
    /// How often to poll for new jobs. Default is 1 second.
    pub poll_interval: Duration,
}

impl Default for WorkerOptions {
    fn default() -> Self {
        Self {
            concurrency: 1,
            poll_interval: Duration::from_secs(1),
        }
    }
}

mod duration_millis {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_u64(d.as_millis() as u64)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        let ms = u64::deserialize(d)?;
        Ok(Duration::from_millis(ms))
    }
}
