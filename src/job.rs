use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::error::{BullmqError, BullmqResult};
use crate::types::{BackoffStrategy, JobOptions, JobState};

/// A job with a typed data payload.
///
/// Jobs are created by [`Queue::add`](crate::Queue::add) and processed by
/// [`Worker::start`](crate::Worker::start). The `data` field contains your
/// custom payload, which must implement `Serialize` and `DeserializeOwned`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job<T> {
    /// Unique job identifier.
    pub id: String,
    /// Job name (used for categorization).
    pub name: String,
    /// The job payload.
    pub data: T,
    /// Current lifecycle state.
    pub state: JobState,
    /// When the job was created.
    pub timestamp: DateTime<Utc>,
    /// Priority (lower = higher priority).
    pub priority: i32,
    /// Delay in milliseconds before the job becomes available.
    pub delay: u64,
    /// Number of attempts made so far.
    pub attempts_made: u32,
    /// Maximum number of attempts allowed.
    pub max_attempts: u32,
    /// Backoff strategy for retries.
    pub backoff: Option<BackoffStrategy>,
    /// Time-to-live in milliseconds.
    pub ttl: Option<u64>,
    /// Job progress (0-100).
    pub progress: Option<u32>,
    /// When the job started being processed.
    pub processed_on: Option<DateTime<Utc>>,
    /// When the job finished (completed or failed).
    pub finished_on: Option<DateTime<Utc>>,
    /// Reason for failure, if the job failed.
    pub failed_reason: Option<String>,
    /// Return value from the handler (serialized as JSON).
    pub return_value: Option<serde_json::Value>,
}

impl<T: Serialize + DeserializeOwned> Job<T> {
    /// Create a new job with the given name, data, and options.
    pub fn new(id: String, name: String, data: T, opts: Option<JobOptions>) -> Self {
        let opts = opts.unwrap_or_default();
        Job {
            id,
            name,
            data,
            state: if opts.delay.is_some() {
                JobState::Delayed
            } else {
                JobState::Waiting
            },
            timestamp: Utc::now(),
            priority: opts.priority.unwrap_or(0),
            delay: opts.delay.map(|d| d.as_millis() as u64).unwrap_or(0),
            attempts_made: 0,
            max_attempts: opts.attempts.unwrap_or(1),
            backoff: opts.backoff,
            ttl: opts.ttl.map(|d| d.as_millis() as u64),
            progress: None,
            processed_on: None,
            finished_on: None,
            failed_reason: None,
            return_value: None,
        }
    }

    /// Serialize the job into a list of (field, value) pairs for Redis HSET.
    pub fn to_redis_hash(&self) -> BullmqResult<Vec<(String, String)>> {
        let data_json = serde_json::to_string(&self.data)?;
        let mut fields = vec![
            ("name".to_string(), self.name.clone()),
            ("data".to_string(), data_json),
            ("state".to_string(), self.state.to_string()),
            ("timestamp".to_string(), self.timestamp.timestamp_millis().to_string()),
            ("priority".to_string(), self.priority.to_string()),
            ("delay".to_string(), self.delay.to_string()),
            ("attempts_made".to_string(), self.attempts_made.to_string()),
            ("max_attempts".to_string(), self.max_attempts.to_string()),
        ];

        if let Some(ref backoff) = self.backoff {
            fields.push(("backoff".to_string(), serde_json::to_string(backoff)?));
        }
        if let Some(ttl) = self.ttl {
            fields.push(("ttl".to_string(), ttl.to_string()));
        }
        if let Some(progress) = self.progress {
            fields.push(("progress".to_string(), progress.to_string()));
        }
        if let Some(ref ts) = self.processed_on {
            fields.push(("processed_on".to_string(), ts.timestamp_millis().to_string()));
        }
        if let Some(ref ts) = self.finished_on {
            fields.push(("finished_on".to_string(), ts.timestamp_millis().to_string()));
        }
        if let Some(ref reason) = self.failed_reason {
            fields.push(("failed_reason".to_string(), reason.clone()));
        }
        if let Some(ref val) = self.return_value {
            fields.push(("return_value".to_string(), serde_json::to_string(val)?));
        }

        Ok(fields)
    }

    /// Deserialize a job from a Redis hash (field-value map).
    pub fn from_redis_hash(id: &str, map: &HashMap<String, String>) -> BullmqResult<Self> {
        let get = |key: &str| -> BullmqResult<&String> {
            map.get(key).ok_or_else(|| {
                BullmqError::Other(format!("Missing field '{}' in job hash", key))
            })
        };

        let data: T = serde_json::from_str(get("data")?)?;
        let state: JobState = get("state")?
            .parse()
            .map_err(|e: String| BullmqError::Other(e))?;
        let timestamp_ms: i64 = get("timestamp")?
            .parse()
            .map_err(|_| BullmqError::Other("Invalid timestamp".to_string()))?;
        let timestamp = DateTime::from_timestamp_millis(timestamp_ms)
            .unwrap_or_else(Utc::now);

        let priority: i32 = get("priority")?
            .parse()
            .map_err(|_| BullmqError::Other("Invalid priority".to_string()))?;
        let delay: u64 = get("delay")?
            .parse()
            .map_err(|_| BullmqError::Other("Invalid delay".to_string()))?;
        let attempts_made: u32 = get("attempts_made")?
            .parse()
            .map_err(|_| BullmqError::Other("Invalid attempts_made".to_string()))?;
        let max_attempts: u32 = get("max_attempts")?
            .parse()
            .map_err(|_| BullmqError::Other("Invalid max_attempts".to_string()))?;

        let backoff = map
            .get("backoff")
            .map(|s| serde_json::from_str(s))
            .transpose()?;
        let ttl = map
            .get("ttl")
            .map(|s| s.parse::<u64>())
            .transpose()
            .map_err(|_| BullmqError::Other("Invalid ttl".to_string()))?;
        let progress = map
            .get("progress")
            .map(|s| s.parse::<u32>())
            .transpose()
            .map_err(|_| BullmqError::Other("Invalid progress".to_string()))?;
        let processed_on = map
            .get("processed_on")
            .and_then(|s| s.parse::<i64>().ok())
            .and_then(DateTime::from_timestamp_millis);
        let finished_on = map
            .get("finished_on")
            .and_then(|s| s.parse::<i64>().ok())
            .and_then(DateTime::from_timestamp_millis);
        let failed_reason = map.get("failed_reason").cloned();
        let return_value = map
            .get("return_value")
            .map(|s| serde_json::from_str(s))
            .transpose()?;

        Ok(Job {
            id: id.to_string(),
            name: get("name")?.clone(),
            data,
            state,
            timestamp,
            priority,
            delay,
            attempts_made,
            max_attempts,
            backoff,
            ttl,
            progress,
            processed_on,
            finished_on,
            failed_reason,
            return_value,
        })
    }
}
