use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

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
            (
                "timestamp".to_string(),
                self.timestamp.timestamp_millis().to_string(),
            ),
            ("priority".to_string(), self.priority.to_string()),
            ("delay".to_string(), self.delay.to_string()),
            ("atm".to_string(), self.attempts_made.to_string()),
            ("opts".to_string(), self.bullmq_opts_json()?),
        ];

        if let Some(progress) = self.progress {
            fields.push(("progress".to_string(), progress.to_string()));
        }
        if let Some(ref ts) = self.processed_on {
            fields.push(("processedOn".to_string(), ts.timestamp_millis().to_string()));
        }
        if let Some(ref ts) = self.finished_on {
            fields.push(("finishedOn".to_string(), ts.timestamp_millis().to_string()));
        }
        if let Some(ref reason) = self.failed_reason {
            fields.push(("failedReason".to_string(), reason.clone()));
        }
        if let Some(ref val) = self.return_value {
            fields.push(("returnvalue".to_string(), serde_json::to_string(val)?));
        }

        Ok(fields)
    }

    /// Deserialize a job from a Redis hash (field-value map).
    pub fn from_redis_hash(id: &str, map: &HashMap<String, String>) -> BullmqResult<Self> {
        let get = |key: &str| -> BullmqResult<&String> {
            map.get(key)
                .ok_or_else(|| BullmqError::Other(format!("Missing field '{}' in job hash", key)))
        };
        let get_any =
            |keys: &[&str]| -> Option<&String> { keys.iter().find_map(|key| map.get(*key)) };

        let opts = get_any(&["opts"])
            .map(|raw| parse_bullmq_opts(raw))
            .transpose()?
            .unwrap_or_default();

        let data: T = serde_json::from_str(get("data")?)?;
        let state = get_any(&["state"])
            .map(|value| value.parse().map_err(|e: String| BullmqError::Other(e)))
            .transpose()?
            .unwrap_or(JobState::Waiting);
        let timestamp_ms: i64 = get("timestamp")?
            .parse()
            .map_err(|_| BullmqError::Other("Invalid timestamp".to_string()))?;
        let timestamp = DateTime::from_timestamp_millis(timestamp_ms).unwrap_or_else(Utc::now);

        let priority = parse_i32(get_any(&["priority"]), "Invalid priority")?
            .or(opts.priority)
            .unwrap_or(0);
        let delay = parse_u64(get_any(&["delay"]), "Invalid delay")?
            .or(opts.delay)
            .unwrap_or(0);
        let attempts_made = parse_u32(
            get_any(&["attempts_made", "attemptsMade", "atm"]),
            "Invalid attempts_made",
        )?
        .unwrap_or(0);
        let max_attempts = parse_u32(get_any(&["max_attempts"]), "Invalid max_attempts")?
            .or(opts.attempts)
            .unwrap_or(1);

        let backoff = map
            .get("backoff")
            .map(|s| serde_json::from_str(s))
            .transpose()?
            .or(opts.backoff);
        let ttl = parse_u64(map.get("ttl"), "Invalid ttl")?.or(opts.ttl);
        let progress = map
            .get("progress")
            .map(|s| s.parse::<u32>())
            .transpose()
            .map_err(|_| BullmqError::Other("Invalid progress".to_string()))?;
        let processed_on = get_any(&["processed_on", "processedOn"])
            .and_then(|s| s.parse::<i64>().ok())
            .and_then(DateTime::from_timestamp_millis);
        let finished_on = get_any(&["finished_on", "finishedOn"])
            .and_then(|s| s.parse::<i64>().ok())
            .and_then(DateTime::from_timestamp_millis);
        let failed_reason = get_any(&["failed_reason", "failedReason"]).cloned();
        let return_value = get_any(&["return_value", "returnvalue"])
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

    fn bullmq_opts_json(&self) -> BullmqResult<String> {
        let mut opts = Map::new();

        if self.priority != 0 {
            opts.insert("priority".to_string(), Value::from(self.priority));
        }
        if self.delay != 0 {
            opts.insert("delay".to_string(), Value::from(self.delay));
        }
        if self.max_attempts > 1 {
            opts.insert("attempts".to_string(), Value::from(self.max_attempts));
        }
        if let Some(ref backoff) = self.backoff {
            opts.insert("backoff".to_string(), serde_json::to_value(backoff)?);
        }
        if let Some(ttl) = self.ttl {
            opts.insert("ttl".to_string(), Value::from(ttl));
        }

        Ok(Value::Object(opts).to_string())
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BullmqWireOptions {
    priority: Option<i32>,
    delay: Option<u64>,
    attempts: Option<u32>,
    backoff: Option<BackoffStrategy>,
    ttl: Option<u64>,
}

fn parse_bullmq_opts(raw: &str) -> BullmqResult<BullmqWireOptions> {
    serde_json::from_str(raw).map_err(Into::into)
}

fn parse_i32(value: Option<&String>, error: &str) -> BullmqResult<Option<i32>> {
    value
        .map(|s| s.parse::<i32>())
        .transpose()
        .map_err(|_| BullmqError::Other(error.to_string()))
}

fn parse_u32(value: Option<&String>, error: &str) -> BullmqResult<Option<u32>> {
    value
        .map(|s| s.parse::<u32>())
        .transpose()
        .map_err(|_| BullmqError::Other(error.to_string()))
}

fn parse_u64(value: Option<&String>, error: &str) -> BullmqResult<Option<u64>> {
    value
        .map(|s| s.parse::<u64>())
        .transpose()
        .map_err(|_| BullmqError::Other(error.to_string()))
}
