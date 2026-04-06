use std::collections::HashMap;
use std::marker::PhantomData;

use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::connection::RedisConnection;
use crate::error::{BullmqError, BullmqResult};
use crate::job::Job;
use crate::types::{JobOptions, JobState};

/// A typed job queue backed by Redis.
///
/// Use [`QueueBuilder`] to create a queue instance.
///
/// # Example
/// ```rust,no_run
/// use bullmq_rs::{QueueBuilder, RedisConnection, JobOptions};
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct MyJob { url: String }
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let queue = QueueBuilder::new("downloads")
///     .connection(RedisConnection::new("redis://127.0.0.1:6379"))
///     .build::<MyJob>()
///     .await?;
///
/// queue.add("fetch", MyJob { url: "https://example.com".into() }, None).await?;
/// # Ok(())
/// # }
/// ```
pub struct Queue<T> {
    pub(crate) name: String,
    pub(crate) prefix: String,
    pub(crate) conn: ConnectionManager,
    _phantom: PhantomData<T>,
}

impl<T: Serialize + DeserializeOwned + Send + Sync + 'static> Queue<T> {
    /// Add a job to the queue.
    ///
    /// Returns the created job with its assigned ID.
    pub async fn add(&self, name: &str, data: T, opts: Option<JobOptions>) -> BullmqResult<Job<T>> {
        let mut conn = self.conn.clone();

        // Generate job ID
        let job_id: String = match opts.as_ref().and_then(|o| o.job_id.clone()) {
            Some(custom_id) => custom_id,
            None => {
                let id: i64 = redis::cmd("INCR")
                    .arg(self.key("id"))
                    .query_async(&mut conn)
                    .await?;
                id.to_string()
            }
        };

        let job = Job::new(job_id.clone(), name.to_string(), data, opts);

        // Store job hash
        let fields = job.to_redis_hash()?;
        let job_key = self.key(&format!("{}", job_id));
        redis::cmd("HSET")
            .arg(&job_key)
            .arg(&fields)
            .query_async::<()>(&mut conn)
            .await?;

        // Add to appropriate sorted set
        match job.state {
            JobState::Delayed => {
                let process_at = job.timestamp.timestamp_millis() + job.delay as i64;
                redis::cmd("ZADD")
                    .arg(self.key("delayed"))
                    .arg(process_at)
                    .arg(&job_id)
                    .query_async::<()>(&mut conn)
                    .await?;
            }
            _ => {
                // Score: negate priority so lower priority values get processed first
                let score = -(job.priority as f64);
                redis::cmd("ZADD")
                    .arg(self.key("waiting"))
                    .arg(score)
                    .arg(&job_id)
                    .query_async::<()>(&mut conn)
                    .await?;
            }
        }

        // Set TTL on the job key if specified
        if let Some(ttl) = job.ttl {
            let ttl_secs = (ttl / 1000).max(1);
            redis::cmd("EXPIRE")
                .arg(&job_key)
                .arg(ttl_secs)
                .query_async::<()>(&mut conn)
                .await?;
        }

        Ok(job)
    }

    /// Get a job by its ID.
    pub async fn get_job(&self, job_id: &str) -> BullmqResult<Option<Job<T>>> {
        let mut conn = self.conn.clone();
        let job_key = self.key(job_id);

        let map: HashMap<String, String> = conn.hgetall(&job_key).await?;
        if map.is_empty() {
            return Ok(None);
        }

        let job = Job::from_redis_hash(job_id, &map)?;
        Ok(Some(job))
    }

    /// Get the number of jobs in each state.
    pub async fn get_job_counts(&self) -> BullmqResult<HashMap<JobState, u64>> {
        let mut conn = self.conn.clone();
        let mut counts = HashMap::new();

        let waiting: u64 = redis::cmd("ZCARD")
            .arg(self.key("waiting"))
            .query_async(&mut conn)
            .await?;
        let delayed: u64 = redis::cmd("ZCARD")
            .arg(self.key("delayed"))
            .query_async(&mut conn)
            .await?;
        let active: u64 = redis::cmd("SCARD")
            .arg(self.key("active"))
            .query_async(&mut conn)
            .await?;
        let completed: u64 = redis::cmd("ZCARD")
            .arg(self.key("completed"))
            .query_async(&mut conn)
            .await?;
        let failed: u64 = redis::cmd("ZCARD")
            .arg(self.key("failed"))
            .query_async(&mut conn)
            .await?;

        counts.insert(JobState::Waiting, waiting);
        counts.insert(JobState::Delayed, delayed);
        counts.insert(JobState::Active, active);
        counts.insert(JobState::Completed, completed);
        counts.insert(JobState::Failed, failed);

        Ok(counts)
    }

    /// Remove a job by its ID from all state sets and delete its hash.
    pub async fn remove(&self, job_id: &str) -> BullmqResult<()> {
        let mut conn = self.conn.clone();
        let job_key = self.key(job_id);

        // Remove from all state sets
        redis::cmd("ZREM")
            .arg(self.key("waiting"))
            .arg(job_id)
            .query_async::<()>(&mut conn)
            .await?;
        redis::cmd("ZREM")
            .arg(self.key("delayed"))
            .arg(job_id)
            .query_async::<()>(&mut conn)
            .await?;
        redis::cmd("SREM")
            .arg(self.key("active"))
            .arg(job_id)
            .query_async::<()>(&mut conn)
            .await?;
        redis::cmd("ZREM")
            .arg(self.key("completed"))
            .arg(job_id)
            .query_async::<()>(&mut conn)
            .await?;
        redis::cmd("ZREM")
            .arg(self.key("failed"))
            .arg(job_id)
            .query_async::<()>(&mut conn)
            .await?;

        // Delete the job hash
        redis::cmd("DEL")
            .arg(&job_key)
            .query_async::<()>(&mut conn)
            .await?;

        Ok(())
    }

    /// Remove all jobs from the queue (drain).
    pub async fn drain(&self) -> BullmqResult<()> {
        let mut conn = self.conn.clone();

        // Get all job IDs from all sets
        let waiting: Vec<String> = redis::cmd("ZRANGE")
            .arg(self.key("waiting"))
            .arg(0i64)
            .arg(-1i64)
            .query_async(&mut conn)
            .await?;
        let delayed: Vec<String> = redis::cmd("ZRANGE")
            .arg(self.key("delayed"))
            .arg(0i64)
            .arg(-1i64)
            .query_async(&mut conn)
            .await?;
        let completed: Vec<String> = redis::cmd("ZRANGE")
            .arg(self.key("completed"))
            .arg(0i64)
            .arg(-1i64)
            .query_async(&mut conn)
            .await?;
        let failed: Vec<String> = redis::cmd("ZRANGE")
            .arg(self.key("failed"))
            .arg(0i64)
            .arg(-1i64)
            .query_async(&mut conn)
            .await?;
        let active: Vec<String> = redis::cmd("SMEMBERS")
            .arg(self.key("active"))
            .query_async(&mut conn)
            .await?;

        // Delete all job hashes
        let all_ids: Vec<&String> = waiting
            .iter()
            .chain(delayed.iter())
            .chain(completed.iter())
            .chain(failed.iter())
            .chain(active.iter())
            .collect();

        for id in &all_ids {
            redis::cmd("DEL")
                .arg(self.key(id))
                .query_async::<()>(&mut conn)
                .await?;
        }

        // Delete all state sets and the ID counter
        redis::cmd("DEL")
            .arg(self.key("waiting"))
            .arg(self.key("delayed"))
            .arg(self.key("active"))
            .arg(self.key("completed"))
            .arg(self.key("failed"))
            .arg(self.key("id"))
            .query_async::<()>(&mut conn)
            .await?;

        Ok(())
    }

    /// Update the progress of a job (0-100).
    pub async fn update_progress(&self, job_id: &str, progress: u32) -> BullmqResult<()> {
        let mut conn = self.conn.clone();
        let job_key = self.key(job_id);

        let exists: bool = redis::cmd("EXISTS")
            .arg(&job_key)
            .query_async(&mut conn)
            .await?;
        if !exists {
            return Err(BullmqError::JobNotFound(job_id.to_string()));
        }

        redis::cmd("HSET")
            .arg(&job_key)
            .arg("progress")
            .arg(progress.min(100))
            .query_async::<()>(&mut conn)
            .await?;

        Ok(())
    }

    /// Get the queue name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Build a Redis key with the queue prefix.
    pub(crate) fn key(&self, suffix: &str) -> String {
        format!("{}:{}:{}", self.prefix, self.name, suffix)
    }
}

/// Builder for creating a [`Queue`].
pub struct QueueBuilder {
    name: String,
    connection: RedisConnection,
    prefix: String,
}

impl QueueBuilder {
    /// Create a new queue builder with the given queue name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            connection: RedisConnection::default(),
            prefix: "bull".to_string(),
        }
    }

    /// Set the Redis connection configuration.
    pub fn connection(mut self, conn: RedisConnection) -> Self {
        self.connection = conn;
        self
    }

    /// Set a custom key prefix (default: "bull").
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    /// Build the queue, establishing the Redis connection.
    pub async fn build<T: Serialize + DeserializeOwned + Send + Sync + 'static>(
        self,
    ) -> BullmqResult<Queue<T>> {
        let conn = self.connection.get_manager().await?;
        Ok(Queue {
            name: self.name,
            prefix: self.prefix,
            conn,
            _phantom: PhantomData,
        })
    }
}
