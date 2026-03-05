use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use chrono::Utc;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::{watch, Semaphore};
use tokio::time::sleep;

use crate::connection::RedisConnection;
use crate::error::{BullmqError, BullmqResult};
use crate::job::Job;
use crate::types::{JobState, WorkerOptions};

/// A worker that processes jobs from a queue.
///
/// Use [`WorkerBuilder`] to create a worker, then call [`Worker::start`] with
/// an async handler function.
///
/// # Example
/// ```rust,no_run
/// use bullmq_rs::{WorkerBuilder, RedisConnection};
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Serialize, Deserialize, Debug, Clone)]
/// struct MyJob { message: String }
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let worker = WorkerBuilder::new("my_queue")
///     .connection(RedisConnection::new("redis://127.0.0.1:6379"))
///     .concurrency(5)
///     .build::<MyJob>()
///     .await?;
///
/// let handle = worker.start(|job| async move {
///     println!("Processing: {:?}", job.data);
///     Ok(())
/// }).await?;
///
/// // Later: graceful shutdown
/// handle.shutdown();
/// handle.wait().await?;
/// # Ok(())
/// # }
/// ```
pub struct Worker<T> {
    name: String,
    prefix: String,
    conn: ConnectionManager,
    options: WorkerOptions,
    on_completed: Option<Arc<dyn Fn(&Job<serde_json::Value>) + Send + Sync>>,
    on_failed: Option<Arc<dyn Fn(&Job<serde_json::Value>, &BullmqError) + Send + Sync>>,
    _phantom: PhantomData<T>,
}

/// Handle to a running worker, used for shutdown.
pub struct WorkerHandle {
    shutdown_tx: watch::Sender<bool>,
    join_handle: tokio::task::JoinHandle<()>,
}

impl WorkerHandle {
    /// Signal the worker to stop after finishing current jobs.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    /// Wait for the worker to fully stop.
    pub async fn wait(self) -> BullmqResult<()> {
        self.join_handle
            .await
            .map_err(|e| BullmqError::Other(format!("Worker task panicked: {}", e)))?;
        Ok(())
    }
}

impl<T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static> Worker<T> {
    /// Start processing jobs with the given async handler.
    ///
    /// Returns a [`WorkerHandle`] that can be used to shut down the worker.
    pub async fn start<F, Fut>(self, handler: F) -> BullmqResult<WorkerHandle>
    where
        F: Fn(Job<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), BullmqError>> + Send,
    {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handler = Arc::new(handler);
        let semaphore = Arc::new(Semaphore::new(self.options.concurrency));
        let poll_interval = self.options.poll_interval;
        let conn = self.conn.clone();
        let name = self.name.clone();
        let prefix = self.prefix.clone();
        let on_completed = self.on_completed.clone();
        let on_failed = self.on_failed.clone();

        let join_handle = tokio::spawn(async move {
            let mut shutdown_rx = shutdown_rx;

            loop {
                // Check for shutdown
                if *shutdown_rx.borrow() {
                    break;
                }

                // Move delayed jobs to waiting
                if let Err(e) = promote_delayed_jobs(&conn, &prefix, &name).await {
                    tracing::warn!("Error promoting delayed jobs: {}", e);
                }

                // Try to acquire a concurrency permit
                let permit = match semaphore.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        // All slots busy, wait and retry
                        tokio::select! {
                            _ = sleep(poll_interval) => continue,
                            _ = shutdown_rx.changed() => break,
                        }
                    }
                };

                // Pop next job from waiting set
                let mut conn_clone = conn.clone();
                let waiting_key = format!("{}:{}:waiting", prefix, name);

                let result: redis::RedisResult<Vec<(String, f64)>> = redis::cmd("ZPOPMAX")
                    .arg(&waiting_key)
                    .arg(1)
                    .query_async(&mut conn_clone)
                    .await;

                match result {
                    Ok(items) if items.is_empty() => {
                        // No jobs available, wait
                        drop(permit);
                        tokio::select! {
                            _ = sleep(poll_interval) => continue,
                            _ = shutdown_rx.changed() => break,
                        }
                    }
                    Ok(items) => {
                        let (job_id, _score) = &items[0];
                        let job_id = job_id.clone();

                        // Fetch job data from hash
                        let job_key = format!("{}:{}:{}", prefix, name, job_id);
                        let map: HashMap<String, String> =
                            match conn_clone.hgetall(&job_key).await {
                                Ok(m) => m,
                                Err(e) => {
                                    tracing::error!("Failed to fetch job {}: {}", job_id, e);
                                    drop(permit);
                                    continue;
                                }
                            };

                        if map.is_empty() {
                            tracing::warn!("Job {} hash is empty, skipping", job_id);
                            drop(permit);
                            continue;
                        }

                        let mut job: Job<T> = match Job::from_redis_hash(&job_id, &map) {
                            Ok(j) => j,
                            Err(e) => {
                                tracing::error!("Failed to deserialize job {}: {}", job_id, e);
                                drop(permit);
                                continue;
                            }
                        };

                        // Mark as active
                        job.state = JobState::Active;
                        job.processed_on = Some(Utc::now());
                        let _ = redis::cmd("SADD")
                            .arg(format!("{}:{}:active", prefix, name))
                            .arg(&job_id)
                            .query_async::<()>(&mut conn_clone)
                            .await;
                        let _ = redis::cmd("HSET")
                            .arg(&job_key)
                            .arg("state")
                            .arg("active")
                            .arg("processed_on")
                            .arg(Utc::now().timestamp_millis().to_string())
                            .query_async::<()>(&mut conn_clone)
                            .await;

                        // Process in a spawned task
                        let handler = handler.clone();
                        let mut task_conn = conn.clone();
                        let task_prefix = prefix.clone();
                        let task_name = name.clone();
                        let on_completed = on_completed.clone();
                        let on_failed = on_failed.clone();

                        tokio::spawn(async move {
                            let _permit = permit;
                            let job_key =
                                format!("{}:{}:{}", task_prefix, task_name, job.id);

                            match handler(job.clone()).await {
                                Ok(()) => {
                                    // Move to completed
                                    let now = Utc::now();
                                    let _ = redis::cmd("SREM")
                                        .arg(format!(
                                            "{}:{}:active",
                                            task_prefix, task_name
                                        ))
                                        .arg(&job.id)
                                        .query_async::<()>(&mut task_conn)
                                        .await;
                                    let _ = redis::cmd("ZADD")
                                        .arg(format!(
                                            "{}:{}:completed",
                                            task_prefix, task_name
                                        ))
                                        .arg(now.timestamp_millis())
                                        .arg(&job.id)
                                        .query_async::<()>(&mut task_conn)
                                        .await;
                                    let _ = redis::cmd("HSET")
                                        .arg(&job_key)
                                        .arg("state")
                                        .arg("completed")
                                        .arg("finished_on")
                                        .arg(now.timestamp_millis().to_string())
                                        .query_async::<()>(&mut task_conn)
                                        .await;

                                    if let Some(ref cb) = on_completed {
                                        if let Ok(val_job) = convert_job_to_value(&job) {
                                            cb(&val_job);
                                        }
                                    }

                                    tracing::debug!("Job {} completed", job.id);
                                }
                                Err(err) => {
                                    let attempts = job.attempts_made + 1;

                                    // Remove from active
                                    let _ = redis::cmd("SREM")
                                        .arg(format!(
                                            "{}:{}:active",
                                            task_prefix, task_name
                                        ))
                                        .arg(&job.id)
                                        .query_async::<()>(&mut task_conn)
                                        .await;

                                    if attempts < job.max_attempts {
                                        // Retry: compute backoff delay and re-queue as delayed
                                        let backoff_delay = job
                                            .backoff
                                            .as_ref()
                                            .map(|b| b.delay_for_attempt(attempts))
                                            .unwrap_or(std::time::Duration::from_secs(1));

                                        let process_at = Utc::now().timestamp_millis()
                                            + backoff_delay.as_millis() as i64;

                                        let _ = redis::cmd("HSET")
                                            .arg(&job_key)
                                            .arg("state")
                                            .arg("delayed")
                                            .arg("attempts_made")
                                            .arg(attempts)
                                            .query_async::<()>(&mut task_conn)
                                            .await;
                                        let _ = redis::cmd("ZADD")
                                            .arg(format!(
                                                "{}:{}:delayed",
                                                task_prefix, task_name
                                            ))
                                            .arg(process_at)
                                            .arg(&job.id)
                                            .query_async::<()>(&mut task_conn)
                                            .await;

                                        tracing::warn!(
                                            "Job {} failed (attempt {}/{}), retrying in {:?}: {}",
                                            job.id,
                                            attempts,
                                            job.max_attempts,
                                            backoff_delay,
                                            err
                                        );
                                    } else {
                                        // Max attempts reached, move to failed
                                        let now = Utc::now();
                                        let _ = redis::cmd("ZADD")
                                            .arg(format!(
                                                "{}:{}:failed",
                                                task_prefix, task_name
                                            ))
                                            .arg(now.timestamp_millis())
                                            .arg(&job.id)
                                            .query_async::<()>(&mut task_conn)
                                            .await;
                                        let _ = redis::cmd("HSET")
                                            .arg(&job_key)
                                            .arg("state")
                                            .arg("failed")
                                            .arg("finished_on")
                                            .arg(now.timestamp_millis().to_string())
                                            .arg("failed_reason")
                                            .arg(err.to_string())
                                            .arg("attempts_made")
                                            .arg(attempts)
                                            .query_async::<()>(&mut task_conn)
                                            .await;

                                        if let Some(ref cb) = on_failed {
                                            if let Ok(val_job) = convert_job_to_value(&job) {
                                                cb(&val_job, &err);
                                            }
                                        }

                                        tracing::error!(
                                            "Job {} permanently failed after {} attempts: {}",
                                            job.id,
                                            attempts,
                                            err
                                        );
                                    }
                                }
                            }
                        });
                    }
                    Err(e) => {
                        tracing::error!("Redis error while fetching job: {}", e);
                        drop(permit);
                        tokio::select! {
                            _ = sleep(poll_interval) => continue,
                            _ = shutdown_rx.changed() => break,
                        }
                    }
                }
            }

            tracing::info!("Worker '{}' shut down", name);
        });

        Ok(WorkerHandle {
            shutdown_tx,
            join_handle,
        })
    }
}

/// Move delayed jobs whose timestamp has passed to the waiting set.
async fn promote_delayed_jobs(
    conn: &ConnectionManager,
    prefix: &str,
    queue_name: &str,
) -> BullmqResult<()> {
    let mut conn = conn.clone();
    let delayed_key = format!("{}:{}:delayed", prefix, queue_name);
    let waiting_key = format!("{}:{}:waiting", prefix, queue_name);
    let now = Utc::now().timestamp_millis();

    // Get all delayed jobs whose score (process_at) is <= now
    let jobs: Vec<(String, f64)> = redis::cmd("ZRANGEBYSCORE")
        .arg(&delayed_key)
        .arg(0i64)
        .arg(now)
        .arg("WITHSCORES")
        .query_async(&mut conn)
        .await?;

    for (job_id, _score) in &jobs {
        // Move from delayed to waiting
        redis::cmd("ZREM")
            .arg(&delayed_key)
            .arg(job_id)
            .query_async::<()>(&mut conn)
            .await?;

        // Get priority from job hash
        let job_key = format!("{}:{}:{}", prefix, queue_name, job_id);
        let priority: i32 = redis::cmd("HGET")
            .arg(&job_key)
            .arg("priority")
            .query_async::<Option<i32>>(&mut conn)
            .await?
            .unwrap_or(0);

        let score = -(priority as f64);
        redis::cmd("ZADD")
            .arg(&waiting_key)
            .arg(score)
            .arg(job_id)
            .query_async::<()>(&mut conn)
            .await?;

        // Update state in hash
        redis::cmd("HSET")
            .arg(&job_key)
            .arg("state")
            .arg("waiting")
            .query_async::<()>(&mut conn)
            .await?;
    }

    Ok(())
}

/// Convert a `Job<T>` to `Job<serde_json::Value>` for type-erased callbacks.
fn convert_job_to_value<T: Serialize>(job: &Job<T>) -> BullmqResult<Job<serde_json::Value>> {
    let value = serde_json::to_value(&job.data)?;
    Ok(Job {
        id: job.id.clone(),
        name: job.name.clone(),
        data: value,
        state: job.state,
        timestamp: job.timestamp,
        priority: job.priority,
        delay: job.delay,
        attempts_made: job.attempts_made,
        max_attempts: job.max_attempts,
        backoff: job.backoff.clone(),
        ttl: job.ttl,
        progress: job.progress,
        processed_on: job.processed_on,
        finished_on: job.finished_on,
        failed_reason: job.failed_reason.clone(),
        return_value: job.return_value.clone(),
    })
}

/// Builder for creating a [`Worker`].
pub struct WorkerBuilder {
    name: String,
    connection: RedisConnection,
    prefix: String,
    options: WorkerOptions,
    on_completed: Option<Arc<dyn Fn(&Job<serde_json::Value>) + Send + Sync>>,
    on_failed: Option<Arc<dyn Fn(&Job<serde_json::Value>, &BullmqError) + Send + Sync>>,
}

impl WorkerBuilder {
    /// Create a new worker builder for the given queue name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            connection: RedisConnection::default(),
            prefix: "bull".to_string(),
            options: WorkerOptions::default(),
            on_completed: None,
            on_failed: None,
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

    /// Set the number of jobs to process concurrently (default: 1).
    pub fn concurrency(mut self, n: usize) -> Self {
        self.options.concurrency = n.max(1);
        self
    }

    /// Set the polling interval (default: 1 second).
    pub fn poll_interval(mut self, d: std::time::Duration) -> Self {
        self.options.poll_interval = d;
        self
    }

    /// Set a callback invoked when a job completes successfully.
    pub fn on_completed<F>(mut self, f: F) -> Self
    where
        F: Fn(&Job<serde_json::Value>) + Send + Sync + 'static,
    {
        self.on_completed = Some(Arc::new(f));
        self
    }

    /// Set a callback invoked when a job permanently fails.
    pub fn on_failed<F>(mut self, f: F) -> Self
    where
        F: Fn(&Job<serde_json::Value>, &BullmqError) + Send + Sync + 'static,
    {
        self.on_failed = Some(Arc::new(f));
        self
    }

    /// Build the worker, establishing the Redis connection.
    pub async fn build<T: Serialize + DeserializeOwned + Send + Sync + 'static>(
        self,
    ) -> BullmqResult<Worker<T>> {
        let conn = self.connection.get_manager().await?;
        Ok(Worker {
            name: self.name,
            prefix: self.prefix,
            conn,
            options: self.options,
            on_completed: self.on_completed,
            on_failed: self.on_failed,
            _phantom: PhantomData,
        })
    }
}
