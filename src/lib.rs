//! # bullmq-rs
//!
//! A Rust implementation of [BullMQ](https://bullmq.io/) — a Redis-based
//! distributed job queue with support for priorities, delays, retries with
//! backoff, concurrency control, and typed job payloads.
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use bullmq_rs::{Queue, QueueBuilder, Worker, WorkerBuilder, RedisConnection, JobOptions};
//! use serde::{Serialize, Deserialize};
//! use std::time::Duration;
//!
//! #[derive(Serialize, Deserialize, Debug, Clone)]
//! struct Email {
//!     to: String,
//!     subject: String,
//!     body: String,
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let conn = RedisConnection::new("redis://127.0.0.1:6379");
//!
//!     // Create a typed queue
//!     let queue: Queue<Email> = QueueBuilder::new("emails")
//!         .connection(conn.clone())
//!         .build()
//!         .await?;
//!
//!     // Add a job
//!     queue.add("welcome", Email {
//!         to: "user@example.com".into(),
//!         subject: "Welcome!".into(),
//!         body: "Hello and welcome.".into(),
//!     }, None).await?;
//!
//!     // Create a worker to process jobs
//!     let worker: Worker<Email> = WorkerBuilder::new("emails")
//!         .connection(conn)
//!         .concurrency(3)
//!         .build()
//!         .await?;
//!
//!     let handle = worker.start(|job| async move {
//!         println!("Sending email to {}", job.data.to);
//!         Ok(())
//!     }).await?;
//!
//!     // Graceful shutdown
//!     handle.shutdown();
//!     handle.wait().await?;
//!     Ok(())
//! }
//! ```

pub mod connection;
pub mod error;
pub mod job;
pub mod queue;
pub mod types;
pub mod worker;

pub use connection::RedisConnection;
pub use error::{BullmqError, BullmqResult};
pub use job::Job;
pub use queue::{Queue, QueueBuilder};
pub use types::{BackoffStrategy, JobOptions, JobState, WorkerOptions};
pub use worker::{Worker, WorkerBuilder, WorkerHandle};
