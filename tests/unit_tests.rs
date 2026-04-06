use bullmq_rs::*;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct TestData {
    message: String,
    count: u32,
}

#[test]
fn test_job_creation_defaults() {
    let job = bullmq_rs::Job::new(
        "1".to_string(),
        "test".to_string(),
        TestData {
            message: "hello".into(),
            count: 42,
        },
        None,
    );

    assert_eq!(job.id, "1");
    assert_eq!(job.name, "test");
    assert_eq!(job.state, JobState::Waiting);
    assert_eq!(job.priority, 0);
    assert_eq!(job.delay, 0);
    assert_eq!(job.attempts_made, 0);
    assert_eq!(job.max_attempts, 1);
    assert!(job.backoff.is_none());
    assert!(job.ttl.is_none());
    assert!(job.progress.is_none());
    assert!(job.processed_on.is_none());
    assert!(job.finished_on.is_none());
    assert!(job.failed_reason.is_none());
    assert_eq!(job.data.message, "hello");
    assert_eq!(job.data.count, 42);
}

#[test]
fn test_job_creation_with_options() {
    let opts = JobOptions {
        priority: Some(5),
        delay: Some(Duration::from_secs(10)),
        attempts: Some(3),
        backoff: Some(BackoffStrategy::Fixed {
            delay: Duration::from_secs(2),
        }),
        ttl: Some(Duration::from_secs(3600)),
        job_id: Some("custom-id".into()),
    };

    let job = bullmq_rs::Job::new(
        "custom-id".to_string(),
        "important".to_string(),
        TestData {
            message: "urgent".into(),
            count: 1,
        },
        Some(opts),
    );

    assert_eq!(job.id, "custom-id");
    assert_eq!(job.state, JobState::Delayed); // has delay
    assert_eq!(job.priority, 5);
    assert_eq!(job.delay, 10_000); // 10 seconds in ms
    assert_eq!(job.max_attempts, 3);
    assert!(job.backoff.is_some());
    assert_eq!(job.ttl, Some(3_600_000)); // 1 hour in ms
}

#[test]
fn test_job_serialization_roundtrip() {
    let job = bullmq_rs::Job::new(
        "1".to_string(),
        "test".to_string(),
        TestData {
            message: "hello".into(),
            count: 42,
        },
        None,
    );

    let hash = job.to_redis_hash().unwrap();
    let map: std::collections::HashMap<String, String> = hash.into_iter().collect();

    let restored: bullmq_rs::Job<TestData> = bullmq_rs::Job::from_redis_hash("1", &map).unwrap();

    assert_eq!(restored.id, "1");
    assert_eq!(restored.name, "test");
    assert_eq!(restored.data, job.data);
    assert_eq!(restored.state, JobState::Waiting);
    assert_eq!(restored.priority, 0);
    assert_eq!(restored.max_attempts, 1);
}

#[test]
fn test_job_serialization_uses_bullmq_field_names() {
    let job = bullmq_rs::Job::new(
        "1".to_string(),
        "test".to_string(),
        TestData {
            message: "hello".into(),
            count: 42,
        },
        Some(JobOptions {
            priority: Some(5),
            delay: Some(Duration::from_secs(10)),
            attempts: Some(3),
            backoff: Some(BackoffStrategy::Fixed {
                delay: Duration::from_secs(2),
            }),
            ttl: Some(Duration::from_secs(30)),
            job_id: Some("1".into()),
        }),
    );

    let hash = job.to_redis_hash().unwrap();
    let map: std::collections::HashMap<String, String> = hash.into_iter().collect();

    assert!(map.contains_key("opts"));
    assert!(map.contains_key("timestamp"));
    assert!(map.contains_key("priority"));
    assert!(map.contains_key("delay"));
    assert!(map.contains_key("atm"));
    assert!(!map.contains_key("attempts_made"));
    assert!(!map.contains_key("max_attempts"));

    let opts: serde_json::Value = serde_json::from_str(map.get("opts").unwrap()).unwrap();
    assert_eq!(opts["delay"], 10_000);
    assert_eq!(opts["attempts"], 3);
    assert_eq!(opts["priority"], 5);
    assert_eq!(opts["backoff"]["type"], "fixed");
    assert_eq!(opts["backoff"]["delay"], 2_000);
}

#[test]
fn test_job_deserializes_bullmq_field_names() {
    let mut map = std::collections::HashMap::new();
    map.insert("name".to_string(), "test".to_string());
    map.insert(
        "data".to_string(),
        r#"{"message":"hello","count":42}"#.to_string(),
    );
    map.insert("state".to_string(), "delayed".to_string());
    map.insert("timestamp".to_string(), "1712536623456".to_string());
    map.insert("priority".to_string(), "5".to_string());
    map.insert("delay".to_string(), "10000".to_string());
    map.insert("atm".to_string(), "2".to_string());
    map.insert(
        "opts".to_string(),
        r#"{"attempts":3,"delay":10000,"priority":5,"backoff":{"type":"fixed","delay":2000},"ttl":30000}"#
            .to_string(),
    );
    map.insert("processedOn".to_string(), "1712536624456".to_string());
    map.insert("finishedOn".to_string(), "1712536625456".to_string());
    map.insert("failedReason".to_string(), "boom".to_string());
    map.insert("returnvalue".to_string(), r#"{"ok":true}"#.to_string());

    let restored: bullmq_rs::Job<TestData> = bullmq_rs::Job::from_redis_hash("1", &map).unwrap();

    assert_eq!(restored.id, "1");
    assert_eq!(restored.name, "test");
    assert_eq!(restored.state, JobState::Delayed);
    assert_eq!(restored.priority, 5);
    assert_eq!(restored.delay, 10_000);
    assert_eq!(restored.attempts_made, 2);
    assert_eq!(restored.max_attempts, 3);
    assert_eq!(restored.failed_reason.as_deref(), Some("boom"));
    assert_eq!(
        restored.return_value,
        Some(serde_json::json!({ "ok": true }))
    );
    assert!(restored.processed_on.is_some());
    assert!(restored.finished_on.is_some());
}

#[test]
fn test_job_state_display() {
    assert_eq!(JobState::Waiting.to_string(), "waiting");
    assert_eq!(JobState::Delayed.to_string(), "delayed");
    assert_eq!(JobState::Active.to_string(), "active");
    assert_eq!(JobState::Completed.to_string(), "completed");
    assert_eq!(JobState::Failed.to_string(), "failed");
}

#[test]
fn test_job_state_parse() {
    assert_eq!("waiting".parse::<JobState>().unwrap(), JobState::Waiting);
    assert_eq!("delayed".parse::<JobState>().unwrap(), JobState::Delayed);
    assert_eq!("active".parse::<JobState>().unwrap(), JobState::Active);
    assert_eq!(
        "completed".parse::<JobState>().unwrap(),
        JobState::Completed
    );
    assert_eq!("failed".parse::<JobState>().unwrap(), JobState::Failed);
    assert!("invalid".parse::<JobState>().is_err());
}

#[test]
fn test_backoff_fixed() {
    let strategy = BackoffStrategy::Fixed {
        delay: Duration::from_secs(5),
    };
    assert_eq!(strategy.delay_for_attempt(0), Duration::from_secs(5));
    assert_eq!(strategy.delay_for_attempt(1), Duration::from_secs(5));
    assert_eq!(strategy.delay_for_attempt(10), Duration::from_secs(5));
}

#[test]
fn test_backoff_exponential() {
    let strategy = BackoffStrategy::Exponential {
        base: Duration::from_secs(1),
        max: Duration::from_secs(30),
    };
    assert_eq!(strategy.delay_for_attempt(0), Duration::from_secs(1));
    assert_eq!(strategy.delay_for_attempt(1), Duration::from_secs(2));
    assert_eq!(strategy.delay_for_attempt(2), Duration::from_secs(4));
    assert_eq!(strategy.delay_for_attempt(3), Duration::from_secs(8));
    // Should cap at max
    assert_eq!(strategy.delay_for_attempt(10), Duration::from_secs(30));
}

#[test]
fn test_job_options_default() {
    let opts = JobOptions::default();
    assert!(opts.priority.is_none());
    assert!(opts.delay.is_none());
    assert!(opts.attempts.is_none());
    assert!(opts.backoff.is_none());
    assert!(opts.ttl.is_none());
    assert!(opts.job_id.is_none());
}

#[test]
fn test_worker_options_default() {
    let opts = WorkerOptions::default();
    assert_eq!(opts.concurrency, 1);
    assert_eq!(opts.poll_interval, Duration::from_secs(1));
}

#[test]
fn test_redis_connection_default() {
    let conn = RedisConnection::default();
    assert_eq!(conn.url(), "redis://127.0.0.1:6379");
}

#[test]
fn test_redis_connection_custom() {
    let conn = RedisConnection::new("redis://myhost:6380/1");
    assert_eq!(conn.url(), "redis://myhost:6380/1");
}

#[test]
fn test_error_display() {
    let err = BullmqError::JobNotFound("123".into());
    assert_eq!(err.to_string(), "Job not found: 123");

    let err = BullmqError::WorkerClosed;
    assert_eq!(err.to_string(), "Worker has been shut down");

    let err = BullmqError::Other("custom error".into());
    assert_eq!(err.to_string(), "custom error");
}

#[test]
fn test_error_from_redis() {
    let redis_err = redis::RedisError::from((redis::ErrorKind::IoError, "connection refused"));
    let err: BullmqError = redis_err.into();
    assert!(matches!(err, BullmqError::Redis(_)));
    assert!(err.to_string().contains("connection refused"));
}

#[test]
fn test_backoff_strategy_serialization() {
    let strategy = BackoffStrategy::Exponential {
        base: Duration::from_secs(1),
        max: Duration::from_secs(60),
    };
    let json = serde_json::to_string(&strategy).unwrap();
    let restored: BackoffStrategy = serde_json::from_str(&json).unwrap();

    assert_eq!(restored.delay_for_attempt(0), Duration::from_secs(1));
    assert_eq!(restored.delay_for_attempt(5), Duration::from_secs(32));
}
