use bullmq_rs::*;
use std::time::Duration;

// ---------------------------------------------------------------------------
// JobState tests
// ---------------------------------------------------------------------------

#[test]
fn test_job_state_display() {
    assert_eq!(JobState::Wait.to_string(), "wait");
    assert_eq!(JobState::Delayed.to_string(), "delayed");
    assert_eq!(JobState::Active.to_string(), "active");
    assert_eq!(JobState::Completed.to_string(), "completed");
    assert_eq!(JobState::Failed.to_string(), "failed");
}

#[test]
fn test_job_state_parse() {
    assert_eq!("wait".parse::<JobState>().unwrap(), JobState::Wait);
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
fn test_job_state_wait_variant() {
    // Wait
    assert_eq!(JobState::Wait.to_string(), "wait");
    assert_eq!("wait".parse::<JobState>().unwrap(), JobState::Wait);

    // Paused
    assert_eq!(JobState::Paused.to_string(), "paused");
    assert_eq!("paused".parse::<JobState>().unwrap(), JobState::Paused);

    // WaitingChildren
    assert_eq!(JobState::WaitingChildren.to_string(), "waiting-children");
    assert_eq!(
        "waiting-children".parse::<JobState>().unwrap(),
        JobState::WaitingChildren
    );
}

#[test]
fn test_job_state_serde_roundtrip() {
    let states = vec![
        (JobState::Wait, "\"wait\""),
        (JobState::Paused, "\"paused\""),
        (JobState::WaitingChildren, "\"waiting-children\""),
        (JobState::Delayed, "\"delayed\""),
        (JobState::Active, "\"active\""),
        (JobState::Completed, "\"completed\""),
        (JobState::Failed, "\"failed\""),
    ];
    for (state, expected_json) in states {
        let json = serde_json::to_string(&state).unwrap();
        assert_eq!(json, expected_json, "serializing {:?}", state);
        let restored: JobState = serde_json::from_str(&json).unwrap();
        assert_eq!(restored, state, "deserializing {:?}", state);
    }
}

// ---------------------------------------------------------------------------
// Backoff tests
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// JobOptions tests
// ---------------------------------------------------------------------------

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
fn test_job_options_u32_priority() {
    let opts = JobOptions {
        priority: Some(42u32),
        ..Default::default()
    };
    assert_eq!(opts.priority, Some(42u32));

    // Ensure u32 max is representable
    let opts_max = JobOptions {
        priority: Some(u32::MAX),
        ..Default::default()
    };
    assert_eq!(opts_max.priority, Some(u32::MAX));
}

// ---------------------------------------------------------------------------
// WorkerOptions tests
// ---------------------------------------------------------------------------

#[test]
fn test_worker_options_v2_defaults() {
    let opts = WorkerOptions::default();
    assert_eq!(opts.concurrency, 1);
    assert_eq!(opts.lock_duration, Duration::from_secs(30));
    assert_eq!(opts.stalled_interval, Duration::from_secs(30));
    assert_eq!(opts.max_stalled_count, 1);
    assert!(!opts.skip_stalled_check);
}

// ---------------------------------------------------------------------------
// RedisConnection tests
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Error tests
// ---------------------------------------------------------------------------

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
    let redis_err = redis::RedisError::from((
        redis::ErrorKind::Io,
        "connection refused",
    ));
    let err: BullmqError = redis_err.into();
    assert!(matches!(err, BullmqError::Redis(_)));
    assert!(err.to_string().contains("connection refused"));
}

#[test]
fn test_error_new_variants() {
    let err = BullmqError::LockMismatch;
    assert_eq!(
        err.to_string(),
        "Lock token mismatch: job was stalled and recovered"
    );

    let err = BullmqError::QueuePaused;
    assert_eq!(err.to_string(), "Queue is paused");

    let err = BullmqError::ScriptError("NOSCRIPT No matching script".into());
    assert_eq!(
        err.to_string(),
        "Script error: NOSCRIPT No matching script"
    );
}
