use bullmq_rs::*;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct TestJob {
    value: String,
}

fn redis_conn() -> RedisConnection {
    RedisConnection::new(
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string()),
    )
}

#[tokio::test]
#[ignore = "requires running Redis"]
async fn test_queue_add_and_get_job() {
    let queue = QueueBuilder::new("test_add_get")
        .connection(redis_conn())
        .build::<TestJob>()
        .await
        .unwrap();

    // Clean up
    queue.drain().await.unwrap();

    let job = queue
        .add(
            "test",
            TestJob {
                value: "hello".into(),
            },
            None,
        )
        .await
        .unwrap();

    let fetched = queue.get_job(&job.id).await.unwrap().unwrap();
    assert_eq!(fetched.name, "test");
    assert_eq!(fetched.data.value, "hello");
    assert_eq!(fetched.state, JobState::Waiting);

    // Clean up
    queue.drain().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Redis"]
async fn test_queue_job_counts() {
    let queue = QueueBuilder::new("test_counts")
        .connection(redis_conn())
        .build::<TestJob>()
        .await
        .unwrap();

    queue.drain().await.unwrap();

    for i in 0..3 {
        queue
            .add(
                &format!("job_{}", i),
                TestJob {
                    value: format!("val_{}", i),
                },
                None,
            )
            .await
            .unwrap();
    }

    let counts = queue.get_job_counts().await.unwrap();
    assert_eq!(*counts.get(&JobState::Waiting).unwrap(), 3);
    assert_eq!(*counts.get(&JobState::Active).unwrap(), 0);

    queue.drain().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Redis"]
async fn test_queue_remove_job() {
    let queue = QueueBuilder::new("test_remove")
        .connection(redis_conn())
        .build::<TestJob>()
        .await
        .unwrap();

    queue.drain().await.unwrap();

    let job = queue
        .add(
            "to_remove",
            TestJob {
                value: "bye".into(),
            },
            None,
        )
        .await
        .unwrap();

    queue.remove(&job.id).await.unwrap();
    let fetched = queue.get_job(&job.id).await.unwrap();
    assert!(fetched.is_none());

    queue.drain().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Redis"]
async fn test_delayed_job() {
    let queue = QueueBuilder::new("test_delayed")
        .connection(redis_conn())
        .build::<TestJob>()
        .await
        .unwrap();

    queue.drain().await.unwrap();

    let job = queue
        .add(
            "delayed",
            TestJob {
                value: "later".into(),
            },
            Some(JobOptions {
                delay: Some(Duration::from_secs(60)),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

    assert_eq!(job.state, JobState::Delayed);

    let counts = queue.get_job_counts().await.unwrap();
    assert_eq!(*counts.get(&JobState::Delayed).unwrap(), 1);
    assert_eq!(*counts.get(&JobState::Waiting).unwrap(), 0);

    queue.drain().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Redis"]
async fn test_worker_processes_job() {
    let conn = redis_conn();

    let queue = QueueBuilder::new("test_worker")
        .connection(conn.clone())
        .build::<TestJob>()
        .await
        .unwrap();

    queue.drain().await.unwrap();

    queue
        .add(
            "work",
            TestJob {
                value: "process me".into(),
            },
            None,
        )
        .await
        .unwrap();

    let worker = WorkerBuilder::new("test_worker")
        .connection(conn)
        .poll_interval(Duration::from_millis(100))
        .build::<TestJob>()
        .await
        .unwrap();

    let handle = worker.start(|_job| async move { Ok(()) }).await.unwrap();

    // Wait for processing
    tokio::time::sleep(Duration::from_secs(2)).await;
    handle.shutdown();
    handle.wait().await.unwrap();

    let counts = queue.get_job_counts().await.unwrap();
    assert_eq!(*counts.get(&JobState::Completed).unwrap(), 1);
    assert_eq!(*counts.get(&JobState::Waiting).unwrap(), 0);

    queue.drain().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Redis"]
async fn test_worker_retry_then_fail() {
    let conn = redis_conn();

    let queue = QueueBuilder::new("test_retry")
        .connection(conn.clone())
        .build::<TestJob>()
        .await
        .unwrap();

    queue.drain().await.unwrap();

    queue
        .add(
            "will_fail",
            TestJob {
                value: "fail".into(),
            },
            Some(JobOptions {
                attempts: Some(2),
                backoff: Some(BackoffStrategy::Fixed {
                    delay: Duration::from_millis(100),
                }),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

    let worker = WorkerBuilder::new("test_retry")
        .connection(conn)
        .poll_interval(Duration::from_millis(100))
        .build::<TestJob>()
        .await
        .unwrap();

    let handle = worker
        .start(|_job| async move { Err(BullmqError::Other("intentional failure".into())) })
        .await
        .unwrap();

    // Wait for retries to complete
    tokio::time::sleep(Duration::from_secs(3)).await;
    handle.shutdown();
    handle.wait().await.unwrap();

    let counts = queue.get_job_counts().await.unwrap();
    assert_eq!(*counts.get(&JobState::Failed).unwrap(), 1);

    queue.drain().await.unwrap();
}
