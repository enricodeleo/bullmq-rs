use std::collections::HashMap;

/// A typed event from a BullMQ queue's event stream.
///
/// Parsed from Redis stream entries emitted by Lua scripts via XADD.
/// The stream key is `{prefix}:{queue}:events`.
#[derive(Debug, Clone, PartialEq)]
pub enum QueueEvent {
    Added { job_id: String, name: String },
    Waiting { job_id: String, prev: Option<String> },
    Active { job_id: String, prev: Option<String> },
    Completed { job_id: String, return_value: serde_json::Value },
    Failed { job_id: String, reason: String },
    /// The `delay` field is an absolute Unix timestamp in milliseconds
    /// (not a relative duration). This matches what the Lua scripts emit.
    Delayed { job_id: String, delay: u64 },
    Progress { job_id: String, data: serde_json::Value },
    Stalled { job_id: String },
    Priority { job_id: String, priority: u32 },
    Removed { job_id: String, prev: Option<String> },
    WaitingChildren { job_id: String },
    Paused,
    Resumed,
    Drained,
    /// Custom or unrecognized events (e.g. from QueueEventsProducer).
    Unknown { event: String, fields: HashMap<String, String> },
}

impl QueueEvent {
    /// Parse a stream entry's field-value map into a QueueEvent.
    pub fn parse(fields: &HashMap<String, String>) -> Self {
        let event_type = fields.get("event").map(|s| s.as_str()).unwrap_or("");
        let job_id = || fields.get("jobId").cloned().unwrap_or_default();
        let prev = || {
            fields.get("prev").and_then(|s| {
                if s.is_empty() { None } else { Some(s.clone()) }
            })
        };

        match event_type {
            "added" => QueueEvent::Added {
                job_id: job_id(),
                name: fields.get("name").cloned().unwrap_or_default(),
            },
            "waiting" => QueueEvent::Waiting { job_id: job_id(), prev: prev() },
            "active" => QueueEvent::Active { job_id: job_id(), prev: prev() },
            "completed" => QueueEvent::Completed {
                job_id: job_id(),
                return_value: fields.get("returnvalue")
                    .and_then(|s| serde_json::from_str(s).ok())
                    .unwrap_or(serde_json::Value::Null),
            },
            "failed" => QueueEvent::Failed {
                job_id: job_id(),
                reason: fields.get("failedReason").cloned().unwrap_or_default(),
            },
            "delayed" => QueueEvent::Delayed {
                job_id: job_id(),
                delay: fields.get("delay").and_then(|s| s.parse().ok()).unwrap_or(0),
            },
            "progress" => QueueEvent::Progress {
                job_id: job_id(),
                data: fields.get("data")
                    .and_then(|s| serde_json::from_str(s).ok())
                    .unwrap_or(serde_json::Value::Null),
            },
            "stalled" => QueueEvent::Stalled { job_id: job_id() },
            "priority" => QueueEvent::Priority {
                job_id: job_id(),
                priority: fields.get("priority").and_then(|s| s.parse().ok()).unwrap_or(0),
            },
            "removed" => QueueEvent::Removed { job_id: job_id(), prev: prev() },
            "waiting-children" => QueueEvent::WaitingChildren { job_id: job_id() },
            "paused" => QueueEvent::Paused,
            "resumed" => QueueEvent::Resumed,
            "drained" => QueueEvent::Drained,
            _ => QueueEvent::Unknown {
                event: event_type.to_string(),
                fields: fields.clone(),
            },
        }
    }
}
