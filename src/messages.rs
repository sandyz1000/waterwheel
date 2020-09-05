use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// TODO - move this out into general code
#[derive(PartialEq, Hash, Eq, Clone, Debug)]
pub struct Token {
    pub task_id: Uuid,
    pub trigger_datetime: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TaskDef {
    pub task_id: String,
    pub task_name: String,
    pub job_id: String,
    pub job_name: String,
    pub project_id: String,
    pub project_name: String,
    pub trigger_datetime: String,
    pub image: Option<String>,
    pub args: Vec<String>,
    pub env: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TaskResult {
    pub task_id: String,
    pub trigger_datetime: String,
    pub result: String,
    pub worker_id: Uuid,
}

impl TaskResult {
    pub fn get_token(&self) -> Result<Token> {
        Ok(Token {
            task_id: Uuid::parse_str(&self.task_id)?,
            trigger_datetime: DateTime::parse_from_rfc3339(&self.trigger_datetime)?
                .with_timezone(&Utc),
        })
    }
}

#[repr(u8)]
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum TaskPriority {
    BackFill = 0,
    Low = 1,
    Normal = 2,
    High = 3,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct WorkerHeartbeat {
    pub uuid: Uuid,
    pub addr: String,
    pub last_seen_datetime: DateTime<Utc>,
}
