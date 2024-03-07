use crate::error::RSHDFSError;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

#[cfg(test)]
use mockall::automock;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub(crate) enum Operation {
    #[default]
    PUT,
    DELETE,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub(crate) struct LogEntry {
    pub(crate) lsn: i32,
    pub(crate) date_time: DateTime<Utc>,
    pub(crate) operation: Operation,
    pub(crate) target_path: PathBuf,
}

#[cfg_attr(test, automock)]
#[async_trait]
pub trait EditLogManager {
    async fn flush(&self) -> Result<(), RSHDFSError>;
    async fn insert_entry(
        &mut self,
        operation: Operation,
        target_path: PathBuf,
    ) -> Result<LogEntry, RSHDFSError>;
}

#[derive(Default)]
pub struct EditLog {
    log_queue: VecDeque<LogEntry>,
    file_path: PathBuf,
}

impl EditLog {
    pub fn new(file_path: PathBuf) -> Self {
        EditLog {
            log_queue: VecDeque::new(),
            file_path,
        }
    }
}

#[cfg_attr(test, automock)]
#[async_trait]
impl EditLogManager for EditLog {
    async fn flush(&self) -> Result<(), RSHDFSError> {
        let file_path = self.file_path.clone();
        match File::open(file_path).await {
            Ok(mut entry_log_file) => {
                for entry in self.log_queue.iter() {
                    match serde_json::to_string_pretty(entry) {
                        Ok(log_entry_json_string) => {
                            match entry_log_file.write(log_entry_json_string.as_bytes()).await {
                                Ok(_) => {}
                                Err(e) => {
                                    return Err(RSHDFSError::EditLogError(e.to_string()));
                                }
                            }
                        }
                        Err(e) => {
                            return Err(RSHDFSError::EditLogError(e.to_string()));
                        }
                    }
                }
                entry_log_file.flush().await.expect("failed to flush file");
                Ok(())
            }
            Err(e) => {
                return Err(RSHDFSError::EditLogError(e.to_string()));
            }
        }
    }

    async fn insert_entry(
        &mut self,
        operation: Operation,
        target_path: PathBuf,
    ) -> Result<LogEntry, RSHDFSError> {
        let log_entry = LogEntry {
            lsn: self.log_queue.len() as i32 + 1,
            date_time: Utc::now(),
            target_path,
            operation,
        };
        self.log_queue.push_back(log_entry.clone());
        Ok(log_entry)
    }
}

#[cfg(test)]
mod tests {
    use crate::namenode::edit_log::{EditLogManager, LogEntry, MockEditLog, Operation};
    use std::path::PathBuf;

    #[tokio::test]
    async fn insert_entry_expects_success() {
        let mut mock_edit_log = MockEditLog::new();
        mock_edit_log
            .expect_insert_entry()
            .times(1)
            .returning(|operation, target_path| {
                Ok(LogEntry {
                    lsn: 0,
                    date_time: Default::default(),
                    operation,
                    target_path,
                })
            });

        let log_entry = LogEntry::default();

        let result = mock_edit_log
            .insert_entry(Operation::PUT, PathBuf::from(""))
            .await;

        assert_eq!(result, Ok(log_entry.clone()));
    }
}
