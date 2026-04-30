//! checkpoint.rs — durable sync cursor persistence for crash-safe resume.

use std::{
    fs::File,
    io::BufWriter,
    path::{Path, PathBuf},
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::fs;
use uuid::Uuid;

use crate::{
    error::{SyncError, SyncResult},
    proto::clawsync::v1::SyncCursor,
    sync::{pull::PullStats, push::PushStats},
};

/// Persisted checkpoint state stored on disk.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointedState {
    /// Cursor to resume sync from.
    pub cursor: SyncCursor,
    /// Time at which the checkpoint was saved.
    pub saved_at: DateTime<Utc>,
    /// Optional push statistics captured at save time.
    pub push_stats: Option<PushStats>,
    /// Optional pull statistics captured at save time.
    pub pull_stats: Option<PullStats>,
}

/// File-backed store for sync checkpoints.
#[derive(Debug, Clone)]
pub struct CheckpointStore {
    data_dir: PathBuf,
}

impl CheckpointStore {
    /// Creates the checkpoint store and its `checkpoints/` subdirectory.
    pub fn new(data_dir: &Path) -> SyncResult<Self> {
        std::fs::create_dir_all(data_dir.join("checkpoints"))?;
        Ok(Self {
            data_dir: data_dir.to_path_buf(),
        })
    }

    /// Persists the supplied cursor atomically.
    pub async fn save(&self, cursor: &SyncCursor) -> SyncResult<()> {
        self.save_state(&CheckpointedState {
            cursor: cursor.clone(),
            saved_at: Utc::now(),
            push_stats: None,
            pull_stats: None,
        })
        .await
    }

    /// Persists a full checkpoint state atomically.
    pub async fn save_state(&self, state: &CheckpointedState) -> SyncResult<()> {
        let path = self.cursor_path(
            Uuid::parse_str(&state.cursor.workspace_id).map_err(|error| {
                SyncError::Validation(format!("invalid checkpoint workspace id: {error}"))
            })?,
            Uuid::parse_str(&state.cursor.device_id).map_err(|error| {
                SyncError::Validation(format!("invalid checkpoint device id: {error}"))
            })?,
        );
        let state = state.clone();
        tokio::task::spawn_blocking(move || atomic_write_json(&path, &state))
            .await
            .map_err(|error| SyncError::Io(std::io::Error::other(error.to_string())))?
    }

    /// Loads the checkpoint cursor for a workspace/device pair.
    pub async fn load(
        &self,
        workspace_id: Uuid,
        device_id: Uuid,
    ) -> SyncResult<Option<SyncCursor>> {
        let path = self.cursor_path(workspace_id, device_id);
        if !fs::try_exists(&path).await? {
            return Ok(None);
        }

        let state: CheckpointedState = serde_json::from_slice(&fs::read(path).await?)?;
        Ok(Some(state.cursor))
    }

    /// Deletes the checkpoint file for a workspace/device pair.
    pub async fn delete(&self, workspace_id: Uuid, device_id: Uuid) -> SyncResult<()> {
        let path = self.cursor_path(workspace_id, device_id);
        if fs::try_exists(&path).await? {
            fs::remove_file(path).await?;
        }
        Ok(())
    }

    /// Lists every persisted checkpoint cursor.
    pub async fn list_all(&self) -> SyncResult<Vec<SyncCursor>> {
        let mut entries = fs::read_dir(self.checkpoints_dir()).await?;
        let mut cursors = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            if entry
                .path()
                .extension()
                .and_then(|extension| extension.to_str())
                != Some("json")
            {
                continue;
            }

            let state: CheckpointedState = serde_json::from_slice(&fs::read(entry.path()).await?)?;
            cursors.push(state.cursor);
        }
        cursors.sort_by(|left, right| {
            left.workspace_id
                .cmp(&right.workspace_id)
                .then(left.device_id.cmp(&right.device_id))
        });
        Ok(cursors)
    }

    /// Returns the full checkpoint state when it exists.
    pub async fn load_state(
        &self,
        workspace_id: Uuid,
        device_id: Uuid,
    ) -> SyncResult<Option<CheckpointedState>> {
        let path = self.cursor_path(workspace_id, device_id);
        if !fs::try_exists(&path).await? {
            return Ok(None);
        }

        Ok(Some(serde_json::from_slice(&fs::read(path).await?)?))
    }

    fn checkpoints_dir(&self) -> PathBuf {
        self.data_dir.join("checkpoints")
    }

    fn cursor_path(&self, workspace_id: Uuid, device_id: Uuid) -> PathBuf {
        self.checkpoints_dir()
            .join(format!("{workspace_id}-{device_id}.json"))
    }
}

/// Atomically writes a JSON file to `path` via a temporary file, fsync, and rename.
pub fn atomic_write_json<T: Serialize>(path: &Path, value: &T) -> SyncResult<()> {
    let tmp_path = path.with_extension("tmp");
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(SyncError::Io)?;
    }

    let file = File::create(&tmp_path).map_err(SyncError::Io)?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, value)?;
    let file = writer
        .into_inner()
        .map_err(|error| SyncError::Io(error.into_error()))?;
    file.sync_all().map_err(SyncError::Io)?;
    std::fs::rename(&tmp_path, path).map_err(SyncError::Io)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use tempfile::tempdir;
    use uuid::Uuid;

    use super::{atomic_write_json, CheckpointStore};
    use crate::{
        proto::clawsync::v1::SyncCursor,
        sync::{pull::PullStats, push::PushStats},
    };

    #[tokio::test]
    async fn save_and_load_round_trip() {
        let dir = tempdir().expect("tempdir should exist");
        let store = CheckpointStore::new(dir.path()).expect("store should initialise");
        let cursor = SyncCursor {
            workspace_id: Uuid::from_bytes([1; 16]).to_string(),
            device_id: Uuid::from_bytes([2; 16]).to_string(),
            lamport_clock: 42,
            hlc_timestamp: "hlc".into(),
            state_hash: vec![1, 2, 3],
        };

        store.save(&cursor).await.expect("save should succeed");
        let loaded = store
            .load(Uuid::from_bytes([1; 16]), Uuid::from_bytes([2; 16]))
            .await
            .expect("load should succeed")
            .expect("checkpoint should exist");
        assert_eq!(loaded.lamport_clock, 42);
        assert_eq!(loaded.state_hash, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn save_state_preserves_stats_metadata() {
        let dir = tempdir().expect("tempdir should exist");
        let store = CheckpointStore::new(dir.path()).expect("store should initialise");
        let cursor = SyncCursor {
            workspace_id: Uuid::from_bytes([3; 16]).to_string(),
            device_id: Uuid::from_bytes([4; 16]).to_string(),
            lamport_clock: 7,
            hlc_timestamp: "hlc-2".into(),
            state_hash: vec![9],
        };
        store
            .save_state(&super::CheckpointedState {
                cursor: cursor.clone(),
                saved_at: chrono::Utc::now(),
                push_stats: Some(PushStats::default()),
                pull_stats: Some(PullStats::default()),
            })
            .await
            .expect("save_state should succeed");

        let state = store
            .load_state(Uuid::from_bytes([3; 16]), Uuid::from_bytes([4; 16]))
            .await
            .expect("load_state should succeed")
            .expect("state should exist");
        assert_eq!(state.cursor, cursor);
        assert!(state.push_stats.is_some());
        assert!(state.pull_stats.is_some());
    }

    #[tokio::test]
    async fn list_all_returns_every_saved_cursor() {
        let dir = tempdir().expect("tempdir should exist");
        let store = CheckpointStore::new(dir.path()).expect("store should initialise");

        for byte in [5_u8, 6_u8] {
            store
                .save(&SyncCursor {
                    workspace_id: Uuid::from_bytes([byte; 16]).to_string(),
                    device_id: Uuid::from_bytes([byte + 1; 16]).to_string(),
                    lamport_clock: i64::from(byte),
                    hlc_timestamp: format!("hlc-{byte}"),
                    state_hash: vec![byte],
                })
                .await
                .expect("save should succeed");
        }

        let cursors = store.list_all().await.expect("list_all should succeed");
        assert_eq!(cursors.len(), 2);
    }

    #[test]
    fn atomic_write_json_replaces_target_file() {
        let dir = tempdir().expect("tempdir should exist");
        let path = dir.path().join("checkpoint.json");

        atomic_write_json(Path::new(&path), &serde_json::json!({ "value": 1 }))
            .expect("first write should succeed");
        atomic_write_json(Path::new(&path), &serde_json::json!({ "value": 2 }))
            .expect("second write should succeed");

        let content = std::fs::read_to_string(path).expect("checkpoint should be readable");
        assert!(content.contains('2'));
    }
}
