use std::{
    fmt,
    path::PathBuf,
    sync::mpsc::{self, Sender},
};

use notify::{Event, EventKind, Watcher};

use crate::{error::FolderMeshError, fs::walk};

const DATA_SUBDIR: &str = ".sync";
const DB_FILENAME: &str = "sqlite.db";

pub struct Folder {
    pub path: PathBuf,
    pub data_dir_path: PathBuf,
    pub db_path: PathBuf,
}

#[derive(Debug)]
pub enum FolderError {
    NotFound(PathBuf),
    NotDirectory(PathBuf),
    CanonicalizeFailed {
        path: PathBuf,
        source: std::io::Error,
    },
}

impl fmt::Display for FolderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FolderError::NotFound(path) => write!(f, "folder does not exist: {}", path.display()),
            FolderError::NotDirectory(path) => {
                write!(f, "path is not a directory: {}", path.display())
            }
            FolderError::CanonicalizeFailed { path, source } => write!(
                f,
                "failed to canonicalize directory {}: {}",
                path.display(),
                source
            ),
        }
    }
}

impl Folder {
    pub fn new(path: &String) -> Result<Self, FolderMeshError> {
        let path = Folder::extract_path(path)?;
        let data_dir_path = path.join(DATA_SUBDIR);
        let db_path = data_dir_path.join(DB_FILENAME);

        std::fs::create_dir_all(&data_dir_path)?;

        Ok(Folder {
            path,
            data_dir_path,
            db_path,
        })
    }

    pub fn watch_sync_directory(&self, sync_tx: Sender<Event>) -> notify::Result<()> {
        let (notify_tx, notify_rx) = mpsc::channel::<notify::Result<Event>>();
        let mut watcher = notify::recommended_watcher(notify_tx)?;

        watcher.watch(&self.path, notify::RecursiveMode::Recursive)?;

        for res in notify_rx {
            match res {
                Ok(event) => self.handle_event(event, &sync_tx),
                Err(e) => eprintln!("watch error: {:?}", e),
            }
        }

        Ok(())
    }

    fn should_ignore_event(&self, event: &Event) -> bool {
        match event.kind {
            EventKind::Access(_) => return true,
            _ => {}
        }

        event
            .paths
            .iter()
            .any(|path| path.starts_with(&self.data_dir_path))
    }

    fn handle_event(&self, event: Event, sync_tx: &Sender<Event>) {
        if self.should_ignore_event(&event) {
            return;
        }

        if let Err(e) = sync_tx.send(event) {
            eprintln!("failed to send filesystem event to engine thread");
        }
    }

    fn extract_path(path_string: &String) -> Result<PathBuf, FolderError> {
        let path_buf = PathBuf::from(path_string);
        path_buf
            .canonicalize()
            .map_err(|source| FolderError::CanonicalizeFailed {
                path: path_buf.clone(),
                source,
            })
    }

    fn get_folder_state(&self) {
        let node = walk::Node::new(&self.path, Some(&self.data_dir_path));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
