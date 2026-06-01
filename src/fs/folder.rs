use std::{fmt, path::PathBuf, sync::mpsc};

use notify::{Event, Watcher};

use crate::fs::walk;

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
    pub fn new(path: &String) -> Result<Self, FolderError> {
        let path = Folder::extract_path(path)?;
        let data_dir_path = path.join(DATA_SUBDIR);
        let db_path = data_dir_path.join(DB_FILENAME);

        Ok(Folder {
            path,
            data_dir_path,
            db_path,
        })
    }

    pub fn watch_sync_directory(&self) -> notify::Result<()> {
        let (tx, rx) = mpsc::channel::<notify::Result<Event>>();
        let mut watcher = notify::recommended_watcher(tx)?;

        watcher.watch(&self.path, notify::RecursiveMode::Recursive)?;

        for res in rx {
            match res {
                Ok(event) => self.handle_event(&event),
                Err(e) => println!("watch error: {:?}", e),
            }
        }

        Ok(())
    }

    fn should_ignore_event(&self, event: &Event) -> bool {
        event
            .paths
            .iter()
            .any(|path| path.starts_with(&self.data_dir_path))
    }

    fn handle_event(&self, event: &Event) {
        // Ignore events in the ".sync" directory
        if self.should_ignore_event(event) {
            return;
        }

        println!("event: {:?}", event);
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
