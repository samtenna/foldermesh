use std::{
    fmt,
    path::{Path, PathBuf},
    sync::mpsc,
};

use notify::{Event, Watcher};

pub struct Folder {
    path: PathBuf,
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
        Ok(Folder {
            path: Folder::extract_path(path)?,
        })
    }

    pub fn watch_sync_directory(&self) -> notify::Result<()> {
        let (tx, rx) = mpsc::channel::<notify::Result<Event>>();
        let mut watcher = notify::recommended_watcher(tx)?;

        watcher.watch(&self.path, notify::RecursiveMode::Recursive)?;

        for res in rx {
            match res {
                Ok(event) => println!("event: {:?}", event),
                Err(e) => println!("watch error: {:?}", e),
            }
        }

        Ok(())
    }

    fn extract_path(path_string: &String) -> Result<PathBuf, FolderError> {
        let mut path_buf = PathBuf::new();
        path_buf = path_buf.join(path_string);
        path_buf
            .canonicalize()
            .map_err(|source| FolderError::CanonicalizeFailed {
                path: path_buf.clone(),
                source,
            })?;

        Ok(path_buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
