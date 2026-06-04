use std::io;

use crate::fs::folder::FolderError;

#[derive(Debug)]
pub enum FolderMeshError {
    Folder(FolderError),
    Notify(notify::Error),
    Db(rusqlite::Error),
    Io(std::io::Error),
    Other(String),
}

impl From<FolderError> for FolderMeshError {
    fn from(value: FolderError) -> Self {
        FolderMeshError::Folder(value)
    }
}

impl From<notify::Error> for FolderMeshError {
    fn from(value: notify::Error) -> Self {
        FolderMeshError::Notify(value)
    }
}

impl From<rusqlite::Error> for FolderMeshError {
    fn from(value: rusqlite::Error) -> Self {
        FolderMeshError::Db(value)
    }
}

impl From<io::Error> for FolderMeshError {
    fn from(value: io::Error) -> Self {
        FolderMeshError::Io(value)
    }
}
