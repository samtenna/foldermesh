use crate::fs::folder::FolderError;

#[derive(Debug)]
pub enum FolderMeshError {
    Folder(FolderError),
    Notify(notify::Error),
    // Io(std::io::Error),
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
