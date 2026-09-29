use std::{
    fmt,
    path::PathBuf,
    sync::mpsc::{self, Sender},
};

use notify::{Event, EventKind, Watcher};

use crate::{error::FolderMeshError, fs::walk};

const DATA_SUBDIR: &str = ".sync";
const DB_FILENAME: &str = "sqlite.db";

#[derive(Debug)]
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
                Ok(event) => {
                    if !self.handle_event(event, &sync_tx) {
                        break;
                    }
                }
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

    fn handle_event(&self, event: Event, sync_tx: &Sender<Event>) -> bool {
        if self.should_ignore_event(&event) {
            return true;
        }

        if let Err(_e) = sync_tx.send(event) {
            eprintln!("failed to send filesystem event to engine thread");
            return false;
        }

        true
    }

    fn extract_path(path_string: &String) -> Result<PathBuf, FolderError> {
        let path_buf = PathBuf::from(path_string);
        let canonical = path_buf
            .canonicalize()
            .map_err(|source| {
                if source.kind() == std::io::ErrorKind::NotFound {
                    FolderError::NotFound(path_buf.clone())
                } else {
                    FolderError::CanonicalizeFailed {
                        path: path_buf.clone(),
                        source,
                    }
                }
            })?;

        if !canonical.is_dir() {
            return Err(FolderError::NotDirectory(canonical));
        }

        Ok(canonical)
    }

    pub fn get_folder_state(&self) -> std::io::Result<walk::Node> {
        walk::Node::new(&self.path, Some(&self.data_dir_path))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs, io,
        path::PathBuf,
        sync::mpsc::{self, TryRecvError},
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    fn test_root(name: &str) -> io::Result<PathBuf> {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("foldermesh-folder-{name}-{nanos}"));
        fs::create_dir(&path)?;
        Ok(path)
    }

    #[test]
    fn new_initializes_paths_and_creates_sync_dir() -> io::Result<()> {
        let root = test_root("init-paths")?;
        let root_str = root.to_string_lossy().to_string();

        let folder = Folder::new(&root_str).expect("Folder::new should succeed");

        assert_eq!(folder.path, root.canonicalize()?);
        assert_eq!(folder.data_dir_path, folder.path.join(DATA_SUBDIR));
        assert_eq!(folder.db_path, folder.data_dir_path.join(DB_FILENAME));
        assert!(folder.data_dir_path.is_dir());

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn new_succeeds_when_sync_dir_already_exists() -> io::Result<()> {
        let root = test_root("sync-exists")?;
        fs::create_dir(root.join(DATA_SUBDIR))?;
        let root_str = root.to_string_lossy().to_string();

        let folder = Folder::new(&root_str);
        assert!(folder.is_ok());

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn new_fails_on_nonexistent_path() {
        let missing = std::env::temp_dir().join("foldermesh-nonexistent-folder-path");
        let missing_str = missing.to_string_lossy().to_string();

        let result = Folder::new(&missing_str);
        match result {
            Err(FolderMeshError::Folder(FolderError::NotFound(path))) => {
                assert_eq!(path, missing);
            }
            Err(FolderMeshError::Folder(FolderError::CanonicalizeFailed { path, .. })) => {
                assert_eq!(path, missing);
            }
            other => panic!("expected NotFound or CanonicalizeFailed, got {:?}", other),
        }
    }

    #[test]
    fn new_fails_when_path_is_a_file() -> io::Result<()> {
        let root = test_root("file-path")?;
        let file_path = root.join("file.txt");
        fs::write(&file_path, "hello")?;
        let file_str = file_path.to_string_lossy().to_string();

        let result = Folder::new(&file_str);
        assert!(matches!(
            result,
            Err(FolderMeshError::Folder(FolderError::NotDirectory(_)))
        ));

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn extract_path_resolves_relative_and_dot_paths() -> io::Result<()> {
        let root = test_root("extract-path")?;
        let sub = root.join("sub");
        fs::create_dir(&sub)?;

        let dot_path = sub.join("..").join("sub");
        let path_str = dot_path.to_string_lossy().to_string();

        let canonical = Folder::extract_path(&path_str).expect("extract_path should succeed");
        assert_eq!(canonical, sub.canonicalize()?);

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn should_ignore_access_events() -> io::Result<()> {
        let root = test_root("ignore-access")?;
        let root_str = root.to_string_lossy().to_string();
        let folder = Folder::new(&root_str).unwrap();

        let event = Event {
            kind: EventKind::Access(notify::event::AccessKind::Read),
            paths: vec![folder.path.join("file.txt")],
            attrs: Default::default(),
        };

        assert!(folder.should_ignore_event(&event));

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn should_ignore_internal_sync_dir_events() -> io::Result<()> {
        let root = test_root("ignore-sync")?;
        let root_str = root.to_string_lossy().to_string();
        let folder = Folder::new(&root_str).unwrap();

        let db_event = Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Any,
            )),
            paths: vec![folder.db_path.clone()],
            attrs: Default::default(),
        };
        assert!(folder.should_ignore_event(&db_event));

        let nested_sync_event = Event {
            kind: EventKind::Create(notify::event::CreateKind::File),
            paths: vec![folder.data_dir_path.join("journal.tmp")],
            attrs: Default::default(),
        };
        assert!(folder.should_ignore_event(&nested_sync_event));

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn should_not_ignore_workspace_mutations() -> io::Result<()> {
        let root = test_root("keep-mutations")?;
        let root_str = root.to_string_lossy().to_string();
        let folder = Folder::new(&root_str).unwrap();

        let create_event = Event {
            kind: EventKind::Create(notify::event::CreateKind::File),
            paths: vec![folder.path.join("new_file.txt")],
            attrs: Default::default(),
        };
        assert!(!folder.should_ignore_event(&create_event));

        let modify_event = Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Any,
            )),
            paths: vec![folder.path.join("existing.txt")],
            attrs: Default::default(),
        };
        assert!(!folder.should_ignore_event(&modify_event));

        let remove_event = Event {
            kind: EventKind::Remove(notify::event::RemoveKind::File),
            paths: vec![folder.path.join("deleted.txt")],
            attrs: Default::default(),
        };
        assert!(!folder.should_ignore_event(&remove_event));

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn should_not_ignore_paths_with_sync_prefix_substring() -> io::Result<()> {
        let root = test_root("sync-prefix-sub")?;
        let root_str = root.to_string_lossy().to_string();
        let folder = Folder::new(&root_str).unwrap();

        let event = Event {
            kind: EventKind::Create(notify::event::CreateKind::File),
            paths: vec![folder.path.join(".sync-notes.txt")],
            attrs: Default::default(),
        };
        assert!(!folder.should_ignore_event(&event));

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn should_ignore_multi_path_events_affecting_sync_dir() -> io::Result<()> {
        let root = test_root("multi-path")?;
        let root_str = root.to_string_lossy().to_string();
        let folder = Folder::new(&root_str).unwrap();

        let event = Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Any),
            paths: vec![
                folder.path.join("normal.txt"),
                folder.data_dir_path.join("sqlite.db"),
            ],
            attrs: Default::default(),
        };
        assert!(folder.should_ignore_event(&event));

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn handle_event_forwards_valid_events() -> io::Result<()> {
        let root = test_root("handle-forward")?;
        let root_str = root.to_string_lossy().to_string();
        let folder = Folder::new(&root_str).unwrap();
        let (tx, rx) = mpsc::channel();

        let target_path = folder.path.join("user_file.txt");
        let event = Event {
            kind: EventKind::Create(notify::event::CreateKind::File),
            paths: vec![target_path.clone()],
            attrs: Default::default(),
        };

        folder.handle_event(event, &tx);

        let received = rx.try_recv().expect("event should be forwarded");
        assert_eq!(received.paths, vec![target_path]);

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn handle_event_drops_ignored_events() -> io::Result<()> {
        let root = test_root("handle-drop")?;
        let root_str = root.to_string_lossy().to_string();
        let folder = Folder::new(&root_str).unwrap();
        let (tx, rx) = mpsc::channel();

        let access_event = Event {
            kind: EventKind::Access(notify::event::AccessKind::Read),
            paths: vec![folder.path.join("file.txt")],
            attrs: Default::default(),
        };
        folder.handle_event(access_event, &tx);
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);

        let sync_dir_event = Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Any),
            paths: vec![folder.db_path.clone()],
            attrs: Default::default(),
        };
        folder.handle_event(sync_dir_event, &tx);
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn handle_event_handles_disconnected_channel_gracefully() -> io::Result<()> {
        let root = test_root("handle-disconnect")?;
        let root_str = root.to_string_lossy().to_string();
        let folder = Folder::new(&root_str).unwrap();
        let (tx, rx) = mpsc::channel();
        drop(rx);

        let event = Event {
            kind: EventKind::Create(notify::event::CreateKind::File),
            paths: vec![folder.path.join("file.txt")],
            attrs: Default::default(),
        };

        let result = folder.handle_event(event, &tx);
        assert!(!result);

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn watch_sync_directory_captures_file_creation() -> io::Result<()> {
        let root = test_root("watch-capture")?;
        let root_str = root.to_string_lossy().to_string();
        let folder = std::sync::Arc::new(Folder::new(&root_str).unwrap());
        let (tx, rx) = mpsc::channel();
        let folder_clone = std::sync::Arc::clone(&folder);

        let handle = std::thread::spawn(move || {
            let _ = folder_clone.watch_sync_directory(tx);
        });

        std::thread::sleep(Duration::from_millis(50));

        let test_file = folder.path.join("created.txt");
        fs::write(&test_file, "content")?;

        let received = rx.recv_timeout(Duration::from_millis(2000));
        assert!(received.is_ok(), "expected to receive filesystem event");
        let event = received.unwrap();
        assert!(event.paths.iter().any(|p| p.ends_with("created.txt")));

        drop(rx);
        let _ = fs::write(folder.path.join("shutdown_trigger.txt"), "stop");
        let _ = handle.join();

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn watch_sync_directory_filters_sync_dir_writes() -> io::Result<()> {
        let root = test_root("watch-filter")?;
        let root_str = root.to_string_lossy().to_string();
        let folder = std::sync::Arc::new(Folder::new(&root_str).unwrap());
        let (tx, rx) = mpsc::channel();
        let folder_clone = std::sync::Arc::clone(&folder);

        let handle = std::thread::spawn(move || {
            let _ = folder_clone.watch_sync_directory(tx);
        });

        std::thread::sleep(Duration::from_millis(50));

        let internal_file = folder.data_dir_path.join("ignored.txt");
        fs::write(&internal_file, "content")?;

        let received = rx.recv_timeout(Duration::from_millis(200));
        assert!(
            received.is_err(),
            "should not receive events for .sync directory changes"
        );

        drop(rx);
        let _ = fs::write(folder.path.join("shutdown_trigger.txt"), "stop");
        let _ = handle.join();

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn get_folder_state_returns_walk_node() -> io::Result<()> {
        let root = test_root("get-state")?;
        let root_str = root.to_string_lossy().to_string();
        let folder = Folder::new(&root_str).unwrap();

        let visible_file = folder.path.join("visible.txt");
        fs::write(&visible_file, "data")?;
        let internal_file = folder.data_dir_path.join("internal.txt");
        fs::write(&internal_file, "ignored")?;

        let node = folder.get_folder_state().expect("get_folder_state should succeed");
        let walk::Node::Directory(dir) = node else {
            panic!("expected directory node");
        };

        assert_eq!(dir.children.len(), 1);
        assert!(matches!(
            &dir.children[0],
            walk::Node::File(f) if f.path == visible_file && f.name == "visible.txt"
        ));

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn folder_error_display_formatting() {
        let path = PathBuf::from("test_dir");

        let not_found = FolderError::NotFound(path.clone());
        assert_eq!(
            format!("{not_found}"),
            format!("folder does not exist: {}", path.display())
        );

        let not_dir = FolderError::NotDirectory(path.clone());
        assert_eq!(
            format!("{not_dir}"),
            format!("path is not a directory: {}", path.display())
        );

        let io_err = io::Error::new(io::ErrorKind::PermissionDenied, "access denied");
        let canon_failed = FolderError::CanonicalizeFailed {
            path: path.clone(),
            source: io_err,
        };
        assert_eq!(
            format!("{canon_failed}"),
            format!("failed to canonicalize directory {}: access denied", path.display())
        );
    }

    #[test]
    fn folder_error_into_foldermesh_error() {
        let err = FolderError::NotFound(PathBuf::from("dummy"));
        let mesh_err: FolderMeshError = err.into();
        assert!(matches!(
            mesh_err,
            FolderMeshError::Folder(FolderError::NotFound(_))
        ));
    }
}
