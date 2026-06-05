use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, mpsc::Receiver},
    time::{Duration, Instant},
};

use crate::{
    db::{Db, File},
    error::FolderMeshError,
    fs::{
        folder::Folder,
        walk::{self, hash_at_path},
    },
};

pub struct SyncEngine {
    updates: Receiver<notify::Event>,
    folder: Arc<Folder>,
    db: Db,
    pending_paths: HashMap<PathBuf, Instant>,
    debounce: Duration,
}

impl SyncEngine {
    pub fn new(
        folder: Arc<Folder>,
        debounce_duration_ms: u64,
        updates: Receiver<notify::Event>,
    ) -> Result<Self, rusqlite::Error> {
        Ok(SyncEngine {
            updates,
            db: Db::new(&folder.db_path)?,
            folder,
            pending_paths: HashMap::new(),
            debounce: Duration::from_millis(debounce_duration_ms),
        })
    }

    pub fn run(&mut self) -> Result<(), FolderMeshError> {
        self.check_db_consistency()?;

        loop {
            if let Ok(event) = self.updates.try_recv() {
                for p in event.paths {
                    self.pending_paths.insert(p, Instant::now());
                }
            }

            // check pending paths
            for (path, arrival_time) in self.pending_paths.clone() {
                if arrival_time.elapsed() >= self.debounce {
                    // debounce time has passed since last event on the path
                    println!(
                        "processing event for path: {}",
                        path.to_string_lossy().to_string(),
                    );
                    self.pending_paths.remove(&path);
                    self.process_change(&path)?;
                }
            }

            // TODO: sleeping quick fix, feels icky change to blocking at some point
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Check the path's current status in the DB and disk and sync them accordingly.
    fn process_change(&self, path: &PathBuf) -> Result<(), FolderMeshError> {
        let db_path = self.relative_path(path)?;

        if self.is_internal_path(path) {
            self.db.delete_file(&db_path)?;
            return Ok(());
        }

        if db_path.as_os_str().is_empty() {
            self.db.delete_file(&db_path)?;
            return Ok(());
        }

        if let Some(db_file) = self.db.get_file_from_path(&db_path)? {
            if path.exists() {
                let mut file = self.file_from_path(path)?;
                file.path = db_file.path;
                self.db.update_file(file)?;
            } else {
                self.db.delete_file(&db_path)?;
            }
        } else if path.exists() {
            self.db.create_file(self.file_from_path(path)?)?;
        }

        Ok(())
    }

    fn is_internal_path(&self, path: &PathBuf) -> bool {
        path.starts_with(&self.folder.data_dir_path)
    }

    fn relative_path(&self, path: &PathBuf) -> Result<PathBuf, FolderMeshError> {
        path.strip_prefix(&self.folder.path)
            .map(|path| path.to_path_buf())
            .map_err(|_| {
                FolderMeshError::Other(format!(
                    "path is outside sync folder: {}",
                    path.to_string_lossy()
                ))
            })
    }

    fn file_from_path(&self, path: &PathBuf) -> Result<File, FolderMeshError> {
        let name = path
            .file_name()
            .ok_or_else(|| FolderMeshError::Other("path has no file name".into()))?
            .to_string_lossy()
            .into_owned();
        let metadata = path.metadata()?;
        let hash = if metadata.is_dir() {
            String::new()
        } else {
            hash_at_path(path)?.to_string()
        };

        Ok(File::new(
            name,
            self.relative_path(path)?.to_string_lossy().into_owned(),
            hash,
            metadata.len(),
        ))
    }

    /// Compares the file structure on disk and stored info in the sync sqlite.db file.
    /// If differences are found it rectifies them by adding/removing/editing records to match the disk state.
    /// This should only be run at startup to sync up the DB and filesystem, the notify watcher updates state otherwise to avoid comparing the entire tree each time.
    fn check_db_consistency(&self) -> Result<bool, FolderMeshError> {
        let disk_tree = walk::Node::new(&self.folder.path, Some(&self.folder.data_dir_path))?;
        let disk_items = disk_tree.flatten();
        let disk_by_path: HashMap<PathBuf, Item> = disk_items
            .into_iter()
            .filter_map(|item| {
                let relative_path = self.relative_path(item.relative_path()).ok()?;

                if relative_path.as_os_str().is_empty() {
                    return None;
                }

                let item = Item::new(
                    relative_path.clone(),
                    item.name().to_string(),
                    item.size(),
                    item.hash().to_string(),
                );

                Some((relative_path, item))
            })
            .collect();

        let db_items: Vec<Item> = self
            .db
            .get_files()?
            .into_iter()
            .map(|f| Item {
                relative_path: PathBuf::from(f.path),
                name: f.name,
                size: f.size,
                hash: f.hash,
            })
            .collect();
        let db_by_path: HashMap<PathBuf, Item> = db_items
            .into_iter()
            .map(|item| (item.relative_path().clone(), item))
            .collect();

        // compare db and disk contents
        // missing item in db: add to db
        // missing item on disk: remove from db (add to changelog?)
        // TODO: changelog
        for (path, item) in &disk_by_path {
            if !db_by_path.contains_key(path) {
                // item missing in db
                self.db.create_file(File::new(
                    item.name().to_string(),
                    item.relative_path().to_string_lossy().into_owned(),
                    item.hash().to_string(),
                    item.size(),
                ))?;
            }
        }

        for (path, item) in &db_by_path {
            if let Some(disk_item) = disk_by_path.get(path) {
                if disk_item.hash() != item.hash() {
                    // file has changed, update db info
                    self.db.update_file(File::new(
                        disk_item.name().to_string(),
                        disk_item.relative_path().to_string_lossy().into_owned(),
                        disk_item.hash().to_string(),
                        disk_item.size(),
                    ))?;
                }
            } else {
                // item missing on disk
                self.db.delete_file(item.relative_path())?;
            }
        }

        Ok(true)
    }
}

pub struct Item {
    relative_path: PathBuf,
    name: String,
    size: u64,
    hash: String,
}

impl Item {
    pub fn new(relative_path: PathBuf, name: String, size: u64, hash: String) -> Self {
        Item {
            relative_path,
            name,
            size,
            hash,
        }
    }

    pub fn relative_path(&self) -> &PathBuf {
        &self.relative_path
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn hash(&self) -> &str {
        &self.hash
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs, io,
        sync::mpsc,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn test_root(name: &str) -> io::Result<PathBuf> {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("foldermesh-engine-{name}-{nanos}"));
        fs::create_dir(&path)?;
        fs::create_dir(path.join(".sync"))?;
        Ok(path)
    }

    fn test_engine(name: &str) -> io::Result<(PathBuf, SyncEngine)> {
        let root = test_root(name)?;
        let folder = Arc::new(Folder::new(&root.to_string_lossy().to_string()).unwrap());
        let (_tx, rx) = mpsc::channel();
        let engine = SyncEngine::new(folder, 0, rx).unwrap();
        Ok((root, engine))
    }

    #[test]
    fn process_change_creates_missing_file_in_db() -> io::Result<()> {
        let (root, engine) = test_engine("process-creates")?;
        let file_path = engine.folder.path.join("created.txt");
        let db_path = PathBuf::from("created.txt");
        fs::write(&file_path, "created contents")?;

        engine.process_change(&file_path).unwrap();

        let file = engine.db.get_file_from_path(&db_path).unwrap().unwrap();

        assert_eq!(file.name, "created.txt");
        assert_eq!(file.path, "created.txt");
        assert_eq!(file.size, 16);
        assert_eq!(file.hash, blake3::hash(b"created contents").to_string());

        drop(engine);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn process_change_updates_existing_file_in_db() -> io::Result<()> {
        let (root, engine) = test_engine("process-updates")?;
        let file_path = engine.folder.path.join("updated.txt");
        let db_path = PathBuf::from("updated.txt");
        fs::write(&file_path, "updated contents")?;
        engine
            .db
            .create_file(File::new(
                "old-name.txt".to_string(),
                db_path.to_string_lossy().into_owned(),
                "old-hash".to_string(),
                1,
            ))
            .unwrap();

        engine.process_change(&file_path).unwrap();

        let file = engine.db.get_file_from_path(&db_path).unwrap().unwrap();

        assert_eq!(file.name, "updated.txt");
        assert_eq!(file.path, "updated.txt");
        assert_eq!(file.size, 16);
        assert_eq!(file.hash, blake3::hash(b"updated contents").to_string());

        drop(engine);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn process_change_creates_missing_directory_in_db() -> io::Result<()> {
        let (root, engine) = test_engine("process-creates-dir")?;
        let dir_path = engine.folder.path.join("nested");
        let db_path = PathBuf::from("nested");
        fs::create_dir(&dir_path)?;

        engine.process_change(&dir_path).unwrap();

        let file = engine.db.get_file_from_path(&db_path).unwrap().unwrap();

        assert_eq!(file.name, "nested");
        assert_eq!(file.path, "nested");
        assert_eq!(file.size, dir_path.metadata()?.len());
        assert!(file.hash.is_empty());

        drop(engine);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn process_change_ignores_sync_directory_paths() -> io::Result<()> {
        let (root, engine) = test_engine("process-ignores-sync-dir")?;
        let sync_file_path = engine.folder.data_dir_path.join("internal.tmp");
        fs::write(&sync_file_path, "internal")?;

        engine.process_change(&engine.folder.data_dir_path).unwrap();
        engine.process_change(&sync_file_path).unwrap();
        engine.process_change(&engine.folder.db_path).unwrap();

        let files = engine.db.get_files().unwrap();

        assert!(
            !files
                .iter()
                .any(|file| file.path == PathBuf::from(".sync").to_string_lossy())
        );
        assert!(
            !files
                .iter()
                .any(|file| file.path == PathBuf::from(".sync/internal.tmp").to_string_lossy())
        );
        assert!(
            !files
                .iter()
                .any(|file| file.path == PathBuf::from(".sync/sqlite.db").to_string_lossy())
        );

        drop(engine);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn check_db_consistency_adds_missing_disk_items_to_db() -> io::Result<()> {
        let (root, engine) = test_engine("adds-missing")?;
        let file_path = engine.folder.path.join("hello.txt");
        fs::write(&file_path, "hello")?;

        engine.check_db_consistency().unwrap();

        let files = engine.db.get_files().unwrap();

        assert!(files.iter().any(|file| {
            file.path == "hello.txt"
                && file.name == "hello.txt"
                && file.size == 5
                && file.hash == blake3::hash(b"hello").to_string()
        }));
        assert!(!files.iter().any(|file| file.path.contains(".sync")));

        drop(engine);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn check_db_consistency_deletes_items_missing_from_disk() -> io::Result<()> {
        let (root, engine) = test_engine("deletes-missing")?;
        let stale_path = PathBuf::from("stale.txt");
        engine
            .db
            .create_file(File::new(
                "stale.txt".to_string(),
                stale_path.to_string_lossy().into_owned(),
                "stale-hash".to_string(),
                10,
            ))
            .unwrap();

        engine.check_db_consistency().unwrap();

        let files = engine.db.get_files().unwrap();

        assert!(
            !files
                .iter()
                .any(|file| file.path == stale_path.to_string_lossy())
        );

        drop(engine);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn check_db_consistency_updates_changed_disk_items() -> io::Result<()> {
        let (root, engine) = test_engine("updates-changed")?;
        let file_path = engine.folder.path.join("changed.txt");
        let db_path = PathBuf::from("changed.txt");
        fs::write(&file_path, "new contents")?;
        engine
            .db
            .create_file(File::new(
                "changed.txt".to_string(),
                db_path.to_string_lossy().into_owned(),
                "old-hash".to_string(),
                1,
            ))
            .unwrap();

        engine.check_db_consistency().unwrap();

        let files = engine.db.get_files().unwrap();
        let updated = files
            .iter()
            .find(|file| file.path == "changed.txt")
            .expect("expected changed file to remain in db");

        assert_eq!(updated.name, "changed.txt");
        assert_eq!(updated.size, 12);
        assert_eq!(updated.hash, blake3::hash(b"new contents").to_string());

        drop(engine);
        fs::remove_dir_all(root)?;
        Ok(())
    }
}
