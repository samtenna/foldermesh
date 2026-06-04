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
        let name = path
            .file_name()
            .ok_or_else(|| FolderMeshError::Other("path has no file name".into()))?
            .to_string_lossy()
            .into_owned();

        if path.is_dir() {
            return Ok(());
        }

        if let Some(db_file) = self.db.get_file_from_path(path)? {
            // Already exists in the db, want to compare to the file system
            if path.exists() {
                self.db.update_file(File::new(
                    name,
                    db_file.path,
                    hash_at_path(path)?.to_string(),
                    path.metadata()?.len(),
                ))?;
            } else {
                self.db.delete_file(path)?;
            }
        } else {
            // Add the new db item accordingly
            self.db.create_file(File::new(
                name,
                path.to_string_lossy().into_owned(),
                hash_at_path(path)?.to_string(),
                path.metadata()?.len(),
            ))?;
        }

        Ok(())
    }

    /// Compares the file structure on disk and stored info in the sync sqlite.db file.
    /// If differences are found it rectifies them by adding/removing/editing records to match the disk state.
    /// This should only be run at startup to sync up the DB and filesystem, the notify watcher updates state otherwise to avoid comparing the entire tree each time.
    fn check_db_consistency(&self) -> Result<bool, FolderMeshError> {
        let disk_tree = walk::Node::new(&self.folder.path, Some(&self.folder.data_dir_path))?;
        let disk_items = disk_tree.flatten();
        let disk_by_path: HashMap<PathBuf, Item> = disk_items
            .into_iter()
            .map(|item| (item.relative_path().clone(), item))
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
        // missing file in db: add to db
        // missing file on disk: remove from db (add to changelog?)
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
        fs::write(&file_path, "created contents")?;

        engine.process_change(&file_path).unwrap();

        let file = engine.db.get_file_from_path(&file_path).unwrap().unwrap();

        assert_eq!(file.name, "created.txt");
        assert_eq!(file.path, file_path.to_string_lossy());
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
        fs::write(&file_path, "updated contents")?;
        engine
            .db
            .create_file(File::new(
                "old-name.txt".to_string(),
                file_path.to_string_lossy().into_owned(),
                "old-hash".to_string(),
                1,
            ))
            .unwrap();

        engine.process_change(&file_path).unwrap();

        let file = engine.db.get_file_from_path(&file_path).unwrap().unwrap();

        assert_eq!(file.name, "updated.txt");
        assert_eq!(file.path, file_path.to_string_lossy());
        assert_eq!(file.size, 16);
        assert_eq!(file.hash, blake3::hash(b"updated contents").to_string());

        drop(engine);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn process_change_ignores_directories() -> io::Result<()> {
        let (root, engine) = test_engine("process-ignores-dirs")?;
        let dir_path = engine.folder.path.join("nested");
        fs::create_dir(&dir_path)?;

        engine.process_change(&dir_path).unwrap();

        assert!(engine.db.get_file_from_path(&dir_path).unwrap().is_none());

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
            file.path == file_path.to_string_lossy()
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
        let stale_path = engine.folder.path.join("stale.txt");
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
        fs::write(&file_path, "new contents")?;
        engine
            .db
            .create_file(File::new(
                "changed.txt".to_string(),
                file_path.to_string_lossy().into_owned(),
                "old-hash".to_string(),
                1,
            ))
            .unwrap();

        engine.check_db_consistency().unwrap();

        let files = engine.db.get_files().unwrap();
        let updated = files
            .iter()
            .find(|file| file.path == file_path.to_string_lossy())
            .expect("expected changed file to remain in db");

        assert_eq!(updated.name, "changed.txt");
        assert_eq!(updated.size, 12);
        assert_eq!(updated.hash, blake3::hash(b"new contents").to_string());

        drop(engine);
        fs::remove_dir_all(root)?;
        Ok(())
    }
}
