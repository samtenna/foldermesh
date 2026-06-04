use std::path::PathBuf;

use rusqlite::{Connection, OptionalExtension, Result};

pub struct Db {
    conn: Connection,
}

#[derive(Debug, PartialEq, Eq)]
pub struct File {
    pub name: String,
    pub path: String,
    pub hash: String,
    pub size: u64,
}

impl File {
    pub fn new(name: String, path: String, hash: String, size: u64) -> Self {
        File {
            name,
            path,
            hash,
            size,
        }
    }
}

impl Db {
    pub fn new(db_path: &PathBuf) -> Result<Self, rusqlite::Error> {
        let db = Db {
            conn: Connection::open(db_path)?,
        };

        Db::setup_tables(&db)?;

        Ok(db)
    }

    pub fn setup_tables(&self) -> Result<(), rusqlite::Error> {
        self.conn.execute(
            "
                CREATE TABLE IF NOT EXISTS file (
                    id      INTEGER PRIMARY KEY,
                    name    TEXT NOT NULL,
                    path    TEXT NOT NULL UNIQUE,
                    hash    TEXT NOT NULL,
                    size    INTEGER NOT NULL
                )
            ",
            (),
        )?;

        Ok(())
    }

    pub fn create_file(&self, file: File) -> rusqlite::Result<()> {
        self.conn.execute(
            "
                INSERT INTO file (name, path, hash, size) VALUES (?1, ?2, ?3, ?4)
            ",
            (file.name, file.path, file.hash, file.size as i64),
        )?;

        Ok(())
    }

    pub fn delete_file(&self, path: &PathBuf) -> rusqlite::Result<()> {
        self.conn.execute(
            "DELETE FROM file WHERE path = ?1",
            (path.to_string_lossy().as_ref(),),
        )?;

        Ok(())
    }

    /// Replaces the file in the database with the path `new_file.path` with `new_file`.
    pub fn update_file(&self, new_file: File) -> rusqlite::Result<()> {
        self.conn.execute(
            "UPDATE file SET name = ?2, size = ?3, hash = ?4 WHERE path = ?1",
            (
                new_file.path,
                new_file.name,
                new_file.size as i64,
                new_file.hash,
            ),
        )?;

        Ok(())
    }

    pub fn get_files(&self) -> rusqlite::Result<Vec<File>> {
        let mut stmt = self
            .conn
            .prepare("SELECT name, path, hash, size FROM file")?;
        let file_iter = stmt.query_map([], |row| {
            let size: i64 = row.get(3)?;

            Ok(File::new(
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                size as u64,
            ))
        })?;

        file_iter.collect()
    }

    pub fn get_file_from_path(&self, path: &PathBuf) -> rusqlite::Result<Option<File>> {
        self.conn
            .query_one(
                "SELECT name, path, hash, size FROM file WHERE path = ?1",
                (path.to_string_lossy().as_ref(),),
                |row| {
                    let size: i64 = row.get(3)?;

                    Ok(File::new(
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        size as u64,
                    ))
                },
            )
            .optional()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs, io,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn test_db_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        std::env::temp_dir().join(format!("foldermesh-db-{name}-{nanos}.sqlite"))
    }

    fn remove_db(path: &PathBuf) -> io::Result<()> {
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err),
        }
    }

    #[test]
    fn new_creates_file_table() -> Result<(), Box<dyn std::error::Error>> {
        let path = test_db_path("creates-table");
        let db = Db::new(&path)?;

        let table_count: i32 = db.conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'file'",
            [],
            |row| row.get(0),
        )?;

        assert_eq!(table_count, 1);

        drop(db);
        remove_db(&path)?;
        Ok(())
    }

    #[test]
    fn create_file_inserts_file() -> Result<(), Box<dyn std::error::Error>> {
        let path = test_db_path("insert-file");
        let db = Db::new(&path)?;
        let file = File::new(
            "hello.txt".to_string(),
            "nested/hello.txt".to_string(),
            "abc123".to_string(),
            42,
        );

        db.create_file(file)?;

        let count: i32 = db
            .conn
            .query_row("SELECT COUNT(*) FROM file", [], |row| row.get(0))?;
        assert_eq!(count, 1);

        drop(db);
        remove_db(&path)?;
        Ok(())
    }

    #[test]
    fn delete_file_removes_file_by_path() -> Result<(), Box<dyn std::error::Error>> {
        let path = test_db_path("delete-file");
        let db = Db::new(&path)?;
        let deleted = File::new(
            "delete-me.txt".to_string(),
            "delete-me.txt".to_string(),
            "delete-hash".to_string(),
            10,
        );
        let retained = File::new(
            "keep-me.txt".to_string(),
            "keep-me.txt".to_string(),
            "keep-hash".to_string(),
            20,
        );

        db.create_file(File::new(
            deleted.name.clone(),
            deleted.path.clone(),
            deleted.hash.clone(),
            deleted.size,
        ))?;
        db.create_file(File::new(
            retained.name.clone(),
            retained.path.clone(),
            retained.hash.clone(),
            retained.size,
        ))?;

        db.delete_file(&PathBuf::from(deleted.path.clone()))?;

        let files = db.get_files()?;

        assert_eq!(files, vec![retained]);

        drop(db);
        remove_db(&path)?;
        Ok(())
    }

    #[test]
    fn update_file_updates_file_by_path() -> Result<(), Box<dyn std::error::Error>> {
        let path = test_db_path("update-file");
        let db = Db::new(&path)?;
        let original = File::new(
            "old-name.txt".to_string(),
            "same-path.txt".to_string(),
            "old-hash".to_string(),
            10,
        );
        let retained = File::new(
            "other.txt".to_string(),
            "other.txt".to_string(),
            "other-hash".to_string(),
            20,
        );
        let updated = File::new(
            "new-name.txt".to_string(),
            "same-path.txt".to_string(),
            "new-hash".to_string(),
            30,
        );

        db.create_file(original)?;
        db.create_file(File::new(
            retained.name.clone(),
            retained.path.clone(),
            retained.hash.clone(),
            retained.size,
        ))?;

        db.update_file(File::new(
            updated.name.clone(),
            updated.path.clone(),
            updated.hash.clone(),
            updated.size,
        ))?;

        let files = db.get_files()?;

        assert_eq!(files.len(), 2);
        assert!(files.contains(&updated));
        assert!(files.contains(&retained));

        drop(db);
        remove_db(&path)?;
        Ok(())
    }

    #[test]
    fn get_file_from_path_returns_matching_file() -> Result<(), Box<dyn std::error::Error>> {
        let path = test_db_path("get-file-from-path");
        let db = Db::new(&path)?;
        let first = File::new(
            "a.txt".to_string(),
            "a.txt".to_string(),
            "hash-a".to_string(),
            1,
        );
        let second = File::new(
            "b.txt".to_string(),
            "nested/b.txt".to_string(),
            "hash-b".to_string(),
            2,
        );

        db.create_file(first)?;
        db.create_file(File::new(
            second.name.clone(),
            second.path.clone(),
            second.hash.clone(),
            second.size,
        ))?;

        let file = db
            .get_file_from_path(&PathBuf::from("nested/b.txt"))?
            .unwrap();

        assert_eq!(file, second);

        drop(db);
        remove_db(&path)?;
        Ok(())
    }

    #[test]
    fn get_files_returns_inserted_files() -> Result<(), Box<dyn std::error::Error>> {
        let path = test_db_path("get-files");
        let db = Db::new(&path)?;
        let first = File::new(
            "a.txt".to_string(),
            "a.txt".to_string(),
            "hash-a".to_string(),
            1,
        );
        let second = File::new(
            "b.txt".to_string(),
            "nested/b.txt".to_string(),
            "hash-b".to_string(),
            2,
        );

        db.create_file(first)?;
        db.create_file(second)?;

        let files = db.get_files()?;

        assert_eq!(files.len(), 2);
        assert_eq!(
            files[0],
            File::new(
                "a.txt".to_string(),
                "a.txt".to_string(),
                "hash-a".to_string(),
                1,
            )
        );
        assert_eq!(
            files[1],
            File::new(
                "b.txt".to_string(),
                "nested/b.txt".to_string(),
                "hash-b".to_string(),
                2,
            )
        );

        drop(db);
        remove_db(&path)?;
        Ok(())
    }
}
