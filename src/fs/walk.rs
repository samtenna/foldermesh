use std::{
    fs,
    io::{self, Read},
    path::PathBuf,
};

use blake3::Hasher;

use crate::sync::engine::Item;

#[derive(Debug)]
pub struct Directory {
    path: PathBuf,
    name: String,
    size: u64,
    hash: String,
    children: Vec<Node>,
}

#[derive(Debug)]
pub struct File {
    path: PathBuf,
    name: String,
    size: u64,
    hash: String,
}

#[derive(Debug)]
pub enum Node {
    Directory(Directory),
    File(File),
}

pub fn hash_at_path(path: &PathBuf) -> io::Result<blake3::Hash> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Hasher::new();
    let mut buf = [0u8; 8 * 1024]; // 8kb chunks ?change?

    loop {
        let n = file.read(&mut buf)?;

        if n == 0 {
            break;
        }

        hasher.update(&buf[..n]);
    }

    Ok(hasher.finalize())
}

impl Node {
    // recursively walk file structure
    // TODO: probably want to switch to an iterative algorithm eventually so it doesn't blow the stack with arbitrarily nested folders
    pub fn new(path: &PathBuf, ignore_dir: Option<&PathBuf>) -> io::Result<Self> {
        let metadata = fs::metadata(path)?;

        Ok(if metadata.is_dir() {
            let mut dir = Directory {
                path: path.clone(),
                name: path
                    .file_name()
                    .map(|name| name.to_string_lossy().to_string())
                    .unwrap_or_default(),
                size: metadata.len(),
                hash: String::new(),
                children: vec![],
            };

            for entry in fs::read_dir(path)? {
                let child_path = entry?.path();

                if let Some(sync_dir) = ignore_dir {
                    if child_path.starts_with(sync_dir) {
                        continue;
                    }
                }

                dir.children.push(Node::new(&child_path, ignore_dir)?);
            }

            Node::Directory(dir)
        } else {
            let hash = hash_at_path(path)?;
            let file = File {
                path: path.clone(),
                name: path
                    .file_name()
                    .map(|name| name.to_string_lossy().to_string())
                    .unwrap_or_default(),
                hash: hash.to_string(),
                size: metadata.len(),
            };

            Node::File(file)
        })
    }

    fn path(&self) -> PathBuf {
        match self {
            Node::Directory(dir) => dir.path.clone(),
            Node::File(file) => file.path.clone(),
        }
    }

    fn name(&self) -> String {
        match self {
            Node::Directory(dir) => dir.name.clone(),
            Node::File(file) => file.name.clone(),
        }
    }

    fn size(&self) -> u64 {
        match self {
            Node::Directory(dir) => dir.size,
            Node::File(file) => file.size,
        }
    }

    fn hash(&self) -> String {
        match self {
            Node::Directory(dir) => dir.hash.clone(),
            Node::File(file) => file.hash.clone(),
        }
    }

    pub fn flatten(&self) -> Vec<Item> {
        let mut items = vec![Item::new(
            self.path(),
            self.name(),
            self.size(),
            self.hash(),
        )];

        // recurse on children if directory
        if let Node::Directory(dir) = self {
            for c in &dir.children {
                items.append(&mut c.flatten());
            }
        }

        items
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs, io,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    // create a temp dir for testing
    fn test_root(name: &str) -> io::Result<PathBuf> {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("foldermesh-walk-{name}-{nanos}"));
        fs::create_dir(&path)?;

        Ok(path)
    }

    #[test]
    fn creates_file_node_with_metadata() -> io::Result<()> {
        let root = test_root("file-node")?;
        let file_path = root.join("hello.txt");
        fs::write(&file_path, "hello")?;

        let node = Node::new(&file_path, None)?;

        match node {
            Node::File(file) => {
                assert_eq!(file.path, file_path);
                assert_eq!(file.name, "hello.txt");
                assert_eq!(file.size, 5);
            }
            Node::Directory(_) => panic!("expected file node"),
        }

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn creates_directory_node_with_direct_children() -> io::Result<()> {
        let root = test_root("directory-node")?;
        let file_path = root.join("a.txt");
        let nested_dir = root.join("nested");
        fs::write(&file_path, "a")?;
        fs::create_dir(&nested_dir)?;

        let node = Node::new(&root, None)?;

        match node {
            Node::Directory(dir) => {
                assert_eq!(dir.path, root);
                assert_eq!(dir.children.len(), 2);
                assert!(dir.children.iter().any(|child| matches!(
                    child,
                    Node::File(file) if file.name == "a.txt"
                )));
                assert!(dir.children.iter().any(|child| matches!(
                    child,
                    Node::Directory(child_dir) if child_dir.path == nested_dir
                )));
            }
            Node::File(_) => panic!("expected directory node"),
        }

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn recursively_creates_nested_children() -> io::Result<()> {
        let root = test_root("nested-children")?;
        let nested_dir = root.join("nested");
        let nested_file = nested_dir.join("b.txt");
        fs::create_dir(&nested_dir)?;
        fs::write(&nested_file, "nested")?;

        let node = Node::new(&root, None)?;

        let Node::Directory(root_dir) = node else {
            panic!("expected root directory node");
        };
        let nested_node = root_dir
            .children
            .iter()
            .find(|child| matches!(child, Node::Directory(dir) if dir.path == nested_dir))
            .expect("expected nested directory child");

        let Node::Directory(nested) = nested_node else {
            panic!("expected nested directory node");
        };

        assert_eq!(nested.children.len(), 1);
        assert!(matches!(
            &nested.children[0],
            Node::File(file) if file.path == nested_file && file.name == "b.txt" && file.size == 6
        ));

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn hash_at_path_hashes_file_contents() -> io::Result<()> {
        let root = test_root("hash-file")?;
        let file_path = root.join("hello.txt");
        fs::write(&file_path, "hello")?;

        let hash = hash_at_path(&file_path)?;

        assert_eq!(hash.to_string(), blake3::hash(b"hello").to_string());

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn flatten_returns_directory_and_descendant_items() -> io::Result<()> {
        let root = test_root("flatten")?;
        let root_file = root.join("a.txt");
        let nested_dir = root.join("nested");
        let nested_file = nested_dir.join("b.txt");
        fs::write(&root_file, "a")?;
        fs::create_dir(&nested_dir)?;
        fs::write(&nested_file, "nested")?;

        let node = Node::new(&root, None)?;
        let items = node.flatten();

        assert_eq!(items.len(), 4);
        assert!(items.iter().any(|item| {
            item.relative_path() == &root
                && item.name() == root.file_name().unwrap().to_string_lossy()
                && item.hash().is_empty()
        }));
        assert!(items.iter().any(|item| {
            item.relative_path() == &root_file
                && item.name() == "a.txt"
                && item.size() == 1
                && item.hash() == blake3::hash(b"a").to_string()
        }));
        assert!(items.iter().any(|item| {
            item.relative_path() == &nested_dir && item.name() == "nested" && item.hash().is_empty()
        }));
        assert!(items.iter().any(|item| {
            item.relative_path() == &nested_file
                && item.name() == "b.txt"
                && item.size() == 6
                && item.hash() == blake3::hash(b"nested").to_string()
        }));

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn ignores_sync_dir() -> io::Result<()> {
        let root = test_root("ignore-sync-dir")?;
        let visible_file = root.join("visible.txt");
        let sync_dir = root.join(".sync");
        let ignored_file = sync_dir.join("sqlite.db");

        fs::write(&visible_file, "visible")?;
        fs::create_dir(&sync_dir)?;
        fs::write(&ignored_file, "ignored")?;

        let node = Node::new(&root, Some(&sync_dir))?;

        let Node::Directory(dir) = node else {
            panic!("expected directory node");
        };

        assert_eq!(dir.children.len(), 1);
        assert!(matches!(
            &dir.children[0],
            Node::File(file) if file.path == visible_file && file.name == "visible.txt"
        ));
        assert!(!dir.children.iter().any(
            |child| matches!(child, Node::Directory(child_dir) if child_dir.path == sync_dir)
        ));

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn errors_when_path_does_not_exist() {
        let path = std::env::temp_dir().join("foldermesh-walk-missing-path");

        let err = Node::new(&path, None).expect_err("missing path should error");

        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }
}
