use crate::fs_utils::*;
use crate::Result;
use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;
use std::path::PathBuf;

// store stuff in paths with slashes
// splits on "/" and creates sub-dirs
pub struct AnyBucket<V> {
    dir: PathBuf,
    max_file_name: Option<usize>,
    _v: PhantomData<V>,
}

/// Store things in bucket
impl<V: Serialize + DeserializeOwned> AnyBucket<V> {
    pub(crate) fn new(dir: PathBuf, max_file_name: Option<usize>) -> Self {
        Self {
            dir,
            max_file_name: max_file_name,
            _v: PhantomData,
        }
    }
    /// dir of this bucket
    pub fn dir(&self) -> String {
        self.dir.to_string_lossy().to_string()
    }
    /// Check if a key exists
    pub fn exists(&self, key: &str) -> bool {
        let pz = self.maxify_and_make(Some(key));
        pz.exists()
    }
    /// Create a key
    pub fn put(&self, key: &str, value: &V) -> Result<()> {
        let pz = self.maxify_and_make(Some(key));
        fs_put(&pz, value)
    }
    /// Create a key and write raw
    pub fn put_raw(&self, key: &str, value: &[u8]) -> Result<()> {
        let pz = self.maxify_and_make(Some(key));
        fs_put_raw(&pz, value)
    }
    /// Get a key
    pub fn get(&self, key: &str) -> Result<V> {
        let path = self.maxify_and_make(Some(key));
        fs_get(&path)
    }
    /// Get a key (raw value)
    pub fn get_raw(&self, key: &str) -> Result<Vec<u8>> {
        let path = self.maxify_and_make(Some(key));
        Ok(fs_get_raw(&path)?)
    }
    /// Get a key
    pub fn read_8(&self, key: &str) -> Result<[u8; 8]> {
        let path = self.maxify_and_make(Some(key));
        Ok(fs_read_8(&path)?)
    }
    /// Delete a file
    pub fn remove(&self, key: &str) -> Result<()> {
        let path = self.maxify_and_make(Some(key));
        fs_remove(&path)
    }
    /// List keys in this bucket (or sub-buckets in this bucket)
    pub fn list(&self, dir: &str) -> Result<Vec<String>> {
        let path = self.maxify_and_make(Some(dir));
        fs_list(&path)
    }
    /// List keys in this bucket (or sub-buckets in this bucket)
    pub fn list_all(&self) -> Result<Vec<String>> {
        let mut entries = Vec::new();
        self.list_recursive(None, &mut entries)?;
        Ok(entries)
    }
    /// Clear all keys in this bucket
    pub fn clear(&self) -> Result<()> {
        let path = self.dir.clone();
        fs_clear(&path)
    }
    fn list_recursive(&self, dir: Option<&str>, entries: &mut Vec<String>) -> Result<()> {
        let path = self.maxify_and_make(dir);
        let ls = fs_list(&path)?;
        for l in ls {
            let fullpath = format!("{}/{}", path.display(), l);
            let meta = metadata(&fullpath)?;
            let this_path = match dir {
                Some(d) => format!("{}/{}", d, l),
                None => l,
            };
            if meta.is_dir() {
                self.list_recursive(Some(&this_path), entries)?;
            } else if meta.is_file() {
                entries.push(this_path)
            }
        }
        Ok(())
    }
    fn maxify_and_make(&self, name_opt: Option<&str>) -> PathBuf {
        let mut fulldir = self.dir.clone();
        if let Some(name) = name_opt {
            fulldir.push(name);
        }
        let first_char = if self.dir.is_relative() { "" } else { "/" };
        let path = fulldir.to_string_lossy().to_string();
        let parts = path.split("/").collect::<Vec<&str>>();
        let mut fin = PathBuf::from(first_char);
        for (i, part) in parts.iter().enumerate() {
            if part.len() == 0 {
                continue;
            }
            let pz = maxify(part, self.max_file_name);
            fin.push(&pz);
            // create sub-dirs as we go
            if i != parts.len() - 1 {
                if let Err(e) = fs_create_dir_if_not_exist(&fin) {
                    println!("ERROR {:?}", e)
                }
            }
        }
        fin
    }
}
