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
        let pz = self.maxify_and_make(key);
        pz.exists()
    }
    /// Create a key
    pub fn put(&self, key: &str, value: V) -> Result<()> {
        let pz = self.maxify_and_make(key);
        fs_put(pz, value)
    }
    /// Get a key
    pub fn get(&self, key: &str) -> Result<V> {
        let path = self.maxify_and_make(key);
        fs_get(path)
    }
    /// Delete a file
    pub fn remove(&self, key: &str) -> Result<()> {
        let path = self.maxify_and_make(key);
        fs_remove(path)
    }
    /// List keys in this bucket (or sub-buckets in this bucket)
    pub fn list(&self, dir: &str) -> Result<Vec<String>> {
        let path = self.maxify_and_make(dir);
        fs_list(path)
    }
    /// Clear all keys in this bucket
    pub fn clear(&self) -> Result<()> {
        let path = self.dir.clone();
        fs_clear(path)
    }
    fn maxify_and_make(&self, name: &str) -> PathBuf {
        let mut fulldir = self.dir.clone();
        fulldir.push(name);
        let path = fulldir.to_string_lossy().to_string();
        let parts = path.split("/").collect::<Vec<&str>>();
        let mut fin = String::from("");
        for (i, part) in parts.iter().enumerate() {
            let pz = maxify(part, self.max_file_name);
            if i != 0 {
                fin.push_str("/");
            }
            fin.push_str(&pz);
            // create sub-dirs as we go
            if i != parts.len() - 1 {
                if let Err(e) = fs_create_dir_if_not_exist(PathBuf::from(fin.clone())) {
                    println!("ERROR {:?}", e)
                }
            }
        }
        PathBuf::from(fin)
    }
}
