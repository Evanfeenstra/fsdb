use crate::fs_utils::*;
use crate::Result;
use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

pub struct DoubleBucket<V> {
    dir: PathBuf,
    max_file_name: Option<usize>,
    _v: PhantomData<V>,
}

/// DoubleBucket stores things one level deeper
impl<V: Serialize + DeserializeOwned> DoubleBucket<V> {
    pub(crate) fn new(dir: PathBuf, max_file_name: Option<usize>) -> Self {
        Self {
            dir,
            max_file_name: max_file_name,
            _v: PhantomData,
        }
    }
    /// Check if a key exists within sub-bucket
    pub fn exists(&self, sub: &str, key: &str) -> bool {
        let mut path = self.dir.clone();
        path.push(self.maxify(sub));
        path.push(self.maxify(key));
        path.exists()
    }
    /// Create a key in a sub-bucket
    pub fn put(&self, sub: &str, key: &str, value: V) -> Result<()> {
        let mut path = self.dir.clone();
        path.push(self.maxify(sub));
        if !Path::new(&path).exists() {
            std::fs::create_dir(path.clone())?;
        }
        path.push(self.maxify(key));
        fs_put(path, value)
    }
    /// Get a key in a sub-bucket
    pub fn get(&self, sub: &str, key: &str) -> Result<V> {
        let mut path = self.dir.clone();
        path.push(self.maxify(sub));
        path.push(self.maxify(key));
        fs_get(path)
    }
    /// Delete a file in a sub-bucket
    pub fn remove(&self, sub: &str, key: &str) -> Result<()> {
        let mut path = self.dir.clone();
        path.push(self.maxify(sub));
        path.push(self.maxify(key));
        fs_remove(path)
    }
    /// List keys in this bucket's sub-bucket
    pub fn list(&self, sub: &str) -> Result<Vec<String>> {
        let mut path = self.dir.clone();
        path.push(self.maxify(sub));
        fs_list(path)
    }
    /// Clear all keys in this sub-bucket
    pub fn clear(&self, sub: &str) -> Result<()> {
        let mut path = self.dir.clone();
        path.push(self.maxify(sub));
        fs_clear(path)
    }
    /// Clear all keys in the bucket
    pub fn clear_all(&self) -> Result<()> {
        let path = self.dir.clone();
        fs_clear(path)
    }
    fn maxify(&self, name: &str) -> String {
        maxify(name, self.max_file_name)
    }
}
