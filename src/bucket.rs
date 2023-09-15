use crate::fs_utils::*;
use crate::Result;
use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;
use std::path::PathBuf;

pub struct Bucket<V> {
    dir: PathBuf,
    max_file_name: Option<usize>,
    _v: PhantomData<V>,
}

/// Store things in bucket
impl<V: Serialize + DeserializeOwned> Bucket<V> {
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
        let mut path = self.dir.clone();
        path.push(self.maxify(key));
        path.exists()
    }
    /// Create a key
    pub fn put(&self, key: &str, value: &V) -> Result<()> {
        let mut path = self.dir.clone();
        path.push(self.maxify(key));
        fs_put(&path, value)
    }
    /// Create a key and write raw
    pub fn put_raw(&self, key: &str, value: &[u8]) -> Result<()> {
        let mut path = self.dir.clone();
        path.push(self.maxify(key));
        fs_put_raw(&path, value)
    }
    /// Get a key
    pub fn get(&self, key: &str) -> Result<V> {
        let mut path = self.dir.clone();
        path.push(self.maxify(key));
        fs_get(&path)
    }
    /// Get a key (raw value)
    pub fn get_raw(&self, key: &str) -> Result<Vec<u8>> {
        let mut path = self.dir.clone();
        path.push(self.maxify(key));
        fs_get_raw(&path)
    }
    /// Delete a file
    pub fn remove(&self, key: &str) -> Result<()> {
        let mut path = self.dir.clone();
        path.push(self.maxify(key));
        fs_remove(&path)
    }
    /// List keys in this bucket (or sub-buckets in this bucket)
    pub fn list(&self) -> Result<Vec<String>> {
        let path = self.dir.clone();
        fs_list(&path)
    }
    /// Clear all keys in this bucket
    pub fn clear(&self) -> Result<()> {
        let path = self.dir.clone();
        fs_clear(&path)
    }
    fn maxify(&self, name: &str) -> String {
        maxify(name, self.max_file_name)
    }
}
