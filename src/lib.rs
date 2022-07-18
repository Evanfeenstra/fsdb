mod bucket;
mod double_bucket;
mod fs_utils;

use bucket::Bucket;
use double_bucket::DoubleBucket;

use std::fmt::Debug;
use std::fs;
use std::path::{Path, PathBuf};

extern crate serde;

use serde::{de::DeserializeOwned, Serialize};

pub struct Fsdb {
    dir: PathBuf,
}

pub trait FsBucket {}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("encode error: {0}")]
    Encode(#[from] rmp_serde::encode::Error),
    #[error("dncode error: {0}")]
    Decode(#[from] rmp_serde::decode::Error),
}

type Result<T> = std::result::Result<T, Error>;

impl Fsdb {
    /// Create a new Fsdb
    pub fn new(dir: &str) -> Result<Self> {
        if !Path::new(dir).exists() {
            fs::create_dir_all(dir)?;
        }
        Ok(Self { dir: dir.into() })
    }

    // Create new bucket
    pub fn bucket<V: Serialize + DeserializeOwned>(&self, p: &str) -> Result<Bucket<V>> {
        Ok(Bucket::new(self.make_dir(p)?.into(), None))
    }

    // Create new bucket with another level directory
    pub fn double_bucket<V: Serialize + DeserializeOwned>(
        &self,
        p: &str,
    ) -> Result<DoubleBucket<V>> {
        Ok(DoubleBucket::new(self.make_dir(p)?.into(), None))
    }

    fn make_dir(&self, p: &str) -> Result<PathBuf> {
        let mut dir = self.dir.clone();
        dir.push(p);
        if !Path::new(&dir).exists() {
            fs::create_dir(dir.clone())?;
        }
        Ok(dir)
    }
}

#[cfg(test)]
mod tests {
    use crate::Fsdb;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
    struct Thing {
        n: u8,
    }

    #[test]
    fn test_db() {
        let db = Fsdb::new("testdb").expect("fail Fsdb::new");
        let mut b = db.bucket("hi").expect("fail bucket");
        b.set_max_file_name(8);
        let t1 = Thing { n: 1 };
        b.put("keythatisverylong", t1.clone())
            .expect("failed to save");
        let t2: Thing = b.get("keythatisverylong").expect("fail to load");
        println!("t {:?}", t2.clone());
        assert_eq!(t1, t2);
        let list = b.list().expect("fail list");
        assert_eq!(list, vec!["keythati".to_string()]);
    }

    #[test]
    fn test_double() {
        let db = Fsdb::new("testdb2").expect("fail Fsdb::new");
        let b = db.double_bucket("hi").expect("fail bucket");
        let t1 = Thing { n: 1 };
        b.put("sub1", "key", t1.clone()).expect("failed to save");
        let t2: Thing = b.get("sub1", "key").expect("fail to load");
        println!("t {:?}", t2.clone());
        assert_eq!(t1, t2);
        let list = b.list("sub1").expect("fail list");
        assert_eq!(list, vec!["key".to_string()]);
    }
}
