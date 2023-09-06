mod any_bucket;
mod bucket;
mod double_bucket;
mod fs_utils;

pub use any_bucket::AnyBucket;
pub use bucket::Bucket;
pub use double_bucket::DoubleBucket;

use fs_utils::maxify;
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
    pub fn bucket<V: Serialize + DeserializeOwned>(
        &self,
        p: &str,
        max_file_name: Option<usize>,
    ) -> Result<Bucket<V>> {
        let p2 = maxify(p, max_file_name);
        Ok(Bucket::new(self.make_dir(&p2)?.into(), max_file_name))
    }

    // Create new bucket with another level directory
    pub fn double_bucket<V: Serialize + DeserializeOwned>(
        &self,
        p: &str,
        max_file_name: Option<usize>,
    ) -> Result<DoubleBucket<V>> {
        let p2 = maxify(p, max_file_name);
        Ok(DoubleBucket::new(self.make_dir(&p2)?.into(), max_file_name))
    }

    // Create new bucket with another level directory
    pub fn any_bucket<V: Serialize + DeserializeOwned>(
        &self,
        max_file_name: Option<usize>,
    ) -> Result<AnyBucket<V>> {
        let dir = self.dir.clone();
        if !Path::new(&dir).exists() {
            fs::create_dir(dir.clone())?;
        }
        Ok(AnyBucket::new(dir, max_file_name))
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
    use crate::{AnyBucket, Fsdb};
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
    struct Thing {
        n: u8,
    }

    #[test]
    fn test_db() {
        let db = Fsdb::new("testdb/db1").expect("fail Fsdb::new");
        let b = db.bucket("hi1234567890", Some(8)).expect("fail bucket");
        let t1 = Thing { n: 1 };
        println!("PUT NOW");
        b.put("keythatisverylong", &t1).expect("failed to save");
        println!("PUTTED!");
        let t2: Thing = b.get("keythatisverylong").expect("fail to load");
        println!("GOTTEN");
        println!("t {:?}", t2.clone());
        assert_eq!(t1, t2);
        let list = b.list().expect("fail list");
        assert_eq!(list, vec!["keythati".to_string()]);
    }

    #[test]
    fn test_double() {
        let db = Fsdb::new("testdb/db2").expect("fail Fsdb::new");
        let b = db.double_bucket("hi", None).expect("fail bucket");
        let t1 = Thing { n: 1 };
        b.put("sub1", "key", &t1).expect("failed to save");
        let t2: Thing = b.get("sub1", "key").expect("fail to load");
        println!("t {:?}", t2.clone());
        assert_eq!(t1, t2);
        let list = b.list("sub1").expect("fail list");
        assert_eq!(list, vec!["key".to_string()]);
    }

    // cargo test test_any -- --nocapture
    #[test]
    fn test_any() {
        let db = Fsdb::new("testdb/db3").expect("fail Fsdb::new");
        let b: AnyBucket<Thing> = db.any_bucket(None).expect("fail bucket");

        b.put("one", &Thing { n: 1 }).unwrap();
        let exists = b.exists("one");
        assert_eq!(exists, true);

        b.put("dir1/two", &Thing { n: 2 }).unwrap();
        let exists2 = b.exists("dir1/two");
        assert_eq!(exists2, true);

        b.remove("one").unwrap();
        let exists = b.exists("one");
        assert_eq!(exists, false);

        b.remove("dir1/two").unwrap();
        let exists2 = b.exists("dir1/two");
        assert_eq!(exists2, false);

        b.put("dir1/three", &Thing { n: 3 }).unwrap();
        let exists2 = b.exists("dir1/three");
        assert_eq!(exists2, true);

        let three = b.get("dir1/three").unwrap();
        assert_eq!(three.n, 3);

        let list1 = b.list("").unwrap();
        assert_eq!(list1, vec!["dir1"]);

        let list2 = b.list("dir1").unwrap();
        assert_eq!(list2, vec!["three"]);

        b.clear().unwrap();

        let list3 = b.list("").unwrap();
        assert_eq!(list3, Vec::<String>::new());
    }

    // cargo test test_list_recurse -- --nocapture
    #[test]
    fn test_list_recurse() {
        let db = Fsdb::new("testdb/db4").expect("fail Fsdb::new");
        let b: AnyBucket<Thing> = db.any_bucket(None).expect("fail bucket");

        b.put("one/two/three/four/five", &Thing { n: 1 }).unwrap();
        let exists = b.exists("one/two/three/four/five");
        assert_eq!(exists, true);

        b.put("one/two/three/four/six", &Thing { n: 1 }).unwrap();

        b.put("one/two/hello", &Thing { n: 1 }).unwrap();

        let l = b.list_all().unwrap();
        assert_eq!(
            l,
            vec![
                "one/two/three/four/six".to_string(),
                "one/two/three/four/five".to_string(),
                "one/two/hello".to_string(),
            ]
        );
    }
}
