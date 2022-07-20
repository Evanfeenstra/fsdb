use crate::Result;
use rmp_serde::{decode, encode};
use serde::{de::DeserializeOwned, Serialize};
use std::fs;
use std::path::PathBuf;

pub fn fs_put<V: Serialize + DeserializeOwned>(path: PathBuf, value: V) -> Result<()> {
    let mut f = fs::File::create(path.clone())?;
    encode::write(&mut f, &value)?;
    Ok(())
}
pub fn fs_get<V: Serialize + DeserializeOwned>(path: PathBuf) -> Result<V> {
    let f = fs::File::open(path)?;
    Ok(decode::from_read(f)?)
}
pub fn fs_remove(path: PathBuf) -> Result<()> {
    Ok(std::fs::remove_file(path)?)
}
pub fn fs_list(path: PathBuf) -> Result<Vec<String>> {
    let paths = fs::read_dir(path)?;
    let mut r = Vec::new();
    paths.for_each(|name| {
        if let Ok(na) = name {
            let pathbuf = na.path();
            let path = pathbuf.to_string_lossy();
            if let Some(file) = path.split("/").last() {
                r.push(file.to_string());
            }
        }
    });
    Ok(r)
}
pub fn fs_clear(path: PathBuf) -> Result<()> {
    Ok(fs::remove_dir_all(path)?)
}
pub fn maxify(name: &str, max_file_name: Option<usize>) -> String {
    if let Some(max) = max_file_name {
        let mut s = name.to_string();
        s.truncate(max);
        s
    } else {
        name.to_owned()
    }
}
