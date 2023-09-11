use crate::Result;
use rmp_serde::{decode, encode};
use serde::{de::DeserializeOwned, Serialize};
use std::fs::{self, Metadata};
use std::path::PathBuf;

pub fn fs_put<V: Serialize + DeserializeOwned>(path: &PathBuf, value: &V) -> Result<()> {
    let mut f = fs::File::create(path.clone())?;
    encode::write(&mut f, value)?;
    Ok(())
}
pub fn fs_put_raw(path: &PathBuf, value: &[u8]) -> Result<()> {
    fs::write(path, value)?;
    Ok(())
}
pub fn fs_get<V: Serialize + DeserializeOwned>(path: &PathBuf) -> Result<V> {
    let f = fs::File::open(path)?;
    Ok(decode::from_read(f)?)
}
pub fn fs_get_raw(path: &PathBuf) -> Result<Vec<u8>> {
    let val = fs::read(path)?;
    Ok(val)
}
pub fn fs_read_8(path: &PathBuf) -> Result<[u8; 8]> {
    use std::io::BufReader;
    use std::io::Read;
    let f = fs::File::open(path)?;
    let mut buf_reader = BufReader::with_capacity(8, f);
    let mut buffer = [0u8; 8];
    buf_reader.read(&mut buffer)?;
    Ok(buffer)
}
pub fn fs_remove(path: &PathBuf) -> Result<()> {
    Ok(std::fs::remove_file(path)?)
}
pub fn fs_list(path: &PathBuf) -> Result<Vec<String>> {
    let paths = fs::read_dir(path)?;
    let mut r = Vec::new();
    paths.for_each(|name| {
        if let Ok(na) = name {
            if let Ok(n) = na.file_name().into_string() {
                r.push(n);
            }
        }
    });
    Ok(r)
}
pub fn fs_create_dir_if_not_exist(path: PathBuf) -> Result<()> {
    if !path.exists() {
        fs::create_dir(path)?;
    }
    Ok(())
}
pub fn fs_clear(path: &PathBuf) -> Result<()> {
    Ok(fs::remove_dir_all(path)?)
}
pub fn metadata(path: &str) -> Result<Metadata> {
    Ok(fs::metadata(path)?)
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
