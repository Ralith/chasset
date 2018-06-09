//! Tools for a repository that stores one file per asset.

use std::path::{Path, PathBuf};
use std::io;
use std::fs::{self, File};
use std::sync::Arc;

use data_encoding::BASE32_NOPAD;
use rand;
use memmap::Mmap;

use {Hash, HashKind, Hasher, Asset};

/// A repository that stores each asset as a separate file.
///
/// This type of repository supports easy insertion of new assets, even when being accessed by multiple processes
/// simultaneously. On the other hand, traversing the filesystem for each access may lead to suboptimal performance when
/// reading large numbers of assets.
///
/// The repository is organized into one directory per hash type, each containing one file per asset identified by that
/// hash, split into subdirectories to reduce the number of files in a single directory, as large numbers reduce
/// performance on some systems. A "temp" directory is placed adjacent to the hash directories to buffer incomplete
/// streaming writes.
///
/// Unexpected interruptions (such as power loss) may cause incomplete writes to be left in the "temp" directory. Any
/// file in the "temp" directory which is not currently open by any process arose from such an event, and may be safely
/// deleted.
pub struct LooseFiles {
    prefix: PathBuf,
}

impl LooseFiles {
    /// Open a repository located at `prefix`, creating it if necessary.
    pub fn open(prefix: PathBuf) -> io::Result<Self> {
        fs::create_dir_all(&prefix)?;
        Ok(Self { prefix })
    }

    /// Access the asset identified by `hash`.
    ///
    /// The returned `File` is in read-only mode.
    pub fn get(&self, hash: &Hash) -> io::Result<Asset> {
        let path = path_for(&self.prefix, hash);
        let map = Arc::new(unsafe { Mmap::map(&File::open(path)?) }?);
        Ok(Asset {
            start: 0,
            len: map.len(),
            map,
        })
    }

    /// Determine whether the asset identified by `hash` exists in the repository.
    pub fn contains(&self, hash: &Hash) -> bool {
        let path = path_for(&self.prefix, hash);
        path.exists()
    }

    /// Create a `Writer` for streaming data into the repository in constant memory.
    pub fn make_writer(&self) -> io::Result<Writer> {
        let mut path = self.prefix.join("temp");
        match fs::create_dir(&path) {
            Ok(()) => {}
            Err(ref e) if e.kind() == io::ErrorKind::AlreadyExists => {}
            Err(e) => { return Err(e); }
        }
        loop {
            path.push(format!("{:08X}", rand::random::<u64>()));
            match fs::OpenOptions::new().read(false).write(true).create_new(true).open(&path) {
                Ok(file) => { return Writer::new(file, path); }
                Err(ref e) if e.kind() == io::ErrorKind::AlreadyExists => { path.pop(); continue; }
                Err(e) => { return Err(e); }
            }
        }
    }

    /// Write `data` directly into the repository.
    pub fn put(&self, mut data: &[u8]) -> io::Result<Hash> {
        let mut writer = self.make_writer()?;
        io::copy(&mut data, &mut writer)?;
        writer.store()
    }

    /// Enumerate assets stored in the repository.
    ///
    /// This should only be used for diagnostic purposes. It almost never makes sense to access an asset you don't
    /// already know the hash of.
    pub fn list(&self) -> impl Iterator<Item=Hash> {
        fs::read_dir(&self.prefix).ok().into_iter()
            .flat_map(|x| x.flat_map(|result| result.into_iter()))
            .filter_map(|x| {
                let name = x.file_name();
                if &name != "temp" {
                    Some(list_hash(x.path()))
                } else {
                    None
                }
            })
            .flat_map(|x| x)
    }
}

fn list_hash(hash_dir: PathBuf) -> impl Iterator<Item=Hash> {
    hash_dir.file_name().unwrap().to_str().map(|x| x.to_string()).into_iter()
        .flat_map(|x| x.parse::<HashKind>().into_iter())
        .flat_map(move |kind| {
            fs::read_dir(&hash_dir).into_iter()
                .flat_map(|x| x.into_iter())
                .flat_map(|x| x.into_iter())
                .flat_map(move |x| list_leaf(kind, x.path()))
        })
}

fn list_leaf(kind: HashKind, leaf_dir: PathBuf) -> impl Iterator<Item=Hash> {
    leaf_dir.file_name().unwrap().to_str().map(|x| x.to_string()).into_iter()
        .flat_map(move |start| {
            fs::read_dir(&leaf_dir).into_iter()
                .flat_map(|x| x.into_iter())
                .flat_map(|x| x.into_iter())
                .flat_map(move |file| {
                    let start = start.clone();
                    file.file_name().to_str().map(|name| {
                        let full = start.to_owned() + name;
                        Hash::parse(kind, &full).into_iter()
                    }).into_iter().flat_map(|x| x)
                })
        })
}

fn path_for(prefix: &Path, hash: &Hash) -> PathBuf {
    let s = BASE32_NOPAD.encode(hash.bytes());
    let dir = &s[0..2];
    let file = &s[2..];
    prefix.join(hash.kind().name()).join(dir).join(file)
}

/// A staging area for streaming data into the repository in constant memory.
///
/// Data written into a `Writer` is used to update a hash computation and buffered in a temporary file on disk.
///
/// `store` must be called to commit data to the repository. Otherwise, it will be deleted when the `Writer` is dropped.
#[derive(Debug)]
pub struct Writer {
    hasher: Option<Hasher>,
    path: PathBuf,
    file: File,
}

impl Drop for Writer {
    fn drop(&mut self) {
        if self.hasher.is_some() { let _ = fs::remove_file(&self.path); }
    }
}

impl Writer {
    fn new(file: File, path: PathBuf) -> io::Result<Self> {
        Ok(Writer { hasher: Some(Hasher::new()), path, file })
    }

    /// Commits the written data to the repository.
    pub fn store(mut self) -> io::Result<Hash> {
        let hash = self.hasher.take().unwrap().result();
        let prefix = self.path.parent().unwrap().parent().unwrap();
        let dest = path_for(prefix, &hash);
        fs::create_dir_all(dest.parent().unwrap())?;
        self.file.sync_data()?;
        fs::rename(&self.path, &dest)?;
        Ok(hash)
    }
}

impl io::Write for Writer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = self.file.write(buf)?;
        self.hasher.as_mut().unwrap().process(&buf[0..written]);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}
