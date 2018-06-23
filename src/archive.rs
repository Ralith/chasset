//! Tools for a repository formed by a collection of archive files, each containing many assets.
//!
//! Uses `carchive` formatted files, with a 2-byte little-endian extension header identifying the hash kind.

use std::path::Path;
use std::io;
use std::fs::{self, File};
use std::collections::HashMap;
use std::sync::Arc;

use failure::Fail;
use memmap::Mmap;
use carchive;

use {Hash, HashKind, Asset};

/// A repository formed by a collection of archive files, each containing many assets.
pub struct ArchiveSet {
    archives: HashMap<HashKind, Vec<carchive::Reader<ArcMap>>>,
}

impl ArchiveSet {
    /// Open a repository located at `dir`, creating it if necessary.
    pub fn open(dir: &Path) -> io::Result<Self> {
        fs::create_dir_all(dir)?;
        let mut archives = HashMap::new();
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let file = File::open(entry.path())?;
            let map = ArcMap(Arc::new(unsafe { Mmap::map(&file) }?));
            let archive = carchive::Reader::new(map)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.compat()))?;
            let kind = {
                let x = archive.extensions(2).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid archive"))?;
                HashKind::from_id(x[0] as u16 | (x[1] as u16) << 8).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "archive uses unknown hash kind"))?
            };
            if kind.len() != archive.key_len() as usize {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "archive key length doesn't match hash type"));
            }
            archives.entry(kind).or_insert_with(Vec::new).push(archive);
        }
        Ok(Self { archives })
    }

    /// Access the asset identified by `hash`.
    pub fn get(&self, hash: &Hash) -> Option<Asset> {
        for archive in self.archives.get(&hash.kind())? {
            if let Some(x) = archive.get(hash.bytes()) {
                let base = x.as_ptr() as usize - archive.get_ref().0.as_ptr() as usize;
                return Some(Asset {
                    map: archive.get_ref().0.clone(),
                    start: base,
                    len: x.len(),
                })
            }
        }
        None
    }

    /// Enumerate assets stored in the repository.
    ///
    /// This should only be used for diagnostic purposes. It almost never makes sense to access an asset you don't
    /// already know the hash of.
    pub fn list<'a>(&'a self) -> impl Iterator<Item=Hash> + 'a {
        self.archives.iter()
            .flat_map(|(&kind, xs)| xs.iter().flat_map(move |archive| {
                archive.iter().map(move |(key, _)| Hash::from_bytes(kind, key).expect("archives with invalid key lengths aren't opened"))
            }))
    }
}

struct ArcMap(Arc<Mmap>);

impl AsRef<[u8]> for ArcMap {
    fn as_ref(&self) -> &[u8] { self.0.as_ref() }
}
