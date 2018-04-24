use std::path::{Path, PathBuf};
use std::io;
use std::fs::{self, File};

use tokio_threadpool;
use futures::{Future, future};
use futures::sync::oneshot;
use data_encoding::BASE32_NOPAD;
use rand::{StdRng, FromEntropy, Rng};

use {Hash, Hasher};

/// A mutable content repository that stores each asset as a separate file, named after the hash.
pub struct LooseFiles {
    pool: tokio_threadpool::Sender,
    prefix: PathBuf,
    rng: StdRng,
}

impl LooseFiles {
    pub fn new(pool: tokio_threadpool::Sender, prefix: PathBuf) -> io::Result<Self> {
        fs::create_dir_all(&prefix)?;
        Ok(Self { pool, prefix, rng: StdRng::from_entropy() })
    }

    pub fn get(&self, hash: &Hash) -> Box<Future<Item=Box<[u8]>, Error=io::Error>> {
        let (send, recv) = oneshot::channel();
        let path = path_for(&self.prefix, hash);
        self.pool.spawn(future::lazy(move || {
            send.send(get(path)).map_err(|_| ())
        })).expect("threadpool full");
        Box::new(recv.then(|x| x.expect("threadpool terminated unexpectedly")))
    }

    pub fn contains(&self, hash: &Hash) -> Box<Future<Item=bool, Error=io::Error>> {
        let (send, recv) = oneshot::channel();
        let path = path_for(&self.prefix, hash);
        self.pool.spawn(future::lazy(move || {
            send.send(path.exists()).map_err(|_| ())
        })).expect("threadpool full");
        Box::new(recv.then(|x| Ok(x.expect("threadpool terminated unexpectedly"))))
    }

    pub fn make_writer(&mut self) -> io::Result<Writer> {
        let mut path = self.prefix.join("temp");
        fs::create_dir(&path)?;
        loop {
            path.push(format!("{:08X}", self.rng.gen::<u64>()));
            match fs::OpenOptions::new().read(false).write(true).create_new(true).open(&path) {
                Ok(file) => { return Writer::new(file, path); }
                Err(ref e) if e.kind() == io::ErrorKind::AlreadyExists => { path.pop(); continue; }
                Err(e) => { return Err(e); }
            }
        }
    }

    pub fn put(&mut self, data: Box<[u8]>) -> Box<Future<Item=Hash, Error=io::Error>> {
        let (send, recv) = oneshot::channel();
        let writer = self.make_writer();
        self.pool.spawn(future::lazy(move || {
            send.send(writer.and_then(|writer| put(data, writer))).map_err(|_| ())
        })).expect("threadpool full");
        Box::new(recv.then(|x| x.expect("threadpool terminated unexpectedly")))
    }
}

fn path_for(base: &Path, hash: &Hash) -> PathBuf {
    let s = BASE32_NOPAD.encode(hash.bytes());
    let dir = &s[0..2];
    let file = &s[2..];
    base.join(hash.algo().name()).join(dir).join(file)
}



fn get(path: PathBuf) -> io::Result<Box<[u8]>> {
    let mut file = File::open(path)?;
    let meta = file.metadata()?;
    let mut buf = Vec::new();
    buf.reserve_exact(meta.len() as usize);
    io::copy(&mut file, &mut buf)?;
    Ok(buf.into())
}

fn put(data: Box<[u8]>, mut writer: Writer) -> io::Result<Hash> {
    let mut cursor = io::Cursor::new(&data);
    io::copy(&mut cursor, &mut writer)?;
    writer.store()
}

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

    /// Returns the hash of the data
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
