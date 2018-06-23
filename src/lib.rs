//! Content-addressed asset storage

#![warn(missing_docs)]

extern crate blake2;
extern crate data_encoding;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate rand;
#[macro_use]
extern crate failure;
extern crate carchive;
extern crate memmap;

pub mod loose_files;
pub use loose_files::LooseFiles;

pub mod archive;
pub use archive::ArchiveSet;

use std::{fmt, io};
use std::str::FromStr;
use std::sync::Arc;

use blake2::digest::{Input, VariableOutput};
use serde::{Serialize, Serializer, Deserialize, Deserializer};
use serde::de::Error;
use serde::ser::SerializeSeq;
use memmap::Mmap;
use data_encoding::{DecodeError, BASE32_NOPAD};

const BLAKE2B_LEN: usize = 25;

/// A hash uniquely identifying some data.
///
/// Hashes have forwards-compatible serialization, and can be encoded in both binary and human-readable forms. New types
/// of hash may be added in the future, but none will ever be removed.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Hash {
    /// A 200-bit blake2b hash.
    ///
    /// This size is evenly divisible into both bytes and base32 code units, allowing for efficient encoding for both
    /// machine and human consumption.
    Blake2b([u8; BLAKE2B_LEN]),
}

impl Serialize for Hash {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        if s.is_human_readable() {
            s.serialize_str(&self.to_string())
        } else {
            let mut seq = s.serialize_seq(Some(self.bytes().len() + 1))?;
            seq.serialize_element(&self.kind())?;
            for x in self.bytes() { seq.serialize_element(x)?; }
            seq.end()
        }
    }
}

/// Errors in the human-readable hash encoding.
#[derive(Debug, Fail)]
pub enum HashParseError {
    /// Missing delimiting ":".
    #[fail(display = "missing delimiting \":\"")]
    MissingDelimiter,
    /// Unknown hash kind.
    ///
    /// May occur when parsing a hash encoded by a future version of this library.
    #[fail(display = "unknown hash kind: {}", _0)]
    UnknownKind(String),
    /// Malformed base32 hash value.
    #[fail(display = "malformed hash value: {}", _0)]
    MalformedValue(data_encoding::DecodeError),
}

impl FromStr for Hash {
    type Err = HashParseError;
    fn from_str(s: &str) -> ::std::result::Result<Self, Self::Err> {
        let delim = s.find(':').ok_or(HashParseError::MissingDelimiter)?;
        let kind = s[0..delim].parse().map_err(|UnknownKind| HashParseError::UnknownKind(s[0..delim].into()))?;
        Hash::parse(kind, &s[delim+1..]).map_err(HashParseError::MalformedValue)
    }
}

impl<'a> Deserialize<'a> for Hash {
    fn deserialize<D>(d: D) -> Result<Self, D::Error>
        where D: Deserializer<'a>
    {
        if d.is_human_readable() {
            let x = <&'a str>::deserialize(d)?;
            Hash::from_str(x).map_err(D::Error::custom)
        } else {
            struct Visitor;
            impl<'de> serde::de::Visitor<'de> for Visitor {
                type Value = Hash;

                fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                    write!(f, "a content hash")
                }

                fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
                    where A: serde::de::SeqAccess<'de>,
                {
                    let kind = seq.next_element::<HashKind>()?.ok_or_else(|| A::Error::missing_field("kind"))?;
                    use self::HashKind::*;
                    match kind {
                        Blake2b => {
                            let mut data = [0; BLAKE2B_LEN];
                            for i in 0..BLAKE2B_LEN { data[i] = seq.next_element::<u8>()?.ok_or_else(|| A::Error::invalid_length(i, &"25 bytes"))?; }
                            Ok(Hash::Blake2b(data))
                        }
                    }
                }
            }

            d.deserialize_seq(Visitor)
        }
    }
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}:{}", self.kind(), data_encoding::BASE32_NOPAD.encode(self.bytes()))
    }
}

/// Unknown hash kind.
#[derive(Debug, Fail)]
#[fail(display = "invalid hash length for given hash kind")]
pub struct InvalidLength;

impl Hash {
    /// Construct a hash that was computed using the `kind` algorithm to produce `bytes`.
    ///
    /// Returns `Err(InvalidLength)` if `bytes` does not match the output length of `kind`.
    pub fn from_bytes(kind: HashKind, bytes: &[u8]) -> Result<Self, InvalidLength> {
        match kind {
            HashKind::Blake2b => {
                if bytes.len() != BLAKE2B_LEN {
                    return Err(InvalidLength);
                }
                let mut result = [0; BLAKE2B_LEN];
                result.copy_from_slice(bytes);
                Ok(Hash::Blake2b(result))
            }
        }
    }

    /// Construct a hash that was computed using the `kind` algorithm to produce `bytes`, encoded human-readably.
    ///
    /// Returns `Err(_)` if `bytes` is not a valid chasset human-readable hash value for `kind`.
    fn parse(kind: HashKind, bytes: &str) -> Result<Self, DecodeError> {
        match kind {
            HashKind::Blake2b => {
                if BASE32_NOPAD.decode_len(bytes.len())? != 25 { return Err(DecodeError { position: 0, kind: data_encoding::DecodeKind::Length }); }
                let mut data = [0; 25];
                BASE32_NOPAD.decode_mut(bytes.as_bytes(), &mut data).map_err(|e| e.error)?;
                Ok(Hash::Blake2b(data))
            }
        }
    }

    /// Get the `HashKind` of this value.
    pub fn kind(&self) -> HashKind {
        use self::Hash::*;
        match *self {
            Blake2b(_) => HashKind::Blake2b,
        }
    }

    /// Get the actual hash.
    pub fn bytes(&self) -> &[u8] {
        use self::Hash::*;
        match *self {
            Blake2b(ref xs) => &xs[..],
        }
    }
}

/// The algorithm used by a hash.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[repr(u16)]
pub enum HashKind {
    /// 200-bit blake2b hash
    Blake2b,
}

impl fmt::Display for HashKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.pad(self.name())
    }
}

/// Unknown hash kind.
#[derive(Debug, Fail)]
#[fail(display = "unknown kind")]
pub struct UnknownKind;

impl FromStr for HashKind {
    type Err = UnknownKind;
    fn from_str(s: &str) -> ::std::result::Result<Self, Self::Err> {
        use self::HashKind::*;
        Ok(match s {
            "blake2b" => Blake2b,
            _ => { return Err(UnknownKind); }
        })
    }
}

impl HashKind {
    /// Concise name for the algorithm used.
    pub fn name(&self) -> &'static str {
        use self::HashKind::*;
        match *self {
            Blake2b => "blake2b",
        }
    }

    /// Length of hash values used for this algorithm.
    pub fn len(&self) -> usize {
        use self::HashKind::*;
        match *self {
            Blake2b => BLAKE2B_LEN,
        }
    }

    /// Integer ID of this kind.
    pub fn id(&self) -> u16 { *self as u16 }

    /// Reconstruct from a value previously obtained with `id`.
    pub fn from_id(x: u16) -> Option<Self> {
        use self::HashKind::*;
        Some(match x {
            0 => Blake2b,
            _ => return None
        })
    }
}

/// Helper to compute a hash of the currently recommended type.
#[derive(Debug, Clone)]
pub struct Hasher {
    inner: blake2::Blake2b,
}

impl io::Write for Hasher {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.process(&buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> { Ok(()) }
}

impl Hasher {
    /// Create an empty hasher.
    pub fn new() -> Self { Self { inner: blake2::Blake2b::new(BLAKE2B_LEN).unwrap() } }
    /// Incrementally hash `bytes`.
    pub fn process(&mut self, bytes: &[u8]) { self.inner.process(bytes); }
    /// Get the hash of all `process`ed bytes.
    pub fn result(self) -> Hash {
        let mut buf = [0; BLAKE2B_LEN];
        self.inner.variable_result(&mut buf).unwrap();
        Hash::Blake2b(buf)
    }
}

/// A refcounted, memory-mapped asset from disk.
#[derive(Debug, Clone)]
pub struct Asset {
    map: Arc<Mmap>,
    start: usize,
    len: usize,
}

impl AsRef<[u8]> for Asset {
    fn as_ref(&self) -> &[u8] { &self.map.as_ref()[self.start..self.start+self.len] }
}

impl ::std::ops::Deref for Asset {
    type Target = [u8];
    fn deref(&self) -> &[u8] { &self.map[self.start..self.start+self.len] }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn hash_string_roundtrip() {
        let hash = Hash::Blake2b([0xAB; 25]);
        let x = hash.to_string();
        let hash2 = x.parse::<Hash>().unwrap();
        assert_eq!(hash, hash2);
    }

    #[test]
    fn parse_err() {
        assert!(Hash::from_str("blake2b:00000").is_err());
        assert!(Hash::from_str("notarealhash:42").is_err());
    }
}
