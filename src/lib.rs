extern crate blake2;
extern crate data_encoding;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate bincode;
extern crate futures;
extern crate tokio;
extern crate rand;

pub mod loose_files;

pub use loose_files::LooseFiles;

use std::fmt;
use std::str::FromStr;

use blake2::digest::{Input, VariableOutput};
use serde::{de, Serialize, Serializer, Deserialize, Deserializer};
use serde::de::{VariantAccess, Error};

const BLAKE2B_LEN: usize = 25;

/// A hash uniquely identifying some data
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Hash {
    Blake2b([u8; BLAKE2B_LEN]),
}

impl Serialize for Hash {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        use self::Hash::*;
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.to_string())
        } else {
            match *self {
                Blake2b(ref xs) => serializer.serialize_newtype_variant("Blake2b", self.algo() as u32, "Hash", xs),
            }
        }
    }
}

impl FromStr for Hash {
    type Err = &'static str;
    fn from_str(s: &str) -> ::std::result::Result<Self, Self::Err> {
        let delim = s.find(':').ok_or("missing delimiting colon")?;
        match &s[0..delim] {
            "blake2b" => {
                let mut data = [0; 25];
                // TODO: Don't discard detailed error
                data_encoding::BASE32_NOPAD.decode_mut(&s.as_bytes()[delim+1..], &mut data).map_err(|_| "malformed base32")?;
                Ok(Hash::Blake2b(data))
            }
            _ => Err("unknown hash type"),
        }
    }
}

impl<'a> Deserialize<'a> for Hash {
    fn deserialize<D>(d: D) -> Result<Self, D::Error>
        where D: Deserializer<'a>
    {
        struct StrVisitor;

        impl<'de> de::Visitor<'de> for StrVisitor {
            type Value = Hash;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                write!(formatter, "a string containing a type-tagged base 32 content hash")
            }

            fn visit_str<E>(self, value: &str) -> Result<Hash, E>
                where E: de::Error
            {
                Hash::from_str(value).map_err(E::custom)
            }
        }

        struct Visitor;

        impl<'de> de::Visitor<'de> for Visitor {
            type Value = Hash;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                write!(formatter, "a content hash")
            }

            fn visit_enum<A>(self, data: A) -> Result<Hash, A::Error>
                where A: de::EnumAccess<'de>
            {
                let (algo, variant) = data.variant()?;
                let data = variant.newtype_variant::<&'de [u8]>()?;
                Hash::new(algo, data).ok_or(A::Error::invalid_length(data.len(), &"a length suitable for the specified hash type"))
            }
        }

        if d.is_human_readable() {
            d.deserialize_str(StrVisitor)
        } else {
            d.deserialize_enum("Hash", &["Blake2b"], Visitor)
        }
    }
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}:{}", self.algo(), data_encoding::BASE32_NOPAD.encode(self.bytes()))
    }
}

impl Hash {
    pub fn new(algo: HashAlgo, bytes: &[u8]) -> Option<Hash> {
        match algo {
            HashAlgo::Blake2b => {
                if bytes.len() != BLAKE2B_LEN {
                    return None;
                }
                let mut result = [0; BLAKE2B_LEN];
                result.copy_from_slice(bytes);
                Some(Hash::Blake2b(result))
            }
        }
    }

    pub fn algo(&self) -> HashAlgo {
        use self::Hash::*;
        match *self {
            Blake2b(_) => HashAlgo::Blake2b,
        }
    }

    pub fn bytes(&self) -> &[u8] {
        use self::Hash::*;
        match *self {
            Blake2b(ref xs) => &xs[..],
        }
    }
}

/// The algorithm used by a hash
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum HashAlgo {
    /// 200-bit blake2b hash
    Blake2b,
}

impl fmt::Display for HashAlgo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.pad(self.name())
    }
}

impl HashAlgo {
    pub fn name(&self) -> &'static str {
        use self::HashAlgo::*;
        match *self {
            Blake2b => "blake2b",
        }
    }
}

/// Helper to compute a hash of the preferred type
#[derive(Debug, Clone)]
pub struct Hasher {
    inner: blake2::Blake2b,
}

impl Hasher {
    pub fn new() -> Self { Self { inner: blake2::Blake2b::new(BLAKE2B_LEN).unwrap() } }
    pub fn process(&mut self, bytes: &[u8]) { self.inner.process(bytes); }
    pub fn result(self) -> Hash {
        let mut buf = [0; BLAKE2B_LEN];
        self.inner.variable_result(&mut buf).unwrap();
        Hash::Blake2b(buf)
    }
}
