extern crate chasset;
#[macro_use]
extern crate structopt;

use std::path::PathBuf;
use std::io::{self, Write};

use structopt::StructOpt;

use chasset::*;

#[derive(StructOpt)]
#[structopt(name = "chasset")]
struct Opt {
    #[structopt(short = "a")]
    /// Path contains archives instead of loose files
    archives: bool,
    #[structopt(parse(from_os_str))]
    /// Location of the chasset repository
    path: PathBuf,
    #[structopt(subcommand)]
    cmd: Command,
}

#[derive(StructOpt)]
enum Command {
    #[structopt(name = "cat")]
    /// Read or write a single asset
    Cat {
        /// Hash of asset to write to stdout. If absent, new data is inserted from stdin.
        hash: Option<chasset::Hash>,
    },
    #[structopt(name = "ls")]
    /// List stored assets
    Ls,
}

fn main() -> io::Result<()> {
    let opt = Opt::from_args();
    if opt.archives {
        let repo = ArchiveSet::open(&opt.path)?;
        match opt.cmd {
            Command::Cat { hash: Some(x) } => {
                let asset = repo.get(&x).ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no such asset"))?;
                io::stdout().write_all(&asset)?;
            }
            Command::Cat { hash: None } => { eprintln!("archive sets are read-only") },
            Command::Ls => {
                for x in repo.list() {
                    println!("{}", x);
                }
            }
        }
    } else {
        let repo = LooseFiles::open(opt.path.clone())?;
        match opt.cmd {
            Command::Cat { hash } => { match hash {
                None => {
                    let mut stage = repo.make_writer()?;
                    let stdin = io::stdin();
                    io::copy(&mut stdin.lock(), &mut stage)?;
                    let (hash, _) = stage.store()?;
                    println!("{}", hash);
                }
                Some(x) => {
                    let mut asset = repo.get(&x)?;
                    io::stdout().write_all(&asset)?;
                }
            }}
            Command::Ls => {
                for x in repo.list() {
                    println!("{}", x);
                }
            }
        }
    }
    Ok(())
}
