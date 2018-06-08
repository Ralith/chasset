extern crate chasset;
#[macro_use]
extern crate structopt;

use std::path::PathBuf;
use std::io;

use structopt::StructOpt;

use chasset::*;

#[derive(StructOpt)]
#[structopt(name = "chasset")]
struct Opt {
    #[structopt(parse(from_os_str))]
    /// Location of the chasset file repository
    prefix: PathBuf,
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
    let mut repo = LooseFiles::open(opt.prefix.clone())?;
    match opt.cmd {
        Command::Cat { hash } => { match hash {
            None => {
                let mut stage = repo.make_writer()?;
                let stdin = io::stdin();
                io::copy(&mut stdin.lock(), &mut stage)?;
                let hash = stage.store()?;
                println!("{}", hash);
            }
            Some(x) => {
                let mut file = repo.get(&x)?;
                let stdout = io::stdout();
                io::copy(&mut file, &mut stdout.lock())?;
            }
        }}
        Command::Ls => {
            for x in repo.list() {
                println!("{}", x);
            }
        }
    }
    Ok(())
}
