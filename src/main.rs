use crate::{
    error::FolderMeshError,
    fs::folder::{Folder, FolderError},
    tui::app::App,
};
use clap::Parser;

pub mod db;
pub mod error;
pub mod fs;
pub mod sync;
pub mod tui;

pub fn main() -> Result<(), FolderMeshError> {
    let args = Args::parse();

    let folder = Folder::new(&args.path)?;
    folder.watch_sync_directory()?;

    println!("Folder is: {}", args.path);

    // TODO: proper error handling
    // ratatui::run(|terminal| App::new(args.folder).run(terminal)).unwrap();

    Ok(())
}

#[derive(Parser, Debug)]
#[command(version, about, long_about=None)]
struct Args {
    path: String,
}
