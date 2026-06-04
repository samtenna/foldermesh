use std::{
    sync::{Arc, mpsc},
    thread,
};

use crate::{error::FolderMeshError, fs::folder::Folder, sync::engine::SyncEngine};
use clap::Parser;

pub mod db;
pub mod error;
pub mod fs;
pub mod sync;
pub mod tui;

pub fn main() -> Result<(), FolderMeshError> {
    let args = Args::parse();

    let (event_tx, event_rx) = mpsc::channel();

    let folder = Arc::new(Folder::new(&args.path)?);
    let mut engine = SyncEngine::new(Arc::clone(&folder), args.debounce_duration, event_rx)?;
    let notify_folder = Arc::clone(&folder);

    let notify_thread = thread::spawn(move || notify_folder.watch_sync_directory(event_tx));
    let engine_thread = thread::spawn(move || engine.run());

    match notify_thread.join() {
        Ok(res) => {}
        Err(e) => {}
    }

    match engine_thread.join() {
        Ok(res) => {}
        Err(e) => {}
    }

    // quit event will be handled by TUI in main thread, should join the other threads and safely close them
    // TODO: proper error handling
    // ratatui::run(|terminal| App::new(args.folder).run(terminal)).unwrap();

    Ok(())
}

#[derive(Parser, Debug)]
#[command(version, about, long_about=None)]
struct Args {
    path: String,
    #[arg(default_value_t = 250)]
    debounce_duration: u64,
}
