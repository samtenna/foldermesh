use crate::tui::app::App;

pub mod tui;

pub fn main() {
    ratatui::run(|terminal| App::default().run(terminal));
}
