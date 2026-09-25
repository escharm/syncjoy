use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;

use tui_input::backend::crossterm::EventHandler;
use tui_input::Input;

const MAX_MESSAGES: usize = 1000;

#[derive(Default)]
pub(crate) struct Screen {
    connected: AtomicBool,
    input: Mutex<Input>,
    input_width: AtomicUsize,
    limit_hinted: AtomicBool,
    messages: Mutex<Vec<String>>,
}

impl Screen {
    pub(crate) fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    pub(crate) fn set_connected(&self, connected: bool) {
        self.connected.store(connected, Ordering::Relaxed);
    }

    pub(crate) fn set_input_width(&self, width: usize) {
        self.input_width.store(width, Ordering::Relaxed);
    }

    pub(crate) fn log(&self, line: String) {
        let mut messages = self.messages.lock().unwrap();
        messages.push(line);
        let overflow = messages.len().saturating_sub(MAX_MESSAGES);
        messages.drain(..overflow);
    }

    pub(crate) fn edit(&self, event: &ratatui::crossterm::event::Event) {
        let mut input = self.input.lock().unwrap();
        let _ = input.handle_event(event);
        if input.value().chars().count() < self.input_width.load(Ordering::Relaxed) {
            self.limit_hinted.store(false, Ordering::Relaxed);
        }
    }

    /// The single-line input refuses to scroll; report the limit once per overflow.
    pub(crate) fn block_over_limit(&self) -> bool {
        let width = self.input_width.load(Ordering::Relaxed);
        if width == 0 {
            return false;
        }
        if self.input.lock().unwrap().value().chars().count() < width {
            return false;
        }
        if !self.limit_hinted.swap(true, Ordering::Relaxed) {
            self.log(format!(
                "input at the {width}-column limit: enter sends, backspace edits"
            ));
        }
        true
    }

    pub(crate) fn messages_snapshot(&self) -> Vec<String> {
        self.messages.lock().unwrap().clone()
    }

    pub(crate) fn take_line(&self) -> String {
        let mut input = self.input.lock().unwrap();
        let line = input.value().to_owned();
        *input = Input::default();
        self.limit_hinted.store(false, Ordering::Relaxed);
        line
    }

    pub(crate) fn input_view(&self) -> (String, usize) {
        let input = self.input.lock().unwrap();
        (input.value().to_owned(), input.cursor())
    }
}
