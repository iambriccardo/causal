use std::io::Write;
use std::thread::sleep;
use std::time::Duration;
use console::{Key, Term};

pub trait InputReceiver {

    fn insert_at(&mut self, position: usize, character: char);

    fn remove_at(&mut self, position: usize);
}

pub struct InputField {
    cursor_position: usize,
    value: String
}

impl InputField {

    pub fn start(string: String, receiver: &mut impl InputReceiver) -> InputField {
        let mut input_field = InputField {
            cursor_position: 0,
            value: string,
        };
        input_field.render(receiver);

        input_field
    }

    fn render(&mut self, receiver: &mut impl InputReceiver) {
        let term = Term::stdout();
        term.hide_cursor().unwrap();

        self.render_value(&term);

        loop {
            if let Ok(key) = term.read_key() {
                match key {
                    Key::ArrowLeft => {
                        self.cursor_backward(&term);
                    }
                    Key::ArrowRight => {
                        self.cursor_forward(&term);
                    },
                    Key::Char(character) => {
                        receiver.insert_at(self.cursor_position, character);
                        self.insert(&term, character);
                    },
                    Key::Backspace => {
                        receiver.remove_at(self.cursor_position);
                        self.remove(&term);
                    }
                    Key::Enter => {
                        break;
                    }
                    _ => { }
                }
             }
        }

        term.clear_screen().unwrap();
        term.show_cursor().unwrap();
    }

    fn render_value(&self, term: &Term) {
        term.clear_screen().unwrap();
        let mut value = &mut self.value.clone();
        value.insert(self.cursor_position, '|');
        term.write_line(value).unwrap();
    }

    fn cursor_backward(&mut self, term: &Term) {
        if self.cursor_position > 0 {
            self.cursor_position -= 1;
            self.render_value(&term);
        }
    }

    fn cursor_forward(&mut self, term: &Term) {
        if self.cursor_position < self.value.len() {
            self.cursor_position += 1;
            self.render_value(&term);
        }
    }

    fn insert(&mut self, term: &Term, character: char) {
        self.value.insert(self.cursor_position, character);
        self.cursor_position += 1;
        self.render_value(&term);
    }

    fn remove(&mut self, term: &Term) {
        if self.cursor_position > 0 {
            self.value.remove(self.cursor_position - 1);
            self.cursor_position -= 1;
            self.render_value(&term);
        }
    }
}