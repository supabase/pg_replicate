use rustyline::DefaultEditor;

use crate::{get_string, CliError};

pub fn get_publication_name(editor: &mut DefaultEditor) -> Result<String, CliError> {
    get_string(editor, "enter publication anme: ")
}
