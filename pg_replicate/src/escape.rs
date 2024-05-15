use std::borrow::Cow;

pub fn quote_identifier(identifier: &str) -> Cow<'_, str> {
    if identifier.find('"').is_some() {
        Cow::Owned(quote_identifier_alloc(identifier))
    } else {
        Cow::Borrowed(identifier)
    }
}

fn quote_identifier_alloc(identifier: &str) -> String {
    let mut quoted_identifier = String::with_capacity(identifier.len());
    for char in identifier.chars() {
        if char == '"' {
            quoted_identifier.push('"');
        }
        quoted_identifier.push(char);
    }
    quoted_identifier
}

pub fn quote_literal(literal: &str) -> String {
    let mut quoted_literal = String::with_capacity(literal.len() + 2);

    if literal.find('\\').is_some() {
        quoted_literal.push('E');
    }

    quoted_literal.push('\'');

    for char in literal.chars() {
        if char == '\'' {
            quoted_literal.push('\'');
        } else if char == '\\' {
            quoted_literal.push('\\');
        }

        quoted_literal.push(char);
    }

    quoted_literal.push('\'');

    quoted_literal
}
