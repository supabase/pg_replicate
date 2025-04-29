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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quote_identifier_without_quotes() {
        let identifier = "column_name";
        let result = quote_identifier(identifier);
        assert_eq!(result, "column_name");
        // Should return the original string as a borrowed value
        assert!(matches!(result, Cow::Borrowed(_)));
    }

    #[test]
    fn test_quote_identifier_with_quotes() {
        let identifier = r#"column"name"#;
        let result = quote_identifier(identifier);
        assert_eq!(result, r#"column""name"#);
        // Should return a new allocated string
        assert!(matches!(result, Cow::Owned(_)));
    }

    #[test]
    fn test_quote_literal_simple() {
        let literal = "value";
        let result = quote_literal(literal);
        assert_eq!(result, "'value'");
    }

    #[test]
    fn test_quote_literal_with_single_quote() {
        let literal = "O'Reilly";
        let result = quote_literal(literal);
        assert_eq!(result, "'O''Reilly'");
    }

    #[test]
    fn test_quote_literal_with_backslash() {
        let literal = r"C:\path\to\file";
        let result = quote_literal(literal);
        assert_eq!(result, r"E'C:\\path\\to\\file'");
    }
}
