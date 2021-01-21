pub(crate) struct Tokenizer {
    query: String,
}

impl Tokenizer {
    pub fn new(query: &str) -> Self {
        Self {
            query: query.to_string(),
        }
    }

    pub fn tokenize(&mut self) -> Result<Vec<Token>, TokenizeError> {
        Ok(vec![])
    }
}
pub(crate) struct TokenizeError {
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum Token {
    EOF,
    Keyword(Keyword),
    Number(String),
    Char(char),
    SingleQuotedString(String),
    Comma,
    Whitespace(Whitespace),
    Eq,        // =
    Neq,       // !=
    Lt,        // <
    Lte,       // <=
    Gt,        // >
    Gte,       // >=
    Plus,      // +
    Minus,     // -
    Mul,       // *
    Div,       // /
    Mod,       // %
    LParen,    // (
    RParen,    // )
    Period,    // .
    SemiColon, // ;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum Keyword {
    Select,
    From,
    Where,
    Insert,
    Into,
    Values,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum Whitespace {
    Space,
    Newline,
    Tab,
}
