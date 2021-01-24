mod tokenizer;
mod ast;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ParserError {
    TokenizeError(String),
    ParseError(String),
}
