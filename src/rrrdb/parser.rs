use ast::{Query, Statement};
use tokenizer::TokenizeError;

use self::{ast::{Expression, Projection, Value}, tokenizer::{Token, Tokenizer}};

mod tokenizer;
mod ast;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ParserError {
    TokenizeError(String),
    ParseError(String),
}
impl From<TokenizeError> for ParserError {
    fn from(e: TokenizeError) -> Self {
        ParserError::TokenizeError(e.message)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
  pub fn new(tokens: Vec<Token>) -> Self { 
    Self { 
      tokens,
      pos: 0,
    }
  }

  pub fn parse(query: &str) -> Result<Statement, ParserError> { 
    let tokens = Tokenizer::new(query).tokenize()?; 
    let mut parser = Self::new(tokens);

    match parser.next_token() {
      (Token::Keyword(tokenizer::Keyword::Select), _) => { 
        parser.parse_select_statement()
      },
      (Token::Keyword(tokenizer::Keyword::Insert), _) => {
        parser.parse_insert_statement()
      },
      (unexpected_token, pos) => {
        Err(ParserError::ParseError(format!("Unexpected token: {} at {}", unexpected_token, pos)))
      }
    }
  }

  fn parse_select_statement(&mut self) -> Result<Statement, ParserError> { 
    let projections: Vec<Projection> = {
      let mut v = vec![];
      self.consume_tokens(|token, pos| {
        match token {
          Token::Keyword(tokenizer::Keyword::From) => Ok(false),
          Token::Comma | Token::Whitespace(_) => Ok(true),
          Token::Mul => {
            v.push(Projection::Wildcard);
            Ok(true)
          },
          Token::SingleQuotedString(s) => {
            v.push(Projection::Expression(Expression::Value(Value::QuotedString(s.to_string()))));
            Ok(true)
          },
          Token::Number(num) => {
            v.push(Projection::Expression(Expression::Value(Value::Number(num.to_string()))));
            Ok(true)
          },
          Token::Word(ident) => {
            v.push(Projection::Expression(Expression::Ident(ident.to_string())));
            Ok(true)
          },
          unexpected_token => {
            Err(ParserError::ParseError(format!("Unexpected token: {} at {}", unexpected_token, pos)))
          }
        }
      });
      v
    };
    let predicates = vec![];
    let from = "";

    let query = Query::new(
      projections,
      from.to_string(),
      predicates,
    );
    Ok(Statement::Select(query))
  }

  fn parse_insert_statement(&mut self) -> Result<Statement, ParserError> { 
    todo!("parse insert")
  }

  fn next_token(&mut self) -> (&Token, usize) {
    loop {
      self.pos += 1;
      match self.tokens.get(self.pos - 1) {
        Some(Token::Whitespace(_)) => { continue },
        Some(token) => { return (token, self.pos) },
        None => { return (&Token::EOF, self.pos) },
      }
    }
  }

  fn consume_tokens(&mut self, mut consumer: impl FnMut(&Token, usize) -> Result<bool, ParserError>) -> Result<(), ParserError> { 
    loop {
      let (t, pos) = self.next_token();
      let _continue = consumer(t, pos)?;
      if _continue { continue }
      else { break }
    }
    Ok(())
  }
}