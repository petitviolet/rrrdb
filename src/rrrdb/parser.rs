use ast::{BinaryOperator, Operator, Predicate, Query, Statement};
use tokenizer::TokenizeError;

use self::{
    ast::{Expression, Projection, Value},
    tokenizer::{Token, Tokenizer},
};

mod ast;
mod tokenizer;

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
        Self { tokens, pos: 0 }
    }

    pub fn parse_sql(query: &str) -> Result<Statement, ParserError> {
        let tokens = Tokenizer::new(query).tokenize()?;
        let mut parser = Self::new(tokens);
        parser.parse()
    }

    pub fn parse(&mut self) -> Result<Statement, ParserError> {
        match self.next_token() {
            (Token::Keyword(tokenizer::Keyword::Select), _) => self.parse_select_statement(),
            (Token::Keyword(tokenizer::Keyword::Insert), _) => self.parse_insert_statement(),
            (unexpected_token, pos) => Err(ParserError::ParseError(format!(
                "Unexpected token: '{}' at {}",
                unexpected_token, pos
            ))),
        }
    }

    fn parse_select_statement(&mut self) -> Result<Statement, ParserError> {
        let projections: Vec<Projection> = {
            let mut v = vec![];
            self.consume_tokens(|token, pos| match token {
                Token::EOF => Ok(false),
                Token::Keyword(tokenizer::Keyword::From) => Ok(false),
                Token::Comma | Token::Whitespace(_) => Ok(true),
                Token::Mul => {
                    v.push(Projection::Wildcard);
                    Ok(true)
                }
                Token::SingleQuotedString(s) => {
                    v.push(Projection::Expression(Expression::Value(
                        Value::QuotedString(s.to_string()),
                    )));
                    Ok(true)
                }
                Token::Number(num) => {
                    v.push(Projection::Expression(Expression::Value(Value::Number(
                        num.to_string(),
                    ))));
                    Ok(true)
                }
                Token::Word(ident) => {
                    v.push(Projection::Expression(Expression::Ident(ident.to_string())));
                    Ok(true)
                }
                unexpected_token => Err(ParserError::ParseError(format!(
                    "Unexpected token while parsing Projections: '{}' at {}",
                    unexpected_token, pos
                ))),
            })?;
            v
        };
        let from = {
            let (token, pos) = self.next_token();
            match token {
                Token::EOF => Ok(None),
                Token::Word(name) => Ok(Some(name.to_string())),
                unexpected_token => Err(ParserError::ParseError(format!(
                    "Unexpected token while parsing From: '{}' at {}",
                    unexpected_token, pos
                ))),
            }
        }?;
        let operators = {
            let mut v = vec![];
            if let (&Token::Keyword(tokenizer::Keyword::Where), pos) = self.next_token() {
                loop {
                    if self.skip_stop_words()? {
                        break;
                    } else {
                        v.push(self.parse_operator()?);
                    }
                }
            }
            v
        };

        let query = Query::new(projections, from, Predicate::new(operators));
        Ok(Statement::Select(query))
    }

    fn parse_insert_statement(&mut self) -> Result<Statement, ParserError> {
        todo!("parse insert")
    }

    // return true if the next token is EOF, otherwise false
    fn skip_stop_words(&mut self) -> Result<bool, ParserError> {
        loop {
            if self.pos >= self.tokens.len() {
                // consider EOF
                return Ok(true);
            }
            match self.tokens.get(self.pos + 1) {
                Some(Token::Whitespace(_)) => {
                    self.pos += 1;
                    continue;
                }
                Some(Token::EOF) => return Ok(true),
                _ => return Ok(false),
            }
        }
    }

    fn parse_operator(&mut self) -> Result<Operator, ParserError> {
        let left = self.parse_expression()?;
        let (token, pos) = self.next_token();
        let binop = match token {
            Token::Eq => Ok(BinaryOperator::Eq),
            unexpected_token => Err(ParserError::ParseError(format!(
                "Unexpected token while parse_operator: '{}' at {}",
                unexpected_token, pos
            ))),
        }?;
        let right = self.parse_expression()?;
        Ok(binop.build(left, right))
    }

    fn parse_expression(&mut self) -> Result<Expression, ParserError> {
        let (token, pos) = self.next_token();
        match token {
            Token::SingleQuotedString(s) => Ok(Expression::quoted_string(&s)),
            Token::Number(num) => Ok(Expression::number(&num)),
            Token::Word(ident) => Ok(Expression::ident(&ident)),
            unexpected_token => Err(ParserError::ParseError(format!(
                "Unexpected token while parse_expression: '{}' at {}",
                unexpected_token, pos
            ))),
        }
    }

    fn next_token(&mut self) -> (&Token, usize) {
        if self.pos >= self.tokens.len() {
            return (&Token::EOF, self.pos);
        }
        loop {
            self.pos += 1;
            match self.tokens.get(self.pos - 1) {
                Some(Token::Whitespace(_)) => continue,
                Some(token) => return (token, self.pos),
                None => return (&Token::EOF, self.pos),
            }
        }
    }

    fn consume_tokens(
        &mut self,
        mut consumer: impl FnMut(&Token, usize) -> Result<bool, ParserError>,
    ) -> Result<(), ParserError> {
        loop {
            let (t, pos) = self.next_token();
            let _continue = consumer(t, pos)?;
            if _continue {
                continue;
            } else {
                break;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::ast::*;
    use super::tokenizer::*;
    use super::*;

    #[test]
    fn parse_select_1() {
        parser_assertion(
            vec![
                Token::Keyword(Keyword::Select),
                Token::Whitespace(Whitespace::Space),
                Token::Number(String::from("1")),
            ],
            Statement::Select(Query::new(
                vec![Projection::Expression(Expression::Value(Value::Number(
                    "1".to_string(),
                )))],
                None,
                Predicate::new(vec![]),
            )),
        );
    }

    #[test]
    fn parse_select_from() {
        parser_assertion(
            vec![
                // SELECT * FROM users
                Token::Keyword(Keyword::Select),
                Token::Whitespace(Whitespace::Space),
                Token::Mul,
                Token::Whitespace(Whitespace::Space),
                Token::Keyword(Keyword::From),
                Token::Whitespace(Whitespace::Space),
                Token::Word("users".to_string()),
            ],
            Statement::Select(Query::new(
                vec![Projection::Wildcard],
                Some("users".to_string()),
                Predicate::new(vec![]),
            )),
        );
    }

    #[test]
    fn parse_select_from_where() {
        parser_assertion(
            vec![
                // SELECT * FROM users WHERE id = 1
                Token::Keyword(Keyword::Select),
                Token::Whitespace(Whitespace::Space),
                Token::Mul,
                Token::Whitespace(Whitespace::Space),
                Token::Keyword(Keyword::From),
                Token::Whitespace(Whitespace::Space),
                Token::Word("users".to_string()),
                Token::Whitespace(Whitespace::Space),
                Token::Keyword(Keyword::Where),
                Token::Whitespace(Whitespace::Space),
                Token::Word("id".to_string()),
                Token::Whitespace(Whitespace::Space),
                Token::Eq,
                Token::Whitespace(Whitespace::Space),
                Token::Number("1".to_string()),
            ],
            Statement::Select(Query::new(
                vec![Projection::Wildcard],
                Some("users".to_string()),
                Predicate::new(vec![Operator::BinOperator {
                    lhs: Expression::Ident("id".to_string()),
                    rhs: Expression::Value(Value::Number("1".to_string())),
                    op: BinaryOperator::Eq,
                }]),
            )),
        );
    }

    fn parser_assertion(tokens: Vec<Token>, expected: Statement) {
        let mut parser = Parser::new(tokens);
        let result = parser.parse();
        assert!(result.is_ok(), "result: {:?}", result);
        let tokens = result.unwrap();

        assert_eq!(expected, tokens);
    }
}
