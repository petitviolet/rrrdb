use std::{convert::TryInto, ops::Deref};

pub(crate) use ast::*;
use tokenizer::*;

mod ast;
mod tokenizer;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ParserError {
    TokenizeError(String),
    ParseError(String),
}
impl ToString for ParserError {
    fn to_string(&self) -> String {
        match self {
            ParserError::TokenizeError(msg) => format!("TokenizeError: {}", msg),
            ParserError::ParseError(msg) => format!("ParseError: {}", msg),
        }
    }
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
        let froms = {
            let (token, pos) = self.next_token();
            let mut v: Vec<String> = vec![];
            match token {
                Token::EOF => Ok(v),
                Token::Word(name) => {
                    v.push(name.to_owned());
                    Ok(v)
                }
                unexpected_token => Err(ParserError::ParseError(format!(
                    "Unexpected token while parsing From: '{}' at {}",
                    unexpected_token, pos
                ))),
            }
        }?;
        let predicate: Predicate = self.parse_predicate()?;

        let query = Query::new(projections, froms, predicate);
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

    fn parse_predicate(&mut self) -> Result<Predicate, ParserError> {
        if let (&Token::Keyword(tokenizer::Keyword::Where), pos) = self.next_token() {
            loop {
                if self.skip_stop_words()? {
                    break;
                } else {
                    let expr = self.parse_expression(None)?;
                    return Ok(Predicate::new(expr));
                }
            }
        }
        Ok(Predicate::empty())
    }

    fn parse_expression(
        &mut self,
        processing: Option<Expression>,
    ) -> Result<Expression, ParserError> {
        let (token, pos) = self.next_token();
        match token {
            Token::SingleQuotedString(s) => {
                let expr = Expression::quoted_string(&s);
                self.continue_parse_expr(expr, processing)
            }
            Token::Number(num) => {
                let expr = Expression::number(&num);
                self.continue_parse_expr(expr, processing)
            }
            Token::Word(ident) => {
                let expr = match ident.as_str() {
                    "true" => Expression::boolean(true),
                    "false" => Expression::boolean(false),
                    s => Expression::ident(s),
                };
                self.continue_parse_expr(expr, processing)
            }
            Token::Eq => self.build_binoperator(BinaryOperator::Eq, processing),
            Token::Neq => self.build_binoperator(BinaryOperator::Neq, processing),
            Token::Lt => self.build_binoperator(BinaryOperator::Lt, processing),
            Token::Lte => self.build_binoperator(BinaryOperator::Lte, processing),
            Token::Gt => self.build_binoperator(BinaryOperator::Gt, processing),
            Token::Gte => self.build_binoperator(BinaryOperator::Gte, processing),
            // Token::Plus => { Ok(BinaryOperator::Plus)},
            // Token::Minus => { Ok(BinaryOperator::Minus)},
            // Token::Mul => { Ok(BinaryOperator::Mul)},
            // Token::Div => { Ok(BinaryOperator::Div)},
            // Token::Mod => { Ok(BinaryOperator::Mod)},
            // Token::LParen => { Ok(BinaryOperator::LParen)},
            // Token::RParen => { Ok(BinaryOperator::RParen)},
            // Token::Period => { Ok(BinaryOperator::Period)},
            // Token::SemiColon => { Ok(BinaryOperator::SemiColon)},
            Token::EOF => processing.ok_or(ParserError::ParseError(format!(
                "Unexpected EOF while parse_expression",
            ))),
            unexpected_token => Err(ParserError::ParseError(format!(
                "Unexpected token while parse_expression: '{}' at {}",
                unexpected_token, pos
            ))),
        }
    }

    fn continue_parse_expr(
        &mut self,
        expr: Expression,
        processing: Option<Expression>,
    ) -> Result<Expression, ParserError> {
        match processing {
            Some(left) => Err(ParserError::ParseError(format!(""))),
            None => self.parse_expression(Some(expr)),
        }
    }

    fn build_binoperator(
        &mut self,
        op: BinaryOperator,
        processing: Option<Expression>,
    ) -> Result<Expression, ParserError> {
        match processing {
            Some(left) => {
                let right = self.parse_expression(None)?;
                Ok(op.build(left, right))
            }
            None => Err(ParserError::ParseError(format!(
                "LeftExpression for '{:?}' doesn't exist while parse_operator: at {}",
                op, self.pos
            ))),
        }
    }

    fn prev_token(&mut self) -> (&Token, usize) {
        if self.pos <= 0 {
            self.pos = 0;
            return (self.tokens.get(0).unwrap_or(&Token::EOF), self.pos);
        }
        loop {
            self.pos -= 1;
            match self.tokens.get(self.pos) {
                Some(Token::Whitespace(_)) => continue,
                Some(token) => return (token, self.pos),
                None => return (&Token::EOF, self.pos),
            }
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
                vec![],
                Predicate::empty(),
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
                vec!["users".to_string()],
                Predicate::empty(),
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
                vec!["users".to_string()],
                Predicate::new(Expression::BinOperator {
                    lhs: Box::new(Expression::Ident("id".to_string())),
                    rhs: Box::new(Expression::Value(Value::Number("1".to_string()))),
                    op: BinaryOperator::Eq,
                }),
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
