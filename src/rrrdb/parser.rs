use std::{
    convert::TryInto,
    fmt::{Debug, Display},
    ops::Deref,
};

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
    database_name: Option<String>,
}

impl Parser {
    pub fn new(tokens: Vec<Token>, database_name: Option<String>) -> Self {
        Self {
            tokens,
            pos: 0,
            database_name,
        }
    }

    pub fn parse_sql(database_name: Option<String>, query: &str) -> Result<Statement, ParserError> {
        let tokens = Tokenizer::new(query).tokenize()?;
        let mut parser = Self::new(tokens, database_name);
        parser.parse()
    }

    fn unexpected_token<A, T: Debug>(
        stage: &str,
        unexpected_token: &T,
        pos: usize,
    ) -> Result<A, ParserError> {
        Err(ParserError::ParseError(format!(
            "Unexpected token found while processing {}. token: '{:?}' at {}",
            stage, unexpected_token, pos
        )))
    }

    pub fn parse(&mut self) -> Result<Statement, ParserError> {
        match self.next_token() {
            (Token::Keyword(tokenizer::Keyword::Select), _) => self.parse_select_statement(),
            (Token::Keyword(tokenizer::Keyword::Insert), _) => self.parse_insert_statement(),
            (Token::Keyword(tokenizer::Keyword::Create), _) => self.parse_create_statement(),
            (unexpected_token, pos) => Self::unexpected_token("parse", unexpected_token, pos),
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
                unexpected_token => Self::unexpected_token("projections", unexpected_token, pos),
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
                unexpected_token => Self::unexpected_token("from statement", unexpected_token, pos),
            }
        }?;
        let predicate: Predicate = self.parse_predicate()?;

        let query = Query::new(projections, froms, predicate);
        Ok(Statement::Select(query))
    }

    fn parse_insert_statement(&mut self) -> Result<Statement, ParserError> {
        match self.next_token() {
            (Token::Keyword(tokenizer::Keyword::Into), _) => {}
            (unexpected_token, pos) => {
                return Self::unexpected_token("insert into statement", unexpected_token, pos);
            }
        }
        match self.next_token() {
            (Token::Word(table_name), _) => {
                let table_name = table_name.to_owned();
                match self.next_token() {
                    (Token::Keyword(Keyword::Values), _) => {
                        let values = self.parse_insert_values()?;
                        Ok(Statement::Insert(Insert::new(table_name, values)))
                    }
                    (unexpected_token, pos) => {
                        return Self::unexpected_token(
                            "insert into statement",
                            unexpected_token,
                            pos,
                        );
                    }
                }
            }
            (unexpected_token, pos) => {
                return Self::unexpected_token("insert into statement", unexpected_token, pos);
            }
        }
    }
    fn parse_insert_values(&mut self) -> Result<Vec<Value>, ParserError> {
        match self.next_token() {
            (&Token::LParen, _) => Ok(()),
            (unexpected_token, pos) => {
                Self::unexpected_token("insert values", unexpected_token, pos)
            }
        }?;
        let mut results = vec![];
        let mut is_rparen = false;
        loop {
            self.consume_tokens(|token, pos| match token {
                &Token::RParen => {
                    is_rparen = true;
                    Ok(false)
                }
                &Token::EOF => Self::unexpected_token("insert values", &Token::EOF, pos),
                &Token::Comma => Ok(false),
                Token::Number(num) => {
                    results.push(Value::Number(num.clone()));
                    Ok(true)
                }
                Token::SingleQuotedString(s) => {
                    results.push(Value::QuotedString(s.clone()));
                    Ok(true)
                }
                unexpected_token => Self::unexpected_token("insert values", unexpected_token, pos),
            })?;
            if is_rparen {
                break;
            }
        }
        Ok(results)
    }

    fn parse_create_statement(&mut self) -> Result<Statement, ParserError> {
        match self.next_token() {
            (Token::Keyword(tokenizer::Keyword::Database), _) => {
                self.parse_create_database_statement()
            }
            (Token::Keyword(tokenizer::Keyword::Table), _) => self.parse_create_table_statement(),
            (unexpected_token, pos) => {
                Self::unexpected_token("create statement", unexpected_token, pos)
            }
        }
    }
    fn parse_create_database_statement(&mut self) -> Result<Statement, ParserError> {
        match self.next_token() {
            (Token::Word(database_name), _) => {
                let stmt = Statement::CreateDatabase(CreateDatabase::new(database_name.to_owned()));
                Ok(stmt)
            }
            (unexpected_token, pos) => {
                Self::unexpected_token("create database statement", unexpected_token, pos)
            }
        }
    }

    fn parse_create_table_statement(&mut self) -> Result<Statement, ParserError> {
        match self.next_token() {
            (Token::Word(table_name), _) => {
                let table_name = table_name.to_owned(); // enable to use self.database_name
                let columns = self.parse_create_table_column_definitions()?;
                let stmt = Statement::CreateTable(CreateTable::new(
                    self.database_name.clone().unwrap().to_string(),
                    table_name,
                    columns,
                ));
                Ok(stmt)
            }
            (unexpected_token, pos) => {
                Self::unexpected_token("create database statement", unexpected_token, pos)
            }
        }
    }

    // create table :table_name \((:column_name :column_type)(, :column_name :column_type)*\)
    fn parse_create_table_column_definitions(
        &mut self,
    ) -> Result<Vec<ColumnDefinition>, ParserError> {
        match self.next_token() {
            (&Token::LParen, _) => Ok(()),
            (unexpected_token, pos) => {
                Self::unexpected_token("create table column definitions", unexpected_token, pos)
            }
        }?;
        let mut results = vec![];
        let mut is_rparen = false;
        loop {
            let mut v = vec![];
            self.consume_tokens(|token, pos| match token {
                &Token::RParen => {
                    is_rparen = true;
                    Ok(false)
                }
                &Token::EOF => {
                    Self::unexpected_token("create table column definitions", &Token::EOF, pos)
                }
                &Token::Comma => Ok(false),
                token => {
                    v.push(token.clone());
                    Ok(true)
                }
            })?;
            if let &[Token::Word(column_name), Token::Word(column_type)] = &&v[..] {
                results.push(ColumnDefinition::new(
                    column_name.to_owned(),
                    column_type.to_owned(),
                ));
            } else {
                return Self::unexpected_token("create table column definitions", &v, self.pos);
            }
            if is_rparen {
                break;
            }
        }

        Ok(results)
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
            unexpected_token => Self::unexpected_token("expression", unexpected_token, pos),
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
            None => Self::unexpected_token("Left expression", &op, self.pos),
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

    #[test]
    fn parse_create_database() {
        parser_assertion(
            vec![
                // CREATE DATABASE test_db
                Token::Keyword(Keyword::Create),
                Token::Whitespace(Whitespace::Space),
                Token::Keyword(Keyword::Database),
                Token::Whitespace(Whitespace::Space),
                Token::Word("test_db".to_string()),
            ],
            Statement::CreateDatabase(CreateDatabase::new("test_db".to_string())),
        );
    }

    #[test]
    fn parse_create_table() {
        parser_assertion(
            vec![
                // CREATE TABLE users (id integer, name varchar),
                Token::Keyword(Keyword::Create),
                Token::Whitespace(Whitespace::Space),
                Token::Keyword(Keyword::Table),
                Token::Whitespace(Whitespace::Space),
                Token::Word("users".to_string()),
                Token::Whitespace(Whitespace::Space),
                Token::LParen,
                Token::Word("id".to_string()),
                Token::Whitespace(Whitespace::Space),
                Token::Word("integer".to_string()),
                Token::Comma,
                Token::Whitespace(Whitespace::Space),
                Token::Word("name".to_string()),
                Token::Whitespace(Whitespace::Space),
                Token::Word("varchar".to_string()),
                Token::RParen,
            ],
            Statement::CreateTable(CreateTable::new(
                "test_db".to_string(),
                "users".to_string(),
                vec![
                    ColumnDefinition::new("id".to_string(), "integer".to_string()),
                    ColumnDefinition::new("name".to_string(), "varchar".to_string()),
                ],
            )),
        );
    }

    #[test]
    fn parse_insert_into() {
        parser_assertion(
            vec![
                // INSERT INTO users VALUES (1, 'alice')
                Token::Keyword(Keyword::Insert),
                Token::Whitespace(Whitespace::Space),
                Token::Keyword(Keyword::Into),
                Token::Whitespace(Whitespace::Space),
                Token::Word("users".to_string()),
                Token::Whitespace(Whitespace::Space),
                Token::Keyword(Keyword::Values),
                Token::Whitespace(Whitespace::Space),
                Token::LParen,
                Token::Number("1".to_string()),
                Token::Comma,
                Token::Whitespace(Whitespace::Space),
                Token::SingleQuotedString("alice".to_string()),
                Token::RParen,
            ],
            Statement::Insert(Insert::new(
                "users".to_string(),
                vec![
                    Value::Number("1".to_string()),
                    Value::QuotedString("alice".to_string()),
                ],
            )),
        );
    }

    fn parser_assertion(tokens: Vec<Token>, expected: Statement) {
        let mut parser = Parser::new(tokens, Some("test_db".to_string()));
        let result = parser.parse();
        assert!(result.is_ok(), "result: {:?}", result);
        let tokens = result.unwrap();

        assert_eq!(expected, tokens);
    }
}
