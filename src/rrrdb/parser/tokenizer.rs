use std::iter::Peekable;
use std::str::Chars;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
        let mut peekable: Peekable<Chars> = self.query.chars().peekable();
        let mut tokens: Vec<Token> = vec![];

        while let Some(token) = self.get_next_token(&mut peekable)? {
            tokens.push(token);
        }
        Ok(tokens)
    }

    fn get_next_token(
        &self,
        peekable: &mut Peekable<Chars>,
    ) -> Result<Option<Token>, TokenizeError> {
        let return_ok = |token| Ok(Some(token));
        let return_err = |message| Err(TokenizeError { message });
        match peekable.peek() {
            None => Ok(None),
            Some(&ch) => match peekable.next().unwrap() {
                '=' => return_ok(Token::Eq),
                '!' => match peekable.peek() {
                    Some('=') => {
                        peekable.next();
                        return_ok(Token::Neq)
                    }
                    Some(x) => return_err(format!("Unknown token: !{}", x)),
                    None => return_err(format!("Unknown token: !")),
                },
                '<' => match peekable.peek() {
                    Some('=') => {
                        peekable.next();
                        return_ok(Token::Lte)
                    }
                    _ => return_ok(Token::Lt),
                },
                '>' => match peekable.peek() {
                    Some('=') => {
                        peekable.next();
                        return_ok(Token::Gte)
                    }
                    _ => return_ok(Token::Gt),
                },
                '+' => return_ok(Token::Plus),
                '-' => return_ok(Token::Minus),
                '*' => return_ok(Token::Mul),
                '/' => return_ok(Token::Div),
                '%' => return_ok(Token::Mod),
                '(' => return_ok(Token::LParen),
                ')' => return_ok(Token::RParen),
                '.' => return_ok(Token::Period),
                ';' => return_ok(Token::SemiColon),
                ',' => return_ok(Token::Comma),
                ' ' => return_ok(Token::Whitespace(Whitespace::Space)),
                '\t' => return_ok(Token::Whitespace(Whitespace::Tab)),
                '\n' => return_ok(Token::Whitespace(Whitespace::Newline)),
                '0'..='9' => {
                    let mut s = String::new();
                    s.push(ch);
                    while let Some(&ch) = peekable.peek() {
                        match ch {
                            '0'..='9' => {
                                peekable.next();
                                s.push(ch);
                            }
                            _ => {
                                break;
                            }
                        }
                    }
                    return_ok(Token::Number(s))
                }
                '\'' => {
                    let mut s = String::new();
                    while let Some(&ch) = peekable.peek() {
                        match ch {
                            '\'' => {
                                peekable.next(); // consume '
                                break;
                            }
                            _ => {
                                peekable.next();
                                s.push(ch);
                            }
                        }
                    }
                    return_ok(Token::SingleQuotedString(s))
                }
                _ => {
                    let mut s = String::new();
                    s.push(ch);
                    while let Some(&ch) = peekable.peek() {
                        match ch {
                            ',' | ' ' | '\n' | '\t' | '(' | ')' => {
                                break;
                            }
                            _ => {
                                peekable.next();
                                s.push(ch);
                            }
                        }
                    }
                    match Keyword::find(s.as_ref()) {
                        Some(keyword) => return_ok(Token::Keyword(keyword)),
                        None => return_ok(Token::Word(s)),
                    }
                }
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct TokenizeError {
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum Token {
    EOF,
    Keyword(Keyword),
    Word(String),
    Number(String),
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

impl std::fmt::Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Token::EOF => write!(f, "EOF"),
            Token::Keyword(keyword) => write!(f, "{}", keyword),
            Token::Word(word) => write!(f, "{}", word),
            Token::Number(number) => write!(f, "{}", number),
            Token::SingleQuotedString(s) => write!(f, "{}", s),
            Token::Comma => write!(f, ","),
            Token::Whitespace(whitespace) => write!(f, "{}", whitespace),
            Token::Eq => write!(f, "="),
            Token::Neq => write!(f, "!="),
            Token::Lt => write!(f, "<"),
            Token::Lte => write!(f, "<="),
            Token::Gt => write!(f, ">"),
            Token::Gte => write!(f, ">="),
            Token::Plus => write!(f, "+"),
            Token::Minus => write!(f, "-"),
            Token::Mul => write!(f, "*"),
            Token::Div => write!(f, "/"),
            Token::Mod => write!(f, "%"),
            Token::LParen => write!(f, "("),
            Token::RParen => write!(f, ")"),
            Token::Period => write!(f, "."),
            Token::SemiColon => write!(f, ";"),
        }
    }
}

macro_rules! define_keywords {
  ($($keyword:ident), *) => {
      #[derive(Debug, Clone, PartialEq, Eq, Hash)]
      pub(crate) enum Keyword {
        $($keyword), *
      }
      impl Keyword {
        pub fn find(s: &str) -> Option<Keyword> {
          match s {
            $(s if s.to_lowercase() == stringify!($keyword).to_lowercase() => { Some(Keyword::$keyword) },)
            *
            _ => None,
          }
        }
      }
      impl std::fmt::Display for Keyword {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
          match self {
            $(Keyword::$keyword => { write!(f, "{}", stringify!($keyword)) }),
            *
          }
        }
      }
      pub(crate) const ALL: &[Keyword] = &[
        $(Keyword::$keyword), *
      ];
  };
}

define_keywords!(Create, Database, Table, Select, From, Where, Insert, Into, Values);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum Whitespace {
    Space,
    Newline,
    Tab,
}
impl std::fmt::Display for Whitespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Whitespace::Space => write!(f, " "),
            Whitespace::Newline => write!(f, "\\n"),
            Whitespace::Tab => write!(f, "\\t"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenize_select_1() {
        tokenizer_assertion(
            "SELECT 1",
            vec![
                Token::Keyword(Keyword::Select),
                Token::Whitespace(Whitespace::Space),
                Token::Number(String::from("1")),
            ],
        );
    }

    #[test]
    fn tokenize_select_from() {
        tokenizer_assertion(
            "SELECT * FROM users",
            vec![
                Token::Keyword(Keyword::Select),
                Token::Whitespace(Whitespace::Space),
                Token::Mul,
                Token::Whitespace(Whitespace::Space),
                Token::Keyword(Keyword::From),
                Token::Whitespace(Whitespace::Space),
                Token::Word("users".to_string()),
            ],
        );
    }

    #[test]
    fn tokenize_select_from_where() {
        tokenizer_assertion(
            "SELECT * FROM users WHERE id = 1",
            vec![
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
        );
    }

    #[test]
    fn tokenize_create_database() {
        tokenizer_assertion(
            "CREATE DATABASE test_db",
            vec![
                Token::Keyword(Keyword::Create),
                Token::Whitespace(Whitespace::Space),
                Token::Keyword(Keyword::Database),
                Token::Whitespace(Whitespace::Space),
                Token::Word("test_db".to_string()),
            ],
        );
    }

    #[test]
    fn tokenize_create_table() {
        tokenizer_assertion(
            "CREATE TABLE users (id integer, name varchar)",
            vec![
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
        );
    }

    #[test]
    fn tokenize_insert_into_simple() {
        tokenizer_assertion(
            "INSERT INTO users VALUES (1, 'alice')",
            vec![
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
        );
    }

    fn tokenizer_assertion(sql: &str, expected: Vec<Token>) {
        let mut tokenizer = Tokenizer::new(sql);
        let result: Result<Vec<Token>, TokenizeError> = tokenizer.tokenize();
        assert!(result.is_ok());
        let tokens = result.unwrap();

        assert_eq!(expected, tokens);
    }
}
