use std::collections::HashMap;
use std::mem;

use crate::lang::types::*;

use crate::error::{Error, Result};

use lazy_static::lazy_static;

pub struct Tokenizer<'a> {
    source: &'a str,
    tokens: Vec<Token>,
    start: usize,
    current: usize,
    line: usize,
}

lazy_static! {
    static ref KEYWORDS: HashMap<&'static str, TokenType> = {
        HashMap::from([
            // ... other keywords
            // ("CAST" , TokenType::Cast ),
            // ("COLLATE" , TokenType::Collate ),
            ("CREATE", TokenType::Create),
            ("DELETE", TokenType::Delete),
            ("let", TokenType::Let),
            ("LJOIN", TokenType::Ljoin),
            ("JOIN", TokenType::Join),
            ("ON", TokenType::On),
            // ("FROM" , TokenType::From ),
            // ("INDEX" , TokenType::Index ),
            // ("INSERT" , TokenType::Insert ),
            // ("INTO" , TokenType::Into ),
            // ("KEY" , TokenType::Key ),
            ("NULL", TokenType::Null),
            // ("ON" , TokenType::On ),
            // ("PRIMARY" , TokenType::Primary ),
            ("SELECT", TokenType::Select),
            ("TABLE", TokenType::Table),
            ("true", TokenType::True),
            ("false", TokenType::False),
            ("||", TokenType::Or),
            ("&&", TokenType::And),
            ("let", TokenType::Let),
            // ("VALUES" , TokenType::Values ),
            // ("WHERE" , TokenType::Where ),
        ])
    };

    static ref METHODS: Vec<&'static str> = vec![
        "orderby",
        "groupby",
        "filter",
        "select",
        "select_distinct",
        "offset",
        "limit",
        "max",
        "min",
        "sum",
        "count",
        "count_distinct",
        "create_table",
        "create_db",
        "insert",
        "delete",
    ];
}


impl<'a> Tokenizer<'a> {
    pub fn new(source: &'a str) -> Tokenizer<'a> {
        Tokenizer {
            source,
            tokens: Vec::new(),
            start: 0,
            current: 0,
            line: 1,
        }
    }

    pub fn scan_tokens(&mut self) -> Result<Vec<Token>> {
        self.tokens = Vec::with_capacity(self.source.len());
        while !self.is_at_end() {
            self.start = self.current;
            match self.scan_token() {
                Ok(()) => (),
                Err(err) => return Err(err),
            }
        }

        self.add_token(TokenType::Eof, None);
        // let mut temp = self.tokens; 
        Ok(mem::take(&mut self.tokens))
    }

    fn is_at_end(&self) -> bool {
        self.current >= self.source.len()
    }

    fn scan_token(&mut self) -> Result<()> {
        let c = self.advance();
        match c {
            '(' => self.add_token(TokenType::ParenLeft, None),
            ')' => self.add_token(TokenType::ParenRight, None),
            // '[' => self.add_token(TokenType::SquareParenLeft, None),
            // ']' => self.add_token(TokenType::SquareParenRight, None),
            ',' => self.add_token(TokenType::Comma, None),
            '.' => self.add_token(TokenType::Dot, None),
            '-' => self.add_token(TokenType::Minus, None),
            '+' => self.add_token(TokenType::Plus, None),
            ';' => self.add_token(TokenType::Semicolon, None),
            '*' => self.add_token(TokenType::Asterisk, None),
            '=' => self.add_token(TokenType::Equal, None),
            '!' => {
                let token = if self.match_char('=') {
                    TokenType::NotEqual
                } else {
                    TokenType::Bang
                };
                self.add_token(token, None);
            }
            '<' => {
                let token = if self.match_char('=') {
                    TokenType::LessEqual
                } else {
                    TokenType::Less
                };
                self.add_token(token, None);
            }
            '>' => {
                let token = if self.match_char('=') {
                    TokenType::GreaterEqual
                } else {
                    TokenType::Greater
                };
                self.add_token(token, None);
            }
            '/' => {
                if self.match_char('/') {
                    // A comment goes until the end of the line.
                    while self.peek() != '\n' && !self.is_at_end() {
                        self.advance();
                    }
                } else {
                    self.add_token(TokenType::Slash, None);
                }
            }
            ' ' | '\r' | '\t' => {
                // Ignore whitespace.
            }
            '\n' => {
                self.line += 1;
            }
            '\'' => {
                if self.match_char('{') || self.match_char('[') {
                    //we have a json string to deal with
                    //collect everything until we get a }'
                    self.json_string()
                } else {
                    self.string('\'')
                }
            }
            '"' => {
                if self.match_char('{') || self.match_char('[') {
                    //we have a json string to deal with
                    //collect everything until we get a }"
                    self.json_string()
                } else {
                    self.string('"')
                }
            }
            _ => {
                if is_digit(c) {
                    self.number();
                } else if is_alpha(c) {
                    self.identifier();
                } else {
                    // Handle unexpected characters
                    println!(
                        "ScanError on {} {} {} {:?} {:?} ",
                        c,
                        self.line,
                        self.current,
                        self.tokens,
                        self.source[self.current..self.source.len()].chars()
                    );
                    return Err(Error::ScanError);
                }
            }
        }
        Ok(())
    }

    fn identifier(&mut self) {
        while is_alphanumeric(self.peek()) {
            self.advance();
        }

        let text = &self.source[self.start..self.current];

        let token = KEYWORDS.get(&text as &str);

        if text == "true" || text == "false" {
            self.add_token(TokenType::Boolean, Some(text.to_string()));
        }

        else if let Some(x) = token {
            // Special identifier
            self.add_token(x.clone(), Some(text.to_string()));
        } else if text
            .chars()
            //checking numeric here is fine because we checked that the first char was alpha earlier
            .all(|x| x.is_alphabetic() || x == '_' || x.is_numeric())
        {
            // check if valid var expression
            // yeah so theres no way a variable could be potentially a method then, since no . , no
            // funny business
            self.add_token(TokenType::Variable, Some(text.to_string()))
        } else {
            // probably illegal right
            self.add_token(TokenType::Illegal, Some(text.to_string()))
        }
    }

    fn number(&mut self) {
        while is_digit(self.peek()) {
            self.advance();
        }

        if self.peek() == '.' && is_digit(self.peek_next()) {
            self.advance(); // Consume the "."

            while is_digit(self.peek()) {
                self.advance();
            }
        }

        let text: String = self.source[self.start..self.current].chars().collect();
        self.add_token(TokenType::Number, Some(text));
    }

    fn string(&mut self, quote: char) {
        while self.peek() != quote && !self.is_at_end() {
            if self.peek() == '\n' {
                self.line += 1;
            }
            self.advance();
        }

        if self.is_at_end() {
            // Handle unterminated string error
            return;
        }

        // Closing quote
        self.advance();

        let text: String = self.source[self.start + 1..self.current - 1]
            .chars()
            .collect();
        self.add_token(TokenType::String, Some(text));
    }

    // needs proper error handling, otherwise program will never run
    fn json_string(&mut self) {
        // println!("Parsing json string now");

        while (true) {
            if self.peek() == '}' || self.peek() == ']' {
                self.advance();
                if self.peek() == '\'' {
                    self.advance();

                    if self.is_at_end() {
                        return;
                    }

                    let text = &self.source[self.start + 1..self.current - 1];

                    self.add_token(TokenType::String, Some(text.to_string()));

                    return;
                }
            } else {
                // println!("TOken {} next token {} ", self.peek(), self.peek_next());
                if self.is_at_end() {
                    // println!("We at the end somehow");
                    //maybe an error
                    return;
                }
                if self.peek() == '\n' || self.peek() == '\\' {
                    self.line += 1;
                }
                self.advance();
            }
        }

        // Closing quotes
    }

    fn peek(&self) -> char {
        if self.is_at_end() {
            '\0'
        } else {
            self.source[self.current..].chars().next().unwrap_or('\0')
        }
    }

    fn peek_next(&self) -> char {
        if self.current + 1 >= self.source.len() {
            '\0'
        } else {
            self.source[self.current + 1..].chars().next().unwrap_or('\0')
        }
    }

    fn match_char(&mut self, expected: char) -> bool {
        if self.is_at_end() {
            return false;
        }

        if self.source.chars().nth(self.current).unwrap() != expected {
            return false;
        }

        self.current += 1;
        true
    }

    fn advance(&mut self) -> char {
        let c = self.peek();
        self.current += 1;
        c
    }

    fn add_token(&mut self, token_type: TokenType, literal: Option<String>) {
        let text: String = self.source[self.start..self.current].chars().collect();
        self.tokens
            .push(Token::new(token_type, text, literal, self.line));
    }
}

fn is_digit(c: char) -> bool {
    c >= '0' && c <= '9'
}

fn is_alpha(c: char) -> bool {
    (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'
}

fn is_alphanumeric(c: char) -> bool {
    is_alpha(c) || is_digit(c)
}

#[cfg(test)]
mod tests {
    use super::*;

    //need to tests this a lot, will come back

    #[test]
    fn test_some_string() {
        let mut tokenizer = Tokenizer::new(
            "
        let x = db.TABLES.b.filter(); 
        let y = db.TABLES.x ; 
        let z = x JOIN y on x.id=y.id;  x.select(a,b,c,d);
        ",
        );
        let tokens = tokenizer.scan_tokens().unwrap();
        println!("Tokens: {:?}", tokens);
        ()
    }
}
