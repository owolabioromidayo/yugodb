use std::collections::HashMap;

use crate::lang::types::*; 


use crate::error::{Result, Error};


pub struct Tokenizer<'a> {
    source: &'a str,
    tokens: Vec<Token>,
    start: usize,
    current: usize,
    line: usize,
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

    pub fn scan_tokens(&mut self) -> Result<Vec<Token>>  {
        while !self.is_at_end() {
            self.start = self.current;
            match self.scan_token() {
                Ok(()) => (),
                Err(err) => return Err(err)
            }
        }

        self.add_token(TokenType::Eof, None); 
        Ok(self.tokens.clone())
    }

    fn is_at_end(&self) -> bool {
        self.current >= self.source.len()
    }

    fn scan_token(&mut self) -> Result<()> {
        let c = self.advance();
        match c {
            '(' => self.add_token(TokenType::LeftParen, None),
            ')' => self.add_token(TokenType::RightParen, None),
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
            '\'' => self.string(),
            '"' => self.string(),
            _ => {
                if is_digit(c) {
                    self.number();
                } else if is_alpha(c) {
                    self.identifier();
                } else {
                    // Handle unexpected characters
                    return Err(Error::ScanError)
                }
            }
        }
        Ok(())
    }

    fn identifier(&mut self) {
        while is_alphanumeric(self.peek()) {
            self.advance();
        }

        let text = self.source[self.start..self.current].chars().collect();

        //for some reason I cannot make this global
        let  keywords : HashMap<&str, TokenType> = HashMap::from([
            // ("CAST" , TokenType::Cast ),
            // ("COLLATE" , TokenType::Collate ),
            ("CREATE" , TokenType::Create ),
            ("DELETE" , TokenType::Delete ),
            ("let", TokenType::Let),
            ("LJOIN", TokenType::Ljoin),
            ("JOIN", TokenType::Join),
            // ("FROM" , TokenType::From ),
            // ("INDEX" , TokenType::Index ),
            // ("INSERT" , TokenType::Insert ),
            // ("INTO" , TokenType::Into ),
            // ("KEY" , TokenType::Key ),
            ("NULL" , TokenType::Null ),
            // ("ON" , TokenType::On ),
            // ("PRIMARY" , TokenType::Primary ),
            ("SELECT" , TokenType::Select ),
            ("TABLE" , TokenType::Table ),
            ("true" , TokenType::True),
            ("false" , TokenType::False),
            ("||" , TokenType::Or ),
            ("&&" , TokenType::And ),
            ("let", TokenType::Let),
            // ("VALUES" , TokenType::Values ),
            // ("WHERE" , TokenType::Where ),
        ]);

        let methods = vec!["orderby", "groupby", "filter", "select", "select_distinct",
                "offset", "limit", "max", "min", "sum", "count", "count_distinct"]; 

        let token = keywords.get(&text as &str);
        
        if &text == "true" || &text == "false" {
            self.add_token(TokenType::Boolean, Some(text)); 

        }
        else if text.contains('.') {
            //is attribute
            if self.peek() == '(' {
                //this means the last part of the attr is a method
                let parts: Vec<&str> = text.rsplitn(2, '.').collect();
                let first_part = parts.last().unwrap_or(&"");
                let second_part = parts.first().unwrap_or(&"");

                self.add_token(TokenType::Attribute, Some(first_part.to_string())); 

                //we need to ensure method validity
                if methods.contains(second_part){
                    self.add_token(TokenType::Method, Some(second_part.to_string()));
                } else{
                    //we need some illegal method error preferraably
                    self.add_token(TokenType::Illegal, Some(second_part.to_string()));
                }



            } else {
                self.add_token(TokenType::Attribute, Some(text))
            }
        }else if let Some(x) = token {
            // Special identifier
            self.add_token(x.clone(), Some(text)); 

        } else  if text.chars().all(char::is_alphabetic) {
            // check if valid var expression
            // yeah so theeres no way a variable could be potentially a method then, since no . , no 
            // funny business
            self.add_token(TokenType::Variable, Some(text))
        } else {
                // probably illegal right
                self.add_token(TokenType::Illegal, Some(text))
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

    fn string(&mut self) {
        while self.peek() != '"' && !self.is_at_end() {
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

        let text: String = self.source[self.start + 1..self.current - 1].chars().collect();
        self.add_token(TokenType::String, Some(text));
    }

    fn peek(&self) -> char {
        if self.is_at_end() {
            '\0'
        } else {
            self.source.chars().nth(self.current).unwrap()
        }
    }

    fn peek_next(&self) -> char {
        if self.current + 1 >= self.source.len() {
            '\0'
        } else {
            self.source.chars().nth(self.current + 1).unwrap()
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
        self.tokens.push(Token::new(token_type.clone(), text, literal, self.line));
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
    fn test_some_string(){
        let mut tokenizer = Tokenizer::new("
        let x = db.TABLES.b.filter(); 
        let y = db.TABLES.x ; 
        let z = x JOIN y on x.id=y.id;  x.select(a,b,c,d);
        ");
        let tokens = tokenizer.scan_tokens().unwrap();
        println!("Tokens: {:?}", tokens);
        ()
    }
}