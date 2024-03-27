pub mod tokenizer;


use std::fmt;

#[derive(Debug, Clone)]
struct ScanError;

impl fmt::Display for ScanError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Invalid input string.")
    }
}


#[derive(Hash, Eq, Debug, PartialEq, Clone, Copy)]
pub enum TokenType {
    // Keywords
    As,
    Cast,
    Collate,
    Create,
    Delete,
    From,
    Index,
    Insert,
    Into,
    Key,
    Null,
    On,
    Primary,
    Select,
    Table,
    Values,
    Where,

    // Symbols
    Space,
    LeftParen,
    RightParen,
    Asterisk,
    Plus,
    Comma,
    Minus,
    Dot,
    Semicolon,
    Tilde,
    Bang,

    Equal,
    NotEqual,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
    BitOr,
    Concat,

    Number,
    String,

    // Literals
    // Identifier(MaybeQuotedBytes<'a>),
    // String(MaybeQuotedBytes<'a>),
    // // Blob(HexedBytes<'a>),
    // // Only contains 0-9 chars.
    // Integer(&'a [u8]),
    // Float(&'a [u8]),
    Illegal,
}

#[derive(Debug, Clone)]
struct Token {
    _type : TokenType,
    lexeme : String,
    literal : Option<String>, 
    line : usize,
} 

impl Token {
    fn new(token_type: TokenType, text: String, literal: Option<String>, line: usize) -> Token {
        Token {
            _type: token_type,
            lexeme: text, 
            literal,
            line
        }
    }

}