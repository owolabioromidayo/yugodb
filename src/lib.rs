pub mod tokenizer;
pub mod pager;
pub mod error;
pub mod btree;




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