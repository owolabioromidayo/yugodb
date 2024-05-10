use std::collections::HashMap;
use std::fmt;

#[derive(Hash, Eq, Debug, PartialEq, Clone, Copy)]
pub enum TokenType {
    // Keywords
    Create,
    Delete,
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

    Join,
    Ljoin,

    //most of these things have not been introduced into the tokenizer
    Attribute,
    Variable,
    Method, // we can tokenize this right?
    Let,

    Eof,
    Or,
    And,

    Slash,
    Star,


    // Symbols
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
    Boolean,

    True,
    False,

    Illegal,
}

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub enum MethodType{ 
    OrderBy,  
    GroupBy, 
    Filter, 
    Select, 
    SelectDistinct,
    Offset, 
    Limit, 
    Max, 
    Min, 
    Sum, 
    Count, 
    CountDistinct,
    Illegal
}


#[derive(Hash, Eq, Debug, PartialEq, Clone)]
pub struct Token {
    pub _type : TokenType,
    pub lexeme : String,
    pub literal : Option<String>, 
    pub line : usize,
} 

impl Token {
    pub fn new(token_type: TokenType, text: String, literal: Option<String>, line: usize) -> Token {
        Token {
            _type: token_type,
            lexeme: text, 
            literal,
            line
        }
    }
    pub fn to_string(&self) -> String {
        format!(
            "{:?} {} {:?}",
            self._type, self.lexeme, self.literal
        )
    }

}

impl MethodType {
    pub fn new(token : &Token) -> Self {
        match token.lexeme.as_str() {
            "orderby" => MethodType::OrderBy,
            "groupby" => MethodType::GroupBy,
            "filter" => MethodType::Filter,
            "select" => MethodType::Select,
            "select_distinct" => MethodType::SelectDistinct,
            "offset" => MethodType::Offset,
            "limit" => MethodType::Limit,
            "max" => MethodType::Max,
            "min" => MethodType::Min,
            "sum" => MethodType::Sum,
            "count" => MethodType::Count,
            "count_distinct" => MethodType::CountDistinct,
            _ => MethodType::Illegal
            
        }
    }
}


#[derive(Debug, Clone, Copy, PartialEq)]



pub enum ValueType {
    String,
    Number,
    Nil,
    Boolean,
    Callable, 
}

impl fmt::Display for ValueType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValueType::String => write!(f, "string"),
            ValueType::Number => write!(f, "number"),
            ValueType::Nil => write!(f, "nil"),
            ValueType::Boolean => write!(f, "boolean"),
            ValueType::Callable => write!(f, "callable"),
        }
    }
}


#[derive(Debug, Clone)]
pub struct Value {
    pub value_type: ValueType,
    pub value: ValueData,
}


#[derive(Debug, Clone)]
pub enum ValueData {
    Number(f64),
    String(String),
    Bool(bool),
    //TODO: put our data types here? like NUmeric and Date and ID?
}

impl Value {
    pub fn new_number(value: f64) -> Self {
        Value {
            value_type: ValueType::Number,
            value: ValueData::Number(value),
        }
    }

    pub fn new_string(value: String) -> Self {
        Value {
            value_type: ValueType::String,
            value: ValueData::String(value),
        }
    }

    pub fn new_bool(value: bool) -> Self {
        Value {
            value_type: ValueType::Boolean,
            value: ValueData::Bool(value),
        }
    }

    pub fn new_nil() -> Self {
        Value {
            value_type: ValueType::Nil,
            value: ValueData::Bool(false),
        }
    }

    pub fn view(&self) -> String {
        match &self.value {
            ValueData::Number(number) => number.to_string(),
            ValueData::String(string) => string.clone(),
            ValueData::Bool(bool_) => bool_.to_string(),
            // ValueData::Callable(callable) => callable.to_string(),
        }
    }
}

pub trait ExprVisitor<T: ?Sized, U> {
    fn visit_binary(&mut self, expr: &Binary) -> U;
    fn visit_grouping(&mut self, expr: &Grouping) -> U;
    fn visit_literal(&mut self, expr: &Literal) -> U;
    fn visit_logical_expr(&mut self, expr: &Logical) -> U;
    fn visit_unary(&mut self, expr: &Unary) -> U;

    fn visit_variable(&mut self, expr: &Variable) -> T; //hmm
    fn visit_attribute(&mut self, expr: &Attribute) -> T;
    fn visit_assign(&mut self, expr: &Assign) -> T; // we dont need this
    fn visit_data_call(&mut self, expr: &DataCall) -> T;
    fn visit_data_expr(&mut self, expr: &DataExpr) -> T;
}

pub trait StmtVisitor<T> {
    fn visit_print_stmt(&mut self, stmt: &PrintStmt) -> ();
    fn visit_expr_stmt(&mut self, stmt: &ExprStmt) -> T;
    fn visit_var_stmt(&mut self, stmt: &VarStmt) -> ();
    // fn visit_block_stmt(&mut self, stmt: &BlockStmt);
    // fn visit_function_stmt(&mut self, stmt: &FunctionStmt);
    // fn visit_return_stmt(&mut self, stmt: &ReturnStmt);
}


#[derive(Debug, Clone)]
pub enum Expr {
    Binary(Binary),
    Grouping(Grouping),
    Literal(Literal),
    Unary(Unary),
    // Call(Call),
    Variable(Variable),
    Assign(Assign),
    Logical(Logical),
    DataCall(DataCall),
    DataExpr(DataExpr),
    Attribute(Attribute),
}




#[derive(Debug, Clone)]
pub struct DataExpr {
   pub left: Box<Expr>,
   pub right: Box<Expr>,
   pub join: Token,
   pub join_expr: Box<Expr>
}


#[derive(Debug, Clone)]
pub struct DataCall {
    pub attr: Attribute, // attr / left
    pub methods: Vec<MethodType>, // ordered method composition
    pub arguments: Vec<Vec<Expr>>,
}


#[derive(Debug, Clone)]
pub struct Binary {
    pub left: Box<Expr>,
    pub operator: Token,
    pub right: Box<Expr>,
}


#[derive(Debug, Clone)]
pub struct Grouping {
    pub expression: Box<Expr>,
}


#[derive(Debug, Clone)]
pub struct Literal {
    pub value: Value,
    pub literal_type: TokenType,
}


#[derive(Debug, Clone)]
pub struct Unary {
    pub operator: Token,
    pub right: Box<Expr>,
}


// #[derive(Debug, Clone)]
// pub struct Call {
//     pub callee: Box<Expr>,
//     pub paren: Token,
//     pub arguments: Vec<Expr>,
// }


#[derive(Debug, Clone)]
pub struct Variable {
    pub name: Token,
}

#[derive(Debug, Clone)]
pub struct Attribute{
    pub tokens : Vec<Token>
}

#[derive(Debug, Clone)]
pub struct Assign {
    pub name: Token,
    pub value: Box<Expr>,
}


#[derive(Debug, Clone)]
pub struct Logical {
    pub left: Box<Expr>,
    pub operator: Token,
    pub right: Box<Expr>,
}


#[derive(Debug)]
pub enum Stmt {
    Expression(ExprStmt),
    Print(PrintStmt),
    Var(VarStmt),
    // Block(BlockStmt),
    // If(IfStmt),
    // While(WhileStmt),
    // Function(FunctionStmt),
    // Return(ReturnStmt),
}

// impl Stmt {
//     pub fn accept(&self, visitor: &mut dyn StmtVisitor<T>) -> Option<T> {
//         match self {
//             Stmt::Expression(stmt) => visitor.visit_expr_stmt(stmt),
//             Stmt::Print(stmt) => visitor.visit_print_stmt(stmt),
//             Stmt::Var(stmt) => visitor.visit_var_stmt(stmt),

//             // would these ever be useful for lambdas perhaps
//             // Stmt::Block(stmt) => visitor.visit_block_stmt(stmt),
//             // Stmt::If(stmt) => visitor.visit_if_stmt(stmt),
//             // Stmt::While(stmt) => visitor.visit_while_stmt(stmt),
//             // Stmt::Function(stmt) => visitor.visit_function_stmt(stmt),
//             // Stmt::Return(stmt) => visitor.visit_return_stmt(stmt),
//         }
//     }
// }

impl Literal {
    pub fn new(value: Value, _type: TokenType) -> Self {
        Self { 
            value,
            literal_type: _type
        }
    }
}



#[derive(Debug)]
pub struct ExprStmt {
    pub expression: Expr,
}


#[derive(Debug)]
pub struct PrintStmt {
    pub expression: Expr,
}


#[derive(Debug)]
pub struct VarStmt {
    pub name: Token,
    pub initializer: Expr,
}


#[derive(Debug)]
pub struct BlockStmt {
    pub statements: Vec<Stmt>,
}


#[derive(Debug)]
pub struct IfStmt {
    pub condition: Expr,
    pub then_branch: Box<Stmt>,
    pub else_branch: Option<Box<Stmt>>,
}


#[derive(Debug)]
pub struct WhileStmt {
    pub condition: Expr,
    pub body: Box<Stmt>,
}


#[derive(Debug)]
pub struct FunctionStmt {
    pub name: Token,
    pub params: Vec<Token>,
    pub body: Vec<Stmt>,
}


#[derive(Debug)]
pub struct ReturnStmt {
    pub keyword: Token,
    pub value: Option<Expr>,
}