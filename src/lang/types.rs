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

    Attribute,
    Variable, 
    Let,


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
    Boolean,
    Illegal,
}

#[derive(Debug, Clone)]
pub struct Token {
    _type : TokenType,
    lexeme : String,
    pub literal : Option<String>, 
    pub line : usize,
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
    pub fn to_string(&self) -> String {
        format!(
            "{:?} {} {}",
            self.token_type, self.lexeme, self.literal
        )
    }

}


use std::fmt;

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

pub struct Value {
    pub value_type: ValueType,
    pub value: ValueData,
}

pub enum ValueData {
    Number(f64),
    String(String),
    Bool(bool),
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
            ValueData::Callable(callable) => callable.to_string(),
        }
    }
}

pub trait ExprVisitor<T> {
    fn visit_binary(&mut self, expr: &Binary) -> T;
    fn visit_grouping(&mut self, expr: &Grouping) -> T;
    fn visit_literal(&mut self, expr: &Literal) -> T;
    fn visit_call_expr(&mut self, expr: &Call) -> T;
    fn visit_unary(&mut self, expr: &Unary) -> T;
    fn visit_variable(&mut self, expr: &Variable) -> T;
    fn visit_assign(&mut self, expr: &Assign) -> T;
    fn visit_logical_expr(&mut self, expr: &Logical) -> T;
}

pub trait StmtVisitor {
    fn visit_print_stmt(&mut self, stmt: &PrintStmt);
    fn visit_expr_stmt(&mut self, stmt: &ExprStmt);
    fn visit_var_stmt(&mut self, stmt: &VarStmt);
    // fn visit_block_stmt(&mut self, stmt: &BlockStmt);
    // fn visit_function_stmt(&mut self, stmt: &FunctionStmt);
    // fn visit_return_stmt(&mut self, stmt: &ReturnStmt);
}

pub enum Expr {
    Binary(Binary),
    Grouping(Grouping),
    Literal(Literal),
    Unary(Unary),
    Call(Call),
    Variable(Variable),
    Assign(Assign),
    Logical(Logical),
}

impl Expr {
    pub fn accept<T>(&self, visitor: &mut dyn ExprVisitor<T>) -> T {
        match self {
            Expr::Binary(expr) => visitor.visit_binary(expr),
            Expr::Grouping(expr) => visitor.visit_grouping(expr),
            Expr::Literal(expr) => visitor.visit_literal(expr),
            Expr::Unary(expr) => visitor.visit_unary(expr),
            Expr::Call(expr) => visitor.visit_call_expr(expr),
            Expr::Variable(expr) => visitor.visit_variable(expr),
            Expr::Assign(expr) => visitor.visit_assign(expr),
            Expr::Logical(expr) => visitor.visit_logical_expr(expr),
        }
    }
}

pub struct DataExpr {

}

pub struct DataCallExpr {

}
pub struct Binary {
    pub left: Box<Expr>,
    pub operator: Token,
    pub right: Box<Expr>,
}

pub struct Grouping {
    pub expression: Box<Expr>,
}

pub struct Literal {
    pub value: String,
    pub literal_type: TokenType,
}

pub struct Unary {
    pub operator: Token,
    pub right: Box<Expr>,
}

pub struct Call {
    pub callee: Box<Expr>,
    pub paren: Token,
    pub arguments: Vec<Expr>,
}

pub struct Variable {
    pub name: Token,
}

pub struct Assign {
    pub name: Token,
    pub value: Box<Expr>,
}

pub struct Logical {
    pub left: Box<Expr>,
    pub operator: Token,
    pub right: Box<Expr>,
}


pub enum Stmt {
    Expression(ExprStmt),
    Print(PrintStmt),
    Var(VarStmt),
    Block(BlockStmt),
    If(IfStmt),
    While(WhileStmt),
    Function(FunctionStmt),
    Return(ReturnStmt),
}

impl Stmt {
    pub fn accept(&self, visitor: &mut dyn StmtVisitor) {
        match self {
            Stmt::Expression(stmt) => visitor.visit_expr_stmt(stmt),
            Stmt::Print(stmt) => visitor.visit_print_stmt(stmt),
            Stmt::Var(stmt) => visitor.visit_var_stmt(stmt),
            Stmt::Block(stmt) => visitor.visit_block_stmt(stmt),
            Stmt::If(stmt) => visitor.visit_if_stmt(stmt),
            Stmt::While(stmt) => visitor.visit_while_stmt(stmt),
            Stmt::Function(stmt) => visitor.visit_function_stmt(stmt),
            Stmt::Return(stmt) => visitor.visit_return_stmt(stmt),
        }
    }
}

pub struct ExprStmt {
    pub expression: Expr,
}

pub struct PrintStmt {
    pub expression: Expr,
}

pub struct VarStmt {
    pub name: Token,
    pub initializer: Option<Expr>,
}

pub struct BlockStmt {
    pub statements: Vec<Stmt>,
}

pub struct IfStmt {
    pub condition: Expr,
    pub then_branch: Box<Stmt>,
    pub else_branch: Option<Box<Stmt>>,
}

pub struct WhileStmt {
    pub condition: Expr,
    pub body: Box<Stmt>,
}

pub struct FunctionStmt {
    pub name: Token,
    pub params: Vec<Token>,
    pub body: Vec<Stmt>,
}

pub struct ReturnStmt {
    pub keyword: Token,
    pub value: Option<Expr>,
}