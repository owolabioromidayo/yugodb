use std::vec::Vec;
use crate::lang::types::*; 
// use crate::token::{Token, TokenType};
// use crate::expr::{Expr, Literal, Variable, Grouping, Unary, Binary, Logical, Call, Assign};
// use crate::stmt::{Stmt, ExprStmt, PrintStmt, VarStmt, BlockStmt, IfStmt, WhileStmt, FunctionStmt, ReturnStmt};

pub struct Parser {
    tokens: Vec<Token>,
    current: usize,
}

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Parser {
        Parser { tokens, current: 0 }
    }

    pub fn parse(&mut self) -> Vec<Stmt> {
        let mut statements = Vec::new();
        while !self.is_at_end() {
            statements.push(self.declaration());
        }
        statements
    }

    fn declaration(&mut self) -> Stmt {
        if self.match_token(TokenType::VAR) {
            self.var_declaration()
        // } else if self.match_token(TokenType::FUN) {
        //     self.fun_declaration("function")
        } else {
            self.statement()
        }
    }

    //TODO: we might end up needing this for our method calls

    // fn fun_declaration(&mut self, kind: &str) -> Stmt {
    //     let name = self.consume(TokenType::IDENTIFIER, &format!("Expect {} name.", kind));

    //     self.consume(TokenType::LEFT_PAREN, &format!("Expect '(' after {} name.", kind));
    //     let mut parameters = Vec::new();
    //     if !self.check(TokenType::RIGHT_PAREN) {
    //         loop {
    //             if parameters.len() >= 255 {
    //                 self.error(self.peek(), "Can't have more than 255 parameters.");
    //             }
    //             parameters.push(self.consume(TokenType::IDENTIFIER, "Expect parameter name."));
    //             if !self.match_token(TokenType::COMMA) {
    //                 break;
    //             }
    //         }
    //     }
    //     self.consume(TokenType::RIGHT_PAREN, "Expect ')' after parameters.");

    //     self.consume(TokenType::LEFT_BRACE, &format!("Expect '{{' before {} body.", kind));
    //     let body = match self.block() {
    //         Stmt::Block(statements) => statements,
    //         _ => unreachable!(),
    //     };
    //     Stmt::Function(FunctionStmt { name, parameters, body })
    // }

    fn var_declaration(&mut self) -> Stmt {
        self.consume(TokenType::Let, "Expect 'let' in variable declaration");

        let name = self.consume(TokenType::Variable, "Expect variable name.");

        let initializer = if self.match_token(TokenType::EQUAL) {
            Some(self.data_expr())
        } else {
            None
        };
        self.consume(TokenType::SEMICOLON, "Expect ';' after variable declaration.");
        Stmt::Var(VarStmt { name, initializer })
    }

    fn statement(&mut self) -> Stmt {
       
        let left = self.data_expr();
        self.consume(TokenType::Semicolon, "Expect ';' after value. ");


    }

    // fn return_statement(&mut self) -> Stmt {
    //     let keyword = self.previous();
    //     let value = if !self.check(TokenType::SEMICOLON) {
    //         Some(self.expression())
    //     } else {
    //         None
    //     };
    //     self.consume(TokenType::SEMICOLON, "Expect ';' after return value.");
    //     Stmt::Return(ReturnStmt { keyword, value })
    // }


    fn data_expr(&mut self) -> Expr {
        let left = self.data_call_expr();
        // self.consume(TokenType::Semicolon, "Expect ';' after value. ");
        // Stmt::Expression(ExprStmt { expr })
    }
    
    fn data_call_expr(&mut self) -> Expr {
        
    }

    // fn print_statement(&mut self) -> Stmt {
    //     let expr = self.expression();
    //     self.consume(TokenType::SEMICOLON, "Expect ';' after value. ");
    //     Stmt::Print(PrintStmt { expr })
    // }

    fn expression_statement(&mut self) -> Stmt {
        let expr = self.expression();
        self.consume(TokenType::Semicolon, "Expect ';' after value. ");
        // Stmt::Expression(ExprStmt { expr })
    }

    fn expression(&mut self) -> Expr {
        let mut left = self.data_call();
        while self.match_token(TokenType::Join) || self.match_token(TokenType::LJoin) {

        } 
        //consume as amny of these as spossible
    }

    fn data_call(&mut self) -> Expr {

    }

    fn assignment(&mut self) -> Expr {
        let expr = self.or();

        if self.match_token(TokenType::EQUAL) {
            let equals = self.previous();
            let value = self.assignment();

            if let Expr::Variable(Variable { name }) = expr {
                return Expr::Assign(Assign { name, value: Box::new(value) });
            }

            self.error(equals, "Invalid assignment target");
        }
        expr
    }

    fn or(&mut self) -> Expr {
        let mut expr = self.and();

        while self.match_token(TokenType::OR) {
            let operator = self.previous();
            let right = self.and();
            expr = Expr::Logical(Logical {
                left: Box::new(expr),
                operator,
                right: Box::new(right),
            });
        }

        expr
    }

    fn and(&mut self) -> Expr {
        let mut expr = self.equality();

        while self.match_token(TokenType::AND) {
            let operator = self.previous();
            let right = self.equality();
            expr = Expr::Logical(Logical {
                left: Box::new(expr),
                operator,
                right: Box::new(right),
            });
        }

        expr
    }

    fn equality(&mut self) -> Expr {
        let mut expr = self.comparison();

        while self.match_token_types(&[TokenType::BANG_EQUAL, TokenType::EQUAL_EQUAL]) {
            let operator = self.previous();
            let right = self.comparison();
            expr = Expr::Binary(Binary {
                left: Box::new(expr),
                operator,
                right: Box::new(right),
            });
        }

        expr
    }

    fn comparison(&mut self) -> Expr {
        let mut expr = self.term();

        while self.match_token_types(&[TokenType::GREATER, TokenType::GREATER_EQUAL, TokenType::LESS, TokenType::LESS_EQUAL]) {
            let operator = self.previous();
            let right = self.term();
            expr = Expr::Binary(Binary {
                left: Box::new(expr),
                operator,
                right: Box::new(right),
            });
        }

        expr
    }

    fn term(&mut self) -> Expr {
        let mut expr = self.factor();

        while self.match_token_types(&[TokenType::MINUS, TokenType::PLUS]) {
            let operator = self.previous();
            let right = self.factor();
            expr = Expr::Binary(Binary {
                left: Box::new(expr),
                operator,
                right: Box::new(right),
            });
        }

        expr
    }

    fn factor(&mut self) -> Expr {
        let mut expr = self.unary();

        while self.match_token_types(&[TokenType::SLASH, TokenType::STAR]) {
            let operator = self.previous();
            let right = self.unary();
            expr = Expr::Binary(Binary {
                left: Box::new(expr),
                operator,
                right: Box::new(right),
            });
        }

        expr
    }

    fn unary(&mut self) -> Expr {
        if self.match_token_types(&[TokenType::BANG, TokenType::MINUS]) {
            let operator = self.previous();
            let right = self.unary();
            Expr::Unary(Unary {
                operator,
                right: Box::new(right),
            })
        } else {
            self.call()
        }
    }

    fn call(&mut self) -> Expr {
        let mut expr = self.primary();

        loop {
            if self.match_token(TokenType::LEFT_PAREN) {
                expr = self.finish_call(expr);
            } else {
                break;
            }
        }

        expr
    }

    fn primary(&mut self) -> Expr {
        if self.match_token(TokenType::FALSE) {
            return Expr::Literal(Literal::Boolean(false));
        }
        if self.match_token(TokenType::TRUE) {
            return Expr::Literal(Literal::Boolean(true));
        }
        if self.match_token(TokenType::NIL) {
            return Expr::Literal(Literal::Nil);
        }
        if self.match_token(TokenType::IDENTIFIER) {
            return Expr::Variable(Variable { name: self.previous() });
        }

        if self.match_token_types(&[TokenType::NUMBER, TokenType::STRING]) {
            return Expr::Literal(match self.previous().token_type {
                TokenType::NUMBER => Literal::Number(self.previous().literal.parse().unwrap()),
                TokenType::STRING => Literal::String(self.previous().literal),
                _ => unreachable!(),
            });
        }

        if self.match_token(TokenType::LEFT_PAREN) {
            let expr = self.expression();
            self.consume(TokenType::RIGHT_PAREN, "Expect ')' after expression");
            Expr::Grouping(Grouping {
                expression: Box::new(expr),
            })
        } else {
            self.error(self.peek(), "Expect expression.")
        }
    }

    fn finish_call(&mut self, callee: Expr) -> Expr {
        let mut arguments = Vec::new();
        if !self.check(TokenType::RIGHT_PAREN) {
            loop {
                if arguments.len() >= 255 {
                    self.error(self.peek(), "Can't have more than 255 arguments.");
                }
                arguments.push(self.expression());
                if !self.match_token(TokenType::COMMA) {
                    break;
                }
            }
        }
        let paren = self.consume(TokenType::RIGHT_PAREN, "Expect ')' after arguments.");
        match callee {
            Expr::Variable(Variable { name }) => Expr::Call(Call { callee: name, paren, arguments }),
            _ => unreachable!(),
        }
    }

    fn is_at_end(&self) -> bool {
        self.peek().token_type == TokenType::EOF
    }

    fn peek(&self) -> &Token {
        &self.tokens[self.current]
    }

    fn previous(&self) -> &Token {
        &self.tokens[self.current - 1]
    }

    fn check(&self, token_type: TokenType) -> bool {
        if self.is_at_end() {
            false
        } else {
            self.peek().token_type == token_type
        }
    }

    fn advance(&mut self) -> &Token {
        if !self.is_at_end() {
            self.current += 1;
        }
        self.previous()
    }

    fn match_token(&mut self, token_type: TokenType) -> bool {
        if self.check(token_type) {
            self.advance();
            true
        } else {
            false
        }
    }

    fn match_token_types(&mut self, token_types: &[TokenType]) -> bool {
        for &token_type in token_types {
            if self.check(token_type) {
                self.advance();
                return true;
            }
        }
        false
    }

    fn consume(&mut self, token_type: TokenType, message: &str) -> Token {
        if self.check(token_type) {
            self.advance().clone()
        } else {
            self.error(self.peek(), message)
        }
    }

    fn error(&self, token: &Token, message: &str) -> ! {
        panic!("[line {}] Error{}: {}", token.line, if token.token_type == TokenType::EOF { " at end" } else { "" }, message)
    }

    fn synchronize(&mut self) {
        self.advance();

        while !self.is_at_end() {
            if self.previous().token_type == TokenType::SEMICOLON {
                return;
            }

            match self.peek().token_type {
                TokenType::CLASS | TokenType::FUN | TokenType::VAR |
                TokenType::FOR | TokenType::IF | TokenType::WHILE |
                TokenType::PRINT | TokenType::RETURN => return,
                _ => {}
            }

            self.advance();
        }
    }
}