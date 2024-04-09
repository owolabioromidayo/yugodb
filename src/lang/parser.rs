use std::vec::Vec;
use crate::lang::types::*; 
use crate::lang::tokenizer::*; 
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
        if self.match_token(TokenType::Let) {
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
        // self.consume(TokenType::Let, "Expect 'let' in variable declaration");

        let name = self.consume(TokenType::Variable, "Expect variable name.");

        let initializer = if self.match_token(TokenType::Equal) {
            self.data_expr()
        } else {
            self.error(self.peek(), "Expected some var initializer");
        };
        self.consume(TokenType::Semicolon, "Expect ';' after variable declaration.");
        Stmt::Var(VarStmt { name, initializer })
    }

    fn statement(&mut self) -> Stmt {
       
        let left = self.data_expr();
        self.consume(TokenType::Semicolon, "Expect ';' after value. ");

        Stmt::Expression(ExprStmt { expression: left })

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
        if self.check(TokenType::Semicolon) {
            return left;
        } else {
            //theres more
            if self.check(TokenType::Join) || self.check(TokenType::Ljoin) {
                let join = self.advance().clone();
                let right = self.data_call_expr();

                if self.check(TokenType::On) {
                   self.consume(TokenType::On, "Expected ON operator" );
                   let join_expr = self.expression();
                   
                   Expr::DataExpr(DataExpr{
                        left : Box::new(left),
                        right: Box::new(right),
                        join: join,
                        join_expr: Box::new(join_expr)
                   }) 
                } else {
                    self.error(self.peek(), "Expected ON operator");
                }
            } else {
                self.error(self.peek(), "Expected JOIN or LJOIN expr");
            }

        }
        // self.consume(TokenType::Semicolon, "Expect ';' after value. ");
        // Stmt::Expression(ExprStmt { expr })
    }
    
    fn data_call_expr(&mut self) -> Expr {
        

        if self.check(TokenType::Variable) {
            // let left = self.consume(TokenType::Variable, "Expected a variable expr")
           // variable is variable, nothing special
           //make it a variable? (fix literal first)

            self.expression()
        
        } else {
            //TODO: realistically, variables should be grouped under attributes
            //nvm, it cant be a variable at this point because of the method call, thats illegal

            //parse out the attr and the method
            //TODO: make variables attributes

            println!("{:?}", self.peek());
            let left =  self.consume(TokenType::Variable, "Expected a variable"); 

            if self.peek()._type == TokenType::Semicolon {
                return Expr::Variable(Variable { name: left })
            } 

            let mut a = Vec::new(); 
            a.push(left);
            self.consume(TokenType::Dot, "Expected a Dot");

            while !self.check(TokenType::LeftParen) { 
                a.push(self.consume(TokenType::Variable, "Expected a var")); 
                if self.check(TokenType::Dot){
                    self.consume(TokenType::Dot, "Expected a Dot");
                }
            }
            //join everything inside an attr
            let new_method = a.pop().unwrap(); 

            //create the method type
            self.consume(TokenType::LeftParen, "Expected '(' for method"); 
                //now we parse the arguments
            let arguments = self.arguments();

            Expr::DataCall( DataCall { attr: a, method: new_method ,  arguments: arguments } ) 
            //check if method (not needed)
            // if self.check(TokenType::Method) {
            //     let m = self.consume(TokenType::Method, "Method should be consumed herer");

            //     self.consume(TokenType::LeftParen, "Expected '(' for method"); 
            //         //now we parse the arguments
            //     let arguments = self.arguments();

            //     Expr::DataCall( DataCall { attr: left, arguments: arguments } ) 
                       
            // } else {
            //      Expr::Variable(Variable { name: left })
            // }

        

        }
        
    }

    fn arguments(&mut self) -> Vec<Expr> {

        let mut arguments = Vec::new();
        if !self.check(TokenType::RightParen) {
            loop {
                if arguments.len() >= 255 { // dk about this fam
                    self.error(self.peek(), "Can't have more than 255 arguments.");
                }
                arguments.push(self.expression());
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
        }
        let paren = self.consume(TokenType::RightParen, "Expect ')' after arguments.");
        // match callee {
        //     Expr::Variable(Variable { name }) => Expr::Call(Call { callee: name, paren, arguments }),
        //     _ => unreachable!(),
        // }
        arguments
    }
    // fn print_statement(&mut self) -> Stmt {
    //     let expr = self.expression();
    //     self.consume(TokenType::SEMICOLON, "Expect ';' after value. ");
    //     Stmt::Print(PrintStmt { expr })
    // }

    // fn expression_statement(&mut self) -> Stmt {
    //     let expr = self.expression();
    //     self.consume(TokenType::Semicolon, "Expect ';' after value. ");
    //     // Stmt::Expression(ExprStmt { expr })
    // }



     /// ALL EXPRESSION STUFF
     /// 
    
    fn expression(&mut self) -> Expr {
        return self.or();
    }


    // fn assignment(&mut self) -> Expr {
    //     let expr = self.or();

    //     if self.match_token(TokenType::EQUAL) {
    //         let equals = self.previous();
    //         let value = self.assignment();

    //         if let Expr::Variable(Variable { name }) = expr {
    //             return Expr::Assign(Assign { name, value: Box::new(value) });
    //         }

    //         self.error(equals, "Invalid assignment target");
    //     }
    //     expr
    // }

    fn or(&mut self) -> Expr {
        let mut expr = self.and();

        while self.match_token(TokenType::Or) {
            let operator = self.previous().clone();
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

        while self.match_token(TokenType::And) {
            let operator = self.previous().clone();
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

        // SQL doesnt use == 
        while self.match_token_types(&[TokenType::NotEqual, TokenType::Equal]) {
            let operator = self.previous().clone();
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

        while self.match_token_types(&[TokenType::Greater, TokenType::GreaterEqual, TokenType::Less, TokenType::LessEqual]) {
            let operator = self.previous().clone();
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

        while self.match_token_types(&[TokenType::Minus, TokenType::Plus]) {
            let operator = self.previous().clone();
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

        while self.match_token_types(&[TokenType::Slash, TokenType::Asterisk]) {
            let operator = self.previous().clone();
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
        if self.match_token_types(&[TokenType::Bang, TokenType::Minus]) {
            let operator = self.previous().clone();
            let right = self.unary();
            Expr::Unary(Unary {
                operator,
                right: Box::new(right),
            })
        } else {
            // self.call()
            return self.primary();
            // self.error(self.peek(), "Expected unary expr");
        }
    }

    // fn call(&mut self) -> Expr {
    //     let mut expr = self.primary();

    //     loop {
    //         if self.match_token(TokenType::LEFT_PAREN) {
    //             expr = self.finish_call(expr);
    //         } else {
    //             break;
    //         }
    //     }

    //     expr
    // }

    fn primary(&mut self) -> Expr {
        if self.match_token(TokenType::False) {
            return Expr::Literal(Literal::new( Value::new_bool(false), TokenType::Boolean)) ;
        }
        if self.match_token(TokenType::True) {
            return Expr::Literal(Literal::new( Value::new_bool(true), TokenType::Boolean)) ;
        }
        if self.match_token(TokenType::Null) {
            return Expr::Literal(Literal::new( Value::new_nil(), TokenType::Null)) ;
        }
        //TODO
        // if self.match_token(TokenType::Attribute) {
        //     return Expr::Attribute(Variable { tokens: self.previous().clone() });
        // }
        if self.match_token(TokenType::Variable) {
            return Expr::Variable(Variable { name: self.previous() .clone()});
        }

        if self.match_token_types(&[TokenType::Number, TokenType::String]) {
    
            return Expr::Literal(match self.previous()._type {
                TokenType::Number => {
                    if let Some(x) = self.previous().literal.clone() {
                        Literal::new(Value::new_number(x.parse().unwrap()), TokenType::Number)
                    } else {
                        Literal::new(Value::new_number(0 as f64), TokenType::Number)
                    }
                }
                TokenType::String => {
                    if let Some(x) = self.previous().literal.clone() {
                        Literal::new(Value::new_string(x), TokenType::String)
                    } else {
                        Literal::new(Value::new_string("".to_string()), TokenType::String)
                    }
                }
                _ => unreachable!(),
            });
        }

        if self.match_token(TokenType::LeftParen) {
            let expr = self.expression();
            self.consume(TokenType::RightParen, "Expect ')' after expression");
            Expr::Grouping(Grouping {
                expression: Box::new(expr),
            })
        } else {
            self.error(self.peek(), "Expect expression.")
        }
    }


    fn is_at_end(&self) -> bool {
        self.peek()._type == TokenType::Eof
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
            self.peek()._type == token_type
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
        println!("{:?}", self.peek());
        panic!("[line {}] Error{}: {}", token.line, if token._type == TokenType::Eof { " at end" } else { "" }, message)
    }

    // fn synchronize(&mut self) {
    //     self.advance();

    //     while !self.is_at_end() {
    //         if self.previous().token_type == TokenType::SEMICOLON {
    //             return;
    //         }

    //         match self.peek().token_type {
    //             TokenType::CLASS | TokenType::FUN | TokenType::VAR |
    //             TokenType::FOR | TokenType::IF | TokenType::WHILE |
    //             TokenType::PRINT | TokenType::RETURN => return,
    //             _ => {}
    //         }

    //         self.advance();
    //     }
    // }
}

#[cfg(test)]
mod tests {
    use super::*;

    //need to tests this a lot, will come back

    #[test]
    fn test_some_string(){
        // let mut tokenizer = Tokenizer::new("
        // let x = db.TABLES.b.filter(); 
        // let y = db.TABLES.x ; 
        // x.filter(); 
        // let z = x JOIN y on x.id=y.id;  x.select(a,b,c,d);
        // ");
        let mut tokenizer = Tokenizer::new("
        let x = db.TABLES.b.filter();
        ");
        let tokens = tokenizer.scan_tokens().unwrap();
        println!("Tokens: {:?}", tokens);
        let mut tree = Parser::new(tokens);
        let statements = tree.parse();
        println!("\n\n\n Statements: {:?}", statements);
        ()
    }
}