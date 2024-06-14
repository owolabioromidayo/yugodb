use crate::error::*;
use crate::lang::tokenizer::*;
use crate::lang::types::*;
use crate::record::*;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::vec::Vec;

pub fn parse_json_to_document_record(json: &str) -> Result<DocumentRecord> {
    let mut jsona = json.replace("'", "\"");
    jsona.trim();
    // jsona.pop();
    println!("{}c", jsona);
    let fields: HashMap<String, serde_json::Value> = serde_json::from_str(jsona.as_str())?;

    let document_fields: HashMap<String, DocumentValue> = fields
        .into_iter()
        .map(|(key, value)| (key, parse_json_value_to_document_value(value)))
        .collect();

    Ok(DocumentRecord {
        //if ID was given, take it
        id: match document_fields.get("id") {
            Some(x) => match x {
                DocumentValue::Number(y) => Some(*y as usize),
                _ => None,
            },
            None => None,
        },
        fields: document_fields,
    })
}

fn parse_json_value_to_document_value(value: serde_json::Value) -> DocumentValue {
    match value {
        serde_json::Value::Null => DocumentValue::Null,
        serde_json::Value::Bool(val) => DocumentValue::Boolean(val),
        serde_json::Value::Number(val) => DocumentValue::Number(val.as_f64().unwrap()),
        serde_json::Value::String(val) => {
            if val.ends_with('D') || val.ends_with('d') {
                if let Ok(numeric_val) = Decimal::from_str(&val[..val.len() - 1]) {
                    DocumentValue::Numeric(numeric_val)
                } else {
                    DocumentValue::String(val)
                }
            } else {
                DocumentValue::String(val)
            }
        }
        serde_json::Value::Array(arr) => {
            let document_arr = arr
                .into_iter()
                .map(parse_json_value_to_document_value)
                .collect();
            DocumentValue::Array(document_arr)
        }
        serde_json::Value::Object(obj) => {
            let document_obj = obj
                .into_iter()
                .map(|(k, v)| (k, parse_json_value_to_document_value(v)))
                .collect();
            DocumentValue::Object(document_obj)
        }
    }
}

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
        self.consume(
            TokenType::Semicolon,
            "Expect ';' after variable declaration.",
        );
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
        println!("\n\n LEft {:?}", left);
        if self.check(TokenType::Semicolon) {
            return left;
        } else {
            //theres more
            if self.check(TokenType::Join) || self.check(TokenType::Ljoin) {
                let join = self.advance().clone();
                let right = self.data_call_expr();

                if self.check(TokenType::On) {
                    self.consume(TokenType::On, "Expected ON operator");
                    let join_expr = self.expression();

                    Expr::DataExpr(DataExpr {
                        left: Box::new(left),
                        right: Box::new(right),
                        join: join,
                        join_expr: Box::new(join_expr),
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
        // this mustnt be working.
        // the left should be some variable either ways
        if let Ok(token) = self.peek_ahead(1) {
            //if its just a semicolon that is after, its a variable for sure
            if token._type == TokenType::Semicolon {
                return self.expression();
            }
        }

        // what about attrs, and methods, lets do that now ?
        //TODO: realistically, variables should be grouped under attributes
        //nvm, it cant be a variable at this point because of the method call, thats illegal

        //parse out the attr and the method
        //TODO: make variables attributes

        // let left =  self.consume(TokenType::Variable, "Expected a variable");

        // we need to ddo this as many times as there are methods (apply multiple)

        // just while theres a dote

        let mut a = Vec::new();
        // a.push(left);
        // self.consume(TokenType::Dot, "Expected a Dot");

        while !self.check_token_types(&[
            TokenType::LeftParen,
            TokenType::Semicolon,
            TokenType::Ljoin,
            TokenType::Join,
            TokenType::On,
        ]) {
            a.push(self.consume(TokenType::Variable, "Expected a variable"));
            if self.check(TokenType::Dot) {
                self.consume(TokenType::Dot, "Expected a dot");
            }
        }

        if !self.check(TokenType::LeftParen) {
            // is an attribute, early exit
            return Expr::Attribute(Attribute { tokens: a });
        }

        // otherwise, there are methods to be parsed

        let new_method = MethodType::new(a.last().unwrap());
        if new_method == MethodType::Illegal {
            self.error(&a.pop().unwrap(), "Unsupported method call");
        }

        a.pop();
        let mut datacall = DataCall {
            attr: Attribute { tokens: a },
            methods: Vec::new(),
            arguments: Vec::new(),
        };

        // its here we need to parse multiple methods
        if self.check(TokenType::LeftParen) {
            self.consume(TokenType::LeftParen, "Expected '(' for method");
            //now we parse the arguments
            let arguments = self.arguments();

            // Expr::DataCall( DataCall { attr: a, method: new_method ,  arguments: arguments } )
            datacall.methods.push(new_method);
            datacall.arguments.push(arguments);
        }

        while self.check(TokenType::Dot) {
            self.consume(TokenType::Dot, "Expected '.' here.");

            let token = self.consume(TokenType::Variable, "Expected a method name here.");
            let method = MethodType::new(&token);
            if method == MethodType::Illegal {
                self.error(&token, "Unsupported method call");
            }
            // convert this to a methodenum

            self.consume(TokenType::LeftParen, "Expected '(' for method");
            //now we parse the arguments
            let arguments = self.arguments();

            datacall.methods.push(method);
            datacall.arguments.push(arguments);
        }

        Expr::DataCall(datacall)
        //parse potential successive methods

        // its just an attr

        //create the method type
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

    fn arguments(&mut self) -> Vec<Expr> {
        let mut arguments = Vec::new();
        if !self.check(TokenType::RightParen) {
            loop {
                if arguments.len() >= 255 {
                    // dk about this fam
                    self.error(self.peek(), "Can't have more than 255 arguments.");
                }
                arguments.push(self.expression());
                if !self.match_token(TokenType::Comma) {
                    break;
                }
            }
        }
        self.consume(TokenType::RightParen, "Expect ')' after arguments.");
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

        while self.match_token_types(&[
            TokenType::Greater,
            TokenType::GreaterEqual,
            TokenType::Less,
            TokenType::LessEqual,
        ]) {
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
            return Expr::Literal(Literal::new(Value::new_bool(false), TokenType::Boolean));
        }
        if self.match_token(TokenType::True) {
            return Expr::Literal(Literal::new(Value::new_bool(true), TokenType::Boolean));
        }
        if self.match_token(TokenType::Null) {
            return Expr::Literal(Literal::new(Value::new_nil(), TokenType::Null));
        }
        //TODO
        // if self.match_token(TokenType::Attribute) {
        //     return Expr::Attribute(Variable { tokens: self.previous().clone() });
        // }
        if self.match_token(TokenType::Variable) {
            return Expr::Variable(Variable {
                name: self.previous().clone(),
            });
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

    fn peek_ahead(&self, n: usize) -> Result<&Token> {
        if self.current + n >= self.tokens.len() {
            Err(Error::AccessError) //TODO: should be indexerror
        } else {
            Ok(&self.tokens[self.current + n])
        }
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

    fn check_token_types(&self, token_types: &[TokenType]) -> bool {
        if self.is_at_end() {
            return false;
        }
        for &token_type in token_types {
            if self.peek()._type == token_type {
                return true;
            }
        }
        false
    }

    fn advance(&mut self) -> &Token {
        if !self.is_at_end() {
            self.current += 1;
        }
        self.previous()
    }

    // fn rewind(&mut self) -> bool {
    //     if self.current > 0 {
    //         self.current -= 1;
    //         true
    //     } else {
    //         false
    //     }

    // }

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
        // println!("{:?}", self.statements);

        panic!(
            "[line {}] Error{}: {}",
            token.line,
            if token._type == TokenType::Eof {
                " at end"
            } else {
                ""
            },
            message
        )
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

//TODO: we need to support assignment operations for join expressions

#[cfg(test)]
mod tests {
    use super::*;

    //need to tests this a lot, will come back

    #[test]
    fn test_some_string() {
        let mut tokenizer = Tokenizer::new(
            "
        let x = db.TABLES.b.filter(); 
        let y = db.TABLES.x.filter() ; 
        let z = x JOIN y ON id;  
        z.select(a,b,c,d) ;
        ",
        );
        // let mut tokenizer = Tokenizer::new("
        // let x = db.TABLES.b.filter().orderby();
        // ");
        let tokens = tokenizer.scan_tokens().unwrap();
        println!("Tokens: {:?}", tokens);
        let mut tree = Parser::new(tokens);
        let statements = tree.parse();
        println!("\n\n\n Statements: {:?}", statements);
        ()
    }

    #[test]
    fn test_parse_json_to_document_record() {
        let json = r#"{"active": true, "name": "John", "age": 30, "balance": "1000.0D"}"#;

        let document_record = parse_json_to_document_record(json).unwrap();
        println!("Parsed DocumentRecord: {:?}", document_record);
    }

    #[test]
    fn test_parse_json_with_nested_objects() {
        // let json = r#"{
        //     "user": {
        //         "name": "Alice",
        //         "age": 25,
        //         "address": {
        //             "street": "123 Main St",
        //             "city": "New York"
        //         }
        //     },
        //     "products": [
        //         {
        //             "name": "Phone",
        //             "price": 999.99
        //         },
        //         {
        //             "name": "Laptop",
        //             "price": 1500
        //         }
        //     ]
        // }"#;

        let json2 = "{ 
                    'id': 0,
                    'name': 'John Doe',
                    'age': 30.0,
                    'city': 'New York',
                    'address': {
                        'street': '123 Main St',
                        'zip': '10001'
                    },
                    'phone_numbers': [
                        '123-456-7890',
                        '987-654-3210'
                    ]
            }'";

        let expected = DocumentRecord {
            id: None,
            fields: HashMap::from([
                (
                    "user".to_string(),
                    DocumentValue::Object(HashMap::from([
                        (
                            "name".to_string(),
                            DocumentValue::String("Alice".to_string()),
                        ),
                        ("age".to_string(), DocumentValue::Number(25.0)),
                        (
                            "address".to_string(),
                            DocumentValue::Object(HashMap::from([
                                (
                                    "street".to_string(),
                                    DocumentValue::String("123 Main St".to_string()),
                                ),
                                (
                                    "city".to_string(),
                                    DocumentValue::String("New York".to_string()),
                                ),
                            ])),
                        ),
                    ])),
                ),
                (
                    "products".to_string(),
                    DocumentValue::Array(vec![
                        DocumentValue::Object(HashMap::from([
                            (
                                "name".to_string(),
                                DocumentValue::String("Phone".to_string()),
                            ),
                            ("price".to_string(), DocumentValue::Number(999.99)),
                        ])),
                        DocumentValue::Object(HashMap::from([
                            (
                                "name".to_string(),
                                DocumentValue::String("Laptop".to_string()),
                            ),
                            ("price".to_string(), DocumentValue::Number(1500.0)),
                        ])),
                    ]),
                ),
            ]),
        };

        let result = parse_json_to_document_record(json2).unwrap();
        // assert_eq!(result, expected);
        println!("Parsed DocumentRecord: {:?}", result);
    }
}
