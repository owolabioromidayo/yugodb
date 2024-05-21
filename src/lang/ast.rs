use std::borrow::BorrowMut;
use std::hash::Hash;
use std::ops::Deref;
use std::vec::Vec;
use crate::lang::types::*; 
use crate::lang::tokenizer::*; 
use crate::lang::parser::*; 
use crate::error::*;
use std::collections::HashMap;

use crate::lang::typechecker::*;



#[derive(Debug, Clone, Copy)]
pub enum JoinType{
    LJoin,
    Join,
}

impl JoinType{
    fn from_token(token: &Token) -> Self {
        match token._type {
            TokenType::Ljoin => JoinType::LJoin, 
            TokenType::Join => JoinType::Join, 
            _ => panic!("Unsupported join type")
        }
    }
}

#[derive(Debug, Clone)]
pub struct Projection {
   pub expr: DataCall 
}

// #[derive(Debug, Clone)]
// pub struct Join { 
//     _type: JoinType,
//     predicate: Expr
// }

#[derive(Debug, Clone)]
pub struct Join { 
    // pub _type: JoinType,
    // pub predicate: Expr,
    pub dataexpr: DataExpr
}

#[derive(Debug, Clone)]
pub struct Source {
    pub source: Expr, // i think we should shift forcing into DataCAll forwards
}


// TODO : I do not believe this 
#[derive(Debug, Clone, Copy)]
pub enum NodeType {
    Projection,
    Source, // a datacall is automatically a root node
    Join, // Join or LJoin
    Variable, // variable expr

}

#[derive(Debug, Clone)]
pub enum NodeData {
    Projection(Projection),
    Source(Source),
    Join(Join),
    Variable(Box<Expr>)
}

#[derive(Debug, Clone)]
pub struct Node  {
    pub _type: NodeType,
    pub data: NodeData,
    pub children: Vec<Option<Node>>
}

pub struct AST{
    pub lookup_table : HashMap<String, Node>, //token.lexeme
    pub root: Option<Node>,
    pub processed_statements: Vec<Option<Node>>, // ordered linkeed list of process statement for  execution
    pub prev_map: HashMap<MethodType, Vec<MethodType>>
}


impl AST  {

    pub fn new()-> Self{
        AST {
            root: None,
            lookup_table: HashMap::new(), 
            processed_statements: Vec::new(),
            prev_map: HashMap::from([
                    (MethodType::OrderBy, vec![MethodType::Filter] ),
                    (MethodType::GroupBy, vec![MethodType::Filter] ),
                    (MethodType::Filter, vec![]),
                    (MethodType::Select, vec![MethodType::Filter]),
                    (MethodType::SelectDistinct, vec![MethodType::Filter]),
                    (MethodType::Offset, vec![MethodType::Filter, MethodType::OrderBy, MethodType::GroupBy] ),
                    (MethodType::Limit, vec![MethodType::Filter, MethodType::OrderBy, MethodType::GroupBy, MethodType::Offset] ),
                    (MethodType::Max, vec![MethodType::Filter, MethodType::GroupBy] ),
                    (MethodType::Min,  vec![MethodType::Filter, MethodType::GroupBy]),
                    (MethodType::Sum,  vec![MethodType::Filter, MethodType::GroupBy]),
                    (MethodType::Count,  vec![MethodType::Filter, MethodType::GroupBy]),
                    (MethodType::CountDistinct, vec![MethodType::Filter, MethodType::GroupBy]),
                    (MethodType::Illegal, vec![]),
                ])
        }
    }


    fn generate_from_expr(&self, expr: &Expr) -> Option<Node> {
        match expr {
            Expr::Attribute(attr) => {
            
                //TODO: no support for let x = a.b.c ; let y  = x.offset() yet
                
                Some(Node {
                    _type: NodeType::Source,
                    data: NodeData::Source(Source{source: Expr::Attribute(attr.clone())}),
                    children: Vec::new(),
                })
                // this shouldnt even be here
                // match self.lookup_table.get(&attr.tokens[0].lexeme.to_string()) {
                //     // need somewhere to store those extra transformations. 
                //     Some(x) => Some(x.clone()),
                //     None => None 
                // }
            }
            Expr::Variable(expr) => {
                match self.lookup_table.get(&expr.name.lexeme.to_string()) {
                    Some(x) => Some(x.clone()),
                    None => None 
                }
            }

            Expr::DataCall(expr) => { 


                // method chaining resolution should be done here
                if (expr.methods.len() > 1) { 
                    for i in 1..expr.methods.len() {
                        let curr = expr.methods[i];                         
                        let prev = expr.methods[i-1];       

                        println!("{:?}.{:?}", &prev, &curr);                   

                        if let Some(r) = self.prev_map.get(&curr) {
                            if !r.contains(&prev)  {
                                self.error(format!("{:?} method cannot precede {:?}.",&prev ,&curr  ).as_str()); 
                            }
                        } else {
                                self.error(format!("Precedence check for {:?} not implemented!",&curr).as_str()); 
                        }
                    }

                }
                             
                Some(Node {
                    _type: NodeType::Source, 
                        data : NodeData::Source((Source{source: Expr::DataCall((*expr).clone()) })),
                        children: Vec::new()
                    }) 


            }  
            Expr::DataExpr(expr) => { 
                //create a join node

                let left_node: Option<Node> = self.generate_from_expr(expr.left.as_ref());

                let right_node= self.generate_from_expr(expr.right.as_ref());


                Some(Node {
                    _type: NodeType::Join, 
                        // data : NodeData::Join((Join{_type : JoinType::from_token(&expr.join), predicate: *(expr.join_expr).clone(), dataexpr: (*expr).clone() })),
                        // children: vec![left_node, right_node]
                        data : NodeData::Join((Join{dataexpr: (*expr).clone() })),
                        children: vec![]
                })

            }      

            _ =>  { 
                println!("{:?}", expr);
                None
            }              
        }


    }

    fn generate_from_stmt(&mut self, statement: &Stmt) {
            match statement{
                Stmt::Var(_) => {
                    // do nothing here, it has been handled already                    
                    self.processed_statements.push(None); 
                }
                Stmt::Expression(stmt) =>  {
                    self.processed_statements.push(self.generate_from_expr(&stmt.expression));

                }
                Stmt::Print(_) => {
                    // TODO: impl print statement support?
                    self.processed_statements.push(None);
                }

            }
    }

    /// Generate the AST from a list of statements
    pub fn generate(&mut self, mut statements: Vec<Stmt>){

        if statements.len() == 0 {
            self.error("Cannot generate AST from an empty list!");
        }
        
        // i think we need a prelim forward pass to get all the variable arguments, then we can make nodes out of 
        for statement in &statements {
            match statement {
                Stmt::Var(varstmt) => {

                    // this will update the old value if any, can reconsider later
                    // we shouldnt allow this, harder to maintain in this backwards traversal

                    if self.lookup_table.contains_key(&varstmt.name.lexeme){
                            self.error("Cannot define the same variable twice!");
                    }
                    let expr = &varstmt.initializer;
                    // we might need to recursively walk through this definition and break it down?, NO , job for the visitor
                    if let Some(fexpr) = self.generate_from_expr(expr) {
                       self.lookup_table.insert(varstmt.name.lexeme.clone(), fexpr); 
                    }
                    ()
                }
                _ => ()
            }
        }

        //we need to ensure the last statement is some projection
        let projection = statements.pop().unwrap();

        match projection {
            Stmt::Expression(expr) => {
                match expr.expression {
                    Expr::DataCall(x) => {
                        self.root = Some(
                            Node {
                            _type: NodeType::Projection, 
                                data : NodeData::Projection((Projection{expr: x})),
                                children: Vec::new()
                            }
                        ); 
                    },
                    _ => {
                        self.error("Final statement in query must be an projection expression!");
                    }
                }

             }
             _ => {
                self.error("Final statement in query must be an projection expression!");
             }
        }

        
        //top down (reversed stmt order), might be easier to handle?
        statements.reverse();

        for statement in statements {
            self.generate_from_stmt(&statement);
        }
    
        //here we want to look ahead and add children as required 
        // TOOD : investigate this further
        if self.processed_statements.len() > 1 { 
            for idx in 0..self.processed_statements.len() -2 {
                let next = self.processed_statements[idx +1].clone();
                let mut node = &mut (self.processed_statements[idx]); 

                match &mut node {
                    &mut Some (ref mut x) => { 
                        x.children.push(next)
                    }
                    None => ()
                    }
            }
        }

        if let Some(ref mut root ) = &mut self.root {
            root.children.push(self.processed_statements[0].clone()); // expensive
        }

    }



    fn error(&self, message: &str) -> ! {
        panic!("{}", message);
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_some_string(){
        let mut tokenizer = Tokenizer::new("
        let x = db.TABLES.b.filter().orderby(); 
        let y = db.TABLES.x ; 
        x.filter(); 
        let z = x JOIN y ON id;  
        z.select(a,b,c,d) ;
        ");

        let tokens = tokenizer.scan_tokens().unwrap();
        println!("Tokens: {:?}", tokens);
        let mut tree = Parser::new(tokens);
        let statements = tree.parse();
        println!("\n\n\n Statements: {:?}", statements);

        let mut ast = AST::new();
        ast.generate(statements);
        println!("\n\n\n Root: {:?}", ast.root);
        println!("\n\n\n AST Lookup Table: {:?}", ast.lookup_table);
        println!("\n\n\n AST Processed: {:?}", ast.processed_statements);

    }
}