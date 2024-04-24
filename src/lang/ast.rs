/// Construct the AST given the parsed statements
/// 
/// So what do we want to do 
/// 
/// we need a new layer of abstractions basically, not just statements and calls
/// 
/// 
/// so very simply, the projection will have one child, as will the datasource and predicate
/// only the join will have 2 children.
/// 
/// now to fin some way to extract this structure from our IR
/// 
/// very simply, we go through each statement in order, keep some variable map, and construct the final IR in a forward fashion
/// 
/// there is still a bunch of stuff missing from all of this, like CREATE, DELETE, etc (nee some seperate way of addressing those,) they should be ignored
/// here. create can maybe be kept under adta source but its ultimately useless. we'll see
/// 
/// 
/// 
/// data sources are root nodes, like variables, they must be defined first
/// the transformations already applied to them must be kept in a vec to be optimize later
/// join expressions can reference these variables, as new childdren.
/// this would mean the predicates are kept inside the data sources vec and the join only sees the source
/// these joins would have to be stored in variables also, so they can be referencedd by successive operations
/// 
/// anything not stored in a variable that doesnt access a variable gets optimize away
/// successive transformations of a variable shoul be taken note of? why woul we filter A after joining A an B? THINK ABOUT THIS
/// 
/// the projection operation should reference only one child, which could be a join
/// ultimately, this should be easier as we will only have data sources and joins, with the predicates stored within and easily shuffleable
/// 
/// 
/// but what use would the projection be? root noe/ tie it together?
/// 
///
/// this shoul be easy to work on.
/// 
/// 
/// things like aggregate expressions and nested subqueries should be considered. think i forgot about those
/// 
/// also, we might need to check the type validity of the program at this stage
/// even validating method ordering might be something to be one at this stage
/// 
/// 
/// our projection pushdown is going to be very weak anyways, unless the final output is going to be very restricted, because we already have projection
/// functions in our transforms

// variable statements are just predefinitions, expressions might use variables, but we still need some final AST


// struct Predicate {

// }



//gonna need to clone expr for this one, or move it into something else cloneable


//TODO
// What is left here?
// We need to merge the variable decls into the tree -> figured out why
// we need to make the statements themselves into a tree
// is there really any model for this? lets just make each prev node child, like a linked list,
//the rest of the structure is buried inside datacall for now, until we handle all exprs I guess
// but getting it to execute first is the main goal


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

// struct Transform {

//     method: String, // preferrably an enum
//     arguments: Vec<Expr> // convert it to a Vec of Value Types
// }


// dont need this again, data call should be sufficient
// struct DataSource { 
//     tableName: String,
//     transforms: Vec<Transform>,
// }

// this is all we need in the tree
// so we need some sort of polymorphic walkable tree construct?


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
    expr: Expr
}

#[derive(Debug, Clone)]
pub struct Join { 
    _type: JoinType, //TODO: uniontypes or type definitions for this
    predicate: Expr
}


#[derive(Debug, Clone)]
pub struct Source {
    source: Expr, // i think we should shift forcing into DataCAll forwards
}

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
    _type: NodeType,
    data: NodeData,
    children: Vec<Option<Node>>
}

// impl Node <'a > {
//     // what do we want here?
// }




pub struct AST{
    pub lookup_table : HashMap<String, Node>, //token.lexeme
    pub root: Option<Node>,
    pub processed_statements: Vec<Option<Node>>,
}



impl AST  {

    // we might have to do some visitor pattern stuff here.
    pub fn new()-> Self{
        AST {
            root: None,
            lookup_table: HashMap::new(), 
            processed_statements: Vec::new()
        }
    }


    fn generate_from_expr(&self, expr: &Expr) -> Option<Node> {
        match expr {

            //TODO: we need to think about handling the lower levels

            //we need to do the visitor pattern thing for the lower levels
            // for now, lets look at higher stuff

            //do we have to match all the lower types? they have no bearing here (shoould be contained in either of these)

            Expr::Attribute(attr) => {

                // let lookup_str = (&attr.tokens).iter().map(|x| x.lexeme.to_string()).collect::<String>();

                //TODO: our lookup table needs support for attributes
                match self.lookup_table.get(&attr.tokens[0].lexeme.to_string()) {
                    // need somewhere to store those extra transformations. 
                    Some(x) => Some(x.clone()),
                    None => None 
                }
                // None
            }
            Expr::Variable(expr) => {
                match self.lookup_table.get(&expr.name.lexeme.to_string()) {
                    Some(x) => Some(x.clone()),
                    None => None 
                }
                // None
            }

            Expr::DataCall(expr) => { 
                // sweet, we have a base node

                // the problem is we are not going deep enough here
                // is it important that we do? i think we just need something runnable first. we have a lookup table, then
                //can come back and redesign this as needed


                // method chaining resolution would have to be done here too
                if expr.methods.len() == 0 {
                    // no chaining to be done
                        
                } else {
                
                    for i in 1..expr.methods.len() {

                    }

                }

                             

                Some(Node {
                    _type: NodeType::Source, 
                        data : NodeData::Source((Source{source: Expr::DataCall((*expr).clone()) })),
                        children: Vec::new()
                    }) 

                // add it as a child of curr
                //TODO: is this proper error handling?
                // curr.children.unwrap().push(new_node);


            }  
            Expr::DataExpr(expr) => { 
                //create a join node

                let left_node: Option<Node> = self.generate_from_expr(expr.left.as_ref());

                let right_node= self.generate_from_expr(expr.right.as_ref());

                // add it as a child of curr
                //TODO: is this proper error handling?

                Some(Node {
                    _type: NodeType::Join, 
                        data : NodeData::Join((Join{_type : JoinType::from_token(&expr.join), predicate: *(expr.join_expr).clone() })),
                        children: vec![left_node, right_node]
                })

                // curr.children.unwrap().push(join_node); 
            }      

            _ =>  { 
                println!("{:?}", expr);
                None
            }              
        }


    }

    fn generate_from_stmt(&mut self, statement: &Stmt) {
            match statement{
                Stmt::Var(stmt) => {
                    //create some new variable and assign it to some 
                    // do nothing here, it has been handled already?                    

                    self.processed_statements.push(None); 
                }
                Stmt::Expression(stmt) =>  {
                    //something to be evaluated on an existing variable
                    // so what do we do in this case?

                    // need to find the variable in use if any

                    //need to find out if its a join or just a datacall expr

                    self.processed_statements.push(self.generate_from_expr(&stmt.expression));

                }
                Stmt::Print(stmt) => {
                    self.processed_statements.push(None);
                }

            }
    }

    /// Generate the AST from a list of statements
    pub fn generate(&mut self, mut statements: Vec<Stmt>){

        // this is a very left to right and bottom to top kind of parsing.

        if statements.len() == 0 {
            self.error("Cannot generate AST from an empty list!");
        }
        
        // i think we need a prelim forward pass to get all the variable arguments, then we can make nodes out of 

        // TODO: need to preprocess all variables and ATTRS
        for statement in &statements {
            match statement {
                Stmt::Var(varstmt) => {

                    // this will update the old value if any, can reconsider later
                    // we shouldnt allow this, harder to maintain in this backwards traversal

                    if self.lookup_table.contains_key(&varstmt.name.lexeme){
                            self.error("Cannot define the same variable twice!");
                    }
                    let expr = &varstmt.initializer;
                    // we might need to recursively walk through this definition and break it down? 
                    if let Some(fexpr) = self.generate_from_expr(expr) {
                       self.lookup_table.insert(varstmt.name.lexeme.clone(), fexpr); 
                    }
                    ()
                }
                _ => ()
            }
        }

        println!("Variables: {:?} ", self.lookup_table);


        //we need to ensure the last statement is some projection, or we just create an empty one 
        let projection = statements.pop().unwrap();

        match projection {
            Stmt::Expression(expr) => {

                self.root = Some(
                     Node {
                       _type: NodeType::Projection, 
                        data : NodeData::Projection((Projection{expr: expr.expression})),
                        children: Vec::new()
                     }
                ); 
             }
             _ => {
                self.error("Final statement in query must be an expression!");
             }
        }

        

        //top down (reversed stmt order), might be easier to handle?
        statements.reverse();

        let mut processed_stmts: Vec<Option<Node>> = Vec::new();  
        // if let Some(curr) = &mut self.root {

        for statement in statements {
            self.generate_from_stmt(&statement);
            // processed_stmts.push(new_node);
            
            }
    
        //here we want to look ahead and add children as required 
        // let mut curr = self.root; 
        for idx in 0..self.processed_statements.len() -2 {
            let next = self.processed_statements[idx +1].clone();
            let mut node = &mut (self.processed_statements[idx]); 

            match &mut node {
                &mut Some (ref mut x) => { 
                    x.children.push(next.clone())
                }
                None => ()
                }
        }

        if let Some(ref mut curr) = &mut self.root {
            curr.children.push(self.processed_statements[0].clone()); // expensive
        }

            // if let  Some(mut x) = &mut *node.borrow_mut() {
                
            //     match x._type {
            //         NodeType::Variable => {
                       
            //         }
            //         NodeType::Join => {
            //             match &mut x.children {
            //                 Some(children) => { 
            //                     children.push(self.processed_statements[idx+1].clone());
            //                 }
            //                 None => {
            //                     x.children = Some(vec![self.processed_statements[idx+1].clone()]);
            //                 }
            //             }
            //         }
            //         NodeType::Source => { 

            //         }
            //         NodeType::Projection => {
            //             //not happening
            //             match &mut x.children {
            //                 Some(children) => { 
            //                     children.push(self.processed_statements[idx+1].clone());
            //                 }
            //                 None => {
            //                     x.children = Some(vec![self.processed_statements[idx+1].clone()]);
            //                 }
            //             }
            //         }
            //     }
            // }
        // }
    }



    fn error(&self, message: &str) -> ! {
        panic!("{}", message);
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    //need to tests this a lot, will come back

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