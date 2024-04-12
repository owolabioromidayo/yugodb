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


use std::hash::Hash;
use std::ops::Deref;
use std::vec::Vec;
use crate::lang::types::*; 
use crate::lang::tokenizer::*; 
use crate::lang::parser::*; 
use crate::error::*;
use std::collections::HashMap;

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

#[derive(Debug)]
struct Projection {
    expr: Expr
}

#[derive(Debug)]
struct Join { 
    _type: JoinType, //TODO: uniontypes or type definitions for this
    predicate: Expr
}


#[derive(Debug)]
struct Source {
    source: Expr, // i think we should shift forcing into DataCAll forwards
}

#[derive(Debug, Clone, Copy)]
pub enum NodeType {
    Projection,
    Source, // a datacall is automatically a root node
    Join, // Join or LJoin
    Variable, // variable expr

}

#[derive(Debug)]
pub enum NodeData {
    Projection(Projection),
    Source(Source),
    Join(Join),
    Variable(Box<Expr>)
}


#[derive(Debug)]
struct Node {
    _type: NodeType,
    data: NodeData,
    children: Option<Vec<Node>>
}

impl Node {
    // what do we want here?
}




struct AST{
    lookup_table : HashMap<String, Node>, //token.lexeme
    root: Option<Node>
}



impl AST{

    // we might have to do some visitor pattern stuff here.
    pub fn new()-> Self{
        AST {
            root: None,
            lookup_table: HashMap::new()
        }
    }


    fn generate_from_expr(&mut self, expr: &Expr) -> Option<Node> {
        match expr {

            //TODO: we need to think about handling the lower levels

            //we need to do the visitor pattern thing for the lower levels
            // for now, lets look at higher stuff

            //do we have to match all the lower types? they have no bearing here (shoould be contained in either of these)

            Expr::Variable(expr) => {
                // return self.lookup_table.get(&expr.name.lexeme.to_string());
                None
            }

            Expr::DataCall(expr) => { 
                // sweet, we have a base node
                Some(Node {
                    _type: NodeType::Source, 
                        data : NodeData::Source((Source{source: Expr::DataCall(*expr) })),
                        children: None
                    }) 

                // add it as a child of curr
                //TODO: is this proper error handling?
                // curr.children.unwrap().push(new_node);


            }  
            Expr::DataExpr(expr) => { 
                //create a join node

                let left_node = self.generate_from_expr(expr.left.as_ref()).unwrap(); 
                let right_node= self.generate_from_expr(expr.right.as_ref()).unwrap(); 


                // add it as a child of curr
                //TODO: is this proper error handling?

                Some(Node {
                    _type: NodeType::Join, 
                        data : NodeData::Join((Join{_type : JoinType::from_token(&expr.join), predicate: *expr.join_expr })),
                        children: Some(vec![left_node, right_node])
                })

                // curr.children.unwrap().push(join_node); 
            }      

            _ => unimplemented!()               
        }


    }

    fn generate_from_stmt(&mut self, statement: &Stmt) -> Option<Node> {
            match statement{
                Stmt::Var(stmt) => {
                    //create some new variable and assign it to some 
                    // do nothing here, it has been handled already?                    

                    return None 
                }
                Stmt::Expression(stmt) =>  {
                    //something to be evaluated on an existing variable
                    // so what do we do in this case?

                    // need to find the variable in use if any

                    //need to find out if its a join or just a datacall expr

                    return  self.generate_from_expr(&stmt.expression);

                }
                Stmt::Print(stmt) => return None

            }
    }

    /// Generate the AST from a list of statements
    fn generate(&mut self, mut statements: Vec<Stmt>){

        // this is a very left to right and bottom to top kind of parsing.

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
                    // we might need to recursively walk through this definition and break it down? 
                    let fexpr =  self.generate_from_expr(expr).unwrap(); 
                    self.lookup_table.insert(varstmt.name.lexeme.clone(), fexpr); 

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
                        children: Some(Vec::new())
                     }
                ); 
             }
             _ => {
                self.error("Final statement in query must be an expression!");
             }
        }

        

        //top down (reversed stmt order), might be easier to handle?
        statements.reverse();
        let curr = self.root.as_ref().unwrap(); 

        for statement in statements {
            let new_node = self.generate_from_stmt(&statement);
            if let Some(x) = new_node {
                match x._type {
                    NodeType::Variable => {
                        
                    }
                    NodeType::Join => {

                    }
                    NodeType::Source => { 

                    }
                    NodeType::Projection => {
                        //not happening
                        ()
                    }
                }
            }
            
            }

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
        // let mut tokenizer = Tokenizer::new("
        // let x = db.TABLES.b.filter(); 
        // let y = db.TABLES.x ; 
        // x.filter(); 
        // let z = x JOIN y on x.id=y.id;  x.select(a,b,c,d);
        // ");

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

    }
}