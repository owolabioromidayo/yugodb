use std::collections::HashMap;
use std::ops::Deref;

use crate::lang::types::*;
use crate::lang::ast::*;
use crate::record::*;
use crate::error::*;
use crate::record_iterator::*;


//maybe we call this a resolver / unroller kind of thing?
// but then, it doesnt modify anything, just visits
// so lets see about that
// we might not be able to optimize for now then

// so, evaluate actually resolves into some data values at the end
// which means our fetcher might need to be built in here
// we cant optimize in here then

// the interpreter will take the tree and materialize some values then
// so we need the DBMS abstraction in place
// lets mock for now

//so, do we materialize the variable into the dataflow expr?
//its a quick and dirty start for sure

//so, this means we really want to evaluate into a set of records
// which will have to layer on top our our whole multi approach
// so we might need some unified record abstraction enum?

// we are going to need iter_vec in here reall soon
// so we would need some sort of records iterator, instead of just getting it all at once


// we are basically creating another tree with this approach, from what IM seeing
// creating a tree of iterators,

//what if we snowball it into one large iterator of sub iterators each with their own exec functions 
// that we define
// so then, we are returning custom record iterators
// think about this

pub struct IterClosure {
    get_next_chunk : Box<dyn Fn() -> Option<Records> >
}




struct Interpreter { 
    //some variables I guess
    // we need a better local state here
    pub variables: HashMap<String, RecordIterator>,
}

impl ExprVisitor<Result<RecordIterator>> for Interpreter {

    //TOOD: these are key to building up everything else
    // funnily, doesnt come from this, but in a roundabout way comes from the datacall and dataexpr
    // learny your own grammar

    
    // we should also clone here, better to have some static copy
    fn visit_variable(&mut self, expr: &Variable) -> Result<RecordIterator> {
        // in this situation, we just return the evaluation of the variable.
        // if it is the first time being defined we store, otherwise, we return from the map

        //TODO: a variable should be guaranteed some literal ; its just unwrap for now
        match self.variables.get((&expr.name.literal.clone().unwrap()))  {
            Some(y) => Ok(y.clone()),
            None => Err(Error::NotFound(format!("Variable {:?} does not exist", expr))),
        }

    }

    // an atrribute could just be some recorditerator with a set of predicates applied to it!
    fn visit_attribute(&mut self, expr: &Attribute) -> Result<RecordIterator> {

        //an attribute is a select statement applied to a variable

        let left = &expr.tokens[0].literal.clone().unwrap();
        if self.variables.contains_key(left) {

            // this only works for things like x.id
            let mut var = self.visit_variable_token(&expr.tokens[0]).unwrap();
            for t in &expr.tokens[1..] {
                var.predicate.select.push(t.literal.clone().unwrap());
            }

            return Ok(var)     
        } else {

            // TODO: what about DB.table.X or some shit
            // have to deal with this here
            unimplemented!()
        }
        

    }

    // TODO: work on the custom exec function for record iterator
    fn visit_binary(&mut self, expr: &Binary) -> Result<RecordIterator> {
        unimplemented!()
    }
    // fn visit_binary(&mut self, expr: &Binary) -> IterClosure{

    //     let left = self.evaluate(&expr.left); 
    //     let right = self.evaluate(&expr.right); 

    //     match &expr.operator._type { 
    //         TokenType::Greater =>  {
    //             IterClosure {
    //                 get_next_chunk :  Box::new(|| {
    //                     let nleft = left.get_next_chunk();
    //                     let nright= right.get_next_chunk();

    //                     if nleft.is_some()  && nright.is_some() {
    //                         let xs = nleft.unwrap(); 
    //                         let ys = nright.unwrap(); 

    //                         match (xs, ys) {

    //                             // TODO: this aint right man, therel be too many
    //                             (Records::DocumentRows(xs), Records::DocumentRows(ys)) => {
    //                                 let final_records: Vec<DocumentRecord> = Vec::new();
    //                                 for (x,y) in xs.iter().zip(ys.iter()) {
                                        
    //                                     // what would greater mean! thats strange?
    //                                     // there has to be some extra information being packed, or this is an attribute being iterated over
    //                                     // anyhow, evalute should take care of that, then this document type comparison would not be neccessary

    //                                 }

    //                             }

    //                             _ => unimplemented!()
    //                         }

    //                     }
    //                 })
    //             }
    //         }   
    //         _ => IterClosure {
    //             get_next_chunk : Box::new(|| {None}), 
    //         } 
    //     }

    // }
    fn visit_grouping(&mut self, expr: &Grouping) -> Result<RecordIterator> {unimplemented!()}
    fn visit_literal(&mut self, expr: &Literal) ->  Result<RecordIterator> {unimplemented!()}
    fn visit_unary(&mut self, expr: &Unary) ->  Result<RecordIterator> {unimplemented!()}



    
    fn visit_assign(&mut self, expr: &Assign) ->  Result<RecordIterator> {unimplemented!()}
    fn visit_logical_expr(&mut self, expr: &Logical) ->  Result<RecordIterator> {unimplemented!()}
    fn visit_data_call(&mut self, expr: &DataCall) -> Result<RecordIterator> {

        let mut left = self.evaluate(&Expr::Attribute(expr.attr.clone())).unwrap();

        // this is where we apply the methods to the iterator
        // method typechecking and application

        // remember , besides that, that we have to generate some iter function
        // we are still working on that
        // TODO: maybe create a custom next : Option<dyn Fn() -> Records> on RecordIterator


        return Ok(left)



    }

    fn visit_data_expr(&mut self, expr: &DataExpr) -> Result<RecordIterator> {

        let left = self.evaluate(&expr.left); 
        let right = self.evaluate(&expr.right);

        match &expr.join._type {
            TokenType::Ljoin  => {
                // here we have to wrory about database types? do we?
                // yeah, records still contains some db type info


                // we have no notion of Fkeys yet
                // fkeys are easy, if they are named appropriately
                // need to extend database tables to support that though
                //TODO: easiest way is to specify the left and right join keys
                // left join, we basically iter and zip where we can (based on id)
                unimplemented!()

            },
            TokenType::Join => {
                // left join, we basically iter and zip where we can (based on id), but we discard non matches
                unimplemented!()

            },
            _ => unimplemented!()
        } 

    }
}

impl StmtVisitor<Result<RecordIterator>> for Interpreter {
    fn visit_print_stmt(&mut self, stmt: &PrintStmt) -> () {
        // println!("Print stmt called!");
        unimplemented!(); 
    }
    fn visit_expr_stmt(&mut self, stmt: &ExprStmt) -> Result<RecordIterator> {
        // so, what the hell would we do here?
        match &stmt.expression {
            Expr::DataExpr(expr) => self.visit_data_expr(&expr),
            _ => unimplemented!()
        }
    }

    fn visit_var_stmt(&mut self, stmt: &VarStmt) -> () {
        // unimplemented!();
        // println!("var stmt called!");
    }

}


impl Interpreter {

    // makes more sense to get a clone we can modify for attrs and elsewhere also
    fn visit_variable_token(&mut self, token: &Token) -> Result<RecordIterator> {
        // in this situation, we just return the evaluation of the variable.
        // if it is the first time being defined we store, otherwise, we return from the map

        //TODO: a variable should be guaranteed some literal ; its just unwrap for now
        match self.variables.get(&token.literal.clone().unwrap()) {
            Some(y) => Ok(y.clone()),
            None => Err(Error::NotFound(format!("Variable {:?} does not exist", token))),
        }

    }

    // TODO: maybe we want a result<RI> instead
    pub fn evaluate(&mut self, expr: &Expr) -> Result<RecordIterator> {

        // we should expect some RecordIterator from this?
        let res = match &expr{
            Expr::Grouping(expr) => self.visit_grouping(expr),
            Expr::Literal(expr) => self.visit_literal( expr ),
            Expr::Unary(expr) => self.visit_unary( expr ),
            Expr::Variable(expr) => self.visit_variable( expr) ,
            Expr::Attribute(expr) => self.visit_attribute( expr) ,
            Expr::Assign(expr) => self.visit_assign( expr),
            Expr::Logical(expr) => self.visit_logical_expr( expr) ,
            Expr::DataCall(expr) => self.visit_data_call( expr) ,
            Expr::DataExpr(expr) => self.visit_data_expr( expr) ,
            _ => unimplemented!()
        };
    
        res
    }

    pub fn execute_processed_stmt(&self, stmt: &Option<Node>){
        if let Some(s) = stmt { 
            for child in &s.children {
                self.execute_processed_stmt(child)
            }

            //execute the actual statement
            
            
        }

    }

    pub fn resolve_variables(&mut self, ast_lookup: &HashMap<String, Node> ){

        for (k,v) in ast_lookup.iter() {
            //resolve the variable into either some value or an IterWrapper kind of thing (with the proper args on it)

                
            let m  = match &v.data {
                NodeData::Join(x) => unimplemented!(),
                NodeData::Source(x) =>  self.evaluate(&x.source).unwrap(),

                // TODO: something like this could be circular i.e x = x
                NodeData::Variable(x ) => unimplemented!(),
                NodeData::Projection(x) => unimplemented!()
            };

            self.variables.insert(k.to_string(), m); 
        }
    }

    pub fn execute(&mut self,  ast: AST) -> Result<Records> { 
        // resolve all variables first
        self.resolve_variables(&ast.lookup_table);


        //traverse statements from earliest to latest
        self.execute_processed_stmt(&ast.root);

        // right now we have it top down, so we have to recurse and come back I guess

        // what this should do is create the record iterator tree, then we iter while we can?

        unimplemented!()
    }
}
