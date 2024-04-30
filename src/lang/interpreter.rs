use std::collections::HashMap;

use crate::lang::types::*;
use crate::lang::ast::*;
use crate::record::*;


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

struct Interpreter { 
    //some variables I guess
    // we need a better local state here
}

impl ExprVisitor<()> for Interpreter {
    fn visit_binary(&mut self, expr: &Binary) -> Records {



    }
    fn visit_grouping(&mut self, expr: &Grouping) -> () {unimplemented!()}
    fn visit_literal(&mut self, expr: &Literal) -> () {unimplemented!()}
    fn visit_unary(&mut self, expr: &Unary) -> () {unimplemented!()}
    fn visit_variable(&mut self, expr: &Variable) -> () {unimplemented!()}
    fn visit_attribute(&mut self, expr: &Attribute) -> () {unimplemented!()}
    fn visit_assign(&mut self, expr: &Assign) -> () {unimplemented!()}
    fn visit_logical_expr(&mut self, expr: &Logical) -> () {unimplemented!()}
    fn visit_data_call(&mut self, expr: &DataCall) -> () {unimplemented!()}
    fn visit_data_expr(&mut self, expr: &DataExpr) -> () {unimplemented!()}
}

impl StmtVisitor for Interpreter {
    fn visit_print_stmt(&mut self, stmt: &PrintStmt) -> () {
        println!("Print stmt called!");
        // unimplemented!(); 
    }
    fn visit_expr_stmt(&mut self, stmt: &ExprStmt) -> () {
        // so, what the hell would we do here?
        match &stmt.expression {
            Expr::DataExpr(expr) => self.visit_data_expr(&expr),
            _ => unimplemented!()
        }
    }

    fn visit_var_stmt(&mut self, stmt: &VarStmt) -> () {
        // unimplemented!();
        println!("var stmt called!");
    }

}


impl Interpreter {


    pub fn execute_processed_stmt(&self, stmt: &Option<Node>){
        if let Some(s) = stmt { 
            for child in &s.children {
                self.execute_processed_stmt(child)
            }

            //execute the actual statement
            
            
        }

    }

    pub fn resolve_variables(&self, ast_lookup: &HashMap<String, Node> ){

        for (k,v) in ast_lookup.iter() {
            //resolve the variable into either some value or an IterWrapper kind of thing (with the proper args on it)
        }
    }

    pub fn execute(&self,  ast: AST) { 
        // resolve all variables first
        self.resolve_variables(&ast.lookup_table);


        //traverse statements from earliest to latest
        self.execute_processed_stmt(&ast.root);

        // right now we have it top down, so we have to recurse and come back I guess
    }
}
