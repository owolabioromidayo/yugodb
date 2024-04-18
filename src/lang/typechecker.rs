/// take some AST, go through all the method calls and data sent to tables, and typecheck 


use crate::lang::ast::*;
use crate::database::Database; 

//TODO: define the method tables here


pub struct TypeChecker {
    //nothing here really
}

impl TypeChecker {

    // db is needed to get the table schemas
    pub fn verify(db: &Database, ast: AST ) {}
}