/// take some AST, materialize results in a bottom up fashion, 
/// either vector or iteration model
/// 
/// 

use crate::lang::ast::*;
use crate::database::Database; 


pub struct QueryExecutor {
    //nothing here really
}

impl QueryExecutor {
    pub fn execute_query(db: &Database, ast: AST ) {}
}