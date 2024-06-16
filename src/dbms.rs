// just responsible for multiple database instances

use std::collections::HashMap;

use crate::btree::*;
use crate::database::*;
use crate::error::*;
use crate::lang::ast::*;
use crate::lang::interpreter::*;
use crate::lang::parser::*;
use crate::lang::tokenizer::*;
use crate::lang::types::*;
use crate::pager::*;
use crate::record::*;
use crate::schema::*;
use crate::table::*;

// use crate::*;

pub struct DBMS {
    pub databases: HashMap<String, Database>,
}

// only exists to manage the tables, no execution happens here
// and to pass around the pager as needed

//TODO: might need some sort of cursor management

impl DBMS {
    pub fn init() {
        //
    }
    pub fn new() -> Self {
        Self {
            databases: HashMap::new(),
        }
    }
    pub fn create_table() {}
    pub fn get_db_mut(&mut self, db_name: &String) -> Option<&mut Database> {
        return self.databases.get_mut(db_name);
    }

    pub fn get_table_mut(&mut self, db_name: &String, table_name: &String) -> Option<&mut Table> {
        if let Some(x) = self.databases.get_mut(db_name) {
            return x.get_table_mut(table_name);
        }
        None
    }
    pub fn delete_table() {}
}

//TODO: need some tests here
