
use std::collections::HashMap; 

use crate::table::Table;
use crate::pager::Pager;



pub struct Database {
    tables: HashMap<String, Table>,
    pager: Pager 
}

// only exists to manage the tables, no execution happens here
// and to pass around the pager as needed

//TODO: might need some sort of cursor management

impl Database {
    pub fn new() {}
    pub fn create_table(){}
    pub fn get_table(){}
    pub fn delete_table(){}    

}