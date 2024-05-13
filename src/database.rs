
use std::collections::HashMap;

use crate::record::DocumentRecord;
use crate::table::*;
use crate::error::*;
use crate::record::*;
use crate::pager::Pager;



pub struct Database {
    pub name: String,
    pub tables: HashMap<String, Table>,
    pub pager: Pager 
}

// only exists to manage the tables, no execution happens here
// and to pass around the pager as needed

//TODO: might need some sort of cursor management
//TODO: we need ser, deser support

impl Database {
    pub fn init() {
        //
    }
    pub fn new(name: String) -> Self {
        Self{
            name: name.clone(), 
            tables: HashMap::new(),
            pager: Pager::new(name)
        }
    }
    pub fn create_table(){}
    pub fn get_table_mut(&mut self, table_name: &String) -> Option<&mut Table>{
        self.tables.get_mut(table_name)
    }
    pub fn get_table(&self, table_name: &String) -> Option<&Table>{
        self.tables.get(table_name)
    }

    pub fn get_pager_mut(&mut self) -> &mut Pager { 
        return  &mut self.pager
    }
    pub fn delete_table(){}    

    pub fn insert_document_row(&mut self, table_name: &String, row:DocumentRecord) -> Result<()> {
       match self.tables.get_mut(table_name) {
        Some(x) => x.insert_document_row(&mut self.pager, row),
        None => Err(Error::Unknown("Table not found".to_string())), 
       }
    }

    pub fn get_rows_in_range(&mut self, table_name: &String, start:usize, end:usize) -> Result<Records> {
       match self.tables.get_mut(table_name) {
        Some(x) => x.get_rows_in_range(&mut self.pager, start, end),
        None => Err(Error::Unknown("Table not found".to_string())), 
       }
    }

}