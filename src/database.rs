
use std::collections::HashMap;

use crate::record::DocumentRecord;
use crate::table::*;
use crate::error::*;
use std::rc::Rc;
use std::cell::RefCell;
use crate::record::*;
use crate::pager::Pager;



pub struct Database {
    pub name: String,
    pub tables: HashMap<String, Table>,
    // pub pager: Rc<RefCell<Pager>>,
    // table_pages: HashMap<String, Vec<usize>>, // keep track of pages in use by tables, last index is curr

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
            // pager: Rc::new(RefCell::new(Pager::new(name))),
            // table_pages: HashMap::new(),
        }
    }
    pub fn create_table(){}
    pub fn get_table_mut(&mut self, table_name: &String) -> Option<&mut Table>{
        self.tables.get_mut(table_name)
    }
    pub fn get_table(&self, table_name: &String) -> Option<&Table>{
        self.tables.get(table_name)
    }

    // pub fn get_pager_mut(&mut self) -> &mut Pager { 
    //     return  &mut self.pager
    // }
    pub fn delete_table(){}    

    pub fn insert_document_row(&mut self, table_name: &String, row:DocumentRecord) -> Result<()> {
       match self.tables.get_mut(table_name) {

        Some(x) =>  {
            // match Rc::clone(&self.pager).try_borrow_mut() {
            // Ok(mut cache_mut) =>  x.insert_document_row(&mut cache_mut, row),
            // Err(_) => Err(Error::Unknown("Failed to borrow pager mutably".to_string())),

            // }   
            x.insert_document_row(row)
        },
        None => Err(Error::Unknown("Table not found".to_string())), 
       }
    }

    pub fn insert_relational_row(&mut self, table_name: &String, row:RelationalRecord) -> Result<()> {
       match self.tables.get_mut(table_name) {

        Some(x) =>  {
            // match Rc::clone(&self.pager).try_borrow_mut() {
            // Ok(mut cache_mut) =>  x.insert_relational_row(&mut cache_mut, row),
            // Err(_) => Err(Error::Unknown("Failed to borrow pager mutably".to_string())),

            // }   
            x.insert_relational_row(row)
        },
        None => Err(Error::Unknown("Table not found".to_string())), 
       }
    }

    pub fn get_rows_in_range(&mut self, table_name: &String, start:usize, end:usize) -> Result<Records> {
       match self.tables.get_mut(table_name) {

        Some(x) =>  {
            // match Rc::clone(&self.pager).try_borrow_mut() {
            // Ok(mut cache_mut) =>  x.get_rows_in_range(&mut cache_mut , start, end),
            // Err(_) => Err(Error::Unknown("Failed to borrow pager mutably".to_string())),
            // }   
            x.get_rows_in_range(start, end)
        },
        None => Err(Error::Unknown("Table not found".to_string())), 
       }

    }

}