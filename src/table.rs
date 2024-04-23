
use std::collections::HashMap;

use crate::record::*;
use crate::btree::*;
use crate::types::*;

use crate::pager::Pager;

pub enum TableType{
    Relational,
    Document
}

pub enum StorageModel {
    Row,
    Column, //TODO: massively influences fetching mechanism (impl after we have row working)
    //Hybrid // wed need more metadata for this
}

pub struct Table {

    name: String, 
    schema: Schema,
    _type: TableType, 
    storage_method: StorageModel, 
    //pager -> it shouldnt have one, will be passed down to it
    indexes : HashMap<String, Option<BPTreeInternalNode>> // need more than one for column dbs
}

// i dont think anything crazy needs to happen here, the predicates will be handled in the executor

impl Table {

    //TODO, we need ot be aware of whether we are doing a relational or document row insert

    // todo, we need some dynamic row object standard
    pub fn new(){}
    // need to be able to package into new pages and update index(es)
    pub fn insert_relational_row(pager : &Pager, row: RelationalRecord){
        unimplemented!()
    }
    
    pub fn insert_document_row(pager : &Pager, row: DocumentRecord){
        unimplemented!()

        // what is the process here?
        // since we are inserting a new row, we need to check the last page the table has
        // access to
        // check page cache, otherwise get raw page, update page cache?
    }
    pub fn insert_document_rows(pager : &Pager, rows: Vec<DocumentRecord>){
        unimplemented!()
    }
    pub fn insert_rows(){

    }
    pub fn delete_row(){} 
    pub fn get_row(){} //takes an id
    pub fn get_all_rows(){} 
    pub fn get_rows_in_range(){}

    // we might also want to selectively filter what gets pushed upstream from here
    pub fn get_row_with_select(){} //takes an id
    pub fn get_all_rows_with_select(){} 
    pub fn get_rows_in_range_with_select(){}


    // for column oriented storage

    pub fn get_column(){} //takes an id
    pub fn get_all_column(){} // get * for that column
    pub fn get_column_in_range(){}

    // we might also want to selectively filter what gets pushed upstream from here
    pub fn get_column_with_select(){} //takes an id
    pub fn get_all_column_with_select(){} // get * for that column
    pub fn get_column_in_range_with_select(){}


    //for a column DB, we know the strat will differ a bit
    pub fn delete_all_rows(){}
    pub fn delete_rows_in_range(){}

    
}


// write tests to
// create a new table
// insert a row, delete a row, get all rows, get a row at id, get rows with select