
use std::collections::HashMap;

use crate::row::Row;
use crate::btree::*;

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
    schema: HashMap<String, String>,
    _type: TableType, 
    storage_method: StorageModel, 
    //pager -> it shouldnt have one, will be passed down to it
    indexes : HashMap<String, Option<BPTreeInternalNode>> // need more than one for column dbs
}

// i dont think anything crazy needs to happen here, the predicates will be handled in the executor

impl Table {

    // todo, we need some dynamic row object standard
    pub fn new(){}
    // need to be able to package into new pages and update index(es)
    pub fn insert_row(){}
    pub fn insert_rows(){}
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