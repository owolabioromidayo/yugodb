
use std::collections::HashMap;

use crate::record::*;
use crate::btree::*;
use crate::types::*;

use crate::pager::*;
use crate::error::*;

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
    //how do we want to store the page indexes 
    // we just need the most recent
    curr_page: usize,
    default_index: HashMap<usize, (String, usize)>, //tbale page index -> filename, file_page_index
    indexes : HashMap<String, Option<BPTreeInternalNode>> // need more than one for column dbs
}

// i dont think anything crazy needs to happen here, the predicates will be handled in the executor

impl Table {

    //TODO, we need ot be aware of whether we are doing a relational or document row insert

    // todo, we need some dynamic row object standard
    pub fn new(){
        // we need to create the struct, and instantiate it with the min pages
        //

    }
    // need to be able to package into new pages and update index(es)

    pub fn insert_relational_row(pager : &Pager, row: RelationalRecord){
        unimplemented!()
    }


    //TODO, ser / deser of different page variants might actually make things easier, not as low level maybe
    // think about it
    
    /// get the number of free bytes left in a page
    pub fn scan_page(&self, page: &Page) -> usize{
        //check for null bytes starting from the right 
        let mut count = 0; 
        for i in page.bytes.iter().rev(){
            if *i == 0u8 {
                count += 1 as usize;
            } else {
                return count;
            }
        }
        return count;
    }

    // we've already verified theres enough storage for this right
    fn append_document_row(serialized_document_row: &Vec<u8>, page: &mut Page) {
        //we need some unique row end marker
        // or do we want to do some unpack and repack as BSON

    }


    pub fn insert_document_row(&mut self, pager : &mut Pager, row: DocumentRecord) -> Result<bool>{

        // what is the process here?
        // since we are inserting a new row, we need to check the last page the table has
        // access to
        // check page cache, otherwise get raw page, update page cache?

        //lets just assume get_page_forced is fixed for now

        //we need more than just the page_index, we need the filename also
        // and we also need some way to scan a page, lest we keep an unmaintainable map of offsets

        // let (filename, offset) = self.default_index.get(self.curr_page)?;

        let mut curr_page = pager.get_page_forced(self.curr_page)?;
        let free_bytes = self.scan_page(&curr_page); 

        //now we need to serialize the row
        // TODO: how do we think about extracting information from a page? do we just bundle rows together
        let new_data = row.serialize()?.into_bytes(); 
        if new_data.len() > free_bytes { 
            // we have to create another page
            let mut new_page = pager.create_new_page()?;

            // now we do the actual insertion into this page, and persist it somehow
            // new_page.
            if new_data.len() > PAGE_SIZE_BYTES {
                Err(Error::Unknown("Document size too large to be written to page".to_string()))
            } else {
                Table::append_document_row(&new_data, &mut new_page); 
                Ok(true)
            }

        } else{
            //theres still space to append baby
            Table::append_document_row(&new_data, &mut curr_page); 
            Ok(true)
        }
        


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