use std::collections::HashMap;

use crate::btree::*;
use crate::record::*;
use crate::types::*;

use crate::btree::*;
use crate::error::*;
use crate::pager::*;
use crate::schema::*;

use bson::{bson, Bson};
use serde::{Deserialize, Serialize};

pub enum TableType {
    Relational,
    Document,
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
    curr_page_id: usize,
    curr_row_id: usize,
    page_index: HashMap<usize, usize>, //table page index -> filename, file_page_index
    default_index: BPTreeInternalNode<usize, (usize, u8, u8)>, // page, offset and len
    indexes: HashMap<String, Option<BPTreeInternalNode< usize, (usize, u8, u8)>>>, // need more than one for column dbs
}

// i dont think anything crazy needs to happen here, the predicates will be handled in the executor

impl Table {
    //TODO, we need ot be aware of whether we are doing a relational or document row insert

    // todo, we need some dynamic row object standard
    pub fn new() {
        // we need to create the struct, and instantiate it with the min pages
        //
    }
    // need to be able to package into new pages and update index(es)

    pub fn insert_relational_row(&mut self, pager: &mut Pager, row: RelationalRecord) -> Result<bool> {
        // unimplemented!()
        let schema = match &self.schema {
            Schema::Relational(x) => x,
            _ => panic!("Unsupported schema type for relational record")
        };

        let mut curr_page = pager.get_page_forced(self.curr_page_id)?;
        let mut document_page = match RelationalRecordPage::deserialize(&curr_page.bytes, &schema) {
            Ok(page) => page,
            Err(_) => RelationalRecordPage::new(),
        };


        let new_data = row.serialize(&schema);
        if new_data.len() > PAGE_SIZE_BYTES {
            return Err(Error::Unknown(
                "Document size too large to be written to page".to_string(),
            ));
        }

        //TODO
        // if bson::to_vec(&document_page)?.len() + new_data.len() > PAGE_SIZE_BYTES {
        //     // Create a new page if adding the new record exceeds the page size
        //     let mut new_page: Page = pager.create_new_page()?;
        //     let mut new_document_page = RelationalRecordPage::new();
        //     new_document_page.add_record(row);
        //     new_page.bytes = bson::to_vec(&new_document_page)?;
        //     self.curr_page_id += 1;
        //     self.page_index.insert(new_page.index, self.curr_page_id);
        //     self.default_index
        //         .insert(self.curr_row_id, self.curr_page_id, 0); // TODO: can offset be useful here?
        //                                                          // , no since we are just doing it on page creation
        //     pager.flush_page(&new_page)?;
        // } else {
        //     // Append the record to the current page
        //     document_page.add_record(row);
        //     curr_page.bytes = bson::to_vec(&document_page)?;
        //     pager.flush_page(&curr_page)?;
        //     // self.curr_page_id
        // }
        Ok(true)
    }

    //TODO, ser / deser of different page variants might actually make things easier, not as low level maybe
    // think about it

    /// get the number of free bytes left in a page
    /// this would only be useful for relational row I feel
    pub fn scan_page(&self, page: &Page) -> usize {
        //check for null bytes starting from the right
        let mut count = 0;
        for i in page.bytes.iter().rev() {
            if *i == 0u8 {
                count += 1 as usize;
            } else {
                return count;
            }
        }
        return count;
    }

    pub fn insert_document_row(&mut self, pager: &mut Pager, row: DocumentRecord) -> Result<bool> {
        let mut curr_page = pager.get_page_forced(self.curr_page_id)?;
        let mut document_page = match DocumentRecordPage::deserialize(&curr_page.bytes) {
            Ok(page) => page,
            Err(_) => DocumentRecordPage::new(),
        };
        let new_data = row.serialize()?;
        if new_data.len() > PAGE_SIZE_BYTES {
            return Err(Error::Unknown(
                "Document size too large to be written to page".to_string(),
            ));
        }

        if bson::to_vec(&document_page)?.len() + new_data.len() > PAGE_SIZE_BYTES {
            // Create a new page if adding the new record exceeds the page size
            let mut new_page = pager.create_new_page()?;
            let mut new_document_page = DocumentRecordPage::new();
            new_document_page.add_record(row);
            new_page.bytes = bson::to_vec(&new_document_page)?;
            self.curr_page_id += 1;
            self.page_index.insert(new_page.index, self.curr_page_id);
            self.default_index
                .insert(self.curr_row_id, (self.curr_page_id, 0, 0)); // TODO: can offset be useful here?
                                                                 // , no since we are just doing it on page creation
            pager.flush_page(&new_page)?;
        } else {
            // Append the record to the current page
            document_page.add_record(row);
            curr_page.bytes = bson::to_vec(&document_page)?;
            pager.flush_page(&curr_page)?;
            // self.curr_page_id
        }
        Ok(true)
    }

    pub fn insert_document_rows(pager: &Pager, rows: Vec<DocumentRecord>) {
        unimplemented!()

        // insert rows until the last page is filled
        // then create new pages here
    }

    pub fn get_document_rows_in_range(&self, start:usize, end:usize) -> Records {
        //retrieval
        unimplemented!()

    }

    pub fn get_rows_in_range(&mut self, start:usize, end:usize) -> Records {
        match (&self._type, &self.storage_method) {
            (TableType::Document, StorageModel::Row) =>  {
                return self.get_document_rows_in_range(start, end)
            }
            _ => unimplemented!()
        }
    // match based on the schema and document model, figure out what to do

    }
    pub fn insert_rows() {}
    pub fn delete_row() {}
    pub fn get_row() {} //takes an id
    pub fn get_all_rows() {}

    // we might also want to selectively filter what gets pushed upstream from here
    pub fn get_row_with_select() {} //takes an id
    pub fn get_all_rows_with_select() {}
    pub fn get_rows_in_range_with_select() {}

    // for column oriented storage

    pub fn get_column() {} //takes an id
    pub fn get_all_column() {} // get * for that column
    pub fn get_column_in_range() {}

    // we might also want to selectively filter what gets pushed upstream from here
    pub fn get_column_with_select() {} //takes an id
    pub fn get_all_column_with_select() {} // get * for that column
    pub fn get_column_in_range_with_select() {}

    //for a column DB, we know the strat will differ a bit
    pub fn delete_all_rows() {}
    pub fn delete_rows_in_range() {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_document_row() {
        // TODO: test get here too
        let mut pager = Pager::new("test".to_string());
        //initialize 10 pages
        for _ in 0..10 {
            pager.create_new_page().unwrap();
        }
        let mut table = Table {
            name: "test_table".to_string(),
            schema: Schema::new(),
            _type: TableType::Document,
            storage_method: StorageModel::Row,
            curr_page_id: 0,
            curr_row_id: 0,
            page_index: HashMap::new(),
            default_index: BPTreeInternalNode::new(),
            indexes: HashMap::new(),
        };

        let record1 = DocumentRecord::new();
        let record2 = DocumentRecord::new();

        let mut record1 = DocumentRecord::new();
        record1.set_field(
            "name".to_string(),
            DocumentValue::String("John Doe".to_string()),
        );
        record1.set_field("age".to_string(), DocumentValue::Number(30.0));
        record1.set_field(
            "city".to_string(),
            DocumentValue::String("New York".to_string()),
        );

        let mut address1 = HashMap::new();
        address1.insert(
            "street".to_string(),
            DocumentValue::String("123 Main St".to_string()),
        );
        address1.insert(
            "zip".to_string(),
            DocumentValue::String("10001".to_string()),
        );
        record1.set_field("address".to_string(), DocumentValue::Object(address1));

        let mut phone_numbers1 = Vec::new();
        phone_numbers1.push(DocumentValue::String("123-456-7890".to_string()));
        phone_numbers1.push(DocumentValue::String("987-654-3210".to_string()));
        record1.set_field(
            "phone_numbers".to_string(),
            DocumentValue::Array(phone_numbers1),
        );

        let mut record2 = DocumentRecord::new();
        record2.set_field(
            "name".to_string(),
            DocumentValue::String("Jane Smith".to_string()),
        );
        record2.set_field("age".to_string(), DocumentValue::Number(25.0));
        record2.set_field(
            "city".to_string(),
            DocumentValue::String("London".to_string()),
        );

        let mut address2 = HashMap::new();
        address2.insert(
            "street".to_string(),
            DocumentValue::String("456 High St".to_string()),
        );
        address2.insert(
            "zip".to_string(),
            DocumentValue::String("SW1A 1AA".to_string()),
        );
        record2.set_field("address".to_string(), DocumentValue::Object(address2));

        let mut phone_numbers2 = Vec::new();
        phone_numbers2.push(DocumentValue::String("020-1234-5678".to_string()));
        record2.set_field(
            "phone_numbers".to_string(),
            DocumentValue::Array(phone_numbers2),
        );

        let mut employment2 = HashMap::new();
        employment2.insert(
            "company".to_string(),
            DocumentValue::String("Acme Inc.".to_string()),
        );
        employment2.insert(
            "position".to_string(),
            DocumentValue::String("Software Engineer".to_string()),
        );
        let mut start_date2 = HashMap::new();
        start_date2.insert("year".to_string(), DocumentValue::Number(2022.0));
        start_date2.insert("month".to_string(), DocumentValue::Number(1.0));
        employment2.insert("start_date".to_string(), DocumentValue::Object(start_date2));
        record2.set_field("employment".to_string(), DocumentValue::Object(employment2));

        // Insert the first record
        let result1 = table.insert_document_row(&mut pager, record1.clone());
        match result1 {
            Ok(_) => (),
            Err(err) => println!("{:?}", err),
        }
        // assert!(result1.is_ok());

        // Insert the second record
        let result2 = table.insert_document_row(&mut pager, record2.clone());
        assert!(result2.is_ok());

        let page = pager.get_page_forced(table.curr_page_id).unwrap();

        // Check if the records are inserted correctly
        let document_page = DocumentRecordPage::deserialize(&page.bytes).unwrap();
        assert_eq!(document_page.records.len(), 2);
        assert_eq!(&document_page.records[0], &record1);
        assert_eq!(&document_page.records[1], &record2);
    }
}
