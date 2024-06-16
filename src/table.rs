use core::borrow;
use std::borrow::Borrow;
use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

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
    pub name: String,
    pub schema: Schema,
    pub _type: TableType,
    pub storage_method: StorageModel,
    pub pager: Rc<RefCell<Pager>>,
    //pager -> it shouldnt have one, will be passed down to it
    //how do we want to store the page indexes
    // we just need the most recent
    pub curr_page_id: usize,
    pub curr_row_id: usize,                // db row count basically
    pub page_index: HashMap<usize, usize>, //table page index -> filename, file_page_index

    pub default_index: BPTreeInternalNode<usize, (usize, u8, u8)>, // page, offset and len
    // pub default_index: BPTreeInternalNode<usize, (usize, u8, u8)>, // page, offset and len
    // TODO : setting the index type here defeats all generic programming
    // fuck, have to use an enum of diff configurations I guess
    // guess making another trait would use more code, fk
    pub indexes: HashMap<String, Option<BPTreeInternalNode<usize, (usize, u8, u8)>>>, // need more than one for column dbs
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

    // THE SCHEMA CHECK DDOESNT HAPPEN HERE, BUT AT THE PARSING STAGE INSTEAD
    pub fn insert_relational_row(&mut self, row: RelationalRecord) -> Result<()> {
        // let id = match row.id {
        //     Some(x) => x.clone(),
        //     None => self.curr_row_id + 1,
        // };
        // unimplemented!()

        let id = self.curr_row_id;
        if let Ok(mut pager) = Rc::clone(&self.pager).try_borrow_mut() {
            let schema = match &self.schema {
                Schema::Relational(x) => x,
                _ => panic!("Unsupported schema type for relational record"),
            };

            let curr_page = pager.get_page_or_force(self.curr_page_id)?;
            let mut relational_page = match RelationalRecordPage::deserialize(
                &(*curr_page).borrow_mut().read_all(),
                &schema,
            ) {
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
            if relational_page.serialize(&schema).len() + new_data.len() > PAGE_SIZE_BYTES {
                // Create a new page if adding the new record exceeds the page size
                let new_page: Page = pager.create_new_page()?;
                let mut new_relational_page = RelationalRecordPage::new();
                new_relational_page.add_record(row);
                new_page.write_all(new_relational_page.serialize(schema));
                self.curr_page_id += 1;
                self.page_index.insert(new_page.index, self.curr_page_id);
                self.default_index
                    .insert(id, (self.curr_page_id, id.clone() as u8, 0))?;
                self.curr_row_id += 1;
                pager.flush_page(&new_page)?;
            } else {
                // Append the record to the current page
                relational_page.add_record(row);
                // how are we enforcing byte lens and all here? need oto clean the rest of the page
                //do it at the page writing level
                (*curr_page)
                    .borrow_mut()
                    .write_all(relational_page.serialize(&schema));

                self.default_index
                    .insert(id.clone(), (self.curr_page_id, id.clone() as u8, 0))
                    .unwrap(); // TODO: can offset be useful here?

                self.curr_row_id += 1;
                pager.flush_page(&(*curr_page).borrow_mut())?;
            }
            Ok(())
        } else {
            return Err(Error::Unknown(
                "Failed to borrow cache mutably from here".to_string(),
            ));
        }
    }

    /// get the number of free bytes left in a page
    /// this would only be useful for relational row I feel
    pub fn scan_page(&self, page: &Page) -> usize {
        //check for null bytes starting from the right
        let mut count = 0;
        for i in page.read_all().iter().rev() {
            if *i == 0u8 {
                count += 1 as usize;
            } else {
                return count;
            }
        }
        return count;
    }

    pub fn insert_document_row(&mut self, row: DocumentRecord) -> Result<()> {
        //TODO: the table should check whether its a document table  or not

        if let Ok(mut pager) = Rc::clone(&self.pager).try_borrow_mut() {
            // let id = match row.id {
            //     Some(x) => x.clone(),
            //     None => self.curr_row_id + 1,
            // };
            let id = self.curr_row_id;
            let mut curr_page = pager.get_page_or_force(self.curr_page_id)?;
            let mut document_page =
                match DocumentRecordPage::deserialize(&(*curr_page).borrow_mut().read_all()) {
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
                let new_page = pager.create_new_page()?;
                let mut new_document_page = DocumentRecordPage::new();
                new_document_page.add_record(row);
                new_page.write_all(bson::to_vec(&new_document_page)?);
                self.curr_page_id += 1;
                self.page_index.insert(new_page.index, self.curr_page_id);
                self.default_index
                    .insert(id, (self.curr_page_id, id.clone() as u8, 0))?;
                self.curr_row_id += 1;
                println!("Index after insertion full: {:?}", &self.default_index);
                pager.flush_page(&new_page)?;
            } else {
                // Append the record to the current page
                document_page.add_record(row);
                (*curr_page)
                    .borrow_mut()
                    .write_all(bson::to_vec(&document_page)?);
                self.default_index
                    .insert(id.clone(), (self.curr_page_id, id.clone() as u8, 0))
                    .unwrap(); // TODO: can offset be useful here?
                               // , no since we are just doing it on page creation
                self.curr_row_id += 1;
                println!("index after insertion{:?}", &self.default_index);
                pager.flush_page(&(*curr_page).borrow_mut())?;
                // self.curr_page_id
            }
            return Ok(());
        } else {
            return Err(Error::Unknown(
                "Failed to borrow cache mutably from here".to_string(),
            ));
        }
    }

    pub fn insert_document_rows(pager: &Pager, rows: Vec<DocumentRecord>) {
        unimplemented!()

        // insert rows until the last page is filled
        // then create new pages here
    }

    pub fn get_document_rows_in_range(&self, start: usize, end: usize) -> Result<Records> {
        let mut records = Vec::new();
        if let Ok(mut pager) = Rc::clone(&self.pager).try_borrow_mut() {
            println!("Getting rows in range {} - {}", start, end);
            println!("Index: {:?}", &self.default_index);

            for row_id in start..=end {
                // Get the page and offset for the current row ID from the default index
                if let Some((page_id, offset, _)) = self.default_index.search(&row_id) {
                    println!("Record found");
                    // Fetch the page from the pager
                    let page = pager.get_page_or_force(*page_id)?;

                    let document_page =
                        match DocumentRecordPage::deserialize(&(*page).borrow_mut().read_all()) {
                            Ok(page) => page,
                            Err(_) => continue, // Skip if deser fails ? Do we panic instead?
                        };

                    //TODO: so the offset value should be the index in the page vec?
                    // how about we just deserialize that portion then?
                    // would need to change our document serialization strat, make it more custom
                    if let Some(record) = document_page.records.get(*offset as usize) {
                        //feels wasteful man
                        println!("Gotten record {:?} ", &record);
                        let mut nrecord: DocumentRecord = record.clone();
                        nrecord.id = Some(row_id);
                        records.push(nrecord);
                    }
                }
            }

            Ok(Records::DocumentRows(records))
        } else {
            return Err(Error::Unknown(
                "Failed to borrow cache mutably from here".to_string(),
            ));
        }
    }

    pub fn get_relational_rows_in_range(&self, start: usize, end: usize) -> Result<Records> {
        let mut records = Vec::new();
        if let Ok(mut pager) = Rc::clone(&self.pager).try_borrow_mut() {
            println!("Getting rows in range {} - {}", start, end);
            println!("Index: {:?}", &self.default_index);

            match &self.schema {
                Schema::Relational(schema) => {
                    for row_id in start..=end {
                        // Get the page and offset for the current row ID from the default index
                        if let Some((page_id, offset, _)) = self.default_index.search(&row_id) {
                            println!("Record found");
                            // Fetch the page from the pager
                            let page = pager.get_page_or_force(*page_id)?;

                            let relational_page = match RelationalRecordPage::deserialize(
                                &(*page).borrow_mut().read_all(),
                                schema,
                            ) {
                                Ok(page) => page,
                                Err(_) => continue, // Skip if deser fails ? Do we panic instead?
                            };

                            if let Some(record) = relational_page.records.get(*offset as usize) {
                                println!("Gotten record {:?} ", &record);
                                let mut nrecord = record.clone();
                                nrecord.id = Some(row_id);
                                records.push(nrecord.clone());
                            }
                        }
                    }

                    Ok(Records::RelationalRows(records))
                }
                _ => Err(Error::TypeError(
                    "Unsupported schema type for relational DB".to_string(),
                )),
            }
        } else {
            return Err(Error::Unknown(
                "Failed to borrow cache mutably from here".to_string(),
            ));
        }
    }

    pub fn get_rows_in_range(&mut self, start: usize, end: usize) -> Result<Records> {
        match (&self._type, &self.storage_method) {
            (TableType::Document, StorageModel::Row) => {
                return self.get_document_rows_in_range(start, end)
            }
            (TableType::Relational, StorageModel::Row) => {
                return self.get_relational_rows_in_range(start, end)
            }
            _ => unimplemented!(),
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

    //TODO: inserting a row in a columnar database
    //TODO: inserting a column in a columnar database

    // we might also want to selectively filter what gets pushed upstream from here
    pub fn get_column_with_select() {} //takes an id
    pub fn get_all_column_with_select() {} // get * for that column
    pub fn get_column_in_range_with_select() {}

    //for a column DB, we know the strat will differ a bit
    pub fn delete_all_rows() {}
    pub fn delete_rows_in_range() {}
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     use rust_decimal::Decimal;
//     use rust_decimal_macros::dec;
//     //TODO: test insert relational row

//     #[test]
//     fn test_insert_document_row() {
//         // TODO: test get here too
//         let mut pager = Pager::new("test".to_string());
//         //initialize 10 pages
//         for _ in 0..10 {
//             pager.create_new_page().unwrap();
//         }
//         let mut table = Table {
//             name: "test_table".to_string(),
//             schema: Schema::new(),
//             _type: TableType::Document,
//             storage_method: StorageModel::Row,
//             curr_page_id: 0,
//             curr_row_id: 0,
//             page_index: HashMap::new(),
//             // default_index: BPTreeInternalNode::new(),
//             default_index: BPTreeInternalNode::new(),
//             indexes: HashMap::new(),
//         };

//         let record1 = DocumentRecord::new();
//         let record2 = DocumentRecord::new();

//         let mut record1 = DocumentRecord::new();
//         record1.set_field(
//             "name".to_string(),
//             DocumentValue::String("John Doe".to_string()),
//         );
//         record1.set_field("age".to_string(), DocumentValue::Number(30.0));
//         record1.set_field(
//             "city".to_string(),
//             DocumentValue::String("New York".to_string()),
//         );

//         let mut address1 = HashMap::new();
//         address1.insert(
//             "street".to_string(),
//             DocumentValue::String("123 Main St".to_string()),
//         );
//         address1.insert(
//             "zip".to_string(),
//             DocumentValue::String("10001".to_string()),
//         );
//         record1.set_field("address".to_string(), DocumentValue::Object(address1));

//         let mut phone_numbers1 = Vec::new();
//         phone_numbers1.push(DocumentValue::String("123-456-7890".to_string()));
//         phone_numbers1.push(DocumentValue::String("987-654-3210".to_string()));
//         record1.set_field(
//             "phone_numbers".to_string(),
//             DocumentValue::Array(phone_numbers1),
//         );

//         let mut record2 = DocumentRecord::new();
//         record2.set_field(
//             "name".to_string(),
//             DocumentValue::String("Jane Smith".to_string()),
//         );
//         record2.set_field("age".to_string(), DocumentValue::Number(25.0));
//         record2.set_field(
//             "city".to_string(),
//             DocumentValue::String("London".to_string()),
//         );

//         let mut address2 = HashMap::new();
//         address2.insert(
//             "street".to_string(),
//             DocumentValue::String("456 High St".to_string()),
//         );
//         address2.insert(
//             "zip".to_string(),
//             DocumentValue::String("SW1A 1AA".to_string()),
//         );
//         record2.set_field("address".to_string(), DocumentValue::Object(address2));

//         let mut phone_numbers2 = Vec::new();
//         phone_numbers2.push(DocumentValue::String("020-1234-5678".to_string()));
//         record2.set_field(
//             "phone_numbers".to_string(),
//             DocumentValue::Array(phone_numbers2),
//         );

//         let mut employment2 = HashMap::new();
//         employment2.insert(
//             "company".to_string(),
//             DocumentValue::String("Acme Inc.".to_string()),
//         );
//         employment2.insert(
//             "position".to_string(),
//             DocumentValue::String("Software Engineer".to_string()),
//         );
//         let mut start_date2 = HashMap::new();
//         start_date2.insert("year".to_string(), DocumentValue::Number(2022.0));
//         start_date2.insert("month".to_string(), DocumentValue::Number(1.0));
//         employment2.insert("start_date".to_string(), DocumentValue::Object(start_date2));
//         record2.set_field("employment".to_string(), DocumentValue::Object(employment2));

//         // Insert the first record
//         let result1 = table.insert_document_row(&mut pager, record1.clone());
//         match result1 {
//             Ok(_) => (),
//             Err(err) => println!("{:?}", err),
//         }
//         // assert!(result1.is_ok());

//         // Insert the second record
//         let result2 = table.insert_document_row(&mut pager, record2.clone());
//         assert!(result2.is_ok());

//         let page = pager.get_page_or_force(table.curr_page_id).unwrap();

//         // Check if the records are inserted correctly
//         let document_page =
//             DocumentRecordPage::deserialize(&((*page).borrow_mut().read_all())).unwrap();
//         assert_eq!(document_page.records.len(), 2);
//         assert_eq!(&document_page.records[0], &record1);
//         assert_eq!(&document_page.records[1], &record2);
//     }

//      #[test]
//     fn test_insert_relational_row() {
//         let mut pager = Pager::new("test".to_string());
//         // initialize 10 pages
//         for _ in 0..10 {
//             pager.create_new_page().unwrap();
//         }
//         let schema: RelationalSchema =   HashMap::from([
//                 ("name".to_string(), (RelationalType::String(50), false)),
//                 ("balance".to_string(), (RelationalType::Numeric, false)),
//             ]) ;

//         let mut table = Table {
//             name: "test_table".to_string(),
//             schema: Schema::Relational(HashMap::from([
//                 ("name".to_string(), (RelationalType::String(50), false)),
//                 ("balance".to_string(), (RelationalType::Numeric, false)),
//             ])),
//             _type: TableType::Relational,
//             storage_method: StorageModel::Row,
//             curr_page_id: 0,
//             curr_row_id: 0,
//             page_index: HashMap::new(),
//             default_index: BPTreeInternalNode::new(),
//             indexes: HashMap::new(),
//         };

//         let record1 = RelationalRecord {
//             id: Some(0),
//             fields: HashMap::from([
//                 (
//                     "name".to_string(),
//                     RelationalValue::String("Jane Smith".to_string()),
//                 ),
//                 (
//                     "balance".to_string(),
//                     RelationalValue::Numeric(dec!(1003434343.4445)),
//                 ),
//             ]),
//         };

//         let record2 = RelationalRecord {
//             id: Some(0),
//             fields: HashMap::from([
//                 (
//                     "name".to_string(),
//                     RelationalValue::String("John Doe".to_string()),
//                 ),
//                 (
//                     "balance".to_string(),
//                     RelationalValue::Numeric(dec!(92381893.4445)),
//                 ),
//             ]),
//         };

//         // Insert the first record
//         let result1 = table.insert_relational_row(&mut pager, record1.clone());
//         assert!(result1.is_ok());

//         // Insert the second record
//         let result2 = table.insert_relational_row(&mut pager, record2.clone());
//         assert!(result2.is_ok());

//         let page = pager.get_page_or_force(table.curr_page_id).unwrap();

//         // Check if the records are inserted correctly
//         let relational_page = RelationalRecordPage::deserialize(
//             &(*page).borrow_mut().read_all(),
//             &schema,
//         ).unwrap();
//         // assert_eq!(relational_page.records.len(), 2);
//         assert_eq!(relational_page.records[0], record1);
//         assert_eq!(relational_page.records[1], record2);
//     }

// }
