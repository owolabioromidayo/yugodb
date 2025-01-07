
use std::collections::HashMap;

use crate::record::DocumentRecord;
use crate::table::*;
use crate::error::*;
use crate::record::*;



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

    pub fn update_document_row(&mut self, table_name: &String, row:DocumentRecord) -> Result<()> {
        match self.tables.get_mut(table_name) {
 
         Some(x) =>  {
             x.update_document_row(row)
         },
         None => Err(Error::Unknown("Table not found".to_string())), 
        }
     
     }


     pub fn delete_document_row(&mut self, table_name: &String, row_id: usize) -> Result<()> {
        match self.tables.get_mut(table_name) {
 
         Some(x) =>  {
             x.delete_document_row(row_id)
         },
         None => Err(Error::Unknown("Table not found".to_string())), 
        }
     }

     pub fn delete_document_rows(&mut self, table_name: &String, row_ids: Vec<usize>) -> Result<()> {
        match self.tables.get_mut(table_name) {
 
         Some(x) =>  {
             x.delete_document_rows(row_ids)
         },
         None => Err(Error::Unknown("Table not found".to_string())), 
        }
     }




    pub fn insert_document_rows(&mut self, table_name: &String, rows:Vec<DocumentRecord>) -> Result<()> {
        match self.tables.get_mut(table_name) {
 
         Some(x) =>  {
             x.insert_document_rows(rows)
         },
         None => Err(Error::Unknown("Table not found".to_string())), 
        }
     }


     pub fn update_document_rows(&mut self, table_name: &String, rows:Vec<DocumentRecord>) -> Result<()> {
        match self.tables.get_mut(table_name) {
 
         Some(x) =>  {
             x.update_document_rows(rows)
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

    pub fn insert_relational_rows(&mut self, table_name: &String, rows:Vec<RelationalRecord>) -> Result<()> {
        match self.tables.get_mut(table_name) {
 
         Some(x) =>  {
             x.insert_relational_rows(rows)
         },
         None => Err(Error::Unknown("Table not found".to_string())), 
        }
     }

     pub fn update_relational_row(&mut self, table_name: &String, row:RelationalRecord) -> Result<()> {
        match self.tables.get_mut(table_name) {
 
         Some(x) =>  {
             x.update_relational_row(row)
         },
         None => Err(Error::Unknown("Table not found".to_string())), 
        }
     
     }

     pub fn update_relational_rows(&mut self, table_name: &String, rows:Vec<RelationalRecord>) -> Result<()> {
        match self.tables.get_mut(table_name) {
 
         Some(x) =>  {
             x.update_relational_rows(rows)
         },
         None => Err(Error::Unknown("Table not found".to_string())), 
        }
     }

     pub fn delete_relational_row(&mut self, table_name: &String, row_id: usize) -> Result<()> {
        match self.tables.get_mut(table_name) {
 
         Some(x) =>  {
             x.delete_relational_row(row_id)
         },
         None => Err(Error::Unknown("Table not found".to_string())), 
        }
     }

     pub fn delete_relational_rows(&mut self, table_name: &String, row_ids: Vec<usize>) -> Result<()> {
        match self.tables.get_mut(table_name) {
 
         Some(x) =>  {
             x.delete_relational_rows(row_ids)
         },
         None => Err(Error::Unknown("Table not found".to_string())), 
        }
     }


     pub fn insert_columnar_document_row(&mut self, table_name: &String, row:ColumnarDocumentRecord) -> Result<()> {
        match self.tables.get_mut(table_name) {
 
         Some(x) =>  {
             x.insert_columnar_document_row(row)
         },
         None => Err(Error::Unknown("Table not found".to_string())), 
        }
     }

     pub fn insert_columnar_document_rows(&mut self, table_name: &String, rows:Vec<ColumnarDocumentRecord>) -> Result<()> {
        match self.tables.get_mut(table_name) {
 
         Some(x) =>  {
             x.insert_columnar_document_rows(rows)
         },
         None => Err(Error::Unknown("Table not found".to_string())), 
        }
     }

    pub fn update_columnar_document_row(&mut self, table_name: &String, row: ColumnarDocumentRecord) -> Result<()> {
        match self.tables.get_mut(table_name) {
            Some(x) => x.update_columnar_document_row(row),
            None => Err(Error::Unknown("Table not found".to_string())),
        }
    }

    pub fn update_columnar_document_rows(&mut self, table_name: &String, rows: Vec<ColumnarDocumentRecord>) -> Result<()> {
        match self.tables.get_mut(table_name) {
            Some(x) => x.update_columnar_document_rows(rows),
            None => Err(Error::Unknown("Table not found".to_string())),
        }
    }

    pub fn delete_columnar_document_row(&mut self, table_name: &String, row_id: usize) -> Result<()> {
        match self.tables.get_mut(table_name) {
            Some(x) => x.delete_columnar_document_row(row_id),
            None => Err(Error::Unknown("Table not found".to_string())),
        }
    }

    pub fn delete_columnar_document_rows(&mut self, table_name: &String, row_ids: Vec<usize>) -> Result<()> {
        match self.tables.get_mut(table_name) {
            Some(x) => x.delete_columnar_document_rows(row_ids),
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



// // Modify Database implementation
// impl Database {
//     pub fn transaction(&mut self) -> Result<TransactionGuard> {
//         TransactionGuard::new(&mut self.transaction_manager)
//     }

//     // Transactional operation wrapper
//     fn execute_in_transaction<F, T>(&mut self, txn: &TransactionGuard, op: F) -> Result<T> 
//     where 
//         F: FnOnce() -> Result<T>
//     {
//         match op() {
//             Ok(result) => Ok(result),
//             Err(e) => {
//                 self.transaction_manager.rollback(txn.get_id())?;
//                 Err(e)
//             }
//         }
//     }

//     // Example wrapped operation
//     pub fn insert_document_row_tx(&mut self, txn: &TransactionGuard, 
//                                 table_name: &String, 
//                                 row: DocumentRecord) -> Result<()> {
//         self.execute_in_transaction(txn, || {
//             self.transaction_manager.wal.log_operation(
//                 txn.get_id(), 
//                 Operation::InsertDocument {
//                     table: table_name.clone(),
//                     record: row.clone(),
//                 }
//             )?;
            
//             self.tables.get_mut(table_name)
//                 .ok_or(Error::Unknown("Table not found".to_string()))?
//                 .insert_document_row(row)
//         })
//     }
// }

// // Usage example:
// /*
// let mut db = Database::new("test_db");
// {
//     let txn = db.transaction()?;
    
//     db.insert_document_row_tx(&txn, &table_name, record1)?;
//     db.insert_document_row_tx(&txn, &table_name, record2)?;
    
//     txn.commit()?; // Commits if successful
// } // Automatic rollback if commit wasn't called or if any operation failed
// */