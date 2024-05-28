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
use crate::pager::Pager;
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

#[cfg(test)]
mod tests {
    use super::*;

    //TODO: test insert relational row
    //TODO: test one command, seems to err out in that scenario, just add a nil template?

    #[test]
    fn test_full_pipeline() {
        // let mut tokenizer = Tokenizer::new(
        //     "
        // let x = dbs.test_db.test_table.limit(10);
        // // let y = dbs.test_db2.tb2.offset(1).limit(10);
        // // let z = x JOIN y ON id=id;
        // // z.select() ;
        // dbs.test_db.test_table.offset(0).limit(10);
        // ",
        // );
        let mut tokenizer = Tokenizer::new(
            "
        let x = dbs.test_db.test_table.offset(0);  
        x.limit(10);
        ",
        );

        let tokens = tokenizer.scan_tokens().unwrap();
        println!("Tokens: {:?}", tokens);
        let mut tree = Parser::new(tokens);
        let statements = tree.parse();
        println!("\n\n\n Statements: {:?}", statements);

        let mut ast = AST::new();
        ast.generate(statements);
        println!("\n\n\n Root: {:?}", ast.root);
        println!("\n\n\n AST Lookup Table: {:?}", ast.lookup_table);
        println!("\n\n\n AST Processed: {:?}", ast.processed_statements);

        let mut table = Table {
            name: "test_table".to_string(),
            schema: Schema::new(),
            _type: TableType::Document,
            storage_method: StorageModel::Row,
            curr_page_id: 0,
            curr_row_id: 0,
            page_index: HashMap::new(),
            default_index: BPTreeLeafNode::new(),
            // default_index: BPTreeInternalNode::new(),
            indexes: HashMap::new(),
        };

        let record1 = DocumentRecord {
            id: Some(0),
            fields: HashMap::from([
                (
                    "name".to_string(),
                    DocumentValue::String("John Doe".to_string()),
                ),
                ("age".to_string(), DocumentValue::Number(30.0)),
                (
                    "city".to_string(),
                    DocumentValue::String("New York".to_string()),
                ),
                (
                    "address".to_string(),
                    DocumentValue::Object(HashMap::from([
                        (
                            "street".to_string(),
                            DocumentValue::String("123 Main St".to_string()),
                        ),
                        (
                            "zip".to_string(),
                            DocumentValue::String("10001".to_string()),
                        ),
                    ])),
                ),
                (
                    "phone_numbers".to_string(),
                    DocumentValue::Array(vec![
                        DocumentValue::String("123-456-7890".to_string()),
                        DocumentValue::String("987-654-3210".to_string()),
                    ]),
                ),
            ]),
        };

        let record2 = DocumentRecord {
            id: Some(1),
            fields: HashMap::from([
                (
                    "name".to_string(),
                    DocumentValue::String("Jane Smith".to_string()),
                ),
                ("age".to_string(), DocumentValue::Number(25.0)),
                (
                    "city".to_string(),
                    DocumentValue::String("London".to_string()),
                ),
                (
                    "address".to_string(),
                    DocumentValue::Object(HashMap::from([
                        (
                            "street".to_string(),
                            DocumentValue::String("456 High St".to_string()),
                        ),
                        (
                            "zip".to_string(),
                            DocumentValue::String("SW1A 1AA".to_string()),
                        ),
                    ])),
                ),
                (
                    "phone_numbers".to_string(),
                    DocumentValue::Array(vec![DocumentValue::String("020-1234-5678".to_string())]),
                ),
                (
                    "employment".to_string(),
                    DocumentValue::Object(HashMap::from([
                        (
                            "company".to_string(),
                            DocumentValue::String("Acme Inc.".to_string()),
                        ),
                        (
                            "position".to_string(),
                            DocumentValue::String("Software Engineer".to_string()),
                        ),
                        (
                            "start_date".to_string(),
                            DocumentValue::Object(HashMap::from([
                                ("year".to_string(), DocumentValue::Number(2022.0)),
                                ("month".to_string(), DocumentValue::Number(1.0)),
                            ])),
                        ),
                    ])),
                ),
            ]),
        };

        // need to make relational records also

        let mut db = Database::new("test_db".to_string());

        //initialize 10 pages
        for _ in 0..10 {
            db.pager.create_new_page().unwrap();
        }

        db.tables.insert("test_table".to_string(), table);

        let mut dbms = DBMS::new();
        dbms.databases.insert("test_db".to_string(), db);

        // get the table ref back
        let db1: &mut Database = dbms.get_db_mut(&"test_db".to_string()).unwrap();

        let table_name = "test_table".to_string();

        // Insert the first record
        let result1 = db1.insert_document_row(&table_name, record1.clone());
        match result1 {
            Ok(_) => (),
            Err(err) => println!("{:?}", err),
        }
        // assert!(result1.is_ok());

        // Insert the second record
        let result2 = db1.insert_document_row(&table_name, record2.clone());
        assert!(result2.is_ok());

        let table1 = db1.get_table(&"test_table".to_string()).unwrap();
        println!("Table index {:?}", &table1.default_index);
        let page = db1.pager.get_page_forced(table1.curr_page_id).unwrap();

        // println!("{:?}", table1.);

        // Check if the records are inserted correctly
        let document_page = DocumentRecordPage::deserialize(&page.bytes).unwrap();
        assert_eq!(document_page.records.len(), 2);
        assert_eq!(&document_page.records[0], &record1);
        assert_eq!(&document_page.records[1], &record2);

        let mut interpreter = Interpreter::new(ast);
        let res = interpreter.execute(&mut dbms);
        println!("{:?}", res);
    }
}
