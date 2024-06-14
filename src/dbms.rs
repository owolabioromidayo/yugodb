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
use crate::pager::*;
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
    use crate::types::RelationalType;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use std::borrow::BorrowMut;
    use std::cell::RefCell;
    use std::rc::Rc;

    use super::*;

    #[test]
    fn test_full_pipeline_two_document_tables_with_dbms_calls() {
        let mut tokenizer = Tokenizer::new(
            "

        dbs.create_db('test_db');
        dbs.create_table('test_db' ,'test_table', 'DOCUMENT', 'ROW');
        dbs.create_table('test_db' ,'test_rtable', 'DOCUMENT', 'ROW');

        dbs.insert('test_db', 'test_table', '{ 
                'id': 0,
                'name': 'John Doe',
                'age': 30.0,
                'city': 'New York',
                'address': {
                    'street': '123 Main St',
                    'zip': '10001'
                },
                'phone_numbers': [
                    '123-456-7890',
                    '987-654-3210'
                ]
        }');

        dbs.insert('test_db', 'test_table', '{
            'id': 1,
            'name': 'Jane Smith',
            'age': 25.0,
            'city': 'London',
            'address': {
                'street': '456 High St',
                'zip': 'SW1A 1AA'
            },
            'phone_numbers': [
                '020-1234-5678'
            ],
            'employment': {
                'company': 'Acme Inc.',
                'position': 'Software Engineer',
                'start_date': {
                'year': 2022.0,
                'month': 1.0
                }
            }
            }');

        dbs.insert('test_db', 'test_rtable', '{
            'id': 0,
            'name': 'Jane Smith',
            'balance': '1003434343.4445D'
        }');

        dbs.insert('test_db', 'test_rtable', '{
            'id': 1,
            'name': 'John Doe',
            'balance': '92381893.4445D'
        }');


        let x = dbs.test_db.test_table.offset(0);  
        let y = dbs.test_db.test_rtable.offset(0);  
        //x.limit(10);
        // y.limit(10);
        let z  = x LJOIN y ON name=name;
        z.limit(10);

      
        ",
        );

        let tokens = tokenizer.scan_tokens().unwrap();
        println!("Tokens: {:?}", tokens);
        let mut tree = Parser::new(tokens);
        let statements = tree.parse();
        println!("\n\n\n Statements: {:?}", statements);

        let mut dbms = DBMS::new();

        let mut interpreter = Interpreter::new(statements);
        let res = interpreter.execute(&mut dbms);
        println!("{:?}", res);
    }

    //TODO: test insert relational row
    //TODO: test one command, seems to err out in that scenario, just add a nil template?
    #[test]
    fn test_full_pipeline_two_document_tables() {
        let mut tokenizer = Tokenizer::new(
            "
        let x = dbs.test_db.test_table.offset(0);  
        let y = dbs.test_db.test_rtable.offset(0);  
        //x.limit(10);
        // y.limit(10);
        let z  = x LJOIN y ON name=name;
        z.limit(10);
        ",
        );

        let tokens = tokenizer.scan_tokens().unwrap();
        println!("Tokens: {:?}", tokens);
        let mut tree = Parser::new(tokens);
        let statements = tree.parse();
        println!("\n\n\n Statements: {:?}", statements);

        // let mut ast = AST::new();
        // ast.generate(statements);
        // println!("\n\n\n Root: {:?}", ast.root);
        // println!("\n\n\n AST Lookup Table: {:?}", ast.lookup_table);
        // println!("\n\n\n AST Processed: {:?}", ast.processed_statements);

        let mut table = Table {
            name: "test_table".to_string(),
            schema: Schema::new(),
            _type: TableType::Document,
            storage_method: StorageModel::Row,
            curr_page_id: 0,
            curr_row_id: 0,
            page_index: HashMap::new(),
            default_index: BPTreeInternalNode::new(),
            pager: Rc::new(RefCell::new(Pager::new("test_db-test_table".to_string()))),
            // default_index: BPTreeInternalNode::new(),
            indexes: HashMap::new(),
        };

        let mut rtable = Table {
            name: "test_rtable".to_string(),
            schema: Schema::new(),
            _type: TableType::Document,
            storage_method: StorageModel::Row,
            curr_page_id: 1,
            curr_row_id: 0,
            page_index: HashMap::new(),
            default_index: BPTreeInternalNode::new(),
            pager: Rc::new(RefCell::new(Pager::new("test_db-test_rtable".to_string()))),
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

        let rrecord1 = DocumentRecord {
            id: Some(0),
            fields: HashMap::from([
                (
                    "name".to_string(),
                    DocumentValue::String("Jane Smith".to_string()),
                ),
                (
                    "balance".to_string(),
                    DocumentValue::Numeric(dec!(1003434343.4445)),
                ),
            ]),
        };

        let rrecord2 = DocumentRecord {
            id: Some(1),
            fields: HashMap::from([
                (
                    "name".to_string(),
                    DocumentValue::String("John Doe".to_string()),
                ),
                (
                    "balance".to_string(),
                    DocumentValue::Numeric(dec!(92381893.4445)),
                ),
            ]),
        };

        // need to make relational records also

        let mut db = Database::new("test_db".to_string());

        //initialize 10 pages
        // for _ in 0..10 {
        //     match db.pager.try_borrow_mut() {
        //         Ok(mut pager) => {
        //             pager.create_new_page().unwrap();
        //             ()
        //         }
        //         Err(err) => {
        //             println!("{:?}", err)
        //         }
        //     }
        // (*db.pager).borrow_mut().create_new_page().unwrap();
        // }

        db.tables.insert("test_table".to_string(), table);
        db.tables.insert("test_rtable".to_string(), rtable);

        let mut dbms = DBMS::new();
        dbms.databases.insert("test_db".to_string(), db);

        // get the table ref back
        let db1: &mut Database = dbms.get_db_mut(&"test_db".to_string()).unwrap();

        let table_name = "test_table".to_string();
        let rtable_name = "test_rtable".to_string();

        // Insert the first record
        let result1 = db1.insert_document_row(&table_name, record1.clone());
        match result1 {
            Ok(_) => println!("First record inserted!"),
            Err(err) => println!("{:?}", err),
        }
        // assert!(result1.is_ok());

        // Insert the second record
        let result2 = db1.insert_document_row(&table_name, record2.clone());
        match result2 {
            Ok(_) => println!("Second record inserted!"),
            Err(err) => println!("{:?}", err),
        }

        let rresult1 = db1.insert_document_row(&rtable_name, rrecord1.clone());
        match rresult1 {
            Ok(_) => println!("First relational record inserted!"),
            Err(err) => println!("{:?}", err),
        }
        // assert!(result1.is_ok());

        // Insert the second record
        let rresult2 = db1.insert_document_row(&rtable_name, rrecord2.clone());
        match rresult2 {
            Ok(_) => println!("Second relational record inserted!"),
            Err(err) => println!("{:?}", err),
        }
        // assert!(result2.is_ok());

        let table1 = db1.get_table(&"test_table".to_string()).unwrap();
        let table2 = db1.get_table(&"test_rtable".to_string()).unwrap();
        println!("Table index {:?}", &table1.default_index);

        let page = (*table1.pager)
            .borrow_mut()
            .get_page_or_force(table1.curr_page_id)
            .unwrap();

        let rpage = (*table2.pager)
            .borrow_mut()
            .get_page_or_force(table2.curr_page_id)
            .unwrap();

        // println!("{:?}", table1.);

        // Check if the records are inserted correctly
        let document_page =
            DocumentRecordPage::deserialize(&page.borrow().borrow_mut().read_all()).unwrap();
        // assert_eq!(document_page.records.len(), 2);
        assert_eq!(&document_page.records[0], &record1);
        assert_eq!(&document_page.records[1], &record2);

        let rdocuments =
            DocumentRecordPage::deserialize(&rpage.borrow().borrow_mut().read_all()).unwrap();
        // assert_eq!(relational_page.records.len(), 2);
        assert_eq!(&rdocuments.records[0], &rrecord1);
        assert_eq!(&rdocuments.records[1], &rrecord2);

        let mut interpreter = Interpreter::new(statements);
        let res = interpreter.execute(&mut dbms);
        println!("{:?}", res);
    }

    #[test]
    fn test_full_pipeline() {
        let mut tokenizer = Tokenizer::new(
            "
        let x = dbs.test_db.test_table.offset(0);  
        let y = dbs.test_db.test_rtable.offset(0);  
        //x.limit(10);
        // y.limit(10);
        let z  = x JOIN y ON name;
        z.limit(10);
        ",
        );

        let tokens = tokenizer.scan_tokens().unwrap();
        println!("Tokens: {:?}", tokens);
        let mut tree = Parser::new(tokens);
        let statements = tree.parse();
        println!("\n\n\n Statements: {:?}", statements);

        // let mut ast = AST::new();
        // ast.generate(statements);
        // println!("\n\n\n Root: {:?}", ast.root);
        // println!("\n\n\n AST Lookup Table: {:?}", ast.lookup_table);
        // println!("\n\n\n AST Processed: {:?}", ast.processed_statements);

        let mut table = Table {
            name: "test_table".to_string(),
            schema: Schema::new(),
            _type: TableType::Document,
            storage_method: StorageModel::Row,
            curr_page_id: 0,
            curr_row_id: 0,
            page_index: HashMap::new(),
            pager: Rc::new(RefCell::new(Pager::new("test_db-test_table".to_string()))),
            default_index: BPTreeInternalNode::new(),
            // default_index: BPTreeInternalNode::new(),
            indexes: HashMap::new(),
        };

        let rschema: RelationalSchema = HashMap::from([
            ("name".to_string(), (RelationalType::String(50), false)),
            ("balance".to_string(), (RelationalType::Numeric, false)),
        ]);

        let mut rtable = Table {
            name: "test_rtable".to_string(),
            schema: Schema::Relational(HashMap::from([
                ("name".to_string(), (RelationalType::String(50), false)),
                ("balance".to_string(), (RelationalType::Numeric, false)),
            ])),
            _type: TableType::Document,
            storage_method: StorageModel::Row,
            curr_page_id: 1,
            curr_row_id: 0,
            pager: Rc::new(RefCell::new(Pager::new("test_db-test_rtable".to_string()))),
            page_index: HashMap::new(),
            default_index: BPTreeInternalNode::new(),
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

        let rrecord1 = RelationalRecord {
            id: Some(0),
            fields: HashMap::from([
                (
                    "name".to_string(),
                    RelationalValue::String("Jane Smith".to_string()),
                ),
                (
                    "balance".to_string(),
                    RelationalValue::Numeric(dec!(1003434343.4445)),
                ),
            ]),
        };

        let rrecord2 = RelationalRecord {
            id: Some(0),
            fields: HashMap::from([
                (
                    "name".to_string(),
                    RelationalValue::String("John Doe".to_string()),
                ),
                (
                    "balance".to_string(),
                    RelationalValue::Numeric(dec!(92381893.4445)),
                ),
            ]),
        };

        // need to make relational records also

        let mut db = Database::new("test_db".to_string());

        //initialize 10 pages
        // for _ in 0..10 {
        //     match db.pager.try_borrow_mut() {
        //         Ok(mut pager) => {
        //             pager.create_new_page().unwrap();
        //             ()
        //         }
        //         Err(err) => {
        //             println!("{:?}", err)
        //         }
        //     }
        //     // (*db.pager).borrow_mut().create_new_page().unwrap();
        // }

        db.tables.insert("test_table".to_string(), table);
        db.tables.insert("test_rtable".to_string(), rtable);

        let mut dbms = DBMS::new();
        dbms.databases.insert("test_db".to_string(), db);

        // get the table ref back
        let db1: &mut Database = dbms.get_db_mut(&"test_db".to_string()).unwrap();

        let table_name = "test_table".to_string();
        let rtable_name = "test_rtable".to_string();

        // Insert the first record
        let result1 = db1.insert_document_row(&table_name, record1.clone());
        match result1 {
            Ok(_) => println!("First record inserted!"),
            Err(err) => println!("{:?}", err),
        }
        // assert!(result1.is_ok());

        // Insert the second record
        let result2 = db1.insert_document_row(&table_name, record2.clone());
        match result2 {
            Ok(_) => println!("Second record inserted!"),
            Err(err) => println!("{:?}", err),
        }

        let rresult1 = db1.insert_relational_row(&rtable_name, rrecord1.clone());
        match rresult1 {
            Ok(_) => println!("First relational record inserted!"),
            Err(err) => println!("{:?}", err),
        }
        // assert!(result1.is_ok());

        // Insert the second record
        let rresult2 = db1.insert_relational_row(&rtable_name, rrecord2.clone());
        match rresult2 {
            Ok(_) => println!("Second relational record inserted!"),
            Err(err) => println!("{:?}", err),
        }
        // assert!(result2.is_ok());

        let table1 = db1.get_table(&"test_table".to_string()).unwrap();
        let table2 = db1.get_table(&"test_rtable".to_string()).unwrap();
        println!("Table index {:?}", &table1.default_index);

        let page = (*table1.pager)
            .borrow_mut()
            .get_page_or_force(table1.curr_page_id)
            .unwrap();

        let rpage = (*table2.pager)
            .borrow_mut()
            .get_page_or_force(table2.curr_page_id)
            .unwrap();

        // println!("{:?}", table1.);

        // Check if the records are inserted correctly
        let document_page =
            DocumentRecordPage::deserialize(&page.borrow().borrow_mut().read_all()).unwrap();
        // assert_eq!(document_page.records.len(), 2);
        assert_eq!(&document_page.records[0], &record1);
        assert_eq!(&document_page.records[1], &record2);

        let relational_page =
            RelationalRecordPage::deserialize(&rpage.borrow().borrow_mut().read_all(), &rschema)
                .unwrap();
        // assert_eq!(relational_page.records.len(), 2);
        assert_eq!(&relational_page.records[0], &rrecord1);
        assert_eq!(&relational_page.records[1], &rrecord2);

        // let mut interpreter = Interpreter::new(ast);
        let mut interpreter = Interpreter::new(statements);
        let res = interpreter.execute(&mut dbms);
        println!("{:?}", res);
    }
}
