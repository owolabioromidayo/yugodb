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
    fn test_full_pipeline_with_dbms_calls() {
        let mut tokenizer = Tokenizer::new(
            "

        dbs.create_db('test_db');
        dbs.create_table('test_db' ,'test_table', 'DOCUMENT', 'ROW');

        dbs.create_table('test_db' ,'test_rtable', 'RELATIONAL', 'ROW', '{
            'name': 'string(50)',
            'balance': ['numeric', true],
            'pob': 'string',
            'active': 'boolean'
        }');

        dbs.insert('test_db', 'test_table', '{ 
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
                'name': 'Jane Smith',
                'balance': '2502034304.2332',
                'pob': 'London',
                'active': true
            }');

            dbs.insert('test_db', 'test_rtable', '{
                'name': 'John Doe',
                'balance': '450.2332',
                'pob': 'New York',
                'active': false
            }');
       


        let x = dbs.test_db.test_table.offset(0);  
        let y = dbs.test_db.test_rtable.offset(0);  
        // x.limit(10);
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
}
