use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::thread;

use std::collections::HashMap;
use yugodb::lang::parser;
use yugodb::lang::tokenizer;

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::rc::Rc;
use yugodb::btree::*;
use yugodb::database::*;
use yugodb::dbms::*;
use yugodb::error::*;
use yugodb::lang::ast::*;
use yugodb::lang::interpreter::*;
use yugodb::lang::parser::*;
use yugodb::lang::tokenizer::*;
use yugodb::lang::types::*;
use yugodb::pager::*;
use yugodb::record::*;
use yugodb::schema::*;
use yugodb::table::*;
use yugodb::types::RelationalType;

fn handle_client(mut stream: TcpStream) {
    let mut buffer = [0 as u8; 1024];
    stream.read(&mut buffer).unwrap();

    let mut resp_buffer = [0 as u8; 1024];
    let resp = "Thank you!";

    resp_buffer[..resp.len()].copy_from_slice(resp.as_bytes());

    match std::str::from_utf8(&buffer) {
        Ok(data) => {
            println!("Decoded UTF-8 string: {}", data);
            stream.write(&resp_buffer).unwrap();
        }
        Err(e) => {
            println!("Error decoding byte string: {}", e);
            stream.shutdown(Shutdown::Both).unwrap();
        }
    }
}

fn handle_query(query_str: String, dbms: &mut DBMS) -> Result<Records> {
    //TODO: should think about auth
    let mut tokenizer = Tokenizer::new(&query_str);
    let tokens = tokenizer.scan_tokens().unwrap();
    println!("Tokens: {:?}", tokens);
    let mut tree = Parser::new(tokens);
    let statements = tree.parse();
    println!("\n\n\n Statements: {:?}", statements);

    let mut interpreter = Interpreter::new(statements);
    interpreter.execute(dbms)
}

fn main() {
    let listener = TcpListener::bind("0.0.0.0:3333").unwrap();
    // accept connections and process them, spawning a new thread for each one
    println!("Server listening on port 3333");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("New connection: {}", stream.peer_addr().unwrap());
                thread::spawn(move || handle_client(stream));
            }
            Err(e) => {
                println!("Error: {}", e);
            }
        }
    }
    drop(listener);
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_thousand_rows() {
        //TODO: should create a query handler
        let seq1 = "

        dbs.create_db('test_db');        

        dbs.create_table('test_db' ,'test_rtable', 'RELATIONAL', 'ROW', '{
            'name': 'string(50)',
            'balance': ['numeric', true],
            'pob': 'string',
            'active': 'boolean'
        }');
        ";

        let seq2 = "
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
       

        ";

        let seq3 = "
        let y = dbs.test_db.test_rtable.offset(0);  
        y.limit(50);
        ";

        let mut dbms = DBMS::new();

        println!("{:?}", handle_query(seq1.to_string(), &mut dbms));
        for _ in 0..1000 {
            // println!("{:?}", handle_query(seq2.to_string(), &mut dbms));
            handle_query(seq2.to_string().repeat(1000), &mut dbms);
        }
        println!("{:?}", handle_query(seq3.to_string(), &mut dbms));
    }

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
