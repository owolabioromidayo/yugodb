use crate::database::Database; 
use crate::error::*; 
use crate::lang::types::*; 

use std::collections::HashMap;








// #[cfg(test)]
// mod tests {
//     // use super::*;
//     // use crate::lang::tokenizer::*; 
//     // use crate::lang::parser::*; 
//     // use crate::lang::ast::*;

//     // #[test]
//     // fn test_valid_query() {
//     //     let method_table = MethodTable::new();

//     //     let method_chain = vec![
//     //         ("filter", vec![]),
//     //         ("orderby", vec![]),
//     //         ("select", vec!["a".to_string(), "b".to_string(), "c".to_string(), "d".to_string()]),
//     //     ];

//     //     let mut prev_method = None;
//     //     for (method_name, args) in method_chain {
//     //         match method_table.check_method(method_name, &args, prev_method) {
//     //             Ok(()) => {
//     //                 prev_method = Some(method_name);
//     //             }
//     //             Err(err) => {
//     //                 panic!("Unexpected error: {}", err);
//     //             }
//     //         }
//     //     }
//     // }

//     // #[test]
//     // fn test_invalid_query() {
//     //     let method_table = MethodTable::new();

//     //     let method_chain = vec![
//     //         ("select", vec!["a".to_string(), "b".to_string(), "c".to_string(), "d".to_string()]),
//     //         ("filter", vec![]),
//     //     ];

//     //     let mut prev_method = None;
//     //     for (method_name, args) in method_chain {
//     //         match method_table.check_method(method_name, &args, prev_method) {
//     //             Ok(()) => {
//     //                 prev_method = Some(method_name);
//     //             }
//     //             Err(err) => {
//     //                 println!("{:?}", err);
//     //                 // assert_eq!(
//     //                 //     err,
//     //                 //     "Method 'filter' cannot follow method 'select'.".to_string()
//     //                 // );
//     //                 return;
//     //             }
//     //         }
//     //     }

//     //     // panic!("Expected an error, but none occurred.");
//     // }

//     // #[test]
//     // fn test_missing_argument() {
//     //     let method_table = MethodTable::new();

//     //     let method_chain = vec![
//     //         ("select", vec![]), // Missing arguments
//     //     ];

//     //     let mut prev_method = None;
//     //     for (method_name, args) in method_chain {
//     //         match method_table.check_method(method_name, &args, prev_method) {
//     //             Ok(()) => {
//     //                 prev_method = Some(method_name);
//     //             }
//     //             Err(err) => {
//     //                 println!("{:?}", err);
//     //                 // assert_eq!(
//     //                 //     err,
//     //                 //     "Invalid number of arguments for method 'select'. Expected 1 arguments, but got 0.".to_string()
//     //                 // );
//     //                 return;
//     //             }
//     //         }
//     //     }

//     //     // panic!("Expected an error, but none occurred.");
//     // }


//     #[test]
//     fn test_some_string(){
//         let mut tokenizer = Tokenizer::new("
//         let x = db.TABLES.b.filter().orderby(); 
//         let y = db.TABLES.x ; 
//         x.filter(); 
//         let z = x JOIN y ON id;  
//         z.select(a,b,c,d) ;
//         ");

//         let tokens = tokenizer.scan_tokens().unwrap();
//         println!("Tokens: {:?}", tokens);
//         let mut tree = Parser::new(tokens);
//         let statements = tree.parse();
//         println!("\n\n\n Statements: {:?}", statements);

//         let mut ast = AST::new();
//         ast.generate(statements);
//         println!("\n\n\n Root: {:?}", ast.root);
//         println!("\n\n\n AST Lookup Table: {:?}", ast.lookup_table);
//         println!("\n\n\n AST Processed: {:?}", ast.processed_statements);

//     }
// }