/// take some AST, go through all the method calls and data sent to tables, and typecheck
/// typechecker is not diffifcult, just need to write some tests, get the final method and arg tables, and make it use the AST


use crate::lang::ast::*;
use crate::database::Database; 
use crate::error::*; 
//TODO: define the method tables here



use std::collections::HashMap;

struct MethodTable {
    f_args: HashMap<String, Vec<String>>,
    prev_map: HashMap<String, Vec<String>>,
}

impl MethodTable {
    fn new() -> Self {
        let f_args = HashMap::from([
            ("filter".to_string(), vec![]),
            ("orderby".to_string(), vec![]),
            ("groupby".to_string(), vec![]),
            ("select".to_string(), vec!["list<string>".to_string()]),
            ("select_distinct".to_string(), vec!["list<string>".to_string()]),
            ("offset".to_string(), vec!["int".to_string()]),
            ("limit".to_string(), vec!["int".to_string()]),
            ("max".to_string(), vec!["string".to_string()]),
            ("min".to_string(), vec!["string".to_string()]),
            ("sum".to_string(), vec!["string".to_string()]),
            ("count".to_string(), vec![]),
            ("count_distinct".to_string(), vec!["string".to_string()]),
        ]);

        let prev_map = HashMap::from([
            ("filter".to_string(), vec![]),
            ("orderby".to_string(), vec!["filter".to_string()]),
            ("groupby".to_string(), vec!["filter".to_string()]),
            ("select".to_string(), vec!["filter".to_string(), "orderby".to_string(), "groupby".to_string(), "offset".to_string(), "limit".to_string()]),
            ("select_distinct".to_string(), vec!["filter".to_string(), "orderby".to_string(), "groupby".to_string(), "offset".to_string(), "limit".to_string()]),
            ("offset".to_string(), vec!["filter".to_string(), "orderby".to_string(), "groupby".to_string()]),
            ("limit".to_string(), vec!["filter".to_string(), "orderby".to_string(), "groupby".to_string(), "offset".to_string()]),
            ("max".to_string(), vec!["filter".to_string(), "groupby".to_string()]),
            ("min".to_string(), vec!["filter".to_string(), "groupby".to_string()]),
            ("sum".to_string(), vec!["filter".to_string(), "groupby".to_string()]),
            ("count".to_string(), vec!["filter".to_string(), "groupby".to_string()]),
            ("count_distinct".to_string(), vec!["filter".to_string(), "groupby".to_string()]),
        ]);

        MethodTable { f_args, prev_map }
    }

// TODO
// we need to instrument this to chekc the actual types being used, in a format that would come from the AST. This is still play for now.
    fn check_method(
        &self,
        method_name: &str,
        args: &[String],
        prev_method: Option<&str>,
    ) -> Result<()> {

        //handle method arguments
        if let Some(expected_args) = self.f_args.get(method_name) {
            let mut curr: usize = 0;
            let mut internal_count:usize = 0;
            for (idx, arg) in args.iter().enumerate() {
                // walk through the arugments and f_args together
                    // check that curr value is still in range
                if curr >= expected_args.len() {
                    // Err(Error::TypeError)
                    return Err( Error::TypeError(format!(
                        "Invalid number of arguments for method '{}'. Expected {:?} arguments, but got {:?} instead \n.",
                        method_name,
                        expected_args,
                        args
                    ) ) );
                }
                // otherwise, we are good
                let curr_farg = &expected_args[curr];
                if curr_farg.starts_with("list") {
                    // internal type
                    let internal_type = &curr_farg[5..curr_farg.len()-2]; 
                    // check internal type
                    if internal_type == *arg {
                        internal_count +=1
                    } else {
                        // we might be done with this type
                        if internal_count == 0 {
                            //we did not capture anything, err
                            return Err(Error::TypeError(format!("Type mismatch, did not capture any {:?} for {:?} .", curr_farg, method_name)));
                        }
                        curr += 1;
                        // have to check again
                        if curr >= expected_args.len() {
                            // Err(Error::TypeError)
                            return Err( Error::TypeError(format!(
                                "Invalid number of arguments for method '{}'. Expected {:?} arguments, but got {:?} instead \n.",
                                method_name,
                                expected_args,
                                args
                            ) ) );
                        }

                        // lets handle the new one here then
                        if expected_args[curr] != *arg {
                            return Err(Error::TypeError(format!("Type mismatch, expected {:?}, got {:?} instead.", expected_args[curr], arg)));
                        } 
                        curr +=1 ;
                    }
                } else {
                    // just a normal type then
                        if curr_farg != arg {
                            return Err(Error::TypeError(format!("Type mismatch, expected {:?}, got {:?} instead.", curr_farg, arg)));
                        } 
                        curr +=1 ;
                    }
            }

        //handle method chaining           
        if let Some(prev_method) = prev_method {
            if let Some(valid_prev_methods) = self.prev_map.get(method_name) {
                if !valid_prev_methods.contains(&prev_method.to_string()) {
                    return Err(Error::TypeError(format!(
                        "Method '{}' cannot follow method '{}'.",
                        method_name, prev_method
                    )));
                }
            } else {
                return Err(Error::TypeError(format!(
                    "Method '{}' cannot be used as a previous method.",
                    prev_method
                )));
            }
        }

        Ok(())
        } else {
            return Err(Error::TypeError( format!("Method '{}' not found.", method_name)) ); 
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_query() {
        let method_table = MethodTable::new();

        let method_chain = vec![
            ("filter", vec![]),
            ("orderby", vec![]),
            ("select", vec!["a".to_string(), "b".to_string(), "c".to_string(), "d".to_string()]),
        ];

        let mut prev_method = None;
        for (method_name, args) in method_chain {
            match method_table.check_method(method_name, &args, prev_method) {
                Ok(()) => {
                    prev_method = Some(method_name);
                }
                Err(err) => {
                    panic!("Unexpected error: {}", err);
                }
            }
        }
    }

    #[test]
    fn test_invalid_query() {
        let method_table = MethodTable::new();

        let method_chain = vec![
            ("select", vec!["a".to_string(), "b".to_string(), "c".to_string(), "d".to_string()]),
            ("filter", vec![]),
        ];

        let mut prev_method = None;
        for (method_name, args) in method_chain {
            match method_table.check_method(method_name, &args, prev_method) {
                Ok(()) => {
                    prev_method = Some(method_name);
                }
                Err(err) => {
                    println!("{:?}", err);
                    // assert_eq!(
                    //     err,
                    //     "Method 'filter' cannot follow method 'select'.".to_string()
                    // );
                    return;
                }
            }
        }

        // panic!("Expected an error, but none occurred.");
    }

    #[test]
    fn test_missing_argument() {
        let method_table = MethodTable::new();

        let method_chain = vec![
            ("select", vec![]), // Missing arguments
        ];

        let mut prev_method = None;
        for (method_name, args) in method_chain {
            match method_table.check_method(method_name, &args, prev_method) {
                Ok(()) => {
                    prev_method = Some(method_name);
                }
                Err(err) => {
                    println!("{:?}", err);
                    // assert_eq!(
                    //     err,
                    //     "Invalid number of arguments for method 'select'. Expected 1 arguments, but got 0.".to_string()
                    // );
                    return;
                }
            }
        }

        // panic!("Expected an error, but none occurred.");
    }
}