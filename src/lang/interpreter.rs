use std::collections::HashMap;
use std::ops::Deref;

use crate::error::*;
use crate::lang::ast::*;
use crate::lang::types::*;
use crate::record::*;
use crate::record_iterator::*;

//maybe we call this a resolver / unroller kind of thing?
// but then, it doesnt modify anything, just visits
// so lets see about that
// we might not be able to optimize for now then

// so, evaluate actually resolves into some data values at the end
// which means our fetcher might need to be built in here
// we cant optimize in here then

// the interpreter will take the tree and materialize some values then
// so we need the DBMS abstraction in place
// lets mock for now

//so, do we materialize the variable into the dataflow expr?
//its a quick and dirty start for sure

//so, this means we really want to evaluate into a set of records
// which will have to layer on top our our whole multi approach
// so we might need some unified record abstraction enum?

// we are going to need iter_vec in here reall soon
// so we would need some sort of records iterator, instead of just getting it all at once

// we are basically creating another tree with this approach, from what IM seeing
// creating a tree of iterators,

//what if we snowball it into one large iterator of sub iterators each with their own exec functions
// that we define
// so then, we are returning custom record iterators
// think about this

pub struct IterClosure {
    get_next_chunk: Box<dyn Fn() -> Option<Records>>,
}

pub const F_ARGS: HashMap<MethodType, Vec<String>> = HashMap::from([
    (MethodType::OrderBy,),
    (MethodType::GroupBy,),
    (MethodType::Filter,),
    (MethodType::Select,),
    (MethodType::SelectDistinct,),
    (MethodType::Offset,),
    (MethodType::Limit,),
    (MethodType::Max,),
    (MethodType::Min,),
    (MethodType::Sum,),
    (MethodType::Count,),
    (MethodType::CountDistinct,),
    (MethodType::Illegal,),
]);

// fn check_method(
//     method_type: MethodType,
//     args: &Vec<Expr>,
//     prev_method: Option<MethodType>,
// ) -> Result<()> {

//     //handle method arguments
//     if let Some(expected_args) = F_ARGS.get(method_type) {
//         let mut curr: usize = 0;
//         let mut internal_count:usize = 0;
//         for (idx, arg) in args.iter().enumerate() {
//             // walk through the arugments and f_args together
//                 // check that curr value is still in range
//             if curr >= expected_args.len() {
//                 // Err(Error::TypeError)
//                 return Err( Error::TypeError(format!(
//                     "Invalid number of arguments for method '{}'. Expected {:?} arguments, but got {:?} instead \n.",
//                     method_name,
//                     expected_args,
//                     args
//                 ) ) );
//             }
//             // otherwise, we are good
//             let curr_farg = &expected_args[curr];
//             if curr_farg.starts_with("list") {
//                 // internal type
//                 let internal_type = &curr_farg[5..curr_farg.len()-2];
//                 // check internal type
//                 if internal_type == *arg {
//                     internal_count +=1
//                 } else {
//                     // we might be done with this type
//                     if internal_count == 0 {
//                         //we did not capture anything, err
//                         return Err(Error::TypeError(format!("Type mismatch, did not capture any {:?} for {:?} .", curr_farg, method_name)));
//                     }
//                     curr += 1;
//                     // have to check again
//                     if curr >= expected_args.len() {
//                         // Err(Error::TypeError)
//                         return Err( Error::TypeError(format!(
//                             "Invalid number of arguments for method '{}'. Expected {:?} arguments, but got {:?} instead \n.",
//                             method_name,
//                             expected_args,
//                             args
//                         ) ) );
//                     }

//                     // lets handle the new one here then
//                     if expected_args[curr] != *arg {
//                         return Err(Error::TypeError(format!("Type mismatch, expected {:?}, got {:?} instead.", expected_args[curr], arg)));
//                     }
//                     curr +=1 ;
//                 }
//             } else {
//                 // just a normal type then
//                     if curr_farg != arg {
//                         return Err(Error::TypeError(format!("Type mismatch, expected {:?}, got {:?} instead.", curr_farg, arg)));
//                     }
//                     curr +=1 ;
//                 }
//         }
//     }
// }

struct Interpreter {
    //some variables I guess
    // we need a better local state here
    pub variables: HashMap<String, RecordIterator>,
}

// some hard rules need to be set in place
// i.e. no binary or unary expressions allowed for record iterators, they should only need joins and aggregate ops

impl ExprVisitor<Result<RecordIterator>, Result<Literal>> for Interpreter {
    //TOOD: these are key to building up everything else
    // funnily, doesnt come from this, but in a roundabout way comes from the datacall and dataexpr
    // learny your own grammar

    // we should also clone here, better to have some static copy
    fn visit_variable(&mut self, expr: &Variable) -> Result<RecordIterator> {
        // in this situation, we just return the evaluation of the variable.
        // if it is the first time being defined we store, otherwise, we return from the map

        //TODO: a variable should be guaranteed some literal ; its just unwrap for now
        match self.variables.get((&expr.name.literal.clone().unwrap())) {
            Some(y) => Ok(y.clone()),
            None => Err(Error::NotFound(format!(
                "Variable {:?} does not exist",
                expr
            ))),
        }
    }

    // an atrribute could just be some recorditerator with a set of predicates applied to it!
    fn visit_attribute(&mut self, expr: &Attribute) -> Result<RecordIterator> {
        //an attribute is a select statement applied to a variable

        let left = &expr.tokens[0].literal.clone().unwrap();
        if self.variables.contains_key(left) {
            // this only works for things like x.id
            let mut var = self.visit_variable_token(&expr.tokens[0]).unwrap();
            for t in &expr.tokens[1..] {
                var.predicate.select.push(t.literal.clone().unwrap());
            }

            return Ok(var);
        } else {
            // TODO: what about DB.table.X or some shit
            // have to deal with this here
            unimplemented!()
        }
    }

    fn visit_binary(&mut self, expr: &Binary) -> Result<Literal> {
        let mut left = self.evaluate_lower(&expr.left)?;
        let mut right = self.evaluate_lower(&expr.right)?;

        match (&left.value.value_type, &right.value.value_type) {
            (ValueType::Number, ValueType::Number) => {
                if let ValueData::Number(y) = &right.value.value {
                    if let ValueData::Number(z) = left.value.value.clone() {
                        match &expr.operator._type {
                            TokenType::Plus => {
                                left.value.value = ValueData::Number(y + z);
                                Ok(left)
                            }
                            TokenType::Minus => {
                                left.value.value = ValueData::Number(y - z);
                                Ok(left)
                            }
                            TokenType::Star => {
                                left.value.value = ValueData::Number(y * z);
                                Ok(left)
                            }
                            TokenType::Slash => {
                                left.value.value = ValueData::Number(y / z);
                                Ok(left)
                            }
                            l => Err(Error::TypeError(format!(
                                "Token {:?} not suppored in binary operation for numbers",
                                l
                            ))),
                        }
                    } else {
                        Err(Error::TypeError(
                            "Could not retrieve value from expr.left in binary expr".to_string(),
                        ))
                    }
                } else {
                    Err(Error::TypeError(
                        "Could not retrieve value from expr.right in binary expr".to_string(),
                    ))
                }
            }
            (ValueType::String, ValueType::String) => {
                if let ValueData::String(y) = &right.value.value.clone() {
                    if let ValueData::String(z) = left.value.value {
                        match &expr.operator._type {
                            TokenType::Plus => {
                                left.value.value = ValueData::String(z + y);
                                Ok(left)
                            }
                            l => Err(Error::TypeError(format!(
                                "Token {:?} not suppored in binary operation for strings",
                                l
                            ))),
                        }
                    } else {
                        Err(Error::TypeError(
                            "Could not retrieve value from expr.leftin binary expr".to_string(),
                        ))
                    }
                } else {
                    Err(Error::TypeError(
                        "Could not retrieve value from expr.right in binary expr".to_string(),
                    ))
                }
            }
            (ValueType::Boolean, ValueType::Boolean) => {
                if let ValueData::Bool(y) = right.value.value.clone() {
                    if let ValueData::Bool(z) = left.value.value.clone() {
                        // a sensible lang shouldnt allow this, but here we are I guess
                        // might delete later
                        match &expr.operator._type {
                            TokenType::Plus => {
                                left.value.value = ValueData::Bool(y || z);
                                Ok(left)
                            }
                            TokenType::Minus => {
                                left.value.value = ValueData::Bool(y && z);
                                Ok(left)
                            }
                            TokenType::Star => {
                                left.value.value = ValueData::Bool(y && z);
                                Ok(left)
                            }
                            l => Err(Error::TypeError(format!(
                                "Token {:?} not suppored in binary operation for booleans",
                                l
                            ))),
                        }
                    } else {
                        Err(Error::TypeError(
                            "Could not retrieve value from expr.left in binary expr".to_string(),
                        ))
                    }
                } else {
                    Err(Error::TypeError(
                        "Could not retrieve value from expr.right in binary expr".to_string(),
                    ))
                }
            }

            (l, m) => Err(Error::TypeError(format!(
                "Binary operations not supported for types {:?} and {:?}",
                l, m
            ))),
        }
    }

    // }
    fn visit_grouping(&mut self, expr: &Grouping) -> Result<Literal> {
        return self.evaluate_lower(&expr.expression);
    }
    fn visit_literal(&mut self, expr: &Literal) -> Result<Literal> {
        // i doubt this place will be called?
        return Ok(expr.clone());
    }
    fn visit_unary(&mut self, expr: &Unary) -> Result<Literal> {
        match **&expr.right {
            Expr::Literal(x) => match &expr.operator._type {
                TokenType::Plus => Ok(x.clone()),
                TokenType::Minus => match &x.value.value_type {
                    ValueType::Number => {
                        let mut m = x.clone();

                        if let ValueData::Number(y) = &x.value.value {
                            m.value.value = ValueData::Number(-y);
                        }
                        return Ok(m);
                    }
                    _ => Err(Error::Unknown(
                        "Unsupported literal value type!".to_string(),
                    )),
                },
                TokenType::Not => match &x.value.value_type {
                    ValueType::Boolean => {
                        let mut m = x.clone();

                        if let ValueData::Bool(y) = &x.value.value {
                            m.value.value = ValueData::Bool(!y);
                        }
                        return Ok(m);
                    }
                    _ => Err(Error::Unknown(
                        "Unsupported literal value type!".to_string(),
                    )),
                },
                _ => Err(Error::Unknown("unsupported unary operator!".to_string())),
            },
            _ => Err(Error::Unknown("unsupported type in unary expr".to_string())),
        }
    }

    fn visit_assign(&mut self, expr: &Assign) -> Result<RecordIterator> {
        // this is not bound to happen as variable assignments have been taken care of
        // maybe when filter closures and the like are being figured out
        unimplemented!()
    }
    fn visit_logical_expr(&mut self, expr: &Logical) -> Result<Value> {
        // only booleans allowed
        let mut left = self.evaluate_lower(&expr.left)?;
        let mut right = self.evaluate_lower(&expr.right)?;

        match (&left.value.value_type, &right.value.value_type) {
            (ValueType::Boolean, ValueType::Boolean) => {
                if let ValueData::Bool(y) = right.value.value.clone() {
                    if let ValueData::Bool(z) = left.value.value.clone() {
                        match &expr.operator._type {
                            TokenType::Or => {
                                left.value.value = ValueData::Bool(y || z);
                                Ok(left)
                            }
                            TokenType::And => {
                                left.value.value = ValueData::Bool(y && z);
                                Ok(left)
                            }
                            TokenType::Star => {
                                left.value.value = ValueData::Bool(y && z);
                                Ok(left)
                            }
                            l => Err(Error::TypeError(format!(
                                "Token {:?} not supported in logical expr",
                                l
                            ))),
                        }
                    } else {
                        Err(Error::TypeError(
                            "Could not retrieve value from expr.left in logical".to_string(),
                        ))
                    }
                } else {
                    Err(Error::TypeError(
                        "Could not retrieve value from expr.right in logical expr".to_string(),
                    ))
                }
            }

            (l, m) => Err(Error::TypeError(format!(
                "Logical  operations not supported for types {:?} and {:?}",
                l, m
            ))),
        }
    }
    fn visit_data_call(&mut self, expr: &DataCall) -> Result<RecordIterator> {
        // okay, so we evaulte the attr into some record iterator
        let mut left = self.evaluate(&Expr::Attribute(expr.attr.clone())).unwrap();

        // this is where we apply the methods to the iterator
        // method typechecking and application

        // so here, we handle the args

        // we have to evalute the args into some unpacked structure, alongside type information maybe?
        // yeah, before we typecheck

        // the static pass is important I guess, runtime errors are more time-costly than a single check

        for (method, arg) in expr.methods.iter().zip(&expr.arguments) {

            // method checking here
        }

        // remember , besides that, that we have to generate some iter function
        // we are still working on that
        // TODO: maybe create a custom next : Option<dyn Fn() -> Records> on RecordIterator

        return Ok(left);
    }

    fn visit_data_expr(&mut self, expr: &DataExpr) -> Result<RecordIterator> {
        let left = self.evaluate(&expr.left);
        let right = self.evaluate(&expr.right);

        match &expr.join._type {
            TokenType::Ljoin => {
                // here we have to wrory about database types? do we?
                // yeah, records still contains some db type info

                // we have no notion of Fkeys yet
                // fkeys are easy, if they are named appropriately
                // need to extend database tables to support that though
                //TODO: easiest way is to specify the left and right join keys
                // left join, we basically iter and zip where we can (based on id)
                unimplemented!()
            }
            TokenType::Join => {
                // left join, we basically iter and zip where we can (based on id), but we discard non matches
                unimplemented!()
            }
            _ => unimplemented!(),
        }
    }
}

impl StmtVisitor<Result<RecordIterator>> for Interpreter {
    fn visit_print_stmt(&mut self, stmt: &PrintStmt) -> () {
        // println!("Print stmt called!");
        unimplemented!();
    }
    fn visit_expr_stmt(&mut self, stmt: &ExprStmt) -> Result<RecordIterator> {
        // so, what the hell would we do here?
        match &stmt.expression {
            Expr::DataExpr(expr) => self.visit_data_expr(&expr),
            _ => unimplemented!(),
        }
    }

    fn visit_var_stmt(&mut self, stmt: &VarStmt) -> () {
        // unimplemented!();
        // println!("var stmt called!");
    }
}

impl Interpreter {
    // makes more sense to get a clone we can modify for attrs and elsewhere also
    fn visit_variable_token(&mut self, token: &Token) -> Result<RecordIterator> {
        // in this situation, we just return the evaluation of the variable.
        // if it is the first time being defined we store, otherwise, we return from the map

        //TODO: a variable should be guaranteed some literal ; its just unwrap for now
        match self.variables.get(&token.literal.clone().unwrap()) {
            Some(y) => Ok(y.clone()),
            None => Err(Error::NotFound(format!(
                "Variable {:?} does not exist",
                token
            ))),
        }
    }

    pub fn evaluate(&mut self, expr: &Expr) -> Result<RecordIterator> {
        let res = match &expr {
            Expr::Variable(expr) => self.visit_variable(expr),
            Expr::Attribute(expr) => self.visit_attribute(expr),
            Expr::Assign(expr) => self.visit_assign(expr),
            Expr::DataCall(expr) => self.visit_data_call(expr),
            Expr::DataExpr(expr) => self.visit_data_expr(expr),
            _ => Err(Error::TypeError(
                "Expr type not supported in eval higher func".to_string(),
            )),
        };

        res
    }

    pub fn evaluate_lower(&mut self, expr: &Expr) -> Result<Literal> {
        let res = match &expr {
            Expr::Grouping(expr) => self.visit_grouping(expr),
            Expr::Literal(expr) => self.visit_literal(expr),
            Expr::Unary(expr) => self.visit_unary(expr),
            Expr::Logical(expr) => self.visit_logical_expr(expr),
            Expr::Binary(expr) => self.visit_binary(expr),
            _ => Err(Error::TypeError(
                "Expr type not supported in eval lower func".to_string(),
            )),
        };

        res
    }

    pub fn execute_processed_stmt(&self, stmt: &Option<Node>) {
        if let Some(s) = stmt {
            for child in &s.children {
                self.execute_processed_stmt(child)
            }

            //execute the actual statement
        }
    }

    pub fn resolve_variables(&mut self, ast_lookup: &HashMap<String, Node>) {
        for (k, v) in ast_lookup.iter() {
            //resolve the variable into either some value or an IterWrapper kind of thing (with the proper args on it)

            let m = match &v.data {
                NodeData::Join(x) => unimplemented!(),
                NodeData::Source(x) => self.evaluate(&x.source).unwrap(),

                // TODO: something like this could be circular i.e x = x
                NodeData::Variable(x) => unimplemented!(),
                NodeData::Projection(x) => unimplemented!(),
            };

            self.variables.insert(k.to_string(), m);
        }
    }

    pub fn execute(&mut self, ast: AST) -> Result<Records> {
        // resolve all variables first
        self.resolve_variables(&ast.lookup_table);

        //traverse statements from earliest to latest
        self.execute_processed_stmt(&ast.root);

        // right now we have it top down, so we have to recurse and come back I guess

        // what this should do is create the record iterator tree, then we iter while we can?

        unimplemented!()
    }
}
