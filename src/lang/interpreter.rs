use std::collections::HashMap;
use std::ops::Deref;

use crate::database::*;
use crate::dbms::*;
use crate::error::*;
use crate::lang::ast::*;
use crate::lang::types::*;
use crate::pager::*;
use crate::record::*;
use crate::record_iterator::*;
use crate::table::*;
use std::clone::Clone;
use std::fmt;

use std::cell::RefCell;

const DEFAULT_CHUNK_SIZE: usize = 32;
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

//TODO: record iterator with a predicate is much easier to work over than the iterclosure
// iterclosure should be for joins, as the final eval thing, record iterator should be for datacall

// TODO: lets work backward from the predicate function?

pub fn get_assign_vars(expr: &Expr) -> Result<Vec<String>> {
    match &expr {
        Expr::Assign(e) => {
            // think about it, this left token is not unwrapped into variable or attr, could be either
            let mut res = vec![e.name.literal.clone().unwrap()]; // this is fine, right

            match &*(e.value) {
                Expr::Attribute(y) => {
                    unimplemented!(); // i think that question is meant to go here actually
                }
                Expr::Variable(y) => {
                    // unimplemented!();  // do we want this?
                    res.push(y.name.literal.clone().unwrap())
                }
                _ => {
                    return Err(Error::TypeError(
                        "Expr type not supported as rval in a predicate expr".to_string(),
                    ))
                }
            }

            Ok(res)
        }
        _ => Err(Error::TypeError(
            "Expr type not supported as a predicate expr".to_string(),
        )),
    }
}

pub struct IterClosure {
    pub get_next_chunk: Box<dyn Fn(&mut DBMS) -> Result<Option<Records>>>,
}

// maybe we are going to far with this?
// it should just be some simple assignment expr like a = b
pub struct PredicateClosure {
    // what happens if its only one record, we create another func, this seems brittle
    eval: Box<dyn Fn(&Record, &Record) -> bool>,
}

struct Interpreter {
    // do we need better local state here? what else?
    pub variables: HashMap<String, IterClosure>,
    pub ast: AST,
}

impl ExprVisitor<Result<IterClosure>, Result<Literal>, Result<RecordIterator>> for Interpreter {
    fn visit_binary(&self, expr: &Binary) -> Result<Literal> {
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
    fn visit_grouping(&self, expr: &Grouping) -> Result<Literal> {
        return self.evaluate_lower(&expr.expression);
    }
    fn visit_literal(&self, expr: &Literal) -> Result<Literal> {
        // i doubt this place will be called?
        return Ok(expr.clone());
    }
    fn visit_unary(&self, expr: &Unary) -> Result<Literal> {
        match &*(expr.right) {
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
                TokenType::Bang => match &x.value.value_type {
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

    fn visit_logical_expr(&self, expr: &Logical) -> Result<Literal> {
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

    fn visit_assign(&self, expr: &Assign) -> Result<IterClosure> {
        // this is not bound to happen as variable assignments have been taken care of
        // maybe when filter closures and the like are being figured out
        unimplemented!()
    }

    // we should also clone here, better to have some static copy
    fn visit_variable(&self, expr: &Variable) -> Result<IterClosure> {
        // in this situation, we just return the evaluation of the variable.
        // if it is the first time being defined we store, otherwise, we return from the map

        //TODO: a variable should be guaranteed some literal ; its just unwrap for now

        return self.visit_variable_token(&expr.name);
        // match self.variables.get((&expr.name.literal.clone().unwrap())) {
        //     Some(y) => Ok(*y),
        //     None => Err(Error::NotFound(format!(
        //         "Variable {:?} does not exist",
        //         expr
        //     ))),
        // }
    }

    // an atrribute could just be some recorditerator with a set of predicates applied to it!

    // TODO: i dont think we need this anymore
    // fn visit_attribute(&mut self, expr: &Attribute) -> Result<IterClosure> {
    //     //an attribute is a select statement applied to a variable

    //     let left = &expr.tokens[0].literal.clone().unwrap();
    //     if self.variables.contains_key(left) {
    //         // this only works for things like x.id
    //         let mut var = self.visit_variable_token(&expr.tokens[0]).unwrap();
    //         for t in &expr.tokens[1..] {
    //             match &var.predicate.select {
    //                 Some(x) => {
    //                     x.push(t.literal.clone().unwrap());
    //                 ()} ,
    //                 None => {
    //                   var.predicate.select =  Some(vec![t.literal.clone().unwrap()]);
    //                   ()
    //                 },
    //             }
    //         }

    //         return Ok(var);
    //     } else {
    //         // TODO: what about DB.table.X or some shit
    //         // have to deal with this here
    //         unimplemented!()
    //     }
    // }

    // i like recorditerator because I can introspect on its predicate, for optimization
    fn visit_data_call(&self, expr: &DataCall) -> Result<RecordIterator> {
        let mut db_name = String::new();
        let mut table_name = String::new();

        // we are expecting dbs.db_name.table_name.transformation()...
        let tokens: Vec<String> = expr.attr.tokens.iter().map(|x| x.lexeme.clone()).collect();

        //should be at least one val right
        match tokens[0].as_str() {
            "dbs" => {
                if tokens.len() >= 3 {
                    db_name = tokens[1].clone();
                    table_name = tokens[2].clone();
                } else {
                    return Err(Error::Unknown(format!(
                        "insufficient datacall attr {:?}",
                        &tokens
                    )));
                }
            }
            _ => {
                return Err(Error::Unknown(format!(
                    "Unsupported datacall attr {}",
                    &tokens[0]
                )))
            }
        }

        //check the table

        let mut pred = RPredicate::new();

        for (method, args) in expr.methods.iter().zip(&expr.arguments) {
            // resolve into lowers
            let mut resolved_args: Vec<Literal> = Vec::new();
            for arg in args {
                match self.evaluate_lower(arg) {
                    Ok(x) => resolved_args.push(x),
                    Err(e) => return Err(e),
                }
            }

            // still need to do the typechecking here
            match method {
                MethodType::Limit => {
                    if resolved_args.len() != 1 {
                        return Err(Error::TypeError(
                            "Limit method requires 1 integer input ONLY.".to_string(),
                        ));
                    }
                    match &resolved_args[0].value.value {
                        ValueData::Number(x) => {
                            pred.offset = Some(*x as usize);
                        }
                        _ => {
                            return Err(Error::TypeError(
                                "Limit method requires 1 INTEGER input only.".to_string(),
                            ))
                        }
                    }
                }
                MethodType::Offset => {
                    if resolved_args.len() != 1 {
                        return Err(Error::TypeError(
                            "Offset method requires 1 integer input ONLY.".to_string(),
                        ));
                    }
                    match &resolved_args[0].value.value {
                        ValueData::Number(x) => {
                            pred.limit = Some(*x as usize);
                        }
                        _ => {
                            return Err(Error::TypeError(
                                "Offset method requires 1 INTEGER input only.".to_string(),
                            ))
                        }
                    }
                }
                _ => unimplemented!(),
            }
        }

        let res = RecordIterator::new(DEFAULT_CHUNK_SIZE, pred, db_name, table_name);
        return Ok(res);
    }

    fn visit_data_expr(&self, expr: &DataExpr) -> Result<IterClosure> {
        //expecting data_call JOIN data_call on id1=id2
        let left = RefCell::new(self.evaluate_call(&expr.left)?);
        let right = RefCell::new(self.evaluate_call(&expr.right)?);

        let join_vals = get_assign_vars(&expr.join_expr)?;

        println!(
            "Join Expr: {:?} ; Join Vals : {:?}",
            &expr.join_expr, join_vals
        );

        match &expr.join._type {
            TokenType::Ljoin => {
                // here we have to wrory about database types? do we?
                // yeah, records still contains some db type info

                // we have no notion of Fkeys yet
                // fkeys are easy, if they are named appropriately
                // need to extend database tables to support that though
                //TODO: easiest way is to specify the left and right join keys
                // left join, we basically iter and zip where we can (based on id)

                // also, we need the join predicate

                let res = IterClosure {
                    get_next_chunk: Box::new(move |mut dbms| {
                        let mut l_chunk = (*left.borrow_mut()).get_next_chunk(&mut dbms)?;
                        let r_chunk = (*right.borrow_mut()).get_next_chunk(&mut dbms)?;

                        //check the nulls
                        if r_chunk.is_none() {
                            return Ok(l_chunk);
                        }
                        if l_chunk.is_none() {
                            return Ok(None);
                        }

                        for (l, r) in l_chunk.iter_mut().zip(r_chunk.iter()) {
                            match (l, r) {
                                (Records::DocumentRows(x), Records::DocumentRows(y)) => {
                                    for (a, b) in x.iter_mut().zip(y) {
                                        if (a.get_field(&join_vals[0])
                                            == b.get_field(&join_vals[1]))
                                            && a.fields.get(&join_vals[0]).is_some()
                                        {
                                            // zip these two then
                                            a.fields.extend(b.fields.clone());
                                        }
                                        // otherwise, we do nothing. no null fields here, need schema for that
                                    }

                                    //hate that I have to clone this
                                    return Ok(Some(Records::DocumentRows(x.clone())));
                                }
                                (Records::DocumentRows(x), Records::RelationalRows(y)) => {
                                    for (a, b) in x.iter_mut().zip(y) {
                                        if (a.get_field_as_relational(&join_vals[0]).as_ref()
                                            == b.get_field(&join_vals[1]))
                                            && a.get_field(&join_vals[0]).is_some()
                                        {
                                            // zip these two then
                                            for (k, v) in b.fields.iter() {
                                                a.fields.insert(k.clone(), v.to_document_value());
                                            }
                                        }
                                        // otherwise, we do nothing. no null fields here, need schema for that
                                    }

                                    return Ok(Some(Records::DocumentRows(x.clone())));
                                }
                                (Records::RelationalRows(x), Records::DocumentRows(y)) => {
                                    // this is the weaker form, should we neglect it?
                                    unimplemented!()
                                }
                                (Records::RelationalRows(x), Records::RelationalRows(y)) => {
                                    // same shit, but construct a new schema?
                                    // or do we just bring it into document form, that would be chill?
                                    // nah
                                    unimplemented!()
                                }
                                _ => {
                                    return Err(Error::TypeError(format!(
                                        "Cannot join {:?} on {:?} yet!",
                                        &l_chunk, &r_chunk
                                    )))
                                }
                            }
                        }
                        return Ok(None);
                    }),
                };
                Ok(res)
            }
            TokenType::Join => {
                // left join, we basically iter and zip where we can (based on id), but we discard non matches
                unimplemented!()
            }
            _ => unimplemented!(),
        }
    }
}

impl StmtVisitor<Result<IterClosure>> for Interpreter {
    fn visit_print_stmt(&mut self, stmt: &PrintStmt) -> () {
        // println!("Print stmt called!");
        unimplemented!();
    }
    fn visit_expr_stmt(&mut self, stmt: &ExprStmt) -> Result<IterClosure> {
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
    fn new(ast: AST) -> Self {
        Self {
            ast: ast,
            variables: HashMap::new(),
        }
    }
    // makes more sense to get a clone we can modify for attrs and elsewhere also
    fn visit_variable_token(&self, token: &Token) -> Result<IterClosure> {
        //TODO: a variable should be guaranteed some literal ; its just unwrap for now
        match self.ast.lookup_table.get(&token.literal.clone().unwrap()) {
            //resolve variable into only IterClosure for now
            Some(y) => {
                let m = match &y.data {
                    NodeData::Join(x) => unimplemented!(),
                    NodeData::Source(x) => Ok(self.evaluate(&x.source).unwrap()),

                    // TODO: something like this could be circular i.e x = x
                    NodeData::Variable(x) => unimplemented!(),
                    NodeData::Projection(x) => unimplemented!(),
                };
                m
            }
            None => Err(Error::NotFound(format!(
                "Variable {:?} does not exist",
                token
            ))),
        }
        // match self.variables.get(&token.literal.clone().unwrap()) {
        //     Some(y) => Ok(*y),
        //     None => Err(Error::NotFound(format!(
        //         "Variable {:?} does not exist",
        //         token
        //     ))),
        // }
    }

    pub fn evaluate(&self, expr: &Expr) -> Result<IterClosure> {
        let res = match &expr {
            Expr::Variable(expr) => self.visit_variable(expr),
            // Expr::Attribute(expr) => self.visit_attribute(expr),
            Expr::Assign(expr) => self.visit_assign(expr),
            Expr::DataCall(expr) => {
                let y = RefCell::new(self.visit_data_call(expr)?);

                // hack to help out with the closure lifetime issue
                let y_static = Box::leak(Box::new(y));

                Ok(IterClosure {
                    get_next_chunk: Box::new(|dbms| (*y_static.borrow_mut()).get_next_chunk(dbms)),
                })
            }
            Expr::DataExpr(expr) => self.visit_data_expr(expr),
            _ => Err(Error::TypeError(
                "Expr type not supported in eval higher func".to_string(),
            )),
        };

        res
    }

    // pub fn evaluate_predicate(&mut self, expr: &Expr) -> Result<PredicateClosure> {

    // }

    pub fn evaluate_call(&self, expr: &Expr) -> Result<RecordIterator> {
        let res = match &expr {
            Expr::DataCall(expr) => self.visit_data_call(expr),
            _ => Err(Error::TypeError(
                "Expr type not supported in eval lower func".to_string(),
            )),
        };

        res
    }

    pub fn evaluate_lower(&self, expr: &Expr) -> Result<Literal> {
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

    // pub fn resolve_variables(&mut self, ast_lookup: &HashMap<String, Node>) {
    //     for (k, v) in ast_lookup.iter() {
    //         //resolve variable into only IterClosure for now

    //         let m = match &v.data {
    //             NodeData::Join(x) => unimplemented!(),
    //             NodeData::Source(x) => self.evaluate(&x.source).unwrap(),

    //             // TODO: something like this could be circular i.e x = x
    //             NodeData::Variable(x) => unimplemented!(),
    //             NodeData::Projection(x) => unimplemented!(),
    //         };

    //         self.variables.insert(k.to_string(), m);
    //     }
    // }

    pub fn execute(&mut self) -> Result<Records> {
        // resolve all variables first
        // self.resolve_variables(&ast.lookup_table);

        //traverse statements from earliest to latest
        self.execute_processed_stmt(&self.ast.root);

        // right now we have it top down, so we have to recurse and come back I guess

        // what this should do is create the record iterator tree, then we iter while we can?

        unimplemented!()
    }
}
