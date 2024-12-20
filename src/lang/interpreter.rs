use std::borrow::BorrowMut;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::ops::Deref;
use std::rc::Rc;

use crate::btree::*;
use crate::database::*;
use crate::dbms::*;
use crate::error::*;
use crate::lang::ast::*;
use crate::lang::parser::*;
use crate::lang::types::*;
use crate::pager::*;
use crate::record;
use crate::record::*;
use crate::record_iterator::*;
use crate::schema::*;
use crate::table::*;
use crate::time_it;
use std::clone::Clone;
use std::fmt;

use colored::* ;

use std::cell::RefCell;

const DEFAULT_CHUNK_SIZE: usize = 2048;

pub fn get_assign_vars(expr: &Expr) -> Result<Vec<String>> {
    // println!("{:?}", expr);
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
        Expr::Binary(e) => {
            // extract the left and right tokens
            let left = match &*e.left {
                Expr::Unary(l) => l.operator.lexeme.clone(),
                Expr::Variable(l) => l.name.lexeme.clone(),
                _ => unimplemented!(),
            };

            let right = match &*e.right {
                Expr::Unary(l) => l.operator.lexeme.clone(),
                Expr::Variable(l) => l.name.lexeme.clone(),
                _ => unimplemented!(),
            };

            let res = vec![left, right]; // this is fine, right

            Ok(res)
        }
        _ => Err(Error::TypeError(
            "Expr type not supported as a predicate expr".to_string(),
        )),
    }
}

#[derive(Clone)]
pub struct IterClosure {
    //we need this func to mirror our get_next_chunk func thats why, so many things intertwined
    pub get_next_chunk: Rc<dyn Fn(&mut DBMS, &RPredicate) -> Result<Option<Records>>>,
    pub pred: RPredicate,
}

impl fmt::Debug for IterClosure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IterClosure")
            .field("get_next_chunk", &"<closure>")
            .field("pred", &self.pred)
            .finish()
    }
}

// maybe we are going to far with this?
// it should just be some simple assignment expr like a = b
pub struct PredicateClosure {
    // what happens if its only one record, we create another func, this seems brittle
    eval: Box<dyn Fn(&Record, &Record) -> bool>,
}

pub struct Interpreter {
    pub variables: HashMap<String, IterClosure>,
    // pub ast: AST,
    pub statements: Vec<Stmt>,
}

impl ExprVisitor<Result<IterClosure>, Result<Literal>, Result<IterClosure>> for Interpreter {
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

        // return self.visit_variable_token(&expr.name);
        unimplemented!()
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

    //TODO: we could also make it so that single line data calls i.e. x.filter() ; just update the
    //iterclosure? thats too bad actually, because the access to the recorditerator would have already been lost

    // the main point of all these optimizations even, is to think about how much data should be pulled in
    // the first place, but this would only really help in like restricted ranges and columnar storage methods

    fn visit_data_call(&self, expr: &DataCall) -> Result<IterClosure> {
        // we need special support for system level prompts
        // println!("Evaluating datacall {:?}", &expr);

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
            tok => {
                // check if var
                // we've not resolved vars having iterclosures and data calls still returning recorditerators
                if let Ok(k) = self.visit_variable_token(&expr.attr.tokens[0]) {
                    // println!("We got a var closure!");
                    // we want to generate our new iterclosure on top of this, and set the variable

                    // prob here is we dont know if this is an assignmnet operation or not, so we have to do the wasteful return,
                    // which does not solve other problems (just make em do x = x.offset(10) I guess, that works)
                    // redefs should update the map

                    let mut pred = self.generate_predicate(&expr.methods, &expr.arguments)?;

                    let mut k_static = Box::leak(Box::new(k.clone()));

                    // this will happen outside, not here
                    // k_static.pred.add(&pred);

                    return Ok(IterClosure {
                        // higher level predicate will be ignored, hunh, its just for mirroring sake anyways
                        // isnt our whole thesis to algebraically join them, now we neglect em? nah
                        get_next_chunk: Rc::new(|dbms, hpred| {
                            ((*k_static).get_next_chunk)(dbms, &((*k_static).pred.add_ret(&hpred)))
                        }),
                        pred: pred,
                    });
                    // into this
                } else {
                    return Err(Error::Unknown(format!(
                        "Unsupported datacall attr {}. You could be trying to ref a var that doesnt exist.",
                        &tokens[0]
                    )));
                }
            }
        }

        let mut pred = Box::leak(Box::new(
            self.generate_predicate(&expr.methods, &expr.arguments)?,
        ));

        //check the table
        let mut y = RefCell::new(RecordIterator::new(DEFAULT_CHUNK_SIZE, db_name, table_name));

        // hack to help out with the closure lifetime issue
        let mut y_static = Box::leak(Box::new(y));

        Ok(IterClosure {
            get_next_chunk: Rc::new(|dbms, hpred| {
                ((*y_static).borrow_mut()).get_next_chunk(dbms, &pred.add_ret(&hpred))
            }),
            pred: pred.clone(),
        })
    }

    fn visit_data_expr(&self, expr: &DataExpr) -> Result<IterClosure> {
        //expecting data_call JOIN data_call on id1=id2
        // println!("Visting data expr \n");
        let a = self.evaluate_call(&expr.left)?;
        let left = RefCell::new(a);
        // println!("Visting data exp1 \n");
        let right = RefCell::new(self.evaluate_call(&expr.right)?);
        // println!("Visting data exp2 \n");
        let join_vals = get_assign_vars(&expr.join_expr)?;
        // println!("Visting data exp3 \n");

        // println!(
        //     "Join Expr: {:?} ; Join Vals : {:?}",
        //     &expr.join_expr, join_vals
        // );

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
                let lpred = left.borrow().pred.clone();
                let rpred = left.borrow().pred.clone();

                let res = IterClosure {
                    get_next_chunk: Rc::new(move |mut dbms: &mut DBMS, _| {
                        // need some beter way to rep empty results, not this
                        let mut l_chunk = ((left.borrow_mut()).get_next_chunk)(&mut dbms, &lpred)?;
                        let r_chunk = ((right.borrow_mut()).get_next_chunk)(&mut dbms, &rpred)?;

                        // //check the nulls
                        // if r_chunk.() {
                        //     println!("Retrieved nothing from R");
                        //     return Ok(l_chunk);
                        // }
                        // if l_chunk.is_none() {
                        //     println!("Retrieved nothing from L");
                        //     return Ok(None);

                        // }
                        // println!("RCHUNK {:?}, RPRED  {:?}, RIGHT {:?}", &r_chunk, &rpred, right.borrow());

                        //TODO: RIGHT NOW, THIS IS JUST AN INCOMPLETE N^2 algorithm, need all the chunks on the right
                        for (l, r) in l_chunk.iter_mut().zip(r_chunk.iter()) {
                            match (l, r) {
                                (Records::DocumentRows(x), Records::DocumentRows(y)) => {
                                    for a in x.iter_mut() {
                                        for b in y.iter() {
                                            if (a.get_field(&join_vals[0])
                                                == b.get_field(&join_vals[1]))
                                                && a.fields.get(&join_vals[0]).is_some()
                                            {
                                                // zip these two then
                                                // println!("Found something to zip");
                                                a.fields.extend(b.fields.clone());
                                            }
                                            // otherwise, we do nothing. no null fields here, need schema for that
                                        }
                                    }

                                    //hate that I have to clone this
                                    return Ok(Some(Records::DocumentRows(x.clone())));
                                }
                                (Records::DocumentRows(x), Records::RelationalRows(y)) => {
                                    for a in x.iter_mut() {
                                        for b in y.iter() {
                                            if (a.get_field_as_relational(&join_vals[0]).as_ref()
                                                == b.get_field(&join_vals[1]))
                                                && a.get_field(&join_vals[0]).is_some()
                                            {
                                                // zip these two then
                                                for (k, v) in b.fields.iter() {
                                                    a.fields
                                                        .insert(k.clone(), v.to_document_value());
                                                }
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
                    pred: RPredicate::new(),
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
    // pub fn new(ast: AST) -> Self {
    //     Self {
    //         ast: ast,
    //         variables: HashMap::new(),
    //     }
    // }

    pub fn new(stmts: Vec<Stmt>) -> Self {
        Self {
            statements: stmts,
            variables: HashMap::new(),
        }
    }

    pub fn generate_predicate(
        &self,
        methods: &Vec<MethodType>,
        arguments: &Vec<Vec<Expr>>,
    ) -> Result<RPredicate> {
        let mut pred = RPredicate::new();

        for (method, args) in methods.iter().zip(arguments) {
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
                            pred.limit = Some(*x as usize);
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
                            pred.offset = Some(*x as usize);
                        }
                        _ => {
                            return Err(Error::TypeError(
                                "Offset method requires 1 INTEGER input only.".to_string(),
                            ))
                        }
                    }
                }
                MethodType::Select => {}
                //support for more things, creata_db, create_table, insert, delete
                _ => unimplemented!(),
            }
        }
        Ok(pred)
    }

    // makes more sense to get a clone we can modify for attrs and elsewhere also
    fn visit_variable_token(&self, token: &Token) -> Result<IterClosure> {
        //TODO: a variable should be guaranteed some literal ; its just unwrap for now
        // match self.ast.lookup_table.get(&token.literal.clone().unwrap()) {
        //     //resolve variable into only IterClosure for now (recorditerator would be better for optimization)

        //     // we should be able to deal with attrs like db.TABLES.x , or do we enforce something like .offset(0) for now?
        //     // this way seems like something I should get able to execute rn.
        //     Some(y) => {
        //         let m = match &y.data {
        //             NodeData::Join(x) => {
        //                 println!("Visiting variable {:?} ", x.dataexpr);
        //                 self.evaluate(&Expr::DataExpr(x.dataexpr.clone()))
        //             }
        //             NodeData::Source(x) => self.evaluate(&x.source),

        //             // TODO: something like this could be circular i.e x = x
        //             NodeData::Variable(x) => unimplemented!(),
        //             NodeData::Projection(x) => unimplemented!(),
        //         };
        //         m
        //     }
        //     None => Err(Error::NotFound(format!(
        //         "Variable {:?} does not exist",
        //         token
        //     ))),
        // }
        match self.variables.get(&token.literal.clone().unwrap()) {
            Some(y) => Ok(y.clone()),
            None => Err(Error::NotFound(format!(
                "Variable {:?} does not exist",
                token
            ))),
        }
    }

    pub fn evaluate(&self, expr: &Expr) -> Result<IterClosure> {
        let res = match &expr {
            Expr::Variable(expr) => self.visit_variable(expr),
            // Expr::Attribute(expr) => self.visit_(expr),
            Expr::Assign(expr) => self.visit_assign(expr),
            Expr::DataCall(expr) => self.visit_data_call(expr),
            Expr::DataExpr(expr) => self.visit_data_expr(expr),
            _ => Err(Error::TypeError(
                "Expr type not supported in eval higher func".to_string(),
            )),
        };

        res
    }

    // pub fn evaluate_predicate(&mut self, expr: &Expr) -> Result<PredicateClosure> {

    // }

    pub fn visit_dbms_call_or_pass(&self, dbms: &mut DBMS, expr: &Expr) -> Result<()> {
        //lot of err information is missed by doing this bool thing, should change later
        // need to rework it so we can calssify the error type outside, know if real failure or not
        // lotta things need refactoring

        // so we have DBMSCall, TypeError, then everything else should be unknown

        match expr {
            Expr::DataCall(e) => {
                // println!("Evaluating dbms operation {:?}", &e);

                // let mut db_name = String::new();
                // let mut table_name = String::new();

                // we are expecting dbs.db_name.table_name.transformation()...
                let tokens: Vec<String> = e.attr.tokens.iter().map(|x| x.lexeme.clone()).collect();

                //should be at least one val right
                match tokens[0].as_str() {
                    "dbs" => {
                        if e.methods.len() != 1 {
                            return Err(Error::Unknown(("not a dbms call".to_string())));
                        }

                        let mut resolved_args: Vec<Literal> = Vec::new();
                        for arg in &e.arguments[0] {
                            match self.evaluate_lower(arg) {
                                Ok(x) => resolved_args.push(x),
                                Err(e) => {
                                    return Err(Error::Unknown(
                                        ("unresolvable argument error".to_string()),
                                    ))
                                }
                            }
                        }

                        match &e.methods[0] {
                            MethodType::CreateDB => {
                                if resolved_args.len() != 1 {
                                    return Err(Error::TypeError(
                                        "create_db(name: string).".to_string(),
                                    ));
                                }
                                match &resolved_args[0].value.value {
                                    ValueData::String(x) => {
                                        //create db if not exists.
                                        if dbms.databases.contains_key(x) {
                                            return Err(Error::Unknown(
                                                "Database name already exists".to_string(),
                                            ));
                                        }
                                        let db = Database::new(x.clone());
                                        dbms.databases.insert(x.clone(), db);
                                        println!("New database created!");
                                        return Ok(());
                                    }
                                    _ => {
                                        return Err(Error::TypeError(
                                            "name should be STRING for create_db(name: string) ."
                                                .to_string(),
                                        ))
                                    }
                                }
                            }
                            MethodType::CreateTable => {
                                //holup, are we using strings for this shit?
                                if resolved_args.len() < 4 {
                                    return Err(Error::TypeError(
                                    "create_table(db_name:string, table_name: string, DOCUMENT | RELATIONAL, ROW | COLUMN, schema: json_string (OPTIONAL)).".to_string(),
                                ));
                                }

                                if let (
                                    ValueData::String(db_name),
                                    ValueData::String(table_name),
                                    ValueData::String(table_type),
                                    ValueData::String(storage_model),
                                ) = (
                                    &resolved_args[0].value.value,
                                    &resolved_args[1].value.value,
                                    &resolved_args[2].value.value,
                                    &resolved_args[3].value.value,
                                ) {
                                    //try fetch db
                                    let mut db = match dbms.databases.get_mut(db_name) {
                                        Some(x) => x,
                                        _ => {
                                            return Err(Error::DBMSCall(
                                                "Database does not exist".to_string(),
                                            ))
                                        }
                                    };

                                    let _type = match table_type.as_str().trim() {
                                        "DOCUMENT" => TableType::Document,
                                        "RELATIONAL" => TableType::Relational,
                                        _ => {
                                            return Err(Error::TypeError(
                                                "unsupported table type".to_string(),
                                            ))
                                        }
                                    };

                                    let storage = match storage_model.as_str().trim() {
                                        "ROW" => StorageModel::Row,
                                        "COLUMN" => StorageModel::Column,
                                        _ => {
                                            return Err(Error::TypeError(
                                                "unsupported storage model".to_string(),
                                            ))
                                        }
                                    };

                                    if matches!(_type, TableType::Relational) {
                                        // we expect a schema
                                        if resolved_args.len() >= 5 {
                                            if let ValueData::String(schema_str) =
                                                &resolved_args[4].value.value
                                            {
                                                // we should json deser it and match, pain
                                                let schema = parse_json_to_relational_schema(&schema_str)?;
                                                // println!("SChema gotten {:?}", schema);

                                                let new_table = Table {
                                                    name: table_name.to_string(),
                                                    schema: Schema::Relational(schema),
                                                    _type: _type,
                                                    storage_method: storage,
                                                    pager: Rc::new(RefCell::new(Pager::new(
                                                        format!("{}-{}", db_name, table_name),
                                                    ))),

                                                    //fix this, should get available page
                                                    curr_page_id: 0,
                                                    curr_row_id: 0,
                                                    page_index: HashMap::new(),
                                                    // default_index: BPTreeInternalNode::new(),
                                                    default_index: BTreeMap::new(),
                                                    indexes: HashMap::new(),
                                                };
                                                db.tables.insert(table_name.clone(), new_table);
                                                println!(
                                                    "\n\nNew table created! : {} in {} \n\n ",
                                                    table_name, db_name
                                                );

                                                return Ok(());
                                            } else {
                                                return Err(Error::TypeError(
                                                    "Schema field should be a JSON string"
                                                        .to_string(),
                                                ));
                                            }
                                        }
                                        return Err(Error::DBMSCall(
                                            "Schema field expected for relational table creation"
                                                .to_string(),
                                        ));
                                    } else {
                                        let new_table = Table {
                                            name: table_name.to_string(),
                                            schema: Schema::new(),
                                            _type: _type,
                                            storage_method: storage,
                                            pager: Rc::new(RefCell::new(Pager::new(format!(
                                                "{}-{}",
                                                db_name, table_name
                                            )))),
                                            curr_page_id: 0,
                                            curr_row_id: 0,
                                            page_index: HashMap::new(),
                                            // default_index: BPTreeInternalNode::new(),
                                            default_index: BTreeMap::new(),
                                            indexes: HashMap::new(),
                                        };
                                        db.tables.insert(table_name.clone(), new_table);
                                        println!(
                                            "\n\nNew table created! : {} in {} \n\n ",
                                            table_name, db_name
                                        );
                                    }

                                    Ok(())
                                } else {
                                    return Err(Error::DBMSCall(
                                        "Unsupported values for create_table method".to_string(),
                                    ));
                                }
                            }
                            //TODO: we need to be able to batch multiple calls of this
                            MethodType::Insert => {
                                //holup, are we using strings for this shit?
                                if resolved_args.len() != 3 {
                                    return Err(Error::TypeError(
                                    "insert(db_name:string, table_name: string, record: json_string).".to_string(),
                                ));
                                }

                                if let (
                                    ValueData::String(db_name),
                                    ValueData::String(table_name),
                                    ValueData::String(record_str),
                                ) = (
                                    &resolved_args[0].value.value,
                                    &resolved_args[1].value.value,
                                    &resolved_args[2].value.value,
                                ) {
                                    //try fetch db
                                    let db = match dbms.databases.get_mut(db_name) {
                                        Some(x) => x,
                                        _ => {
                                            return Err(Error::DBMSCall(
                                                "Database does not exist".to_string(),
                                            ))
                                        }
                                    };

                                    //try fetch table
                                    let table = match db.tables.get(table_name) {
                                        Some(x) => x,
                                        _ => {
                                            return Err(Error::DBMSCall(
                                                "Table does not exist".to_string(),
                                            ))
                                        }
                                    };

                                    match &table._type {
                                        TableType::Document => {
                                            match parse_json_to_document_record(record_str.as_str())
                                            {
                                                Ok(record) => {
                                                    let _ =
                                                        db.insert_document_row(table_name, record);
                                                    return Ok(());
                                                }
                                                Err(e) => return Err(e),
                                            }
                                        }
                                        TableType::Relational =>{
                                            match &table.schema {
                                                Schema::Relational(schema) => { 
                                                    match parse_json_to_relational_record(record_str.as_str(), schema )
                                                    {
                                                        Ok(record) => {
                                                            let _ =
                                                                db.insert_relational_row(table_name, record);
                                                            return Ok(());
                                                        }
                                                        Err(e) => return Err(e),
                                                    }
                                                } ,
                                                _  =>  return Err(Error::TypeError("Invalid schema type for relational db".to_string()))
                                            }
                                        }
                                    }

                                    Ok(())
                                } else {
                                    return Err(Error::DBMSCall(
                                        "Unsupported argument values for insert method".to_string(),
                                    ));
                                }
                            }

                            //TODO: method types update and delete
                             
                             MethodType::Update => {   
                                if resolved_args.len() != 3 {
                                    return Err(Error::TypeError(
                                    "update(db_name:string, table_name: string, record: json_string).".to_string(),
                                ));
                                }

                                if let (
                                    ValueData::String(db_name),
                                    ValueData::String(table_name),
                                    ValueData::String(record_str),
                                ) = (
                                    &resolved_args[0].value.value,
                                    &resolved_args[1].value.value,
                                    &resolved_args[2].value.value,
                                ) {
                                    //try fetch db
                                    let db = match dbms.databases.get_mut(db_name) {
                                        Some(x) => x,
                                        _ => {
                                            return Err(Error::DBMSCall(
                                                "Database does not exist".to_string(),
                                            ))
                                        }
                                    };

                                    //try fetch table
                                    let table = match db.tables.get(table_name) {
                                        Some(x) => x,
                                        _ => {
                                            return Err(Error::DBMSCall(
                                                "Table does not exist".to_string(),
                                            ))
                                        }
                                    };

                                    match &table._type {
                                        TableType::Document => {
                                            match parse_json_to_document_record(record_str.as_str())
                                            {
                                                Ok(record) => {
                                                    let _ =
                                                        db.update_document_row(table_name, record);
                                                    return Ok(());
                                                }
                                                Err(e) => return Err(e),
                                            }
                                        }
                                        _ =>{
                                            unimplemented!()
                                        }
                                    }

                                    // Ok(())
                                } else {
                                    return Err(Error::DBMSCall(
                                        "Unsupported argument values for update method".to_string(),
                                    ));
                                }
                            }

                            MethodType::Delete => {   
                                if resolved_args.len() != 3 {
                                    return Err(Error::TypeError(
                                    "update(db_name:string, table_name: string, record_id: number).".to_string(),
                                ));
                                }

                                if let (
                                    ValueData::String(db_name),
                                    ValueData::String(table_name),
                                    ValueData::Number(record_id),
                                ) = (
                                    &resolved_args[0].value.value,
                                    &resolved_args[1].value.value,
                                    &resolved_args[2].value.value,
                                ) {
                                    //try fetch db
                                    let db = match dbms.databases.get_mut(db_name) {
                                        Some(x) => x,
                                        _ => {
                                            return Err(Error::DBMSCall(
                                                "Database does not exist".to_string(),
                                            ))
                                        }
                                    };

                                    //try fetch table
                                    let table = match db.tables.get(table_name) {
                                        Some(x) => x,
                                        _ => {
                                            return Err(Error::DBMSCall(
                                                "Table does not exist".to_string(),
                                            ))
                                        }
                                    };

                                    match &table._type {
                                        TableType::Document => { 
                                            return db.delete_document_row(table_name, *record_id as usize);
                                        
                                        }
                                        _ =>{
                                            unimplemented!()
                                        }
                                    }

                                    // Ok(())
                                } else {
                                    return Err(Error::DBMSCall(
                                        "Unsupported argument values for delete method".to_string(),
                                    ));
                                }
                            }


                            MethodType::InsertMany => {
                                
                                if resolved_args.len() != 3 {
                                    return Err(Error::TypeError(
                                    "insert(db_name:string, table_name: string, record: json_string).".to_string(),
                                ));
                                }

                                if let (
                                    ValueData::String(db_name),
                                    ValueData::String(table_name),
                                    ValueData::String(record_str),
                                ) = (
                                    &resolved_args[0].value.value,
                                    &resolved_args[1].value.value,
                                    &resolved_args[2].value.value,
                                ) {
                                    //try fetch db
                                    let db = match dbms.databases.get_mut(db_name) {
                                        Some(x) => x,
                                        _ => {
                                            return Err(Error::DBMSCall(
                                                "Database does not exist".to_string(),
                                            ))
                                        }
                                    };

                                    //try fetch table
                                    let table = match db.tables.get(table_name) {
                                        Some(x) => x,
                                        _ => {
                                            return Err(Error::DBMSCall(
                                                "Table does not exist".to_string(),
                                            ))
                                        }
                                    };

                                    match &table._type {
                                        TableType::Document => {
                                            match parse_json_to_document_records(record_str.as_str())
                                            {
                                                Ok(records) => {
                                                    time_it!("Insertin document rows", {
                                                    db.insert_document_rows(table_name, records)?;
                                                    });
                                                }
                                                Err(e) => return Err(e),
                                            }
                                        }
                                        TableType::Relational =>{
                                            //TODO: unimplemented
                                            match &table.schema {
                                                Schema::Relational(schema) => { 
                                                    match parse_json_to_relational_record(record_str.as_str(), schema )
                                                    {
                                                        Ok(record) => {
                                                            let _ =
                                                                db.insert_relational_row(table_name, record);
                                                            return Ok(());
                                                        }
                                                        Err(e) => return Err(e),
                                                    }
                                                } ,
                                                _  =>  return Err(Error::TypeError("Invalid schema type for relational db".to_string()))
                                            }
                                        }
                                    }

                                    Ok(())
                                } else {
                                    return Err(Error::DBMSCall(
                                        "Unsupported argument values for insert method".to_string(),
                                    ));
                                }
                            }
                            _ => {
                                return Err(Error::Unknown(
                                    "Method is not a db system call".to_string(),
                                ))
                            }
                        }
                    }
                    tok => {
                        return Err(Error::Unknown(
                            "Method is not a db system call (does not start with dbs)".to_string(),
                        ));
                    }
                }
            }
            _ => {
                return Err(Error::Unknown(
                    "Method is not a db system call (not a datacall expr=)".to_string(),
                ))
            }
        }
    }

    pub fn evaluate_call(&self, expr: &Expr) -> Result<IterClosure> {
        let res = match &expr {
            Expr::DataCall(expr) => self.visit_data_call(expr),
            Expr::Attribute(expr) => self.visit_variable_token(&expr.tokens[0]),
            _ => Err(Error::TypeError(
                "Expr type not supported in eval datacall func".to_string(),
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

    pub fn execute_processed_stmt(
        &self,
        stmt: &Option<Node>,
        is_root: bool,
    ) -> Option<Result<IterClosure>> {
        // this is where we roll it all togther, think about how we link to the children.
        // yk what, just defining variables solves our chaining issues

        if let Some(s) = stmt {
            if s.children.len() == 1 {
                //traverse statements from earliest to latest
                // execute its child ( create the var iterclosure that might be propagated here)
                self.execute_processed_stmt(&s.children[0], false);

                //execute the actual statement

                // this is going to be wasted computation for non root nodes
                if is_root {
                    match &s.data {
                        NodeData::Source(x) => {
                            return Some(self.evaluate(&x.source));
                        }
                        NodeData::Join(x) => {
                            let iter_closure = self.visit_data_expr(&x.dataexpr).unwrap();
                            return Some(Ok(iter_closure));
                        }
                        NodeData::Projection(x) => {
                            let iter_closure =
                                self.evaluate(&Expr::DataCall((x.expr).clone())).unwrap();
                            return Some(Ok(iter_closure));
                        }
                        NodeData::Variable(x) => {
                            unimplemented!()
                        }
                        _ => {
                            unimplemented!()
                        }
                    }
                } else {
                    // match &s.data {
                    //     NodeData::Variable(x) => {
                    //         match
                    //     }
                    //     _ => {
                    //         unimplemented!()
                    //     }
                    // }
                    return None;
                }
            } else {
                return Some(Err(Error::Unknown(
                    "Was not expecting multiple chidlren!".to_string(),
                )));
            }
        } else {
            return None; //empty node brings nothing
        }
        // unimplemented!()
    }

    pub fn execute(&mut self, dbms: &mut DBMS) -> Result<Records> {
        // resolve all variables first
        // for (name, stmt) in self.ast.lookup_table.iter() {
        //    match stmt._type {
        //         NodeType::Source => {
        //             match &stmt.data {
        //                 NodeData::Source(x) => {
        //                     match &x.source {
        //                         Expr::DataCall(x) => {
        //                             self.variables.insert(name.clone(),  self.visit_data_call(x)?);
        //                         },
        //                     _ => panic!("Unsupported type 3")
        //                     }
        //                 },
        //                 _ => panic!("Unsupported type 2")
        //             }
        //         },
        //         _ => panic!("Unsupported type")
        //     }
        // }

        // i mean we can just do everything here, since its linear
        // oh, we cant do that, no , we can
        // when all the evaluations are set and done, we just pull from the final expr
        if let Some((root, stmts)) = self.statements.split_last() {
            for stmt in stmts {
                match stmt {
                    Stmt::Var(s) => {
                        self.variables
                            .insert(s.name.lexeme.clone(), self.evaluate(&s.initializer)?);
                    }
                    Stmt::Expression(s) => {
                        // lets try the dbms cal lhere

                        //try evaluate as DBMS call
                        match self.visit_dbms_call_or_pass(dbms, &s.expression) {
                            Ok(_) => (),
                            Err(e) => match e {
                                Error::TypeError(_) => return Err(e),
                                Error::DBMSCall(_) => return Err(e),
                                e => {
                                    println!("{:?}", e); // other errors like unknown are allowed to roam free
                                                         //if none of the above happens, then just execute it as a normal expression
                                    self.evaluate(&s.expression)?;
                                }
                            },
                        }
                    }
                    _ => unimplemented!(),
                }
            }

            match root {
                Stmt::Expression(s) => {
                    match self.visit_dbms_call_or_pass(dbms, &s.expression) {
                        Ok(_) =>  {
                            return Ok(Records::DocumentRows(Vec::new()));
                        },
                        Err(e) => match e {
                            Error::TypeError(_) => return Err(e),
                            Error::DBMSCall(_) => return Err(e),
                            e => {
                                println!("{:?}", e); // other errors like unknown are allowed to roam free
                                                        //if none of the above happens, then just execute it as a normal expression
                                self.evaluate(&s.expression)?;
                            }
                        },
                    }
                    let iter_closure = self.evaluate(&s.expression)?;
                    let mut records: Records;
                    let empty_pred = RPredicate::new();

                    // get initial chunk, for type matching
                    match (iter_closure.get_next_chunk)(dbms, &empty_pred)? {
                        Some(chunk) => {
                            // println!("We got a chunk: {:?}", &chunk);
                            records = chunk;
                        }
                        None => return Ok(Records::DocumentRows(Vec::new())),
                    }

                    loop {
                        match (iter_closure.get_next_chunk)(dbms, &empty_pred)? {
                            Some(chunk) => {
                                // println!("We got a chunk: {:?}", &chunk);
                                match chunk {
                                    Records::DocumentRows(x) => {
                                        if (x.len() == 0) {
                                            break;
                                        }
                                        match records {
                                            Records::DocumentRows(ref mut y) => {
                                                y.extend(x);
                                            }
                                            _ => unimplemented!(), //mismatched types
                                        }
                                    }
                                    Records::RelationalRows(x) => {
                                        if (x.len() == 0) {
                                            break;
                                        }
                                        match records {
                                            Records::RelationalRows(ref mut y) => {
                                                y.extend(x);
                                            }
                                            _ => unimplemented!(), //mismatched types
                                        }
                                    }
                                    Records::DocumentColumns(x) => {
                                        if (x.len() == 0) {
                                            break;
                                        }
                                        match records {
                                            Records::DocumentColumns(ref mut y) => {
                                                y.extend(x);
                                            }
                                            _ => unimplemented!(), //mismatched types
                                        }
                                    }
                                }
                            }
                            None => break,
                        }
                    }

                    return Ok(records);
                }
                _ => panic!("Unsupported projection node"),
            }
        }
        return Err(Error::Unknown("No statements to execute".to_string()));
    }
}
