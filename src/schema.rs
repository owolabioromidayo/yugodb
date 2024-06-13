use crate::record::*; 
use crate::types::*; 
use std::collections::HashMap;

#[derive(Clone)]
pub enum Schema {
    Relational(RelationalSchema),
    Nil
}

pub type RelationalSchema = HashMap<String, (RelationalType, bool)>; // nullable in bool 


impl Schema{

    pub fn new() -> Self {
        Schema::Nil
    }
}