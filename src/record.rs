// This should be general enough for our purposes
use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use crate::types::*;
use crate::error::*;


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentRecord {
    pub id: Option<usize>, // is usize large enough?
    pub fields: HashMap<String, DocumentValue>,
}

//TODO: think about BSON later maybe
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DocumentValue {
    Null,
    Boolean(bool),
    Number(f64),
    String(String),
    Array(Vec<DocumentValue>),
    Object(HashMap<String, DocumentValue>),
}

impl DocumentRecord {
    pub fn new() -> Self {
        DocumentRecord {
            id: None,
            fields: HashMap::new(),
        }
    }

    pub fn with_id(id: usize) -> Self {
        DocumentRecord {
            id: Some(id),
            fields: HashMap::new(),
        }
    }

    pub fn set_id(&mut self, id: usize) {
        self.id = Some(id);
    }

    pub fn get_id(&self) -> Option<usize> {
        self.id.clone() //TODO: is this bad?
    }

    pub fn set_field(&mut self, key: String, value: DocumentValue) {
        self.fields.insert(key, value);
    }

    pub fn get_field(&self, key: &str) -> Option<&DocumentValue> {
        self.fields.get(key)
    }

    pub fn remove_field(&mut self, key: &str) {
        self.fields.remove(key);
    }

    pub fn serialize(&self ) -> Result<String>{
         match serde_json::to_string(&self) {
            Ok(res) => return Ok(res),
            Err(err) => return Err(Error::SerdeError)
         }
         
    }

    pub fn deserialize(s: &str) -> Result<Self>{
         match serde_json::from_str(&s) {
            Ok(res) => return Ok(res),
            Err(err) => return Err(Error::SerdeError)
         }
    }
}



// this shouldnt be hard to do given the schema, right? just convert to byte array and append, return fixed length, boom!

#[derive(Debug, Clone)]
pub struct RelationalRecord {
    pub id: Option<usize>, // is usize large enough?
    pub fields: HashMap<String, RelationalValue>,
}


#[derive(Debug, Clone)]
pub enum RelationalValue {
    Null,
    Boolean(bool),
    Number(f64),
    String(String),
}

impl RelationalRecord{
    pub fn new() -> Self {
        RelationalRecord{
            id: None,
            fields: HashMap::new(),
        }
    }

    pub fn with_id(id: usize) -> Self {
        RelationalRecord {
            id: Some(id),
            fields: HashMap::new(),
        }
    }

    pub fn set_id(&mut self, id: usize) {
        self.id = Some(id);
    }

    pub fn get_id(&self) -> Option<usize> {
        self.id.clone() //TODO: is this bad?
    }

    pub fn set_field(&mut self, key: String, value: RelationalValue) {
        self.fields.insert(key, value);
    }

    pub fn get_field(&self, key: &str) -> Option<&RelationalValue> {
        self.fields.get(key)
    }

    pub fn remove_field(&mut self, key: &str) {
        self.fields.remove(key);
    }

    // we need some new from bytes function
    // we need a deserialization function also
    // this requires the notion of a schema
    pub fn deserialize( bytes: &[u8], schema: &Schema) -> Self{
        unimplemented!()
    }

    pub fn serialize(schema: &Schema)  -> Self{
        unimplemented!()
    }
}


// theres no such thing as relational or document column