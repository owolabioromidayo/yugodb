// This should be general enough for our purposes
use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use crate::types::*;
use crate::error::*;

use bson::{bson, Bson };


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DocumentRecord {
    pub id: Option<usize>, // is usize large enough?
    pub fields: HashMap<String, DocumentValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DocumentValue {
    Null,
    Boolean(bool),
    Number(f64),
    String(String),
    Array(Vec<DocumentValue>),
    Object(HashMap<String, DocumentValue>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DocumentRecordPage {
    pub records: Vec<DocumentRecord>
    // metadata
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

    pub fn serialize(&self ) -> Result<Vec<u8>>{
        match bson::to_vec(&self) {
            Ok( res) => Ok(res),
            Err(err) => return Err(Error::SerializationError)
        }
        // }
            // Err(err) => return Err(Error::SerializationError)
        // }
        //  match serde_json::to_string(&self) {
        //     Ok(res) => return Ok(res),
        //     Err(err) => return Err(Error::SerdeError)
        //  }
         
    }

    pub fn deserialize(s: &Vec<u8>) -> Result<Self>{
        match bson::from_slice(s){
            Ok(res) => return Ok(res),
            Err(err) => return Err(Error::SerializationError)
        }
        //  match serde_json::from_str(&s) {
        //     Ok(res) => return Ok(res),
        //     Err(err) => return Err(Error::SerdeError)
         
    }
}

impl DocumentRecordPage {
    pub fn new() -> Self {
        DocumentRecordPage {
            records: Vec::new(),
        }
    }

    pub fn with_records(records: Vec<DocumentRecord>) -> Self {
        DocumentRecordPage { records }
    }

    pub fn add_record(&mut self, record: DocumentRecord) {
        self.records.push(record);
    }

    pub fn get_records(&self) -> &Vec<DocumentRecord> {
        &self.records
    }

    pub fn clear_records(&mut self) {
        self.records.clear();
    }

    pub fn serialize(&self) -> Result<Vec<u8>> {
        match bson::to_vec(&self) {
            Ok(res) => Ok(res),
            Err(_) => Err(Error::SerializationError),
        }
    }

    pub fn deserialize(s: &Vec<u8>) -> Result<Self>{
        match bson::from_slice(s){
            Ok(res) => return Ok(res),
            Err(err) => return Err(Error::SerializationError)
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


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_document_record() {
        let record = DocumentRecord::new();
        assert_eq!(record.id, None);
        assert!(record.fields.is_empty());
    }

    #[test]
    fn test_document_record_with_id() {
        let record = DocumentRecord::with_id(42);
        assert_eq!(record.id, Some(42));
        assert!(record.fields.is_empty());
    }

    #[test]
    fn test_set_and_get_id() {
        let mut record = DocumentRecord::new();
        record.set_id(42);
        assert_eq!(record.get_id(), Some(42));
    }

    #[test]
    fn test_set_and_get_field() {
        let mut record = DocumentRecord::new();
        let key = "name".to_string();
        let value = DocumentValue::String("John Doe".to_string());
        record.set_field(key.clone(), value.clone());
        assert_eq!(record.get_field(&key), Some(&value));
    }

    #[test]
    fn test_remove_field() {
        let mut record = DocumentRecord::new();
        let key = "name".to_string();
        let value = DocumentValue::String("John Doe".to_string());
        record.set_field(key.clone(), value);
        record.remove_field(&key);
        assert_eq!(record.get_field(&key), None);
    }

    #[test]
    fn test_serialize_and_deserialize() {
        let mut record = DocumentRecord::new();
        record.set_id(42);
        record.set_field("name".to_string(), DocumentValue::String("John Doe".to_string()));

        let serialized = record.serialize().unwrap();
        let deserialized = DocumentRecord::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.id, Some(42));
        assert_eq!(deserialized.get_field("name"), Some(&DocumentValue::String("John Doe".to_string())));
    }


    // DOCUMENT RECORDS

    #[test]
    fn test_new_document_record_page() {
        let page = DocumentRecordPage::new();
        assert!(page.records.is_empty());
    }

    #[test]
    fn test_document_record_page_with_records() {
        let records = vec![
            DocumentRecord::new(),
            DocumentRecord::with_id(42),
        ];
        let page = DocumentRecordPage::with_records(records.clone());
        assert_eq!(page.get_records(), &records);
    }

    #[test]
    fn test_add_record() {
        let mut page = DocumentRecordPage::new();
        let record = DocumentRecord::new();
        page.add_record(record.clone());
        assert_eq!(page.get_records(), &vec![record]);
    }

    #[test]
    fn test_clear_records() {
        let mut page = DocumentRecordPage::new();
        page.add_record(DocumentRecord::new());
        page.add_record(DocumentRecord::with_id(42));
        page.clear_records();
        assert!(page.records.is_empty());
    }

    #[test]
    fn test_serialize_and_deserialize_record_page() {
        let mut page = DocumentRecordPage::new();
        page.add_record(DocumentRecord::new());
        page.add_record(DocumentRecord::with_id(42));

        let serialized = page.serialize().unwrap();
        let deserialized = DocumentRecordPage::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.records.len(), 2);
        assert_eq!(deserialized.records[1].id, Some(42));
    }
}