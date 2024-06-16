// This should be general enough for our purposes
use crate::error::*;
use crate::schema::*;
use crate::types::*;
use itertools::Itertools;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use bson::{bson, Bson};

#[derive(Debug, Clone)]
pub enum Record {
    DocumentRow(DocumentRecord),
    DocumentColumn(ColumnarDocumentRecord),
    RelationalRow(RelationalRecord),
}

// we dont want mixed records flowing in
#[derive(Debug, Clone)]
pub enum Records {
    DocumentRows(Vec<DocumentRecord>),
    DocumentColumns(Vec<ColumnarDocumentRecord>),
    RelationalRows(Vec<RelationalRecord>),
}

// to support vector materialization

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
    Numeric(Decimal),
    String(String),
    Array(Vec<DocumentValue>),
    Object(HashMap<String, DocumentValue>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DocumentRecordPage {
    pub records: Vec<DocumentRecord>, // metadata too maybe?
}

impl DocumentRecord {
    pub fn new() -> Self {
        Self {
            id: None,
            fields: HashMap::new(),
        }
    }

    pub fn with_id(id: usize) -> Self {
        Self {
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
        return self.fields.get(key);
    }

    pub fn get_field_as_relational(&self, key: &str) -> Option<RelationalValue> {
        if let a = self.fields.get(key).unwrap() {
            //TODO: is this all we really want?
            return match a {
                DocumentValue::Null => Some(RelationalValue::Null),
                DocumentValue::Boolean(x) => Some(RelationalValue::Boolean(x.clone())),
                DocumentValue::Number(x) => Some(RelationalValue::Number(x.clone())),
                DocumentValue::Numeric(x) => Some(RelationalValue::Numeric(x.clone())),
                DocumentValue::String(x) => Some(RelationalValue::String(x.clone())),
                DocumentValue::Array(x) => None,
                DocumentValue::Object(x) => None,
            };
        }

        None
    }

    pub fn remove_field(&mut self, key: &str) {
        self.fields.remove(key);
    }

    pub fn serialize(&self) -> Result<Vec<u8>> {
        match bson::to_vec(&self) {
            Ok(res) => Ok(res),
            Err(err) => return Err(Error::SerializationError),
        }
        // }
        // Err(err) => return Err(Error::SerializationError)
        // }
        //  match serde_json::to_string(&self) {
        //     Ok(res) => return Ok(res),
        //     Err(err) => return Err(Error::SerdeError)
        //  }
    }

    pub fn deserialize(s: &Vec<u8>) -> Result<Self> {
        match bson::from_slice(s) {
            Ok(res) => return Ok(res),
            Err(err) => return Err(Error::SerializationError),
        }
        //  match serde_json::from_str(&s) {
        //     Ok(res) => return Ok(res),
        //     Err(err) => return Err(Error::SerdeError)
    }
}

impl DocumentRecordPage {
    pub fn new() -> Self {
        Self {
            records: Vec::new(),
        }
    }

    pub fn with_records(records: Vec<DocumentRecord>) -> Self {
        Self { records }
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

    pub fn deserialize(s: &Vec<u8>) -> Result<Self> {
        match bson::from_slice(s) {
            Ok(res) => return Ok(res),
            Err(err) => return Err(Error::SerializationError),
        }
    }
}

// RELATIONAL ROW RECORDS

#[derive(Debug, Clone, PartialEq)]
pub struct RelationalRecordPage {
    pub records: Vec<RelationalRecord>, // metadata
}

#[derive(Debug, Clone, PartialEq)]
pub struct RelationalRecord {
    pub id: Option<usize>, // is usize large enough?
    pub fields: HashMap<String, RelationalValue>,
}

#[derive(Debug, Clone, PartialEq)]
// TODO : add things like dates and numerics , and nullable fields
pub enum RelationalValue {
    Null,
    Boolean(bool),
    Number(f64),
    Numeric(Decimal),
    String(String),
}

impl RelationalValue {
    pub fn to_document_value(&self) -> DocumentValue {
        match &self {
            RelationalValue::Null => DocumentValue::Null,
            RelationalValue::Boolean(x) => DocumentValue::Boolean(x.clone()),
            RelationalValue::Number(x) => DocumentValue::Number(x.clone()),
            RelationalValue::Numeric(x) => DocumentValue::Numeric(x.clone()),
            RelationalValue::String(x) => DocumentValue::String(x.clone()),
        }
    }
}

impl RelationalType {
    // in bytes
    pub fn len(&self) -> usize {
        match &self {
            RelationalType::Boolean => 1,
            RelationalType::Number => 8,
            RelationalType::Numeric => 16,
            RelationalType::String(len) => len.clone(),
        }
    }
}

pub fn get_byte_size(schema: &RelationalSchema) -> usize {
    let mut res = 0 as usize;
    for (_, (dtype, _)) in schema.iter() {
        res += dtype.len();
    }
    res
}

impl RelationalRecord {
    pub fn new() -> Self {
        Self {
            id: None,
            fields: HashMap::new(),
        }
    }

    pub fn with_id(id: usize) -> Self {
        Self {
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
        return self.fields.get(key);
    }

    pub fn remove_field(&mut self, key: &str) {
        self.fields.remove(key);
    }

    // we need some new from bytes function
    // we need a deserialization function also
    // this requires the notion of a schema

    pub fn deserialize(bytes: &[u8], schema: &RelationalSchema) -> Result<Self> {
        let mut fields = HashMap::new();
        let mut offset = 0;

        for name in schema.keys().sorted() {
            let (dtype, nullable) = schema.get(name).unwrap();

            println!("{:?} {:?}", dtype, name);

            // let value = if *nullable && bytes[offset] == 0 {
            //     offset += 1;
            //     RelationalValue::Null
            let value = if false {
                RelationalValue::Null
            } else {
                match dtype {
                    RelationalType::Boolean => {
                        if offset <=     bytes.len() {
                            let val = bytes[offset] != 0;
                            offset += 1;
                            println!("Bool {:?}", val);
                            RelationalValue::Boolean(val)
                        } else {
                            return Err(Error::Unknown(
                                "Deserialization error on boolean field.".to_string(),
                            ));
                        }
                    }
                    RelationalType::Number => {
                        // so unsafe indexing is still a thing
                        if offset + 8 <= bytes.len() {
                            let val = f64::from_le_bytes(bytes[offset..offset + 8].try_into()?);
                            offset += 8;
                            println!("NUmber {:?}", val);
                            RelationalValue::Number(val)
                        } else {
                            return Err(Error::Unknown(
                                "Deserialization error on Numeric field.".to_string(),
                            ));
                        }
                    }
                    RelationalType::Numeric => {
                        if offset + 16 <= bytes.len() {
                            let val = Decimal::deserialize(bytes[offset..offset + 16].try_into()?);
                            offset += 16;
                            println!("NUmeric {:?}", val);
                            RelationalValue::Numeric(val)
                        } else {
                            return Err(Error::Unknown(
                                "Deserialization error on Numeric field.".to_string(),
                            ));
                        }
                    }
                    RelationalType::String(len) => {
                        if offset + len <= bytes.len() {
                            let val = String::from_utf8_lossy(&bytes[offset..offset + len])
                                .trim_end_matches('\0')
                                .to_string();
                            offset += len;
                            println!("String {:?}", val);
                            RelationalValue::String(val)
                        } else {
                            return Err(Error::Unknown(
                                "Deserialization error on string field.".to_string(),
                            ));
                        }
                    }
                }
            };
            fields.insert(name.clone(), value);
        }
        Ok(Self { id: None, fields })
    }

    pub fn serialize(&self, schema: &RelationalSchema) -> Vec<u8> {
        let mut bytes = Vec::new();
        for name in schema.keys().sorted() {
            let (dtype, nullable) = schema.get(name).unwrap();
            let value = self.fields.get(name).unwrap_or(&RelationalValue::Null);

            //TODO: handle nullable fields, can lead to size mismatch
            match (value, dtype, nullable) {
                //handle nullables (this approach doesnt work obv)
                // (RelationalValue::Null, RelationalType::Boolean, true) => bytes.push(0),
                // (RelationalValue::Null, RelationalType::Number, true) => {
                //     bytes.extend_from_slice(&[0; 8])
                // }
                // (RelationalValue::Null, RelationalType::Numeric, true) => {
                //     bytes.extend_from_slice(&[0; 16])
                // }
                // (RelationalValue::Null, RelationalType::String(len), true) => {
                //     bytes.extend_from_slice(&vec![0; *len])
                // }
                (RelationalValue::Boolean(val), RelationalType::Boolean, _) => {
                    bytes.push(*val as u8)
                }
                (RelationalValue::Number(val), RelationalType::Number, _) => {
                    bytes.extend_from_slice(&val.to_le_bytes())
                }
                (RelationalValue::Numeric(val), RelationalType::Numeric, _) => {
                    bytes.extend_from_slice(&val.serialize())
                }
                (RelationalValue::String(val), RelationalType::String(len), _) => {
                    if val.len() > *len {
                        let new_str = val[0..*len].to_string();
                        let val_bytes = new_str.as_bytes().to_vec();
                        bytes.extend_from_slice(&val_bytes);
                    } else {
                        let mut val_bytes = val.as_bytes().to_vec();
                        val_bytes.resize(*len, 0);
                        bytes.extend_from_slice(&val_bytes);
                    }
                }

                //TODO: make these proper errors
                (RelationalValue::Null, _, false) => panic!("Non-nullable field cannot be null"),
                _ => panic!("Incompatible data type"),
            }
        }
        bytes
    }
}

impl RelationalRecordPage {
    pub fn new() -> Self {
        Self {
            records: Vec::new(),
        }
    }

    pub fn with_records(records: Vec<RelationalRecord>) -> Self {
        Self { records }
    }

    pub fn add_record(&mut self, record: RelationalRecord) {
        self.records.push(record);
    }

    pub fn get_records(&self) -> &Vec<RelationalRecord> {
        &self.records
    }

    pub fn clear_records(&mut self) {
        self.records.clear();
    }

    pub fn serialize(&self, schema: &RelationalSchema) -> Vec<u8> {
        let mut bytes = Vec::new();
        //add the count
        bytes.extend_from_slice(&self.records.len().to_le_bytes());
        for record in &self.records {
            bytes.extend_from_slice(&record.serialize(schema));
        }
        bytes
        //ENFORCING THE SIZE OF THIS WILL BE DONE ELSEWHERE, NEED TO PAD WITH ZEROES
    }

    pub fn deserialize(bytes: &Vec<u8>, schema: &RelationalSchema) -> Result<Self> {
        println!("Bytes given {:?}", bytes.len());
        let mut records = Vec::new();
        let shift_amount = get_byte_size(&schema);
        let mut offset = 0;
        //get count
        let count =
            usize::from_le_bytes(bytes[0..std::mem::size_of::<usize>()].try_into().unwrap());
        offset += std::mem::size_of::<usize>();

        while offset < bytes.len() && records.len() < count {
            match RelationalRecord::deserialize(&bytes[offset..], schema) {
                Ok(x) => records.push(x),
                Err(x) => return Ok(Self { records }),
            }
            //TODO, we need to have a better way of handling record fetch failures
            offset += shift_amount;
        }
        Ok(Self { records })
    }
}

// theres no such thing as relational or document column

// START OF COLUMNAR STUFF

//TODO: feels repetitive, but then, there are some key differences
// what if I instead chose to make different columnar tables from higher up? seems about the same
// while that is nice, this still leaves room to be more efficient ( just by removing the row abstraction)

//maybe there is some stuff I can abstract away?

//TODO: no tests but fairly straightforward, need to think about higher level abstractions

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ColumnarDocumentRecord {
    //TODO : ensure that this holds for other record types (no pub id, or values)
    id: Option<usize>, // is usize large enough?
    value: DocumentValue,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ColumnarDocumentRecordPage {
    pub records: Vec<ColumnarDocumentRecord>,
}

impl ColumnarDocumentRecord {
    pub fn new() -> Self {
        Self {
            id: None,
            value: DocumentValue::Null,
        }
    }

    pub fn with_id(id: usize) -> Self {
        Self {
            id: Some(id),
            value: DocumentValue::Null,
        }
    }

    pub fn set_id(&mut self, id: usize) {
        self.id = Some(id);
    }

    pub fn get_id(&self) -> Option<usize> {
        self.id.clone() //TODO: is this bad?
    }

    pub fn set_value(&mut self, value: DocumentValue) {
        self.value = value;
    }

    pub fn get_value(&self) -> &DocumentValue {
        &self.value
    }

    pub fn serialize(&self) -> Result<Vec<u8>> {
        match bson::to_vec(&self) {
            Ok(res) => Ok(res),
            Err(err) => return Err(Error::SerializationError),
        }
    }

    pub fn deserialize(s: &Vec<u8>) -> Result<Self> {
        match bson::from_slice(s) {
            Ok(res) => return Ok(res),
            Err(err) => return Err(Error::SerializationError),
        }
    }
}

impl ColumnarDocumentRecordPage {
    pub fn new() -> Self {
        Self {
            records: Vec::new(),
        }
    }

    pub fn with_records(records: Vec<ColumnarDocumentRecord>) -> Self {
        Self { records }
    }

    pub fn add_record(&mut self, record: ColumnarDocumentRecord) {
        self.records.push(record);
    }

    pub fn get_records(&self) -> &Vec<ColumnarDocumentRecord> {
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

    pub fn deserialize(s: &Vec<u8>) -> Result<Self> {
        match bson::from_slice(s) {
            Ok(res) => return Ok(res),
            Err(_err) => return Err(Error::SerializationError),
        }
    }
}

// COLUMNAR RELATIONAL RECORDS

// all the schema here will consist of is a single value

#[derive(Debug, Clone)]
pub struct ColumnarRelationalRecordPage {
    pub records: Vec<ColumnarRelationalRecord>, // metadata
}

#[derive(Debug, Clone)]
pub struct ColumnarRelationalRecord {
    id: Option<usize>,
    value: RelationalValue,
}

impl ColumnarRelationalRecord {
    pub fn new() -> Self {
        Self {
            id: None,
            value: RelationalValue::Null,
        }
    }

    pub fn with_id(id: usize) -> Self {
        Self {
            id: Some(id),
            value: RelationalValue::Null,
        }
    }

    //TODO: this really shouldnt be a thing right? creating a new record should be done with an ID
    pub fn set_id(&mut self, id: usize) {
        self.id = Some(id);
    }

    pub fn get_id(&self) -> Option<usize> {
        self.id.clone()
    }

    pub fn set_value(&mut self, value: RelationalValue) {
        self.value = value;
    }

    pub fn get_value(&self, key: &str) -> &RelationalValue {
        &self.value
    }

    //TODO: finish this up on relationalrecord first, before I apply here

    pub fn deserialize(bytes: &[u8], schema: &RelationalSchema) -> Result<Self> {
        unimplemented!()
    }

    pub fn serialize(&self, schema: &RelationalSchema) -> Vec<u8> {
        unimplemented!()
    }
}

impl ColumnarRelationalRecordPage {
    pub fn new() -> Self {
        Self {
            records: Vec::new(),
        }
    }

    pub fn with_records(records: Vec<ColumnarRelationalRecord>) -> Self {
        Self { records }
    }

    pub fn add_record(&mut self, record: ColumnarRelationalRecord) {
        self.records.push(record);
    }

    pub fn get_records(&self) -> &Vec<ColumnarRelationalRecord> {
        &self.records
    }

    pub fn clear_records(&mut self) {
        self.records.clear();
    }

    // not implemeneted for now
    pub fn serialize(&self, schema: &RelationalSchema) -> Vec<u8> {
        let mut bytes = Vec::new();
        for record in &self.records {
            bytes.extend_from_slice(&record.serialize(schema));
        }
        bytes
        //ENFORCING THE SIZE OF THIS WILL BE DONE ELSEWHERE
    }

    pub fn deserialize(bytes: &Vec<u8>, schema: &RelationalSchema) -> Result<Self> {
        let mut records = Vec::new();
        let mut offset = 0;
        while offset < bytes.len() {
            let record = ColumnarRelationalRecord::deserialize(&bytes[offset..], schema)?;
            records.push(record);

            offset += get_byte_size(&schema);
        }
        Ok(Self { records })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    //DOCUMENT RECORD

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
        record.set_field(
            "name".to_string(),
            DocumentValue::String("John Doe".to_string()),
        );

        let serialized = record.serialize().unwrap();
        let deserialized = DocumentRecord::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.id, Some(42));
        assert_eq!(
            deserialized.get_field("name"),
            Some(&DocumentValue::String("John Doe".to_string()))
        );
    }

    #[test]
    fn test_record_ser_deser_string() {
        let record = DocumentRecord {
            id: Some(1),
            fields: HashMap::from([
                (
                    "name".to_string(),
                    DocumentValue::String("John".to_string()),
                ),
                ("age".to_string(), DocumentValue::Number(30.0)),
                ("active".to_string(), DocumentValue::Boolean(true)),
                (
                    "balance".to_string(),
                    DocumentValue::Numeric(Decimal::from(1000)),
                ),
            ]),
        };

        // Serialize the DocumentRecord to JSON
        let json = serde_json::to_string(&record).unwrap();
        println!("Serialized JSON: {}", json);

        // Deserialize the JSON back into a DocumentRecord
        let deserialized: DocumentRecord = serde_json::from_str(&json).unwrap();
        println!("Deserialized DocumentRecord: {:?}", deserialized);
    }

    // DOCUMENT RECORD PAGES

    #[test]
    fn test_new_document_record_page() {
        let page = DocumentRecordPage::new();
        assert!(page.records.is_empty());
    }

    #[test]
    fn test_document_record_page_with_records() {
        let records = vec![DocumentRecord::new(), DocumentRecord::with_id(42)];
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

    // RELATIONAL RECORDS

    #[test]
    fn test_nullable_fields_serialization_deserialization() {
        let schema: RelationalSchema = HashMap::from([
            ("id".to_string(), (RelationalType::Number, false)),
            ("name".to_string(), (RelationalType::String(50), true)),
            ("age".to_string(), (RelationalType::Number, true)),
            ("balance".to_string(), (RelationalType::Numeric, true)),
        ]);

        let mut record = RelationalRecord::new();
        record.set_field("id".to_string(), RelationalValue::Number(1.0));
        record.set_field(
            "name".to_string(),
            RelationalValue::String("John Doe".to_string()),
        );
        record.set_field("age".to_string(), RelationalValue::Null);
        record.set_field(
            "balance".to_string(),
            RelationalValue::Numeric(dec!(100.50)),
        );

        let serialized = record.serialize(&schema);
        println!("Bytes {:?}", serialized);
        let deserialized = RelationalRecord::deserialize(&serialized, &schema).unwrap();
        println!("Deser {:?}", deserialized);

        assert_eq!(
            deserialized.get_field("id"),
            Some(&RelationalValue::Number(1.0))
        );
        assert_eq!(
            deserialized.get_field("name"),
            Some(&RelationalValue::String("John Doe".to_string()))
        );
        assert_eq!(deserialized.get_field("age"), Some(&RelationalValue::Null));
        assert_eq!(
            deserialized.get_field("balance"),
            Some(&RelationalValue::Numeric(dec!(100.50)))
        );
    }

    #[test]
    fn test_non_nullable_fields_serialization_deserialization() {
        let schema: RelationalSchema = HashMap::from([
            ("id".to_string(), (RelationalType::Number, false)),
            ("name".to_string(), (RelationalType::String(50), false)),
            ("age".to_string(), (RelationalType::Number, false)),
            ("balance".to_string(), (RelationalType::Numeric, false)),
        ]);

        let mut record = RelationalRecord::new();
        record.set_field("id".to_string(), RelationalValue::Number(1.0));
        record.set_field(
            "name".to_string(),
            RelationalValue::String("John Doe".to_string()),
        );
        record.set_field("age".to_string(), RelationalValue::Number(30.0));
        record.set_field(
            "balance".to_string(),
            RelationalValue::Numeric(dec!(100.50)),
        );

        let serialized = record.serialize(&schema);
        println!("Bytes {:?}", serialized);
        let deserialized = RelationalRecord::deserialize(&serialized, &schema).unwrap();
        println!("Deser {:?}", deserialized);

        assert_eq!(
            deserialized.get_field("id"),
            Some(&RelationalValue::Number(1.0))
        );
        assert_eq!(
            deserialized.get_field("name"),
            Some(&RelationalValue::String("John Doe".to_string()))
        );
        assert_eq!(
            deserialized.get_field("age"),
            Some(&RelationalValue::Number(30.0))
        );
        assert_eq!(
            deserialized.get_field("balance"),
            Some(&RelationalValue::Numeric(dec!(100.50)))
        );
    }


    #[test]
    fn test_empty_relational_record_page() {
        let schema = RelationalSchema::new();
        let page = RelationalRecordPage::new();

        let serialized = page.serialize(&schema);
        let deserialized = RelationalRecordPage::deserialize(&serialized, &schema).unwrap();

        assert_eq!(deserialized.get_records().len(), 0);
    }

    #[test]
    fn test_single_record_relational_record_page() {
        let schema = RelationalSchema::from([
            ("id".to_string(), (RelationalType::Number, false)),
            ("name".to_string(), (RelationalType::String(10), false)),
        ]);

        let mut record = RelationalRecord::new();
        record.set_field("id".to_string(), RelationalValue::Number(1.0));
        record.set_field("name".to_string(), RelationalValue::String("John".to_string()));

        let mut page = RelationalRecordPage::new();
        page.add_record(record);

        let serialized = page.serialize(&schema);
        let deserialized = RelationalRecordPage::deserialize(&serialized, &schema).unwrap();

        assert_eq!(deserialized.get_records().len(), 1);
        assert_eq!(
            deserialized.get_records()[0].get_field("id"),
            Some(&RelationalValue::Number(1.0))
        );
        assert_eq!(
            deserialized.get_records()[0].get_field("name"),
            Some(&RelationalValue::String("John".to_string()))
        );
    }

    #[test]
    fn test_multiple_records_relational_record_page() {
        let schema = RelationalSchema::from([
            ("id".to_string(), (RelationalType::Number, false)),
            ("name".to_string(), (RelationalType::String(10), false)),
        ]);

        let mut page = RelationalRecordPage::new();

        let mut record1 = RelationalRecord::new();
        record1.set_field("id".to_string(), RelationalValue::Number(1.0));
        record1.set_field("name".to_string(), RelationalValue::String("John".to_string()));
        page.add_record(record1);

        let mut record2 = RelationalRecord::new();
        record2.set_field("id".to_string(), RelationalValue::Number(2.0));
        record2.set_field("name".to_string(), RelationalValue::String("Jane".to_string()));
        page.add_record(record2);

        let serialized = page.serialize(&schema);
        let deserialized = RelationalRecordPage::deserialize(&serialized, &schema).unwrap();

        assert_eq!(deserialized.get_records().len(), 2);
        assert_eq!(
            deserialized.get_records()[0].get_field("id"),
            Some(&RelationalValue::Number(1.0))
        );
        assert_eq!(
            deserialized.get_records()[0].get_field("name"),
            Some(&RelationalValue::String("John".to_string()))
        );
        assert_eq!(
            deserialized.get_records()[1].get_field("id"),
            Some(&RelationalValue::Number(2.0))
        );
        assert_eq!(
            deserialized.get_records()[1].get_field("name"),
            Some(&RelationalValue::String("Jane".to_string()))
        );
    }



    #[test]
    fn test_non_nullable_field_with_null_value() {
        let schema: RelationalSchema = HashMap::from([
            ("id".to_string(), (RelationalType::Number, false)),
            ("name".to_string(), (RelationalType::String(50), false)),
        ]);

        let mut record = RelationalRecord::new();
        record.set_field("id".to_string(), RelationalValue::Number(1.0));
        record.set_field("name".to_string(), RelationalValue::Null);

        let serialized = record.serialize(&schema);
        // assert!(serialized.is_err());
        // assert_eq!(serialized.unwrap_err(), Error::NullValueForNonNullableField);
    }
}
