use std::collections::HashMap;


// This should be general enough for our purposes


#[derive(Debug, Clone)]
pub struct Row {
    pub id: Option<usize>, // is usize large enough?
    pub fields: HashMap<String, Value>,
}

#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Boolean(bool),
    Number(f64),
    String(String),
    Array(Vec<Value>),
    Object(HashMap<String, Value>),
}

impl Row {
    pub fn new() -> Self {
        Row {
            id: None,
            fields: HashMap::new(),
        }
    }

    pub fn with_id(id: usize) -> Self {
        Row {
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

    pub fn set_field(&mut self, key: String, value: Value) {
        self.fields.insert(key, value);
    }

    pub fn get_field(&self, key: &str) -> Option<&Value> {
        self.fields.get(key)
    }

    pub fn remove_field(&mut self, key: &str) {
        self.fields.remove(key);
    }

    pub fn serialize(){
        unimplemented!()
    }
}