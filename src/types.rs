use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum RelationalType{
    Boolean,
    Number, // just floats
    Numeric, // for more sensitive data
    String(usize),
}