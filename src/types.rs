use std::collections::HashMap;

pub enum RelationalType{
    Boolean,
    Number, // just floats
    Numeric, // for more sensitive data
    String(usize),
}