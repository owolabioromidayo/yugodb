use std::convert::Infallible;
use std::array::TryFromSliceError;
// use std::ops::Try;
trait FromResidual<R> {
    fn from_residual(residual: R) -> Self;
}

impl FromResidual<Result<Infallible>> for Error
{
    fn from_residual(residual: Result<Infallible>) -> Self {
        match residual {
            Err(e) => Error::from(e),
            Ok(_) => unreachable!("Infallible can never be constructed"),
        }
    }
}



#[derive(Debug)]
pub enum Error{
    PageError,
    ScanError,
    Unknown(String),
    FileNotFound,
    IoError(std::io::Error),
    AccessError,
    TypeError(String),
    DBMSCall(String),
    NotFound(String),
    SerializationError,
    SerdeSerializationError(serde_json::Error),
    BsonError(bson::ser::Error),
    BsonDeserializationError(bson::de::Error), 
}


pub type Result<T> = std::result::Result<T, Error>;

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::IoError(err)
    }
}

// impl From<error::Error> for Error {
//     fn from(err: std::io::Error) -> Self {
//         Error::IoError(err)
//     }
// }

impl From<bson::ser::Error> for Error {
    fn from(err: bson::ser::Error) -> Self {
        Error::BsonError(err)
    }
}

impl From<bson::de::Error> for Error {
    fn from(err: bson::de::Error) -> Self {
        Error::BsonDeserializationError(err)
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::SerdeSerializationError(err)
    }
}

impl From<TryFromSliceError> for Error {
    fn from(err: TryFromSliceError) -> Self {
        Error::Unknown(format!("TryFromSliceError: {}", err))
    }
}

// 


impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use Error::*;
        match self {
            Unknown(err) => write!(f, "Unclassified Error: {err}"),
            DBMSCall(err) => write!(f, "Error evaluting dbms call: {err}"),
            PageError => write!(f, "Pager Error"),
            ScanError => write!(f, "Invalid input string"),
            FileNotFound => write!(f, "File Not found"),
            IoError(err) => write!(f, "IoError: {err}"),
            AccessError => write!(f, "AccessError: Variable could not be accessed."),
            NotFound(err) => write!(f, "Not found Error: {err} "),
            TypeError(err) => write!(f, "TypeError: {err}"),
            SerializationError => write!(f, "Error serializing or deserializing"),
             BsonError(err) => write!(f, "BSON Serialization Error: {err}"),
            SerdeSerializationError(err) => write!(f, "Serde Deserialization Error: {err}"),
            BsonDeserializationError(err) => write!(f, "BSON Deserialization Error: {err}"),
        }
    }
}