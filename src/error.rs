#[derive(Debug)]
pub enum Error{
    PageError,
    ScanError,
    Unknown(String),
    FileNotFound,
    IoError(std::io::Error),
    AccessError,
}

pub type Result<T> = std::result::Result<T, Error>;

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::IoError(err)
    }
}


impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use Error::*;
        match self {
            Unknown(err) => write!(f, "Unclassified Error: {err}"),
            PageError => write!(f, "Pager Error"),
            ScanError => write!(f, "Invalid input string"),
            FileNotFound => write!(f, "File Not found"),
            IoError(err) => write!(f, "IoError: {err}"),
            AccessError => write!(f, "AccessError: Variable could not be accessed."),
        }
    }
}