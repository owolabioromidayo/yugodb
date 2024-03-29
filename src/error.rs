#[derive(Debug)]
pub enum Error{
    PageError,
    ScanError,
    Unknown(String)
}

pub type Result<T> = std::result::Result<T, Error>;

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use Error::*;
        match self {
            Unknown(err) => write!(f, "Unclassified Error: {err}"),
            PageError => write!(f, "Pager Error"),
            ScanError => write!(f, "Invalid input string"),
        }
    }
}