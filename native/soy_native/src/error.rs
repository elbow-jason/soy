use rustler::{Encoder, Env, Error as RustlerError, Term};
use thiserror::Error as ThisError;

#[derive(ThisError, Debug)]
pub enum Error {
    #[error("column family does not exist: {}", 0)]
    ColumnFamilyDoesNotExist(String),

    #[error("failed to create wal iterator: {}", 0)]
    WalIteratorCreationError(String),
    // #[error("wal iterator was invalid")]
    // WalIteratorInvalid,
    // #[error("column name \"default\" is a reserved name")]
    // NameDefaultIsReserved,
}

impl Encoder for Error {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        format!("{}", self).encode(env)
    }
}

impl From<Error> for RustlerError {
    fn from(e: Error) -> RustlerError {
        RustlerError::Term(Box::new(format!("{}", e)))
    }
}
