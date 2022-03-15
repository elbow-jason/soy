use thiserror::{Error as ThisError};
use rustler::{Encoder, Term, Env};

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