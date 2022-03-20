use crate::{Bin, SoyDbColFam};
use rustler::{NifRecord, NifUntaggedEnum};

#[derive(NifRecord)]
#[tag = "put"]
pub struct PutOp(Bin, Bin);

impl PutOp {
    pub fn key(&self) -> &[u8] {
        &self.0.as_bytes()
    }

    pub fn val(&self) -> &[u8] {
        &self.1.as_bytes()
    }
}

#[derive(NifRecord)]
#[tag = "put_cf"]
pub struct PutCfOp(SoyDbColFam, Bin, Bin);

impl PutCfOp {
    pub fn name(&self) -> &str {
        self.0.name()
    }

    pub fn key(&self) -> &[u8] {
        &self.1.as_bytes()
    }

    pub fn val(&self) -> &[u8] {
        &self.2.as_bytes()
    }
}

#[derive(NifRecord)]
#[tag = "delete"]
pub struct DeleteOp(Bin);

impl DeleteOp {
    pub fn key(&self) -> &[u8] {
        &self.0.as_bytes()
    }
}

#[derive(NifRecord)]
#[tag = "delete_cf"]
pub struct DeleteCfOp(String, Bin);

impl DeleteCfOp {
    pub fn name(&self) -> &str {
        self.0.as_str()
    }
    pub fn key(&self) -> &[u8] {
        &self.1.as_bytes()
    }
}

#[derive(NifUntaggedEnum)]
pub enum DbOp {
    Put(PutOp),
    Delete(DeleteOp),
}

#[derive(NifUntaggedEnum)]
pub enum CfOp {
    Put(PutCfOp),
    Delete(DeleteCfOp),
}

#[derive(NifUntaggedEnum)]
pub enum BatchOp {
    Db(DbOp),
    Cf(CfOp),
}
