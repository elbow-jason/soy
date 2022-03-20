use crate::{Error, SoyDb};
use rocksdb::{ColumnFamilyRef, DB as RocksDb};
use rustler::ResourceArc;

pub(crate) type SoyDbColFam = ResourceArc<DbColFamResource>;

pub(crate) struct DbColFamResource {
    db: SoyDb,
    cf_name: String,
    cf_ref: ColumnFamilyRef<'static>,
}

unsafe impl Send for DbColFamResource {}
unsafe impl Sync for DbColFamResource {}

impl DbColFamResource {
    pub fn new(db: &SoyDb, name: &str) -> Result<DbColFamResource, Error> {
        let db_ref = db.rocks_db_ref();
        let cf_ref = get_cf_handle(db_ref, name)?;
        Ok(DbColFamResource {
            db: db.clone(),
            cf_name: name.to_owned(),
            cf_ref: unsafe { extend_lifetime_cf(cf_ref) },
        })
    }

    pub fn handle<'a>(&'a self) -> &'a ColumnFamilyRef<'a> {
        unsafe { unextend_lifetime_cf(&self.cf_ref) }
    }

    pub fn rocks_db_ref(&self) -> &RocksDb {
        &self.db.rocks_db_ref()
    }

    pub fn soy_db(&self) -> &SoyDb {
        &self.db
    }

    pub fn name(&self) -> &str {
        &self.cf_name[..]
    }
}

pub(crate) fn get_cf_handle<'a>(
    rdb: &'a RocksDb,
    name: &str,
) -> Result<ColumnFamilyRef<'a>, Error> {
    match rdb.cf_handle(name) {
        Some(cf_handle) => Ok(cf_handle),
        None => Err(Error::ColumnFamilyDoesNotExist(name.to_string())),
    }
}

unsafe fn extend_lifetime_cf<'b>(s: ColumnFamilyRef<'b>) -> ColumnFamilyRef<'static> {
    std::mem::transmute::<ColumnFamilyRef<'b>, ColumnFamilyRef<'static>>(s)
}

unsafe fn unextend_lifetime_cf<'b>(r: &ColumnFamilyRef<'static>) -> &'b ColumnFamilyRef<'b> {
    std::mem::transmute::<&ColumnFamilyRef<'static>, &ColumnFamilyRef<'b>>(r)
}
