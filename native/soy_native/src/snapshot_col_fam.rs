use crate::{Error, SoySnapshot};
use rocksdb::{ColumnFamilyRef, Snapshot as RSnapshot, DB as RocksDb};
use rustler::ResourceArc;

pub type SoySsColFam = ResourceArc<SsColFamResource>;

pub struct SsColFamResource {
    ss: SoySnapshot,
    cf_name: String,
    cf_ref: ColumnFamilyRef<'static>,
}

unsafe impl Send for SsColFamResource {}
unsafe impl Sync for SsColFamResource {}

impl SsColFamResource {
    pub fn new(ss: &SoySnapshot, name: &str) -> Result<SsColFamResource, Error> {
        let db_ref = ss.soy_db().rocks_db_ref();
        let cf_ref = get_cf_handle(db_ref, name)?;
        Ok(SsColFamResource {
            ss: ss.clone(),
            cf_name: name.to_owned(),
            cf_ref: unsafe { extend_lifetime_cf(cf_ref) },
        })
    }

    pub fn handle<'a>(&'a self) -> &'a ColumnFamilyRef<'a> {
        unsafe { unextend_lifetime_cf(&self.cf_ref) }
    }

    // pub fn rocks_db_ref(&self) -> &RocksDb {
    //     &self.ss.rocks_db_ref()
    // }

    pub fn rocks_ss_ref(&self) -> &RSnapshot {
        &self.ss.rocks_ss_ref()
    }

    pub fn soy_snapshot(&self) -> &SoySnapshot {
        &self.ss
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
