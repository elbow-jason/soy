use rocksdb::DB as RocksDb;
use rustler::ResourceArc;

pub type SoyDb = ResourceArc<DbResource>;

pub struct DbResource {
    rdb: RocksDb,
}

impl DbResource {
    pub fn new(rdb: RocksDb) -> SoyDb {
        ResourceArc::new(DbResource { rdb })
    }

    pub fn rocks_db_ref(&self) -> &RocksDb {
        &self.rdb
    }
}
