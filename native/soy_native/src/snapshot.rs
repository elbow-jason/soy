use crate::SoyDb;
use rocksdb::Snapshot as RSnapshot;
use rustler::ResourceArc;

pub struct SnapshotResource {
    rss: RSnapshot<'static>,
    _db: SoyDb,
}

unsafe fn extend_lifetime_rss<'b>(r: RSnapshot<'b>) -> RSnapshot<'static> {
    std::mem::transmute::<RSnapshot<'b>, RSnapshot<'static>>(r)
}

impl SnapshotResource {
    pub fn new(db: SoyDb) -> ResourceArc<SnapshotResource> {
        let rss = unsafe { extend_lifetime_rss(db.rocks_db_ref().snapshot()) };
        ResourceArc::new(SnapshotResource { rss, _db: db })
    }

    pub fn soy_db(&self) -> &SoyDb {
        &self._db
    }

    // pub fn rocks_db_ref(&self) -> &RocksDb {
    //     self.db.rocks_db_ref()
    // }

    pub fn rocks_ss_ref(&self) -> &RSnapshot {
        &self.rss
    }
}
