use crate::{atoms, new_binary, Error, SoyDb, SoyIter, SoySnapshot};
use rocksdb::{DBRawIteratorWithThreadMode, DBWALIterator, WriteBatchIterator, DB as RocksDb};
use rustler::{Encoder, Env, ResourceArc, Term};
use std::ops::Drop;

use std::sync::RwLock;

pub type RocksIter<'a> = DBRawIteratorWithThreadMode<'a, RocksDb>;

/// A RockIter that has not been seeked will segfault
/// despite it not being an `unsafe` call.
/// So we protect it against being called by
/// tagging it with Unseeked.
/// An unseeked RocksIter will call seek_to_first when next is called.
/// An unseeked RocksIter will call seek_to_last when prev is called.
/// An unseeked RocksIter will call return None current is called.
/// it's like the root position in a ring. The root has no value,
/// but can enter the ring by either going forward or by going
/// backward.
pub struct SafeIter<'a> {
    is_seeked: bool,
    it: RocksIter<'a>,
}

impl<'a> SafeIter<'a> {
    pub fn new_unseeked(mut it: RocksIter<'a>) -> SafeIter<'a> {
        // Removal of the call seek_to_first leads to UB.
        it.seek_to_first();
        SafeIter {
            is_seeked: false,
            it,
        }
    }

    pub fn next(&mut self) {
        // if we get the logic wrong
        // then calling self.it.next() is UB
        #[allow(unused_unsafe)]
        unsafe {
            match (self.is_seeked, self.it.valid()) {
                (true, true) => self.it.next(),
                (false, true) => {
                    self.it.seek_to_first();
                    self.is_seeked = true;
                }
                (true, false) => {}
                (false, false) => {}
            }
        }
    }

    pub fn prev(&mut self) {
        // if we get the logic wrong
        // then calling self.it.prev() is UB
        #[allow(unused_unsafe)]
        unsafe {
            match (self.is_seeked, self.it.valid()) {
                (true, true) => self.it.prev(),
                (false, true) => {
                    self.it.seek_to_last();
                    self.is_seeked = true;
                }
                (true, false) => {}
                (false, false) => {}
            }
        }
    }

    pub fn valid(&self) -> bool {
        self.it.valid()
    }

    pub fn key(&self) -> Option<&[u8]> {
        if self.is_seeked && self.valid() {
            self.it.key()
        } else {
            None
        }
    }

    pub fn value(&self) -> Option<&[u8]> {
        if self.is_seeked && self.valid() {
            self.it.value()
        } else {
            None
        }
    }

    pub fn key_value(&self) -> Option<(&[u8], &[u8])> {
        if self.is_seeked && self.valid() {
            match (self.it.key(), self.it.value()) {
                (Some(k), Some(v)) => Some((k, v)),
                (None, None) => None,
                _ => unreachable!(),
            }
        } else {
            None
        }
    }

    pub fn seek<K: AsRef<[u8]>>(&mut self, key: K) {
        self.it.seek(key);
        self.is_seeked = true;
    }

    pub fn seek_for_prev<K: AsRef<[u8]>>(&mut self, key: K) {
        self.it.seek_for_prev(key);
        self.is_seeked = true;
    }

    pub fn seek_to_first(&mut self) {
        self.it.seek_to_first();
        self.is_seeked = true;
    }

    pub fn seek_to_last(&mut self) {
        self.it.seek_to_last();
        self.is_seeked = true;
    }
}

pub trait SafeIteration {
    fn safe_iter<'a>(&'a self) -> SafeIter<'a>;
    fn safe_iter_cf<'a>(&'a self, name: &'a str) -> SafeIter<'a>;
}

impl SafeIteration for SoyDb {
    fn safe_iter<'a>(&'a self) -> SafeIter<'a> {
        SafeIter::new_unseeked(self.rocks_db_ref().raw_iterator())
    }

    fn safe_iter_cf<'a>(&'a self, name: &'a str) -> SafeIter<'a> {
        let cf_handle = self.rocks_db_ref().cf_handle(name).unwrap();
        let it = self.rocks_db_ref().raw_iterator_cf(&cf_handle);
        SafeIter::new_unseeked(it)
    }
}

impl SafeIteration for SoySnapshot {
    fn safe_iter<'a>(&'a self) -> SafeIter<'a> {
        SafeIter::new_unseeked(self.rss.raw_iterator())
    }

    fn safe_iter_cf<'a>(&'a self, name: &'a str) -> SafeIter<'a> {
        let cf_handle = self.db.rocks_db_ref().cf_handle(name).unwrap();
        let it = self.rss.raw_iterator_cf(&cf_handle);
        SafeIter::new_unseeked(it)
    }
}

pub trait IterLocker {
    fn lock(&self) -> &RwLock<SafeIter<'static>>;
}

unsafe fn extend_lifetime_safe_iter<'b>(s: SafeIter<'b>) -> SafeIter<'static> {
    std::mem::transmute::<SafeIter<'b>, SafeIter<'static>>(s)
}

unsafe fn unextend_lifetime_safe_iter_rwlock_mut<'b>(
    r: &mut RwLock<SafeIter<'static>>,
) -> &'b mut RwLock<SafeIter<'b>> {
    std::mem::transmute::<&mut RwLock<SafeIter<'static>>, &mut RwLock<SafeIter<'b>>>(r)
}

pub struct OwnedResourceIter<T>
where
    T: SafeIteration,
{
    it: RwLock<SafeIter<'static>>,
    _res: T,
}

impl<T> Drop for OwnedResourceIter<T>
where
    T: SafeIteration,
{
    fn drop(&mut self) {
        // ORDER MATTERS.
        let mut it = unsafe { unextend_lifetime_safe_iter_rwlock_mut(&mut self.it) };
        std::mem::drop(&mut it);
        std::mem::drop(&mut self._res);
    }
}

impl<T> OwnedResourceIter<T>
where
    T: SafeIteration,
{
    fn new(res: T) -> OwnedResourceIter<T> {
        let it_unlocked = unsafe { extend_lifetime_safe_iter(res.safe_iter()) };
        let it = RwLock::new(it_unlocked);
        OwnedResourceIter { _res: res, it }
    }

    fn new_cf(res: T, name: &str) -> OwnedResourceIter<T> {
        let it_unlocked = unsafe { extend_lifetime_safe_iter(res.safe_iter_cf(name)) };
        let it = RwLock::new(it_unlocked);
        OwnedResourceIter { _res: res, it }
    }
}

impl<T> IterLocker for OwnedResourceIter<T>
where
    T: SafeIteration,
{
    fn lock(&self) -> &RwLock<SafeIter<'static>> {
        &self.it
    }
}

pub enum IterResource {
    Ss(OwnedResourceIter<SoySnapshot>),
    SsCf(OwnedResourceIter<SoySnapshot>),
    Db(OwnedResourceIter<SoyDb>),
    DbCf(OwnedResourceIter<SoyDb>),
}

impl IterResource {
    pub fn from_db(db: SoyDb) -> SoyIter {
        let res = OwnedResourceIter::new(db);
        let it = IterResource::Db(res);
        ResourceArc::new(it)
    }

    pub fn from_db_cf(db: SoyDb, name: &str) -> SoyIter {
        let res = OwnedResourceIter::new_cf(db, name);
        let it = IterResource::DbCf(res);
        ResourceArc::new(it)
    }

    pub fn from_ss(ss: SoySnapshot) -> SoyIter {
        let res = OwnedResourceIter::new(ss);
        let it = IterResource::Ss(res);
        ResourceArc::new(it)
    }

    pub fn from_ss_cf(ss: SoySnapshot, name: &str) -> SoyIter {
        let res = OwnedResourceIter::new_cf(ss, name);
        let it = IterResource::SsCf(res);
        ResourceArc::new(it)
    }
}

impl IterLocker for IterResource {
    fn lock(&self) -> &RwLock<SafeIter<'static>> {
        match self {
            IterResource::Ss(res) => res.lock(),
            IterResource::Db(res) => res.lock(),
            IterResource::SsCf(res) => res.lock(),
            IterResource::DbCf(res) => res.lock(),
        }
    }
}

pub struct WalIterator {
    _db: SoyDb,
    it: RwLock<DBWALIterator>,
}

unsafe impl Send for WalIterator {}
unsafe impl Sync for WalIterator {}

impl WalIterator {
    pub fn new(db: SoyDb, since: u64) -> Result<WalIterator, Error> {
        let it = db
            .rocks_db_ref()
            .get_updates_since(since)
            .map_err(|e| Error::WalIteratorCreationError(format!("{}", e)))?;
        Ok(WalIterator {
            _db: db,
            it: RwLock::new(it),
        })
    }

    pub fn next(&self) -> Option<(u64, Vec<WalRow>)> {
        let mut it = self.it.write().unwrap();
        if it.valid() {
            match it.next() {
                None => None,
                Some((seq_number, write_batch)) => {
                    let mut list = WalBatchList { items: Vec::new() };
                    write_batch.iterate(&mut list);
                    Some((seq_number, list.items))
                }
            }
        } else {
            None
        }
    }
}

pub enum WalRow {
    Put { key: Box<[u8]>, value: Box<[u8]> },
    Delete { key: Box<[u8]> },
}

impl Encoder for WalRow {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        match self {
            WalRow::Put {
                key: ref k,
                value: ref v,
            } => (
                atoms::put(),
                new_binary(&k[..], env),
                new_binary(&v[..], env),
            )
                .encode(env),
            WalRow::Delete { key: ref k } => (atoms::put(), new_binary(&k[..], env)).encode(env),
        }
    }
}

pub struct WalBatchList {
    items: Vec<WalRow>,
}

impl WriteBatchIterator for WalBatchList {
    fn put(&mut self, key: Box<[u8]>, value: Box<[u8]>) {
        self.items.push(WalRow::Put { key, value });
    }

    fn delete(&mut self, key: Box<[u8]>) {
        self.items.push(WalRow::Delete { key });
    }
}

// #[derive(NifRecord)]
// #[tag = "prefix"]
// pub struct IterPrefix(Bin);

// impl IterPrefix {
//     pub fn as_bytes(&self) -> &[u8] {
//         self.0.as_bytes()
//     }
// }

// #[derive(NifRecord)]
// #[tag = "prefix"]
// pub struct IterPrefixCf(String, Bin);

// impl IterPrefixCf {
//     pub fn name(&self) -> &str {
//         self.0.as_str()
//     }

//     pub fn as_bytes(&self) -> &[u8] {
//         self.1.as_bytes()
//     }
// }
