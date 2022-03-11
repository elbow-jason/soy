use crate::{SoyDB, SoyIter, SoySnapshot};
use rocksdb::{DBRawIteratorWithThreadMode, DB as RDB};
use rustler::ResourceArc;
use std::ops::Drop;

use std::sync::RwLock;

pub type RocksIter<'a> = DBRawIteratorWithThreadMode<'a, RDB>;

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
}

impl SafeIteration for SoyDB {
    fn safe_iter<'a>(&'a self) -> SafeIter<'a> {
        SafeIter::new_unseeked(self.rdb.raw_iterator())
    }
}

impl SafeIteration for SoySnapshot {
    fn safe_iter<'a>(&'a self) -> SafeIter<'a> {
        SafeIter::new_unseeked(self.rss.raw_iterator())
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
    SS(OwnedResourceIter<SoySnapshot>),
    DB(OwnedResourceIter<SoyDB>),
}

impl IterResource {
    pub fn from_db(db: SoyDB) -> SoyIter {
        let res = OwnedResourceIter::new(db);
        let it = IterResource::DB(res);
        ResourceArc::new(it)
    }

    pub fn from_ss(ss: SoySnapshot) -> SoyIter {
        let res = OwnedResourceIter::new(ss);
        let it = IterResource::SS(res);
        ResourceArc::new(it)
    }
}

impl IterLocker for IterResource {
    fn lock(&self) -> &RwLock<SafeIter<'static>> {
        match self {
            IterResource::SS(res) => res.lock(),
            IterResource::DB(res) => res.lock(),
        }
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
