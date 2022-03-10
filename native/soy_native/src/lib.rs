use rocksdb::checkpoint::Checkpoint;
use rocksdb::{
    DBIterator, Direction, IteratorMode, Options, Snapshot as RSnapshot, WriteBatch, DB as RDB,
};

use rustler::{Atom, Binary, Env, Error, NifResult, ResourceArc, Term};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

mod iteration;
use iteration::IterMode;

mod bin;
use bin::{new_binary, Bin};

mod batching;
use batching::{BatchOp, CfOp, DbOp};

mod write_opts;
use write_opts::SoyWriteOpts;

mod read_opts;
use read_opts::SoyReadOpts;

mod open_opts;
use open_opts::SoyOpenOpts;

mod live_file;
use live_file::SoyLiveFile;

macro_rules! ok_or_err {
    ($res:expr) => {
        match $res {
            Ok(()) => Ok(atoms::ok()),
            Err(e) => Err(Error::Term(Box::new(format!("{}", e)))),
        }
    };
}

struct DBResource {
    rdb: RDB,
}

impl DBResource {
    fn new(rdb: RDB) -> DB {
        ResourceArc::new(DBResource { rdb })
    }
}
type DB = ResourceArc<DBResource>;

unsafe fn extend_lifetime_db_iter<'b>(r: DBIterator<'b>) -> DBIterator<'static> {
    std::mem::transmute::<DBIterator<'b>, DBIterator<'static>>(r)
}

// unsafe fn unextend_lifetime_mut<'b>(r: &mut DBIterator<'static>) -> &'b mut DBIterator<'b> {
//     std::mem::transmute::<&mut DBIterator<'static>, &mut DBIterator<'b>>(r)
// }

struct DbIter {
    _db: DB,
    it: RwLock<DBIterator<'static>>,
}

impl DbIter {
    fn new<'a>(db: DB, iter_mode: IterMode) -> DbIter {
        let rdb = &db.rdb;
        let short_iter = match iter_mode {
            IterMode::First(_) => rdb.iterator(IteratorMode::Start),
            IterMode::Last(_) => rdb.iterator(IteratorMode::End),
            IterMode::Forward(ref fwd) => {
                let mode = IteratorMode::From(fwd.as_bytes(), Direction::Forward);
                rdb.iterator(mode)
            }
            IterMode::Reverse(ref rev) => {
                let mode = IteratorMode::From(rev.as_bytes(), Direction::Reverse);
                rdb.iterator(mode)
            }
            IterMode::Prefix(ref prefix) => rdb.prefix_iterator(prefix.as_bytes()),
            // CF variants
            IterMode::FirstCf(cf) => {
                let cf_handler = rdb.cf_handle(cf.name()).unwrap();
                rdb.iterator_cf(&cf_handler, IteratorMode::Start)
            }
            IterMode::LastCf(cf) => {
                let cf_handler = rdb.cf_handle(cf.name()).unwrap();
                rdb.iterator_cf(&cf_handler, IteratorMode::Start)
            }
            IterMode::ForwardCf(cf) => {
                let cf_handler = rdb.cf_handle(cf.name()).unwrap();
                let mode = IteratorMode::From(cf.as_bytes(), Direction::Forward);
                rdb.iterator_cf(&cf_handler, mode)
            }
            IterMode::ReverseCf(cf) => {
                let cf_handler = rdb.cf_handle(cf.name()).unwrap();
                let mode = IteratorMode::From(cf.as_bytes(), Direction::Reverse);
                rdb.iterator_cf(&cf_handler, mode)
            }
            IterMode::PrefixCf(cf) => {
                let cf_handler = rdb.cf_handle(cf.name()).unwrap();
                rdb.prefix_iterator_cf(&cf_handler, cf.as_bytes())
            }
        };
        let it = unsafe { extend_lifetime_db_iter(short_iter) };
        DbIter {
            _db: db,
            it: RwLock::new(it),
        }
    }
}

struct SsIter {
    _ss: SS,
    it: RwLock<DBIterator<'static>>,
}

impl SsIter {
    fn new<'a>(ss: SS, iter_mode: IterMode) -> SsIter {
        let rss = &ss.rss;
        let rdb = &ss._db.rdb;
        let short_iter = match iter_mode {
            IterMode::First(_) => rss.iterator(IteratorMode::Start),
            IterMode::Last(_) => rss.iterator(IteratorMode::End),
            IterMode::Forward(ref fwd) => {
                let mode = IteratorMode::From(fwd.as_bytes(), Direction::Forward);
                rss.iterator(mode)
            }
            IterMode::Reverse(ref rev) => {
                let mode = IteratorMode::From(rev.as_bytes(), Direction::Reverse);
                rss.iterator(mode)
            }
            // IterMode::Prefix(ref prefix) => rdb.prefix_iterator(prefix.as_bytes()),
            // CF variants
            IterMode::FirstCf(cf) => {
                let cf_handler = rdb.cf_handle(cf.name()).unwrap();
                rss.iterator_cf(&cf_handler, IteratorMode::Start)
            }
            IterMode::LastCf(cf) => {
                let cf_handler = rdb.cf_handle(cf.name()).unwrap();
                rss.iterator_cf(&cf_handler, IteratorMode::Start)
            }
            IterMode::ForwardCf(cf) => {
                let cf_handler = rdb.cf_handle(cf.name()).unwrap();
                let mode = IteratorMode::From(cf.as_bytes(), Direction::Forward);
                rss.iterator_cf(&cf_handler, mode)
            }
            IterMode::ReverseCf(cf) => {
                let cf_handler = rdb.cf_handle(cf.name()).unwrap();
                let mode = IteratorMode::From(cf.as_bytes(), Direction::Reverse);
                rss.iterator_cf(&cf_handler, mode)
            }
            _ => {
                panic!("prefix iteration is not supported for snapshots");
            } // IterMode::PrefixCf(cf) => {
              //     let cf_handler = rdb.cf_handle(cf.name()).unwrap();
              //     rss.prefix_iterator_cf(&cf_handler, cf.as_bytes())
              // }
        };
        let it = unsafe { extend_lifetime_db_iter(short_iter) };
        SsIter {
            _ss: ss,
            it: RwLock::new(it),
        }
    }
}

enum IterResource {
    Db(DbIter),
    Ss(SsIter),
}

impl IterResource {
    fn from_ss(ssi: SsIter) -> KVIter {
        ResourceArc::new(IterResource::Ss(ssi))
    }

    fn from_db(dbi: DbIter) -> KVIter {
        ResourceArc::new(IterResource::Db(dbi))
    }
}

type KVIter = ResourceArc<IterResource>;

impl IterLocker for DbIter {
    fn lock(&self) -> &RwLock<DBIterator<'static>> {
        &self.it
    }
}

impl IterLocker for SsIter {
    fn lock(&self) -> &RwLock<DBIterator<'static>> {
        &self.it
    }
}

impl IterLocker for IterResource {
    fn lock(&self) -> &RwLock<DBIterator<'static>> {
        match self {
            IterResource::Db(it) => it.lock(),
            IterResource::Ss(it) => it.lock(),
        }
    }
}

pub trait IterLocker {
    fn lock(&self) -> &RwLock<DBIterator<'static>>;
}

mod atoms {
    rustler::atoms! {
        ok,
        error,
    }
}

struct SnapshotResource {
    rss: RSnapshot<'static>,
    _db: DB,
}

unsafe fn extend_lifetime_rss<'b>(r: RSnapshot<'b>) -> RSnapshot<'static> {
    std::mem::transmute::<RSnapshot<'b>, RSnapshot<'static>>(r)
}

impl SnapshotResource {
    fn new(db: DB) -> SS {
        let rss = unsafe { extend_lifetime_rss(db.rdb.snapshot()) };
        ResourceArc::new(SnapshotResource { rss, _db: db })
    }
}

type SS = ResourceArc<SnapshotResource>;

#[rustler::nif]
fn open(path: String, open_opts: SoyOpenOpts) -> DB {
    let opts = open_opts.into();
    match RDB::list_cf(&opts, &path[..]) {
        Ok(cfs) => {
            let rdb = RDB::open_cf(&opts, &path[..], cfs).unwrap();
            DBResource::new(rdb)
        }
        Err(_) => {
            let rdb = RDB::open(&opts, &path[..]).unwrap();
            DBResource::new(rdb)
        }
    }
}

#[rustler::nif(name = "path")]
fn db_path(db: DB) -> String {
    db.rdb.path().to_str().unwrap().to_string()
}

#[rustler::nif]
fn checkpoint(db: DB, checkpoint_path: String) -> Atom {
    let cp_path = PathBuf::from(checkpoint_path);
    if db.rdb.path() == cp_path {
        panic!(
            "checkpoint path cannot be the same as the db path - got: {}",
            cp_path.to_str().unwrap()
        );
    }
    let rdb = &db.rdb;
    let checkpoint = Checkpoint::new(rdb).unwrap();
    checkpoint.create_checkpoint(cp_path).unwrap();
    atoms::ok()
}

#[rustler::nif(schedule = "DirtyIo")]
fn destroy(path: String) -> NifResult<Atom> {
    ok_or_err!(RDB::destroy(&Options::default(), path))
}

#[rustler::nif(schedule = "DirtyIo")]
fn repair(path: String) -> NifResult<Atom> {
    ok_or_err!(rocksdb::DB::repair(&Options::default(), path))
}

#[rustler::nif]
fn put(db: DB, key: Binary, val: Binary) -> NifResult<Atom> {
    ok_or_err!(db.rdb.put(&key[..], &val[..]))
}

#[rustler::nif]
fn fetch<'a>(db: DB, key: Binary) -> NifResult<(Atom, Bin)> {
    match db.rdb.get(&key[..]) {
        Ok(Some(v)) => Ok((atoms::ok(), Bin::from_vec(v))),
        Ok(None) => Err(Error::Atom("error")),
        Err(e) => panic!("{}", e),
    }
}

#[rustler::nif]
fn delete(db: DB, key: Binary) -> NifResult<Atom> {
    ok_or_err!(db.rdb.delete(&key[..]))
}

#[rustler::nif]
fn multi_get<'a>(db: DB, keys: Vec<Binary>) -> Vec<Option<Bin>> {
    let keys_it = keys.iter().map(|k| (&k[..]).to_vec());
    db.rdb
        .multi_get(keys_it)
        .into_iter()
        .map(|v| match v.unwrap() {
            Some(data) => Some(Bin::from_vec(data)),
            None => None,
        })
        .collect()
}

#[rustler::nif]
fn multi_get_cf<'a>(db: DB, cf_and_keys: Vec<(String, Binary)>) -> Vec<Option<Bin>> {
    let rdb = &db.rdb;
    let cf_handle_keys: Vec<(Arc<rocksdb::BoundColumnFamily<'_>>, Binary)> = cf_and_keys
        .into_iter()
        .map(|(cf_name, key)| {
            let cf_handler: std::sync::Arc<rocksdb::BoundColumnFamily<'_>> =
                rdb.cf_handle(&cf_name[..]).unwrap();
            (cf_handler, key)
        })
        .collect();
    let keys_it = cf_handle_keys.iter().map(|(h, k)| (&*h, &k[..]));
    db.rdb
        .multi_get_cf(keys_it)
        .into_iter()
        .map(|v| match v.unwrap() {
            Some(data) => Some(Bin::from_vec(data)),
            None => None,
        })
        .collect()
}

#[rustler::nif]
fn live_files(db: DB) -> Vec<SoyLiveFile> {
    db.rdb
        .live_files()
        .unwrap()
        .into_iter()
        .map(|item| SoyLiveFile::from(item))
        .collect()
}

#[rustler::nif]
fn batch<'a>(db: DB, ops: Vec<BatchOp>) -> NifResult<usize> {
    if ops.len() == 0 {
        return Ok(0);
    }
    let rdb = &db.rdb;
    let mut batch = WriteBatch::default();
    for op in ops {
        match op {
            BatchOp::Db(db_op) => match db_op {
                DbOp::Put(p) => batch.put(p.key(), p.val()),
                DbOp::Delete(d) => batch.delete(d.key()),
            },
            BatchOp::Cf(cf_op) => match cf_op {
                CfOp::Put(p) => {
                    let cf_handler = rdb.cf_handle(p.name()).unwrap();
                    batch.put_cf(&cf_handler, p.key(), p.val());
                }
                CfOp::Delete(d) => {
                    let cf_handler = rdb.cf_handle(d.name()).unwrap();
                    batch.delete_cf(&cf_handler, d.key());
                }
            },
        }
    }
    let count = batch.len();
    match rdb.write(batch) {
        Ok(_) => Ok(count),
        Err(e) => Err(Error::Term(Box::new(format!("{}", e)))),
    }
}

#[rustler::nif]
fn iter<'a>(db: DB, maybe_mode: Option<IterMode>) -> KVIter {
    let mode = maybe_mode.unwrap_or(IterMode::default());
    let dbi = DbIter::new(db, mode);
    IterResource::from_db(dbi)
}

#[rustler::nif]
fn iter_next<'a>(env: Env<'a>, kv_iter: KVIter) -> Option<(Binary<'a>, Binary<'a>)> {
    let mut it = kv_iter.lock().write().unwrap();
    it.next()
        .map(|(key, value)| (new_binary(&key[..], env), new_binary(&value[..], env)))
}

#[rustler::nif]
fn iter_set_mode(kv_iter: KVIter, mode: IterMode) -> Atom {
    let mut it = kv_iter.lock().write().unwrap();
    it.set_mode((&mode).into());
    atoms::ok()
}

#[rustler::nif]
fn snapshot(db: DB) -> SS {
    SnapshotResource::new(db)
}

#[rustler::nif]
fn ss_fetch(ss: SS, key: Binary) -> NifResult<(Atom, Bin)> {
    match ss.rss.get(&key[..]) {
        Ok(Some(v)) => Ok((atoms::ok(), Bin::from_vec(v))),
        Ok(None) => Err(Error::Atom("error")),
        Err(e) => panic!("{}", e),
    }
}

#[rustler::nif]
fn ss_fetch_cf(ss: SS, name: String, key: Binary) -> NifResult<(Atom, Bin)> {
    let cf_handler = ss._db.rdb.cf_handle(&name[..]).unwrap();
    match ss.rss.get_cf(&cf_handler, &key[..]) {
        Ok(Some(v)) => Ok((atoms::ok(), Bin::from_vec(v))),
        Ok(None) => Err(Error::Atom("error")),
        Err(e) => panic!("error: {:?}", e),
    }
}

#[rustler::nif]
fn iter_valid<'a>(kv_iter: KVIter) -> bool {
    let it = kv_iter.lock().read().unwrap();
    it.valid()
}

#[rustler::nif]
fn create_cf(db: DB, name: String, open_opts: SoyOpenOpts) -> NifResult<Atom> {
    let opts = open_opts.into();
    ok_or_err!(db.rdb.create_cf(name.as_str(), &opts))
}

#[rustler::nif]
fn list_cf(path: String) -> Vec<String> {
    RDB::list_cf(&Options::default(), path).unwrap()
}

#[rustler::nif]
fn drop_cf(db: DB, name: String) -> NifResult<Atom> {
    ok_or_err!(db.rdb.drop_cf(name.as_str()))
}

#[rustler::nif]
fn put_cf(db: DB, name: String, key: Binary, val: Binary) -> NifResult<Atom> {
    let cf_handler = db.rdb.cf_handle(&name[..]).unwrap();
    ok_or_err!(db.rdb.put_cf(&cf_handler, &key[..], &val[..]))
}

#[rustler::nif]
fn fetch_cf(db: DB, name: String, key: Binary) -> NifResult<(Atom, Bin)> {
    let cf_handler = db.rdb.cf_handle(&name[..]).unwrap();
    match db.rdb.get_cf(&cf_handler, &key[..]) {
        Ok(Some(v)) => Ok((atoms::ok(), Bin::from_vec(v))),
        Ok(None) => Err(Error::Atom("error")),
        Err(e) => panic!("error: {:?}", e),
    }
}

#[rustler::nif]
fn delete_cf(db: DB, name: String, key: Binary) -> NifResult<Atom> {
    let cf_handler = db.rdb.cf_handle(&name[..]).unwrap();
    ok_or_err!(db.rdb.delete_cf(&cf_handler, &key[..]))
}

#[rustler::nif]
fn ss_iter<'a>(ss: SS, maybe_mode: Option<IterMode>) -> KVIter {
    let mode = maybe_mode.unwrap_or(IterMode::default());
    let ssi = SsIter::new(ss, mode);
    IterResource::from_ss(ssi)
}

#[rustler::nif]
fn ss_multi_get_cf<'a>(ss: SS, cf_and_keys: Vec<(String, Binary)>) -> Vec<Option<Bin>> {
    let rss = &ss.rss;
    let rdb = &ss._db.rdb;
    let cf_handle_keys: Vec<(Arc<rocksdb::BoundColumnFamily<'_>>, Binary)> = cf_and_keys
        .into_iter()
        .map(|(cf_name, key)| {
            let cf_handler: std::sync::Arc<rocksdb::BoundColumnFamily<'_>> =
                rdb.cf_handle(&cf_name[..]).unwrap();
            (cf_handler, key)
        })
        .collect();
    let keys_it = cf_handle_keys.iter().map(|(h, k)| (&*h, &k[..]));
    rss.multi_get_cf(keys_it)
        .into_iter()
        .map(|v| match v.unwrap() {
            Some(data) => Some(Bin::from_vec(data)),
            None => None,
        })
        .collect()
}

#[rustler::nif]
fn ss_multi_get<'a>(ss: SS, keys: Vec<Binary>) -> Vec<Option<Bin>> {
    let keys_it = keys.iter().map(|k| (&k[..]).to_vec());
    ss.rss
        .multi_get(keys_it)
        .into_iter()
        .map(|v| match v.unwrap() {
            Some(data) => Some(Bin::from_vec(data)),
            None => None,
        })
        .collect()
}

#[rustler::nif]
fn write_opts_default() -> SoyWriteOpts {
    SoyWriteOpts::default()
}

#[rustler::nif]
fn read_opts_default() -> SoyReadOpts {
    SoyReadOpts::default()
}

fn load(env: Env, _: Term) -> bool {
    rustler::resource!(DBResource, env);
    rustler::resource!(IterResource, env);
    rustler::resource!(SnapshotResource, env);
    true
}

rustler::init!(
    "Elixir.Soy.Native",
    [
        open,
        destroy,
        repair,
        checkpoint,
        db_path,
        put,
        fetch,
        delete,
        batch,
        iter,
        multi_get,
        iter_next,
        iter_valid,
        iter_set_mode,
        live_files,
        list_cf,
        drop_cf,
        put_cf,
        fetch_cf,
        delete_cf,
        create_cf,
        multi_get_cf,
        // snapshot funcs
        snapshot,
        ss_fetch,
        ss_fetch_cf,
        ss_iter,
        ss_multi_get,
        ss_multi_get_cf,
        // write_opts
        write_opts_default,
        // read_opts_default
        read_opts_default
    ],
    load = load
);
