use rocksdb::checkpoint::Checkpoint;
use rocksdb::{Options, Snapshot as RSnapshot, WriteBatch, DB as RocksDb};
use rustler::{
    Atom, Binary, Env, Error, NifRecord, NifResult, NifUnitEnum, NifUntaggedEnum, ResourceArc, Term,
};
use std::path::PathBuf;
use std::sync::Arc;

mod iteration;
use iteration::{IterLocker, IterResource, SafeIter};

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

pub struct DbResource {
    rdb: RocksDb,
}

impl DbResource {
    fn new(rdb: RocksDb) -> SoyDb {
        ResourceArc::new(DbResource { rdb })
    }
}
type SoyDb = ResourceArc<DbResource>;

type SoyIter = ResourceArc<IterResource>;

mod atoms {
    rustler::atoms! {
        ok,
        error,
    }
}

pub struct SnapshotResource {
    rss: RSnapshot<'static>,
    db: SoyDb,
}

unsafe fn extend_lifetime_rss<'b>(r: RSnapshot<'b>) -> RSnapshot<'static> {
    std::mem::transmute::<RSnapshot<'b>, RSnapshot<'static>>(r)
}

impl SnapshotResource {
    fn new(db: SoyDb) -> SoySnapshot {
        let rss = unsafe { extend_lifetime_rss(db.rdb.snapshot()) };
        ResourceArc::new(SnapshotResource { rss, db })
    }
}

type SoySnapshot = ResourceArc<SnapshotResource>;

#[rustler::nif]
fn open(path: String, open_opts: SoyOpenOpts) -> SoyDb {
    let opts = open_opts.into();
    match RocksDb::list_cf(&opts, &path[..]) {
        Ok(cfs) => {
            let rdb = RocksDb::open_cf(&opts, &path[..], cfs).unwrap();
            DbResource::new(rdb)
        }
        Err(_) => {
            let rdb = RocksDb::open(&opts, &path[..]).unwrap();
            DbResource::new(rdb)
        }
    }
}

#[rustler::nif(name = "path")]
fn db_path(db: SoyDb) -> String {
    db.rdb.path().to_str().unwrap().to_string()
}

#[rustler::nif]
fn checkpoint(db: SoyDb, checkpoint_path: String) -> Atom {
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
    ok_or_err!(RocksDb::destroy(&Options::default(), path))
}

#[rustler::nif(schedule = "DirtyIo")]
fn repair(path: String) -> NifResult<Atom> {
    ok_or_err!(rocksdb::DB::repair(&Options::default(), path))
}

#[rustler::nif]
fn put(db: SoyDb, key: Binary, val: Binary) -> NifResult<Atom> {
    ok_or_err!(db.rdb.put(&key[..], &val[..]))
}

#[rustler::nif]
fn fetch<'a>(db: SoyDb, key: Binary) -> NifResult<(Atom, Bin)> {
    match db.rdb.get(&key[..]) {
        Ok(Some(v)) => Ok((atoms::ok(), Bin::from_vec(v))),
        Ok(None) => Err(Error::Atom("error")),
        Err(e) => panic!("{}", e),
    }
}

#[rustler::nif]
fn delete(db: SoyDb, key: Binary) -> NifResult<Atom> {
    ok_or_err!(db.rdb.delete(&key[..]))
}

#[rustler::nif]
fn multi_get<'a>(db: SoyDb, keys: Vec<Binary>) -> Vec<Option<Bin>> {
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
fn multi_get_cf<'a>(db: SoyDb, cf_and_keys: Vec<(String, Binary)>) -> Vec<Option<Bin>> {
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
fn live_files(db: SoyDb) -> Vec<SoyLiveFile> {
    db.rdb
        .live_files()
        .unwrap()
        .into_iter()
        .map(|item| SoyLiveFile::from(item))
        .collect()
}

#[rustler::nif]
fn batch<'a>(db: SoyDb, ops: Vec<BatchOp>) -> NifResult<usize> {
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
fn db_iter<'a>(db: SoyDb) -> SoyIter {
    IterResource::from_db(db)
}

#[derive(Debug, NifUnitEnum)]
enum SeekAtom {
    First,
    Last,
    Next,
    Prev,
}

#[derive(NifRecord)]
#[tag = "next"]
pub struct SeekNext(Bin);

#[derive(NifRecord)]
#[tag = "prev"]
pub struct SeekPrev(Bin);

#[derive(NifUntaggedEnum)]
enum Seek {
    Atom(SeekAtom),
    Next(SeekNext),
    Prev(SeekPrev),
}

#[derive(NifRecord)]
#[tag = "next"]
pub struct SeekNextBin(Bin);

#[rustler::nif]
fn iter_seek<'a>(env: Env<'a>, soy_iter: SoyIter, seek: Seek) -> Option<(Binary<'a>, Binary<'a>)> {
    let mut it = soy_iter.lock().write().unwrap();
    match seek {
        Seek::Atom(SeekAtom::Next) => it.next(),
        Seek::Atom(SeekAtom::Prev) => it.prev(),
        Seek::Atom(SeekAtom::First) => it.seek_to_first(),
        Seek::Atom(SeekAtom::Last) => it.seek_to_last(),
        Seek::Next(SeekNext(key)) => it.seek(key.as_bytes()),
        Seek::Prev(SeekPrev(key)) => it.seek_for_prev(key.as_bytes()),
    }
    do_iter_key_value(env, &it)
}

#[rustler::nif]
fn iter_key(env: Env, soy_iter: SoyIter) -> Option<Binary> {
    soy_iter
        .lock()
        .read()
        .unwrap()
        .key()
        .map(|k| new_binary(&k[..], env))
}

#[rustler::nif]
fn iter_value(env: Env, soy_iter: SoyIter) -> Option<Binary> {
    soy_iter
        .lock()
        .read()
        .unwrap()
        .value()
        .map(|k| new_binary(&k[..], env))
}

#[rustler::nif]
fn iter_key_value(env: Env, soy_iter: SoyIter) -> Option<(Binary, Binary)> {
    let it = soy_iter.lock().read().unwrap();
    do_iter_key_value(env, &it)
}

fn do_iter_key_value<'a>(env: Env<'a>, it: &SafeIter<'a>) -> Option<(Binary<'a>, Binary<'a>)> {
    it.key_value()
        .map(|(k, v)| (new_binary(&k[..], env), new_binary(&v[..], env)))
}

#[rustler::nif]
fn snapshot(db: SoyDb) -> SoySnapshot {
    SnapshotResource::new(db)
}

#[rustler::nif]
fn ss_fetch(ss: SoySnapshot, key: Binary) -> NifResult<(Atom, Bin)> {
    match ss.rss.get(&key[..]) {
        Ok(Some(v)) => Ok((atoms::ok(), Bin::from_vec(v))),
        Ok(None) => Err(Error::Atom("error")),
        Err(e) => panic!("{}", e),
    }
}

#[rustler::nif]
fn ss_fetch_cf(ss: SoySnapshot, name: String, key: Binary) -> NifResult<(Atom, Bin)> {
    let cf_handler = ss.db.rdb.cf_handle(&name[..]).unwrap();
    match ss.rss.get_cf(&cf_handler, &key[..]) {
        Ok(Some(v)) => Ok((atoms::ok(), Bin::from_vec(v))),
        Ok(None) => Err(Error::Atom("error")),
        Err(e) => panic!("error: {:?}", e),
    }
}

#[rustler::nif]
fn iter_valid<'a>(soy_iter: SoyIter) -> bool {
    soy_iter.lock().read().unwrap().valid()
}

#[rustler::nif]
fn create_cf(db: SoyDb, name: String, open_opts: SoyOpenOpts) -> NifResult<Atom> {
    let opts = open_opts.into();
    ok_or_err!(db.rdb.create_cf(name.as_str(), &opts))
}

#[rustler::nif]
fn list_cf(path: String) -> Vec<String> {
    RocksDb::list_cf(&Options::default(), path).unwrap()
}

#[rustler::nif]
fn drop_cf(db: SoyDb, name: String) -> NifResult<Atom> {
    ok_or_err!(db.rdb.drop_cf(name.as_str()))
}

#[rustler::nif]
fn put_cf(db: SoyDb, name: String, key: Binary, val: Binary) -> NifResult<Atom> {
    let cf_handler = db.rdb.cf_handle(&name[..]).unwrap();
    ok_or_err!(db.rdb.put_cf(&cf_handler, &key[..], &val[..]))
}

#[rustler::nif]
fn fetch_cf(db: SoyDb, name: String, key: Binary) -> NifResult<(Atom, Bin)> {
    let cf_handler = db.rdb.cf_handle(&name[..]).unwrap();
    match db.rdb.get_cf(&cf_handler, &key[..]) {
        Ok(Some(v)) => Ok((atoms::ok(), Bin::from_vec(v))),
        Ok(None) => Err(Error::Atom("error")),
        Err(e) => panic!("error: {:?}", e),
    }
}

#[rustler::nif]
fn delete_cf(db: SoyDb, name: String, key: Binary) -> NifResult<Atom> {
    let cf_handler = db.rdb.cf_handle(&name[..]).unwrap();
    ok_or_err!(db.rdb.delete_cf(&cf_handler, &key[..]))
}

#[rustler::nif]
fn ss_iter<'a>(ss: SoySnapshot) -> SoyIter {
    IterResource::from_ss(ss)
}

#[rustler::nif]
fn ss_multi_get_cf<'a>(ss: SoySnapshot, cf_and_keys: Vec<(String, Binary)>) -> Vec<Option<Bin>> {
    let rss = &ss.rss;
    let rdb = &ss.db.rdb;
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
fn ss_multi_get<'a>(ss: SoySnapshot, keys: Vec<Binary>) -> Vec<Option<Bin>> {
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
    rustler::resource!(DbResource, env);
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
        db_iter,
        multi_get,
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
        read_opts_default,
        // iter funcs
        iter_key,
        iter_value,
        iter_key_value,
        // iter_next,
        // iter_prev,
        iter_seek,
        // iter_seek_for_prev,
        iter_valid,
    ],
    load = load
);
