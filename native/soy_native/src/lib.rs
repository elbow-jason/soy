use rocksdb::checkpoint::Checkpoint;
use rocksdb::properties as props;
use rocksdb::{BoundColumnFamily, Options, Snapshot as RSnapshot, WriteBatch, DB as RocksDb};
use rustler::{
    Atom, Binary, Env, Error as NifError, NifRecord, NifResult, NifUnitEnum, NifUntaggedEnum,
    ResourceArc, Term,
};
use std::path::Path;
use std::sync::Arc;

mod iteration;
use iteration::{IterLocker, IterResource, SafeIter, WalIterator, WalRow};

mod bin;
use bin::{new_binary, Bin, BinStr};

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

mod error;
use error::Error;

pub mod merger;

macro_rules! ok_or_err {
    ($res:expr) => {
        match $res {
            Ok(()) => Ok(atoms::ok()),
            Err(e) => Err(NifError::Term(Box::new(format!("{}", e)))),
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
        put,
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
fn path_open_db(path: BinStr, open_opts: SoyOpenOpts) -> SoyDb {
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

type CfHandle<'a> = Arc<BoundColumnFamily<'a>>;

fn get_cf_handle<'a>(rdb: &'a RocksDb, name: &str) -> Result<CfHandle<'a>, Error> {
    match rdb.cf_handle(name) {
        Some(cf_handle) => Ok(cf_handle),
        None => Err(Error::ColumnFamilyDoesNotExist(name.to_string())),
    }
}

#[rustler::nif]
fn db_path(env: Env, db: SoyDb) -> Binary {
    new_binary(db.rdb.path().to_str().unwrap().as_bytes(), env)
}

#[rustler::nif]
fn db_checkpoint(db: SoyDb, checkpoint_path: BinStr) -> Atom {
    let cp_path = Path::new(&checkpoint_path[..]);
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
fn path_destroy(path: BinStr) -> NifResult<Atom> {
    ok_or_err!(RocksDb::destroy(&Options::default(), &path[..]))
}

#[rustler::nif(schedule = "DirtyIo")]
fn path_repair(path: BinStr) -> NifResult<Atom> {
    ok_or_err!(rocksdb::DB::repair(&Options::default(), &path[..]))
}

#[rustler::nif]
fn db_put(db: SoyDb, key: Binary, val: Binary) -> NifResult<Atom> {
    ok_or_err!(db.rdb.put(&key[..], &val[..]))
}

#[rustler::nif]
fn db_fetch<'a>(db: SoyDb, key: Binary) -> NifResult<(Atom, Bin)> {
    match db.rdb.get(&key[..]) {
        Ok(Some(v)) => Ok((atoms::ok(), Bin::from_vec(v))),
        Ok(None) => Err(NifError::Atom("error")),
        Err(e) => panic!("{}", e),
    }
}

#[rustler::nif]
fn db_delete(db: SoyDb, key: Binary) -> NifResult<Atom> {
    ok_or_err!(db.rdb.delete(&key[..]))
}

#[rustler::nif]
fn db_merge(db: SoyDb, key: Binary, val: Binary) -> NifResult<Atom> {
    ok_or_err!(db.rdb.merge(&key[..], &val[..]))
}

#[rustler::nif]
fn db_merge_cf(db: SoyDb, cf_name: BinStr, key: Binary, val: Binary) -> NifResult<Atom> {
    let cf_handle = get_cf_handle(&db.rdb, &cf_name[..]).unwrap();
    ok_or_err!(db.rdb.merge_cf(&cf_handle, &key[..], &val[..]))
}

#[rustler::nif(schedule = "DirtyIo")]
fn db_flush(db: SoyDb) -> NifResult<Atom> {
    ok_or_err!(db.rdb.flush())
}

#[rustler::nif(schedule = "DirtyIo")]
fn db_flush_cf(db: SoyDb, name: BinStr) -> NifResult<Atom> {
    let cf_handle = get_cf_handle(&db.rdb, &name).unwrap();
    ok_or_err!(db.rdb.flush_cf(&cf_handle))
}

#[rustler::nif(schedule = "DirtyIo")]
fn db_flush_wal(db: SoyDb, sync: bool) -> NifResult<Atom> {
    ok_or_err!(db.rdb.flush_wal(sync))
}

#[rustler::nif]
fn db_latest_sequence_number(db: SoyDb) -> u64 {
    db.rdb.latest_sequence_number()
}

#[rustler::nif]
fn db_get_updates_since(
    db: SoyDb,
    sequence_number: u64,
) -> Result<ResourceArc<WalIterator>, Error> {
    let it = WalIterator::new(db, sequence_number)?;
    Ok(ResourceArc::new(it))
}

#[derive(Debug, NifUntaggedEnum)]
pub enum Prop {
    String(String),
    Int(u64),
}

#[rustler::nif]
fn db_get_property(db: SoyDb, prop: &str) -> Option<Prop> {
    do_get_property(&db.rdb, prop)
}

#[rustler::nif]
fn db_list_properties(db: SoyDb) -> Vec<(String, Option<Prop>)> {
    let rdb = &db.rdb;
    vec![
        prop_kv(rdb, props::ACTUAL_DELAYED_WRITE_RATE),
        prop_kv(rdb, props::AGGREGATED_TABLE_PROPERTIES),
        prop_kv(rdb, props::AGGREGATED_TABLE_PROPERTIES_AT_LEVEL),
        prop_kv(rdb, props::BACKGROUND_ERRORS),
        prop_kv(rdb, props::BASE_LEVEL),
        prop_kv(rdb, props::BLOCK_CACHE_CAPACITY),
        prop_kv(rdb, props::BLOCK_CACHE_PINNED_USAGE),
        prop_kv(rdb, props::BLOCK_CACHE_USAGE),
        prop_kv(rdb, props::CFSTATS),
        prop_kv(rdb, props::CFSTATS_NO_FILE_HISTOGRAM),
        prop_kv(rdb, props::CF_FILE_HISTOGRAM),
        prop_kv(rdb, props::COMPACTION_PENDING),
        prop_kv(rdb, props::COMPRESSION_RATIO_AT_LEVEL),
        prop_kv(rdb, props::CURRENT_SUPER_VERSION_NUMBER),
        prop_kv(rdb, props::CUR_SIZE_ACTIVE_MEM_TABLE),
        prop_kv(rdb, props::CUR_SIZE_ALL_MEM_TABLES),
        prop_kv(rdb, props::DBSTATS),
        prop_kv(rdb, props::ESTIMATE_LIVE_DATA_SIZE),
        prop_kv(rdb, props::ESTIMATE_NUM_KEYS),
        prop_kv(rdb, props::ESTIMATE_OLDEST_KEY_TIME),
        prop_kv(rdb, props::ESTIMATE_PENDING_COMPACTION_BYTES),
        prop_kv(rdb, props::ESTIMATE_TABLE_READERS_MEM),
        prop_kv(rdb, props::IS_FILE_DELETIONS_ENABLED),
        prop_kv(rdb, props::IS_WRITE_STOPPED),
        prop_kv(rdb, props::LEVELSTATS),
        prop_kv(rdb, props::LIVE_SST_FILES_SIZE),
        prop_kv(rdb, props::MEM_TABLE_FLUSH_PENDING),
        prop_kv(rdb, props::MIN_LOG_NUMBER_TO_KEEP),
        prop_kv(rdb, props::MIN_OBSOLETE_SST_NUMBER_TO_KEEP),
        prop_kv(rdb, props::NUM_DELETES_ACTIVE_MEM_TABLE),
        prop_kv(rdb, props::NUM_DELETES_IMM_MEM_TABLES),
        prop_kv(rdb, props::NUM_ENTRIES_ACTIVE_MEM_TABLE),
        prop_kv(rdb, props::NUM_ENTRIES_IMM_MEM_TABLES),
        prop_kv(rdb, props::NUM_FILES_AT_LEVEL_PREFIX),
        prop_kv(rdb, props::NUM_IMMUTABLE_MEM_TABLE),
        prop_kv(rdb, props::NUM_IMMUTABLE_MEM_TABLE_FLUSHED),
        prop_kv(rdb, props::NUM_LIVE_VERSIONS),
        prop_kv(rdb, props::NUM_RUNNING_COMPACTIONS),
        prop_kv(rdb, props::NUM_RUNNING_FLUSHES),
        prop_kv(rdb, props::NUM_SNAPSHOTS),
        prop_kv(rdb, props::OLDEST_SNAPSHOT_TIME),
        prop_kv(rdb, props::NUM_RUNNING_COMPACTIONS),
        prop_kv(rdb, props::OPTIONS_STATISTICS),
        prop_kv(rdb, props::SIZE_ALL_MEM_TABLES),
        prop_kv(rdb, props::SSTABLES),
        prop_kv(rdb, props::STATS),
        prop_kv(rdb, props::TOTAL_SST_FILES_SIZE),
    ]
}

fn prop_kv(rdb: &RocksDb, prop: &str) -> (String, Option<Prop>) {
    (prop.to_string(), do_get_property(rdb, prop))
}

fn do_get_property(db: &RocksDb, prop: &str) -> Option<Prop> {
    match db.property_int_value(prop) {
        Ok(Some(int)) => Some(Prop::Int(int)),
        Ok(None) => None,
        Err(_) => match db.property_value(prop) {
            Ok(Some(val)) => Some(Prop::String(val)),
            Ok(None) => None,
            Err(e) => panic!("{}", e),
        },
    }
}

#[rustler::nif]
fn wal_iter_next(w: ResourceArc<WalIterator>) -> Option<(u64, Vec<WalRow>)> {
    w.next()
}

#[rustler::nif]
fn db_multi_get<'a>(db: SoyDb, keys: Vec<Binary>) -> Vec<Option<Bin>> {
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
fn db_multi_get_cf<'a>(db: SoyDb, cf_and_keys: Vec<(BinStr, Binary)>) -> Vec<Option<Bin>> {
    let rdb = &db.rdb;
    let cf_handle_keys: Vec<(Arc<rocksdb::BoundColumnFamily<'_>>, Binary)> = cf_and_keys
        .into_iter()
        .map(|(cf_name, key)| {
            let cf_handle = get_cf_handle(&rdb, &cf_name[..]).unwrap();
            (cf_handle, key)
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
fn db_live_files(db: SoyDb) -> Vec<SoyLiveFile> {
    db.rdb
        .live_files()
        .unwrap()
        .into_iter()
        .map(|item| SoyLiveFile::from(item))
        .collect()
}

#[rustler::nif]
fn db_batch<'a>(db: SoyDb, ops: Vec<BatchOp>) -> NifResult<usize> {
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
                    let cf_handler = get_cf_handle(&rdb, p.name()).unwrap();
                    batch.put_cf(&cf_handler, p.key(), p.val());
                }
                CfOp::Delete(d) => {
                    let cf_handler = get_cf_handle(&rdb, d.name()).unwrap();
                    batch.delete_cf(&cf_handler, d.key());
                }
            },
        }
    }
    let count = batch.len();
    match rdb.write(batch) {
        Ok(_) => Ok(count),
        Err(e) => Err(NifError::Term(Box::new(format!("{}", e)))),
    }
}

#[rustler::nif]
fn db_iter<'a>(db: SoyDb) -> SoyIter {
    IterResource::from_db(db)
}

#[rustler::nif]
fn db_iter_cf<'a>(db: SoyDb, name: BinStr) -> SoyIter {
    IterResource::from_db_cf(db, &name[..])
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
fn db_snapshot(db: SoyDb) -> SoySnapshot {
    SnapshotResource::new(db)
}

#[rustler::nif]
fn ss_fetch(ss: SoySnapshot, key: Binary) -> NifResult<(Atom, Bin)> {
    match ss.rss.get(&key[..]) {
        Ok(Some(v)) => Ok((atoms::ok(), Bin::from_vec(v))),
        Ok(None) => Err(NifError::Atom("error")),
        Err(e) => panic!("{}", e),
    }
}

#[rustler::nif]
fn ss_fetch_cf(ss: SoySnapshot, name: BinStr, key: Binary) -> NifResult<(Atom, Bin)> {
    let cf_handler = get_cf_handle(&ss.db.rdb, &name).unwrap();
    match ss.rss.get_cf(&cf_handler, &key[..]) {
        Ok(Some(v)) => Ok((atoms::ok(), Bin::from_vec(v))),
        Ok(None) => Err(NifError::Atom("error")),
        Err(e) => panic!("error: {:?}", e),
    }
}

#[rustler::nif]
fn iter_valid<'a>(soy_iter: SoyIter) -> bool {
    soy_iter.lock().read().unwrap().valid()
}

#[rustler::nif]
fn db_create_cf(db: SoyDb, name: BinStr, open_opts: SoyOpenOpts) -> NifResult<Atom> {
    let opts = open_opts.into();
    ok_or_err!(db.rdb.create_cf(&name[..], &opts))
}

#[rustler::nif]
fn path_list_cf(path: BinStr) -> Vec<String> {
    RocksDb::list_cf(&Options::default(), &path[..]).unwrap()
}

#[rustler::nif]
fn db_drop_cf(db: SoyDb, name: BinStr) -> NifResult<Atom> {
    ok_or_err!(db.rdb.drop_cf(&name[..]))
}

#[rustler::nif]
fn db_put_cf(db: SoyDb, name: BinStr, key: Binary, val: Binary) -> NifResult<Atom> {
    let cf_handler = get_cf_handle(&db.rdb, &name).unwrap();
    ok_or_err!(db.rdb.put_cf(&cf_handler, &key[..], &val[..]))
}

#[rustler::nif]
fn db_fetch_cf(db: SoyDb, name: BinStr, key: Binary) -> NifResult<(Atom, Bin)> {
    let cf_handler = get_cf_handle(&db.rdb, &name).unwrap();
    match db.rdb.get_cf(&cf_handler, &key[..]) {
        Ok(Some(v)) => Ok((atoms::ok(), Bin::from_vec(v))),
        Ok(None) => Err(NifError::Atom("error")),
        Err(e) => panic!("error: {:?}", e),
    }
}

#[rustler::nif]
fn db_delete_cf(db: SoyDb, name: BinStr, key: Binary) -> NifResult<Atom> {
    let cf_handler = get_cf_handle(&db.rdb, &name).unwrap();
    ok_or_err!(db.rdb.delete_cf(&cf_handler, &key[..]))
}

#[rustler::nif]
fn db_key_may_exist(db: SoyDb, key: Binary) -> bool {
    db.rdb.key_may_exist(&key[..])
}

#[rustler::nif]
fn db_key_may_exist_cf(db: SoyDb, cf: BinStr, key: Binary) -> bool {
    let cf_handle = get_cf_handle(&db.rdb, &cf[..]).unwrap();
    db.rdb.key_may_exist_cf(&cf_handle, &key[..])
}

#[rustler::nif]
fn db_key_exists(db: SoyDb, key: Binary) -> bool {
    let may_exist = db.rdb.key_may_exist(&key[..]);
    if !may_exist {
        return false;
    }
    match db.rdb.get(&key[..]) {
        Ok(Some(_)) => true,
        Ok(None) => false,
        Err(e) => panic!("{}", e),
    }
}

#[rustler::nif]
fn db_key_exists_cf(db: SoyDb, cf: BinStr, key: Binary) -> bool {
    let cf_handle = get_cf_handle(&db.rdb, &cf[..]).unwrap();
    let may_exist = db.rdb.key_may_exist_cf(&cf_handle, &key[..]);
    if !may_exist {
        return false;
    }
    match db.rdb.get(&key[..]) {
        Ok(Some(_)) => true,
        Ok(None) => false,
        Err(e) => panic!("{}", e),
    }
}

#[rustler::nif]
fn ss_iter<'a>(ss: SoySnapshot) -> SoyIter {
    IterResource::from_ss(ss)
}

#[rustler::nif]
fn ss_iter_cf<'a>(ss: SoySnapshot, name: BinStr) -> SoyIter {
    IterResource::from_ss_cf(ss, &name[..])
}

#[rustler::nif]
fn ss_multi_get_cf<'a>(ss: SoySnapshot, cf_and_keys: Vec<(BinStr, Binary)>) -> Vec<Option<Bin>> {
    let rss = &ss.rss;
    let rdb = &ss.db.rdb;
    let cf_handle_keys: Vec<(Arc<rocksdb::BoundColumnFamily<'_>>, Binary)> = cf_and_keys
        .into_iter()
        .map(|(cf_name, key)| {
            let cf_handle = get_cf_handle(&rdb, &cf_name).unwrap();
            (cf_handle, key)
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
    rustler::resource!(WalIterator, env);
    true
}

rustler::init!(
    "Elixir.Soy.Native",
    [
        // path ops
        path_destroy,
        path_repair,
        path_list_cf,
        path_open_db,
        // db ops
        // backups
        db_checkpoint,
        db_path,
        // write ops
        db_put,
        db_delete,
        db_batch,
        db_merge,
        // write cf ops
        db_delete_cf,
        db_merge_cf,
        db_drop_cf,
        db_put_cf,
        db_create_cf,
        // read ops
        db_fetch,
        db_multi_get,
        db_key_may_exist,
        db_key_may_exist_cf,
        db_key_exists,
        db_key_exists_cf,
        // read cf ops
        db_fetch_cf,
        db_multi_get_cf,
        // flushing/sync
        db_flush,
        db_flush_wal,
        db_flush_cf,
        // iter creation
        db_iter,
        db_iter_cf,
        // snaphot creation
        db_snapshot,
        // db props/metadata/introspection
        db_get_property,
        db_list_properties,
        db_live_files,
        // snapshot ops
        ss_iter,
        ss_iter_cf,
        // snapshot funcs
        ss_fetch,
        ss_fetch_cf,
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
        iter_seek,
        iter_valid,
    ],
    load = load
);
