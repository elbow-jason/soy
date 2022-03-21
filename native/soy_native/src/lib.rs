use rocksdb::checkpoint::Checkpoint;
use rocksdb::properties as props;
use rocksdb::{ColumnFamilyRef, Options, WriteBatch, DB as RocksDb};
use rustler::{
    Atom, Binary, Env, Error as NifError, NifRecord, NifResult, NifUnitEnum, NifUntaggedEnum,
    ResourceArc, Term,
};
use std::path::Path;

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

mod db_col_fam;
use db_col_fam::{DbColFamResource, SoyDbColFam};

mod snapshot_col_fam;
use snapshot_col_fam::{SoySsColFam, SsColFamResource};

mod snapshot;
use snapshot::SnapshotResource;

mod soy_db;
use soy_db::{DbResource, SoyDb};

type SoySnapshot = ResourceArc<SnapshotResource>;

macro_rules! ok_or_err {
    ($res:expr) => {
        match $res {
            Ok(()) => Ok(atoms::ok()),
            Err(e) => Err(NifError::Term(Box::new(format!("{}", e)))),
        }
    };
}

type SoyIter = ResourceArc<IterResource>;

mod atoms {
    rustler::atoms! {
        ok,
        error,
        put,
    }
}

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

#[rustler::nif]
fn db_path(env: Env, db: SoyDb) -> Binary {
    new_binary(db.rocks_db_ref().path().to_str().unwrap().as_bytes(), env)
}

#[rustler::nif]
fn db_checkpoint(db: SoyDb, checkpoint_path: BinStr) -> Atom {
    let cp_path = Path::new(&checkpoint_path[..]);
    let rdb = db.rocks_db_ref();
    if rdb.path() == cp_path {
        panic!(
            "checkpoint path cannot be the same as the db path - got: {}",
            cp_path.to_str().unwrap()
        );
    }
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
    ok_or_err!(db.rocks_db_ref().put(&key[..], &val[..]))
}

#[rustler::nif]
fn db_fetch<'a>(db: SoyDb, key: Binary) -> NifResult<(Atom, Bin)> {
    match db.rocks_db_ref().get(&key[..]) {
        Ok(Some(v)) => Ok((atoms::ok(), Bin::from_vec(v))),
        Ok(None) => Err(NifError::Atom("error")),
        Err(e) => panic!("{}", e),
    }
}

#[rustler::nif]
fn db_delete(db: SoyDb, key: Binary) -> NifResult<Atom> {
    ok_or_err!(db.rocks_db_ref().delete(&key[..]))
}

#[rustler::nif]
fn db_merge(db: SoyDb, key: Binary, val: Binary) -> NifResult<Atom> {
    ok_or_err!(db.rocks_db_ref().merge(&key[..], &val[..]))
}

#[rustler::nif]
fn db_merge_cf(db: SoyDb, cf_name: BinStr, key: Binary, val: Binary) -> NifResult<Atom> {
    let rdb = db.rocks_db_ref();
    let cf_handle = get_cf_handle(rdb, &cf_name[..]).unwrap();
    ok_or_err!(rdb.merge_cf(&cf_handle, &key[..], &val[..]))
}

#[rustler::nif(schedule = "DirtyIo")]
fn db_flush(db: SoyDb) -> NifResult<Atom> {
    ok_or_err!(db.rocks_db_ref().flush())
}

#[rustler::nif(schedule = "DirtyIo")]
fn db_cf_flush(db_cf: SoyDbColFam) -> NifResult<Atom> {
    let handle = db_cf.handle();
    ok_or_err!(db_cf.rocks_db_ref().flush_cf(handle))
}

#[rustler::nif(schedule = "DirtyIo")]
fn db_flush_wal(db: SoyDb, sync: bool) -> NifResult<Atom> {
    ok_or_err!(db.rocks_db_ref().flush_wal(sync))
}

#[rustler::nif]
fn db_latest_sequence_number(db: SoyDb) -> u64 {
    db.rocks_db_ref().latest_sequence_number()
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
    do_get_property(&db.rocks_db_ref(), prop)
}

#[rustler::nif]
fn db_list_properties(db: SoyDb) -> Vec<(String, Option<Prop>)> {
    let rdb = db.rocks_db_ref();
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
    db.rocks_db_ref()
        .multi_get(keys_it)
        .into_iter()
        .map(|v| match v.unwrap() {
            Some(data) => Some(Bin::from_vec(data)),
            None => None,
        })
        .collect()
}

#[rustler::nif]
fn db_cf_multi_get<'a>(pairs: Vec<(SoyDbColFam, Binary)>) -> Vec<Option<Bin>> {
    if pairs.len() == 0 {
        return vec![];
    }
    let db_cf = pairs.first().unwrap().clone().0;

    // let cf_handle_keys: Vec<(Arc<rocksdb::BoundColumnFamily<'_>>, Binary)> = cf_and_keys
    //     .into_iter()
    //     .map(|(cf_name, key)| {
    //         let cf_handle = get_cf_handle(&rdb, &cf_name[..]).unwrap();
    //         (cf_handle, key)
    //     })
    //     .collect();
    let keys_it = pairs.iter().map(|(h, k)| (h.handle(), &k[..]));
    db_cf
        .rocks_db_ref()
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
    db.rocks_db_ref()
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
    let rdb = db.rocks_db_ref();
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
fn db_cf_iter<'a>(db_cf: SoyDbColFam) -> SoyIter {
    IterResource::from_db_cf(db_cf)
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
    match ss.rocks_ss_ref().get(&key[..]) {
        Ok(Some(v)) => Ok((atoms::ok(), Bin::from_vec(v))),
        Ok(None) => Err(NifError::Atom("error")),
        Err(e) => panic!("{}", e),
    }
}

#[rustler::nif]
fn ss_open_ss_cf(ss: SoySnapshot, name: BinStr) -> NifResult<SoySsColFam> {
    let ss_cf = SsColFamResource::new(&ss, &name[..])?;
    Ok(ResourceArc::new(ss_cf))
}

#[rustler::nif]
fn iter_valid<'a>(soy_iter: SoyIter) -> bool {
    soy_iter.lock().read().unwrap().valid()
}

#[rustler::nif]
fn db_create_new_cf(db: SoyDb, name: BinStr, open_opts: SoyOpenOpts) -> NifResult<SoyDbColFam> {
    let opts = open_opts.into();
    match db.rocks_db_ref().create_cf(&name[..], &opts) {
        Ok(()) => build_cf_db(&db, &name[..]),
        Err(e) => Err(NifError::Term(Box::new(format!("{}", e)))),
    }
}

#[rustler::nif]
fn db_open_existing_cf(db: SoyDb, name: BinStr) -> NifResult<SoyDbColFam> {
    build_cf_db(&db, &name[..])
}

fn build_cf_db(db: &SoyDb, name: &str) -> NifResult<SoyDbColFam> {
    let resource = DbColFamResource::new(db, name)?;
    Ok(ResourceArc::new(resource))
}

#[rustler::nif]
fn db_cf_put(db_cf: SoyDbColFam, key: Binary, val: Binary) -> NifResult<Atom> {
    let handle = db_cf.handle();
    ok_or_err!(db_cf.rocks_db_ref().put_cf(handle, &key[..], &val[..]))
}

#[rustler::nif]
fn db_cf_fetch(db_cf: SoyDbColFam, key: Binary) -> NifResult<(Atom, Bin)> {
    let handle = db_cf.handle();
    match db_cf.rocks_db_ref().get_cf(handle, &key[..]) {
        Ok(Some(v)) => Ok((atoms::ok(), Bin::from_vec(v))),
        Ok(None) => Err(NifError::Atom("error")),
        Err(e) => panic!("error: {:?}", e),
    }
}

#[rustler::nif]
fn db_cf_name(env: Env, db_cf: SoyDbColFam) -> Binary {
    new_binary(db_cf.name().as_bytes(), env)
}

#[rustler::nif]
fn db_cf_into_db(db_cf: SoyDbColFam) -> SoyDb {
    db_cf.soy_db().clone()
}

#[rustler::nif]
fn db_cf_delete(db_cf: SoyDbColFam, key: Binary) -> NifResult<Atom> {
    let handle = db_cf.handle();
    ok_or_err!(db_cf.rocks_db_ref().delete_cf(handle, &key[..]))
}

#[rustler::nif]
fn db_cf_merge(db_cf: SoyDbColFam, key: Binary, val: Binary) -> NifResult<Atom> {
    let handle = db_cf.handle();
    ok_or_err!(db_cf.rocks_db_ref().merge_cf(handle, &key[..], &val[..]))
}

#[rustler::nif]
fn db_cf_key_may_exist(db_cf: SoyDbColFam, key: Binary) -> bool {
    let handle = db_cf.handle();
    db_cf.rocks_db_ref().key_may_exist_cf(handle, &key[..])
}

#[rustler::nif]
fn db_cf_has_key(db_cf: SoyDbColFam, key: Binary) -> bool {
    let handle = db_cf.handle();
    let may_exist = db_cf.rocks_db_ref().key_may_exist_cf(handle, &key[..]);
    if !may_exist {
        return false;
    }
    match db_cf.rocks_db_ref().get_cf(db_cf.handle(), &key[..]) {
        Ok(Some(_)) => true,
        Ok(None) => false,
        Err(e) => panic!("{}", e),
    }
}

#[rustler::nif]
fn path_list_cf(path: BinStr) -> Vec<String> {
    RocksDb::list_cf(&Options::default(), &path[..]).unwrap()
}

#[rustler::nif]
fn db_drop_cf(db: SoyDb, name: BinStr) -> NifResult<Atom> {
    ok_or_err!(db.rocks_db_ref().drop_cf(&name[..]))
}

#[rustler::nif]
fn db_key_may_exist(db: SoyDb, key: Binary) -> bool {
    db.rocks_db_ref().key_may_exist(&key[..])
}

#[rustler::nif]
fn db_has_key(db: SoyDb, key: Binary) -> bool {
    let rdb = db.rocks_db_ref();
    let may_exist = rdb.key_may_exist(&key[..]);
    if !may_exist {
        return false;
    }
    match rdb.get(&key[..]) {
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
fn ss_cf_fetch(ss_cf: SoySsColFam, key: Binary) -> NifResult<(Atom, Bin)> {
    let handle = ss_cf.handle();
    match ss_cf.rocks_ss_ref().get_cf(handle, &key[..]) {
        Ok(Some(v)) => Ok((atoms::ok(), Bin::from_vec(v))),
        Ok(None) => Err(NifError::Atom("error")),
        Err(e) => panic!("error: {:?}", e),
    }
}

#[rustler::nif]
fn ss_cf_iter<'a>(ss_cf: SoySsColFam) -> SoyIter {
    IterResource::from_ss_cf(ss_cf)
}

#[rustler::nif]
fn ss_cf_name<'a>(env: Env, ss_cf: SoySsColFam) -> Binary {
    new_binary(ss_cf.name().as_bytes(), env)
}

#[rustler::nif]
fn ss_cf_into_ss(ss_cf: SoySsColFam) -> SoySnapshot {
    ss_cf.soy_snapshot().clone()
}

#[rustler::nif]
fn ss_cf_multi_get<'a>(pairs: Vec<(SoySsColFam, Binary)>) -> Vec<Option<Bin>> {
    if pairs.len() == 0 {
        return vec![];
    }
    let ss_cf = pairs.first().unwrap().clone().0;
    let rss = ss_cf.rocks_ss_ref();
    let pairs_it = pairs.iter().map(|(h, k)| (h.handle(), &k[..]));
    rss.multi_get_cf(pairs_it)
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
    ss.rocks_ss_ref()
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

#[derive(NifUntaggedEnum)]
pub enum SoyResource {
    Db(SoyDb),
    DbCf(SoyDbColFam),
    Ss(SoySnapshot),
    SsCf(SoySsColFam),
}

#[derive(NifUnitEnum)]
pub enum SoyResourceKind {
    Db,
    DbCf,
    Ss,
    SsCf,
}

impl SoyResource {
    fn kind(&self) -> SoyResourceKind {
        match self {
            SoyResource::Db(_) => SoyResourceKind::Db,
            SoyResource::DbCf(_) => SoyResourceKind::DbCf,
            SoyResource::Ss(_) => SoyResourceKind::Ss,
            SoyResource::SsCf(_) => SoyResourceKind::SsCf,
        }
    }
}

#[rustler::nif]
fn resource_kind(r: SoyResource) -> SoyResourceKind {
    r.kind()
}

fn load(env: Env, _: Term) -> bool {
    rustler::resource!(DbResource, env);
    rustler::resource!(DbColFamResource, env);
    rustler::resource!(SsColFamResource, env);
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
        // cf create/open/drop ops
        db_open_existing_cf,
        db_create_new_cf,
        db_drop_cf,
        // read ops
        db_fetch,
        db_multi_get,
        db_key_may_exist,
        db_has_key,
        // flushing/sync
        db_flush,
        db_flush_wal,
        // iter creation
        db_iter,
        // snaphot creation
        db_snapshot,
        // db props/metadata/introspection
        db_get_property,
        db_list_properties,
        db_live_files,
        // snapshot ops
        ss_iter,
        // snapshot funcs
        ss_fetch,
        ss_multi_get,
        ss_open_ss_cf,
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
        // cf resource ops
        db_cf_put,
        db_cf_fetch,
        db_cf_name,
        db_cf_into_db,
        db_cf_delete,
        db_cf_merge,
        db_cf_key_may_exist,
        db_cf_has_key,
        db_cf_multi_get,
        db_cf_flush,
        db_cf_iter,
        // cf_ss resource ops
        ss_cf_fetch,
        ss_cf_multi_get,
        ss_cf_iter,
        ss_cf_name,
        ss_cf_into_ss,
    ],
    load = load
);

pub(crate) fn get_cf_handle<'a>(
    rdb: &'a RocksDb,
    name: &str,
) -> Result<ColumnFamilyRef<'a>, Error> {
    match rdb.cf_handle(name) {
        Some(cf_handle) => Ok(cf_handle),
        None => Err(Error::ColumnFamilyDoesNotExist(name.to_string())),
    }
}

// #[rustler::nif]
// fn db_key_may_exist_cf(db: SoyDb, cf: BinStr, key: Binary) -> bool {
//     let cf_handle = get_cf_handle(&db.rdb, &cf[..]).unwrap();
//     db.rdb.key_may_exist_cf(&cf_handle, &key[..])
// }

// #[rustler::nif]
// fn db_put_cf(db: SoyDb, name: BinStr, key: Binary, val: Binary) -> NifResult<Atom> {
//     let cf_handler = get_cf_handle(&db.rdb, &name).unwrap();
//     ok_or_err!(db.rocks_db_ref().put_cf(&cf_handler, &key[..], &val[..]))
// }

// #[rustler::nif]
// fn db_fetch_cf(db: SoyDb, name: BinStr, key: Binary) -> NifResult<(Atom, Bin)> {

//     let cf_handler = get_cf_handle(&db.rdb, &name).unwrap();
//     match db.rocks_db_ref().get_cf(&cf_handler, &key[..]) {
//         Ok(Some(v)) => Ok((atoms::ok(), Bin::from_vec(v))),
//         Ok(None) => Err(NifError::Atom("error")),
//         Err(e) => panic!("error: {:?}", e),
//     }
// }

// #[rustler::nif]
// fn db_delete_cf(db: SoyDb, name: BinStr, key: Binary) -> NifResult<Atom> {
//     let rdb = db.rocks_db_ref();
//     let cf_handler = get_cf_handle(rdb, &name).unwrap();
//     ok_or_err!(rdb.delete_cf(&cf_handler, &key[..]))
// }
