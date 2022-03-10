use rocksdb::WriteOptions;
use rustler::NifStruct;

#[derive(Debug, NifStruct, Clone, Copy)]
#[must_use] // Added to test Issue #152
#[module = "Soy.WriteOpts"]
pub struct SoyWriteOpts {
    disable_wal: bool,
    set_ignore_missing_column_families: bool,
    set_low_pri: bool,
    set_memtable_insert_hint_per_batch: bool,
    set_no_slowdown: bool,
    set_sync: bool,
}

impl Default for SoyWriteOpts {
    fn default() -> SoyWriteOpts {
        SoyWriteOpts {
            disable_wal: false,
            set_ignore_missing_column_families: false,
            set_low_pri: false,
            set_memtable_insert_hint_per_batch: false,
            set_no_slowdown: false,
            set_sync: false,
        }
    }
}

impl From<SoyWriteOpts> for WriteOptions {
    fn from(swo: SoyWriteOpts) -> WriteOptions {
        let mut wo = WriteOptions::default();
        wo.disable_wal(swo.disable_wal);
        wo.set_ignore_missing_column_families(swo.set_ignore_missing_column_families);
        wo.set_low_pri(swo.set_low_pri);
        wo.set_memtable_insert_hint_per_batch(swo.set_memtable_insert_hint_per_batch);
        wo.set_no_slowdown(swo.set_no_slowdown);
        wo.set_sync(swo.set_sync);
        wo
    }
}
