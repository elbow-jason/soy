use rocksdb::{DBCompactionStyle, Options};
use rustler::{NifStruct, NifUnitEnum};

use crate::merger;

#[derive(Debug, NifStruct)]
#[must_use] // Added to test Issue #152
#[module = "Soy.OpenOpts"]
pub struct SoyOpenOpts {
    create_if_missing: Option<bool>,
    create_missing_column_families: Option<bool>,
    set_max_open_files: Option<i32>,
    set_use_fsync: Option<bool>,
    set_bytes_per_sync: Option<u64>,
    optimize_for_point_lookup: Option<u64>,
    set_table_cache_num_shard_bits: Option<i32>,
    set_max_write_buffer_number: Option<i32>,
    set_write_buffer_size: Option<usize>,
    set_target_file_size_base: Option<u64>,
    set_min_write_buffer_number_to_merge: Option<i32>,
    set_level_zero_stop_writes_trigger: Option<i32>,
    set_level_zero_slowdown_writes_trigger: Option<i32>,
    set_disable_auto_compactions: Option<bool>,
    set_compaction_style: Option<CompactionStyle>,
    set_merge_operator_associative: Option<(String, merger::MergeOperator)>,
    prefix_length: Option<usize>,
}

macro_rules! set_opt {
    ($opts:ident, $open_config:ident, $method:ident) => {
        if let Some(val) = $open_config.$method {
            $opts.$method(val);
        }
    };
}

impl From<SoyOpenOpts> for Options {
    fn from(oc: SoyOpenOpts) -> Options {
        let mut opts = Options::default();
        set_opt!(opts, oc, create_if_missing);
        set_opt!(opts, oc, create_missing_column_families);
        set_opt!(opts, oc, set_max_open_files);
        set_opt!(opts, oc, set_use_fsync);
        set_opt!(opts, oc, set_bytes_per_sync);
        set_opt!(opts, oc, optimize_for_point_lookup);
        set_opt!(opts, oc, set_table_cache_num_shard_bits);
        set_opt!(opts, oc, set_max_write_buffer_number);
        set_opt!(opts, oc, set_write_buffer_size);
        set_opt!(opts, oc, set_target_file_size_base);
        set_opt!(opts, oc, set_min_write_buffer_number_to_merge);
        set_opt!(opts, oc, set_level_zero_stop_writes_trigger);
        set_opt!(opts, oc, set_level_zero_stop_writes_trigger);
        set_opt!(opts, oc, set_disable_auto_compactions);
        if let Some(style) = oc.set_compaction_style {
            opts.set_compaction_style(style.into())
        }
        if let Some(len) = oc.prefix_length {
            let prefix_extractor = rocksdb::SliceTransform::create_fixed_prefix(len);
            opts.set_prefix_extractor(prefix_extractor);
        }
        if let Some((name, merge_op)) = oc.set_merge_operator_associative {
            merge_op.set(&mut opts, &name[..])
        }
        opts
    }
    // }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, NifUnitEnum)]
enum CompactionStyle {
    Universal,
    Fifo,
    Level,
}

impl From<CompactionStyle> for DBCompactionStyle {
    fn from(s: CompactionStyle) -> DBCompactionStyle {
        match s {
            CompactionStyle::Universal => DBCompactionStyle::Universal,
            CompactionStyle::Fifo => DBCompactionStyle::Fifo,
            CompactionStyle::Level => DBCompactionStyle::Level,
        }
    }
}
