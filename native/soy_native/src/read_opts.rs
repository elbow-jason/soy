use crate::Bin;
use rocksdb::ReadOptions;
use rustler::NifStruct;

// #[derive(Debug, Clone, Copy, PartialEq, Eq)]
// pub enum SoyReadTier {}

#[derive(Debug, NifStruct, Clone)]
#[must_use] // Added to test Issue #152
#[module = "Soy.ReadOpts"]
pub struct SoyReadOpts {
    fill_cache: Option<bool>,
    set_background_purge_on_iterator_cleanup: Option<bool>,
    set_ignore_range_deletions: Option<bool>,
    set_iterate_lower_bound: Option<Bin>,
    set_iterate_upper_bound: Option<Bin>,
    set_max_skippable_internal_keys: Option<u64>,
    set_pin_data: Option<bool>,
    set_prefix_same_as_start: Option<bool>,
    set_readahead_size: Option<usize>,
    set_tailing: Option<bool>,
    set_total_order_seek: Option<bool>,
    set_verify_checksums: Option<bool>,
    // somehow ReadTier is not public, but the function to set it is?
    // set_read_tier: Option<SoyReadTier>
}

impl Default for SoyReadOpts {
    fn default() -> SoyReadOpts {
        SoyReadOpts {
            fill_cache: Some(true),
            set_iterate_lower_bound: None,
            set_iterate_upper_bound: None,
            set_prefix_same_as_start: Some(false),
            set_total_order_seek: None,
            set_max_skippable_internal_keys: Some(0),
            set_background_purge_on_iterator_cleanup: Some(false),
            set_ignore_range_deletions: Some(false),
            set_verify_checksums: Some(true),
            set_readahead_size: Some(0),
            set_pin_data: Some(false),
            set_tailing: None,
        }
    }
}

macro_rules! set_fields {
    ($sro:ident, $ro:ident, [$( $field_method:ident, )*]) => {
        $(
            if let Some(v) = $sro.$field_method {
                $ro.$field_method(v);
            }
        )*
    };
}

impl From<SoyReadOpts> for ReadOptions {
    fn from(sro: SoyReadOpts) -> ReadOptions {
        let mut ro = ReadOptions::default();
        set_fields!(
            sro,
            ro,
            [
                fill_cache,
                set_background_purge_on_iterator_cleanup,
                set_ignore_range_deletions,
                set_max_skippable_internal_keys,
                set_pin_data,
                set_prefix_same_as_start,
                set_readahead_size,
                set_tailing,
                set_total_order_seek,
                set_verify_checksums,
            ]
        );

        if let Some(v) = sro.set_iterate_lower_bound {
            ro.set_iterate_lower_bound(v.as_bytes().to_vec())
        }
        if let Some(v) = sro.set_iterate_upper_bound {
            ro.set_iterate_upper_bound(v.as_bytes().to_vec())
        }
        ro
    }
}
