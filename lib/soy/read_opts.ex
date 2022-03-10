defmodule Soy.ReadOpts do
  @moduledoc """
  A struct matching RocksDB ReadOptions.

  Defaults chosen according to: https://docs.rs/rocksdb/0.18.0/rocksdb/struct.ReadOptions.html
  """
  defstruct fill_cache: true,
            set_background_purge_on_iterator_cleanup: false,
            set_ignore_range_deletions: false,
            set_iterate_lower_bound: nil,
            set_iterate_upper_bound: nil,
            set_max_skippable_internal_keys: 0,
            set_pin_data: false,
            set_prefix_same_as_start: false,
            set_readahead_size: 0,
            set_tailing: nil,
            set_total_order_seek: nil,
            set_verify_checksums: true
end
