defmodule Soy.WriteOpts do
  @moduledoc """
  A struct matching RocksDB WriteOptions.

  Defaults chosen according to: https://docs.rs/rocksdb/0.18.0/rocksdb/struct.WriteOptions.html
  """
  defstruct disable_wal: false,
            set_ignore_missing_column_families: false,
            set_low_pri: false,
            set_memtable_insert_hint_per_batch: false,
            set_no_slowdown: false,
            set_sync: false
end
