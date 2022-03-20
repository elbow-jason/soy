defmodule Soy.OpenOpts do
  @moduledoc """
  A struct for database options used to tune many, many aspects of RockDB.

  ### Database Options

    * `:create_if_missing` (default: `false`) - If `true`, the database will be
      created if it is missing.
    * `:create_missing_column_families` (default: false) - If `true`, any column
      families that didn’t exist when opening the database will be created.
    * `:increase_parallelism` (default: 1) - By default, RocksDB uses only one
      background thread for flush and compaction. This option will set it up
      such that total of `total_threads` is used. Good value for `total_threads`
      is the number of cores. You almost definitely want to increase this value
      if your system is bottlenecked by RocksDB.
    * `:optimize_for_point_lookup` - makes get and put operations faster by
      creating a bloolfilter and setting the index type to kHashSearch.
    * `:optimize_level_style_compaction` - sets many buffer sizes inside RockDB.
      Increasing this value may prevent write stalls, but will increase memory
      usage.
    * `:prepare_for_bulk_load` - (default: nil) Prepare the DB for bulk loading.
      All data will be in level 0 without any automatic compaction. It’s
      recommended to manually call `compact_range(db, nil, nil)` before reading
      from the database, because otherwise the read can be very slow.

  """

  # TODO: support more options
  # TODO: match RocksDB documented defaults

  # enable_statistics
  # get_statistics
  # set_access_hint_on_compaction_start

  # set_advise_random_on_open
  # set_allow_concurrent_memtable_write
  # set_allow_mmap_reads
  # set_allow_mmap_writes
  # set_allow_os_buffer
  # set_arena_block_size
  # set_atomic_flush
  # set_block_based_table_factory
  # set_bloom_locality
  # set_bottommost_compression_options
  # set_bottommost_compression_type
  # set_bottommost_zstd_max_train_bytes
  # set_bytes_per_sync
  # set_compaction_filter
  # set_compaction_filter_factory
  # set_compaction_readahead_size
  # set_compaction_style
  # set_comparator
  # set_compression_options
  # set_compression_per_level
  # set_compression_type
  # set_cuckoo_table_factory
  # set_db_log_dir
  # set_db_paths
  # set_db_write_buffer_size
  # set_delete_obsolete_files_period_micros
  # set_disable_auto_compactions
  # set_dump_malloc_stats
  # set_enable_pipelined_write
  # set_enable_write_thread_adaptive_yield
  # set_env
  # set_error_if_exists
  # set_fifo_compaction_options
  # set_hard_pending_compaction_bytes_limit
  # set_inplace_update_locks
  # set_inplace_update_support
  # set_is_fd_close_on_exec
  # set_keep_log_file_num
  # set_level_compaction_dynamic_level_bytes
  # set_level_zero_file_num_compaction_trigger
  # set_level_zero_slowdown_writes_trigger
  # set_level_zero_stop_writes_trigger
  # set_log_file_time_to_roll
  # set_log_level
  # set_manifest_preallocation_size
  # set_manual_wal_flush
  # set_max_background_compactions
  # set_max_background_flushes
  # set_max_background_jobs
  # set_max_bytes_for_level_base
  # set_max_bytes_for_level_multiplier
  # set_max_bytes_for_level_multiplier_additional
  # set_max_compaction_bytes
  # set_max_file_opening_threads
  # set_max_log_file_size
  # set_max_manifest_file_size
  # set_max_open_files
  # set_max_sequential_skip_in_iterations
  # set_max_subcompactions
  # set_max_successive_merges
  # set_max_total_wal_size
  # set_max_write_buffer_number
  # set_max_write_buffer_size_to_maintain
  # set_memtable_factory
  # set_memtable_huge_page_size
  # set_memtable_prefix_bloom_ratio
  # set_memtable_whole_key_filtering
  # set_merge_operator
  # set_merge_operator_associative
  # set_min_level_to_compress
  # set_min_write_buffer_number
  # set_min_write_buffer_number_to_merge
  # set_num_levels
  # set_optimize_filters_for_hits
  # set_paranoid_checks
  # set_plain_table_factory
  # set_prefix_extractor
  # set_ratelimiter
  # set_recycle_log_file_num
  # set_report_bg_io_stats
  # set_row_cache
  # set_skip_checking_sst_file_sizes_on_db_open
  # set_skip_stats_update_on_db_open
  # set_soft_pending_compaction_bytes_limit
  # set_stats_dump_period_sec
  # set_stats_persist_period_sec
  # set_table_cache_num_shard_bits
  # set_target_file_size_base
  # set_target_file_size_multiplier
  # set_universal_compaction_options
  # set_unordered_write
  # set_use_adaptive_mutex
  # set_use_direct_io_for_flush_and_compaction
  # set_use_direct_reads
  # set_use_fsync
  # set_wal_bytes_per_sync
  # set_wal_dir
  # set_wal_recovery_mode
  # set_wal_size_limit_mb
  # set_wal_ttl_seconds
  # set_writable_file_max_buffer_size
  # set_write_buffer_size
  # set_zstd_max_train_bytes

  defstruct create_if_missing: true,
            create_missing_column_families: true,
            increase_parallelism: nil,
            set_max_open_files: nil,
            set_use_fsync: true,
            set_bytes_per_sync: 64000,
            optimize_for_point_lookup: nil,
            set_table_cache_num_shard_bits: nil,
            set_max_write_buffer_number: nil,
            set_write_buffer_size: nil,
            set_target_file_size_base: nil,
            set_min_write_buffer_number_to_merge: nil,
            set_level_zero_stop_writes_trigger: nil,
            set_level_zero_slowdown_writes_trigger: nil,
            set_disable_auto_compactions: true,
            set_compaction_style: :universal,
            prefix_length: nil,
            set_merge_operator_associative: nil

  def new(opts) when is_list(opts) do
    struct!(__MODULE__, opts)
  end

  def new(%__MODULE__{} = open_opts) do
    open_opts
  end
end
