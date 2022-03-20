defmodule Soy.Native do
  use Rustler, otp_app: :soy, crate: "soy_native"

  defp err, do: :erlang.nif_error(:nif_not_loaded)

  # path ops
  def path_open_db(_path, _options), do: err()
  def path_destroy(_path), do: err()
  def path_repair(_path), do: err()
  def path_list_cf(_path), do: err()

  def db_checkpoint(_db, _checkpoint_path), do: err()

  # flush/sync to disk
  def db_flush(_db), do: err()
  def db_flush_wal(_db, _sync), do: err()

  # metadata
  def db_path(_db), do: err()
  def db_live_files(_db), do: err()

  # db reads
  def db_fetch(_db, _key), do: err()

  def db_multi_get(_db, _keys), do: err()
  def db_multi_get_cf(_db, _cf_and_key_pairs), do: err()
  def db_key_may_exist(_db, _key), do: err()

  def db_has_key(_db, _key), do: err()

  # db mutations
  def db_merge(_db, _key, _val), do: err()
  def db_delete(_db, _key), do: err()
  def db_put(_db, _key, _val), do: err()
  def db_batch(_db, _ops_list), do: err()

  # cf create/drop
  def db_create_new_cf(_db, _col_name, _open_cfg), do: err()
  def db_open_existing_cf(_db, _col_name), do: err()
  def db_drop_cf(_db, _cf), do: err()

  # iteration for both db and cf based on itermode
  def db_iter(_db), do: err()

  def ss_iter(_ss), do: err()
  def ss_iter_cf(_ss, _name), do: err()

  def iter_seek(_db_iter, _seek), do: err()

  def iter_valid(_db_iter), do: err()
  def iter_key(_it), do: err()
  def iter_value(_it), do: err()
  def iter_key_value(_it), do: err()

  # snapshot
  def db_snapshot(_db), do: err()
  def ss_fetch(_ss, _key), do: err()
  def ss_fetch_cf(_ss, _cf, _key), do: err()

  def ss_multi_get(_ss, _keys), do: err()
  def ss_multi_get_cf(_ss, _cf_and_key_pairs), do: err()

  # write opts
  def write_opts_default, do: err()

  # read opts
  def read_opts_default, do: err()

  # properties
  def db_get_property(_db, _prop), do: err()
  def db_list_properties(_db), do: err()

  def db_cf_delete(_cf, _key), do: err()
  def db_cf_put(_cf, _key, _value), do: err()
  def db_cf_fetch(_cf, _key), do: err()
  def db_cf_merge(_cf, _key, _val), do: err()
  def db_cf_key_may_exist(_cf, _key), do: err()
  def db_cf_has_key(_cf, _key), do: err()
  def db_cf_name(_cf), do: err()
  def db_cf_to_db(_cf), do: err()
  def db_cf_iter(_cf), do: err()
  def db_cf_flush(_cf), do: err()
  def db_cf_multi_get(_cf_key_pairs), do: err()
end
