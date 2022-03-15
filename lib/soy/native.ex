defmodule Soy.Native do
  use Rustler, otp_app: :soy, crate: "soy_native"

  defp err, do: :erlang.nif_error(:nif_not_loaded)

  # create/destoy
  def open(_path, _options), do: err()
  def destroy(_path), do: err()

  # repair
  def checkpoint(_db, _checkpoint_path), do: err()
  def repair(_path), do: err()

  # metadata
  def list_cf(_path), do: err()
  def path(_db), do: err()
  def live_files(_db), do: err()

  # db store
  def put(_db, _key, _val), do: err()
  def fetch(_db, _key), do: err()
  def delete(_db, _key), do: err()

  # batch mutation
  def batch(_db, _ops_list), do: err()

  # get multiple values or nils
  def multi_get(_db, _keys), do: err()

  # iteration for both db and cf based on itermode
  def db_iter(_db), do: err()
  def db_iter_cf(_db, _name), do: err()
  def ss_iter(_ss), do: err()
  def ss_iter_cf(_ss, _name), do: err()

  def iter_seek(_db_iter, _seek), do: err()

  def iter_valid(_db_iter), do: err()
  def iter_key(_it), do: err()
  def iter_value(_it), do: err()
  def iter_key_value(_it), do: err()

  # cf create/drop
  def create_cf(_db, _cf_name, _open_cfg), do: err()
  def drop_cf(_db, _cf_name), do: err()

  # cf store
  def put_cf(_db, _cf_name, _key, _val), do: err()
  def fetch_cf(_db, _cf_name, _key), do: err()
  def delete_cf(_db, _cf_name, _key), do: err()
  def multi_get_cf(_db, _cf_and_key_pairs), do: err()

  # snapshot
  def snapshot(_db), do: err()
  def ss_fetch(_ss, _key), do: err()
  def ss_fetch_cf(_ss, _cf_name, _key), do: err()

  def ss_multi_get(_db, _keys), do: err()
  def ss_multi_get_cf(_ss, _cf_and_key_pairs), do: err()

  # write opts
  def write_opts_default, do: err()

  # read opts
  def read_opts_default, do: err()
end
