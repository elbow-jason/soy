defmodule Soy.Snapshot do
  alias Soy.{DB, Native, ColFam}

  def new(db), do: Native.snapshot(DB.to_ref(db))

  def fetch(ss, key), do: Native.ss_fetch(ss, key)

  def fetch_cf(ss, cf, key), do: Native.ss_fetch_cf(ss, ColFam.name(cf), key)

  def iter(ss, mode), do: Native.ss_iter(ss, mode)

  def multi_get(ss, keys), do: Native.ss_multi_get(ss, keys)

  def multi_get_cf(ss, cf_and_key_pairs), do: Native.ss_multi_get_cf(ss, cf_and_key_pairs)
end
