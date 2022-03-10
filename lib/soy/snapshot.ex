defmodule Soy.Snapshot do
  alias Soy.{DB, Native, ColFam, Snapshot}

  def new(db), do: {Snapshot, Native.snapshot(DB.to_ref(db))}

  def to_ref({Snapshot, ref}) when is_reference(ref), do: ref
  def to_ref(ref) when is_reference(ref), do: ref

  def fetch(ss, key), do: Native.ss_fetch(to_ref(ss), key)

  def fetch_cf(ss, cf, key), do: Native.ss_fetch_cf(to_ref(ss), ColFam.name(cf), key)

  # def iter(ss), do: Native.ss_iter(ss)

  def multi_get(ss, keys), do: Native.ss_multi_get(to_ref(ss), keys)

  def multi_get_cf(ss, cf_and_key_pairs), do: Native.ss_multi_get_cf(to_ref(ss), cf_and_key_pairs)
end
