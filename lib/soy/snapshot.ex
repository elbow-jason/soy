defmodule Soy.Snapshot do
  alias Soy.{Iter, DB, Native, Snapshot}

  def new(db), do: {Snapshot, Native.db_snapshot(DB.to_ref(db))}

  def to_ref({Snapshot, ss_ref}) when is_reference(ss_ref), do: ss_ref
  def to_ref(ss_ref) when is_reference(ss_ref), do: ss_ref

  def fetch(ss, key), do: Native.ss_fetch(to_ref(ss), key)

  def multi_get(ss, keys), do: Native.ss_multi_get(to_ref(ss), keys)

  def iter(ss), do: Iter.new(ss)
end
