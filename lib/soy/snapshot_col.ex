defmodule Soy.SnapshotCol do
  @moduledoc """
  For dealing with a column family.
  """

  alias Soy.{Snapshot, SnapshotCol, Native}

  @doc """
  Builds a tuple with the `db` and `name`.
  """
  def build({Snapshot, ss}, name) do
    {SnapshotCol, {ss, name}}
  end

  @doc """
  The name of the column family.
  """
  def name(ss_cf), do: Soy.Native.ss_cf_name(ss_cf)

  @doc """
  The db of the column family.
  """
  def to_ref({SnapshotCol, ss_cf_ref}), do: ss_cf_ref

  @doc """
  Fetches the binary value of the `key` in the `db` at the column family
  with `name`.
  """
  def fetch(ss_cf, key) do
    Native.ss_cf_fetch(to_ref(ss_cf), key)
  end

  @doc """
  Gets the binary value of `key` in the `db` at the column family
  with `name`.
  """
  def get(cf, key, default \\ nil) do
    case fetch(cf, key) do
      {:ok, got} -> got
      :error -> default
    end
  end

  # @doc """
  # Gets the kv-entry with `key` in the `db` at the column family
  # with `name`.
  # """
  # def delete(cf, key) do
  #   Native.db_delete_cf(db_ref(cf), name(cf), key)
  # end

  @doc """
  Gets binary value or nil for a list of {cf, key} pairs.
  """
  def multi_get(ss_cf, keys) do
    ss_cf_ref = to_ref(ss_cf)
    pairs = Enum.map(keys, fn k -> {ss_cf_ref, k} end)
    Native.db_cf_multi_get(pairs)
  end

  # def fetch_cf(ss, cf, key), do: Native.ss_cf_fetch_cf(to_ref(ss), DBCol.name(cf), key)

  # def multi_get_cf(ss, cf_and_key_pairs), do: Native.ss_multi_get_cf(to_ref(ss), cf_and_key_pairs)
end
