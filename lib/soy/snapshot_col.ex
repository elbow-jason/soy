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
  def name({SnapshotCol, {_ss, name}}), do: name

  @doc """
  The db of the column family.
  """
  def ss({SnapshotCol, {ss, _name}}), do: {Snapshot, ss}

  @doc """
  The db ref of the column family.
  """
  def db_ref({SnapshotCol, {db, _name}}), do: db

  @doc """
  Fetches the binary value of the `key` in the `db` at the column family
  with `name`.
  """
  def fetch(ss_col, key) do
    Native.ss_fetch_cf(ss(ss_col), name(ss_col), key)
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

  @doc """
  Gets the kv-entry with `key` in the `db` at the column family
  with `name`.
  """
  def delete(cf, key) do
    Native.db_delete_cf(db_ref(cf), name(cf), key)
  end

  @doc """
  Gets binary value or nil for a list of {cf, key} pairs.
  """
  def multi_get(cf, keys) do
    cf_name = name(cf)
    pairs = Enum.map(keys, fn k -> {cf_name, k} end)
    Native.db_multi_get_cf(db_ref(cf), pairs)
  end
end
