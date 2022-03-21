defmodule Soy.SnapshotCol do
  @moduledoc """
  For dealing with a column family.
  """

  alias Soy.{Iter, Snapshot, SnapshotCol, Native}

  def new(ss, name) when is_binary(name) do
    ss
    |> Snapshot.to_ref()
    |> Native.ss_open_ss_cf(name)
    |> case do
      cf_ss_ref when is_reference(cf_ss_ref) -> {:ok, {SnapshotCol, cf_ss_ref}}
      {:error, _} = err -> err
    end
  end

  @doc """
  The name of the column family.
  """
  def name(ss_cf), do: Soy.Native.ss_cf_name(to_ref(ss_cf))

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
    pairs = Enum.map(keys, fn k when is_binary(k) -> {ss_cf_ref, k} end)
    Native.ss_cf_multi_get(pairs)
  end

  @doc """
  Gets binary value or nil for a list of {cf, key} pairs.
  """
  def multi_get(pairs) do
    pairs = Enum.map(pairs, fn {ss_cf, k} -> {to_ref(ss_cf), k} end)
    Native.ss_cf_multi_get(pairs)
  end

  def iter(ss_cf) do
    Iter.new(ss_cf)
  end
end
