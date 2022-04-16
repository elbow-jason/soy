defmodule Soy.SnapshotCol do
  @moduledoc """
  For dealing with a column family.
  """

  alias Soy.{Iter, Snapshot, SnapshotCol, DBCol}

  defstruct ss_ref: nil, db_ref: nil, cf_ref: nil, name: nil

  def new(%Snapshot{ss_ref: ss_ref, db_ref: ss_db_ref}, %DBCol{
        name: name,
        db_ref: cf_db_ref,
        cf_ref: cf_ref
      }) do
    if ss_db_ref != cf_db_ref do
      raise "cannot build Soy.SnapshotCol from different db - name: #{inspect(name)}"
    end

    %SnapshotCol{
      name: name,
      ss_ref: ss_ref,
      db_ref: ss_db_ref,
      cf_ref: cf_ref
    }
  end

  def name(%SnapshotCol{name: name}), do: name

  # @doc """
  # The db of the column family.
  # """
  # def ss_ref(%Snapshot{}), do: ss_cf_ref

  @doc """
  Fetches the binary value of the `key` in the `db` at the column family.
  """
  def fetch(%SnapshotCol{} = sc, key) do
    case :rocksdb.get(sc.db_ref, sc.cf_ref, key, snapshot: sc.ss_ref) do
      {:ok, _} = okay -> okay
      :not_found -> :error
    end
  end

  @doc """
  Gets the binary value of `key` in the `db` at the column family.
  """
  def get(cf, key, default \\ nil) do
    case fetch(cf, key) do
      {:ok, got} -> got
      :error -> default
    end
  end

  # @doc """
  # Gets binary value or nil for a list of {cf, key} pairs.
  # """
  # def multi_get(ss_cf, keys) do
  #   ss_cf_ref = to_ref(ss_cf)
  #   pairs = Enum.map(keys, fn k when is_binary(k) -> {ss_cf_ref, k} end)
  #   Native.ss_cf_multi_get(pairs)
  # end

  # @doc """
  # Gets binary value or nil for a list of {cf, key} pairs.
  # """
  # def multi_get(pairs) do
  #   pairs = Enum.map(pairs, fn {ss_cf, k} -> {to_ref(ss_cf), k} end)
  #   Native.ss_cf_multi_get(pairs)
  # end

  def iter(ss_cf) do
    Iter.new(ss_cf)
  end

  def reduce_keys(
        %SnapshotCol{db_ref: db_ref, ss_ref: ss_ref, cf_ref: cf_ref},
        acc,
        opts \\ [],
        func
      )
      when is_function(func, 2) do
    :rocksdb.fold_keys(db_ref, cf_ref, func, acc, [{:snapshot, ss_ref} | opts])
  end

  def reduce(%SnapshotCol{db_ref: db_ref, ss_ref: ss_ref, cf_ref: cf_ref}, acc, opts \\ [], func)
      when is_function(func, 2) do
    :rocksdb.fold(db_ref, cf_ref, func, acc, [{:snapshot, ss_ref} | opts])
  end
end
