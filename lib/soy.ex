defmodule Soy do
  @moduledoc """
  Documentation for `Soy`.
  """

  alias Soy.{DB, DBCol, Native, Snapshot, SnapshotCol}

  def open(path, opts \\ []) do
    DB.open(path, opts)
  end

  def repair(path) do
    DB.repair(path)
  end

  def path({DB, _} = db), do: DB.path(db)

  @doc """
  List the columns of a db given a DB or a path.

  ## Examples

      iex> path = tmp_dir()
      iex> db = Soy.open(path)
      iex> {:ok, _cf} = DBCol.create_new(db, "fam")
      iex> Soy.list_columns(db)
      ["default", "fam"]
      iex> Soy.list_columns(path)
      ["default", "fam"]
  """
  def list_columns(path) when is_binary(path) do
    Native.path_list_cf(path)
  end

  def list_columns({DB, _} = db) do
    DB.list_columns(db)
  end

  def destroy(path) do
    DB.destroy(path)
  end

  def put({impl, _} = store, key, val) do
    impl.put(store, key, val)
  end

  def delete({impl, _} = store, key) do
    impl.delete(store, key)
  end

  def multi_get({impl, _} = store, keys) do
    impl.multi_get(store, keys)
  end

  def multi_get_cf({impl, _} = store, cf_key_pairs) do
    impl.multi_get_cf(store, cf_key_pairs)
  end

  def fetch({impl, _} = store, key) do
    impl.fetch(store, key)
  end

  def fetch!({impl, _} = store, key) do
    case impl.fetch(store, key) do
      {:ok, val} -> val
      :error -> raise KeyError, key: key, term: store
    end
  end

  @doc """
  Get the matching value of the given `key` or `default` (default: `nil`).

  ## Examples

      iex> db = Soy.open(tmp_dir())
      iex> Soy.get(db, "my_key")
      nil

      iex> db = Soy.open(tmp_dir())
      iex> Soy.put(db, "my_key", "my_value")
      iex> Soy.get(db, "my_key")
      "my_value"

      iex> db = Soy.open(tmp_dir())
      iex> {:ok, cf} = Soy.DBCol.create_new(db, "my_cf")
      iex> Soy.get(cf, "my_key")
      nil

      iex> db = Soy.open(tmp_dir())
      iex> {:ok, cf} = Soy.DBCol.create_new(db, "my_cf")
      iex> :ok = Soy.put(cf, "my_key", "my_value_in_cf")
      iex> Soy.get(cf, "my_key")
      "my_value_in_cf"

  """
  def get({impl, _} = store, key, default \\ nil) do
    impl.get(store, key, default)
  end

  def get_cf({impl, _} = store, col_fam, key, default \\ nil) do
    case impl.fetch_cf(store, col_fam, key) do
      {:ok, got} -> got
      :error -> default
    end
  end

  def batch({DB, _} = db, ops) do
    DB.batch(db, ops)
  end

  def snapshot(db), do: DB.snapshot(db)

  def kind(ref), do: Native.resource_kind(ref)

  def impl(ref) do
    case kind(ref) do
      :db -> DB
      :db_cf -> DBCol
      :ss -> Snapshot
      :ss_cf -> SnapshotCol
    end
  end

  if Mix.env() == :dev do
    def tmp_dir do
      Briefly.create!(directory: true)
    end
  end
end
