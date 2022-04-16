defmodule Soy do
  @moduledoc """
  Documentation for `Soy`.
  """

  alias Soy.DB

  def open(path, opts \\ []) do
    DB.open(path, opts)
  end

  def repair(path) do
    DB.repair(path)
  end

  @doc """
  List the columns of a db given a DB or a path.

  ## Examples

      iex> path = tmp_dir()
      iex> {:ok, db, [_]} = Soy.open(path)
      iex> {:ok, _cf} = DBCol.create_new(db, "fam")
      iex> Soy.list_columns(path)
      ["default", "fam"]
  """
  def list_columns(path) when is_binary(path) do
    DB.list_columns(path)
  end

  @doc """
  Destroys a database.

  ## WARNING!!! - this function will cause data loss.
  """
  def destroy(path) do
    DB.destroy(path)
  end

  def put(%impl{} = store, key, val) do
    impl.put(store, key, val)
  end

  def delete(%impl{} = store, key) do
    impl.delete(store, key)
  end

  # def multi_get(%impl{} = store, keys) do
  #   impl.multi_get(store, keys)
  # end

  # def multi_get_cf({impl, _} = store, cf_key_pairs) do
  #   impl.multi_get_cf(store, cf_key_pairs)
  # end

  def fetch(%impl{} = store, key) do
    impl.fetch(store, key)
  end

  def fetch!(%impl{} = store, key) do
    case impl.fetch(store, key) do
      {:ok, val} -> val
      :error -> raise KeyError, key: key, term: store
    end
  end

  @doc """
  Flushes/Synces the db or column family to disk.
  """
  def flush(%impl{} = store, opts \\ []) do
    impl.flush(store, opts)
  end

  @doc """
  Get the matching value of the given `key` or `default` (default: `nil`).

  ## Examples

      iex> {:ok, db, [_]} = Soy.open(tmp_dir())
      iex> Soy.get(db, "my_key")
      nil

      iex> {:ok, db, [_]} = Soy.open(tmp_dir())
      iex> Soy.put(db, "my_key", "my_value")
      iex> Soy.get(db, "my_key")
      "my_value"

      iex> {:ok, db, [_]} = Soy.open(tmp_dir())
      iex> {:ok, cf} = Soy.DBCol.create_new(db, "my_cf")
      iex> Soy.get(cf, "my_key")
      nil

      iex> {:ok, db, [_]} = Soy.open(tmp_dir())
      iex> {:ok, cf} = Soy.DBCol.create_new(db, "my_cf")
      iex> :ok = Soy.put(cf, "my_key", "my_value_in_cf")
      iex> Soy.get(cf, "my_key")
      "my_value_in_cf"

  """
  def get(store, key, default \\ nil)

  def get(%impl{} = store, key, default) when is_binary(key) do
    impl.get(store, key, default)
  end

  def multi_get(list) do
    Enum.map(list, fn {store, key} -> get(store, key, nil) end)
  end

  def multi_get(store, keys) do
    Enum.map(keys, fn key -> get(store, key, nil) end)
  end

  def reduce_keys(%impl{} = store, acc, opts \\ [], func) when is_function(func, 2) do
    impl.reduce_keys(store, acc, opts, func)
  end

  def reduce(%impl{} = store, acc, opts \\ [], func) when is_function(func, 2) do
    impl.reduce(store, acc, opts, func)
  end

  def batch(db, batch), do: DB.write_batch(db, batch)

  def snapshot(db), do: DB.snapshot(db)

  if Mix.env() == :dev do
    def tmp_dir do
      Briefly.create!(directory: true)
    end
  end
end
