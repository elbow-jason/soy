defmodule Soy do
  @moduledoc """
  Documentation for `Soy`.
  """

  alias Soy.{DB, Native}

  def open(path, opts \\ []) do
    DB.open(path, opts)
  end

  def repair(path) do
    DB.repair(path)
  end

  def path({DB, _} = db), do: DB.path(db)

  def list_cf(path) when is_binary(path) do
    Native.list_cf(path)
  end

  def list_cf({DB, _} = db) do
    DB.list_cf(db)
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
    impl.fetch!(store, key)
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
      iex> cf = Soy.ColFam.build(db, "my_cf")
      iex> :ok = Soy.ColFam.create(cf)
      iex> Soy.get(cf, "my_key")
      nil

      iex> db = Soy.open(tmp_dir())
      iex> cf = Soy.ColFam.build(db, "my_cf")
      iex> :ok = Soy.ColFam.create(cf)
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

  if Mix.env() == :dev do
    def tmp_dir do
      Briefly.create!(directory: true)
    end
  end
end
