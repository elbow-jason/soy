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

  def get({impl, _} = store, key, default \\ nil) do
    impl.get(store, key, default)
  end

  def batch({DB, _} = db, ops) do
    DB.batch(db, ops)
  end

  if Mix.env() == :dev do
    def crash do
      alias Soy.Iter
      db = Soy.open(Soy.TestHelpers.tmp_dir())
      ops = [{:put, "k2", "v2"}, {:put, "k3", "v3"}, {:put, "k1", "v1"}]
      3 = Soy.batch(db, ops)
      it = Iter.new(db)
      {"k1", "v1"} = Iter.next(it)
      {"k2", "v2"} = Iter.next(it)
      {"k3", "v3"} = Iter.next(it)
      nil = Iter.next(it)
    end

    def crash2 do
      alias Soy.Iter
      db = Soy.open(Soy.TestHelpers.tmp_dir())
      it = Iter.new(db)
      nil = Iter.next(it)
      nil = Iter.next(it)
      nil = Iter.next(it)
    end
  end
end
