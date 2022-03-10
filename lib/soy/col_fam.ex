defmodule Soy.ColFam do
  @moduledoc """
  For dealing with a column family.
  """

  alias Soy.{ColFam, DB, Native, OpenOpts}

  @doc """
  Creates a column family for with `name` and `opts` in the `db`
  """
  def create(cf, opts \\ [])

  def create(cf, opts) when is_list(opts) do
    create(cf, struct!(OpenOpts, opts))
  end

  def create({ColFam, {db, name}}, %OpenOpts{} = open_config) do
    Native.create_cf(DB.to_ref(db), name, open_config)
  end

  @doc """
  Builds a tuple with the `db` and `name`.
  """
  def build(db, name) do
    {ColFam, {DB.to_ref(db), name}}
  end

  @doc """
  The name of the column family.
  """
  def name({ColFam, {_, name}}), do: name
  def name(name) when is_binary(name), do: name

  @doc """
  The db of the column family.
  """
  def db({ColFam, {db_ref, _name}}), do: {DB, db_ref}

  @doc """
  The db ref of the column family.
  """
  def db_ref({ColFam, {db, _name}}), do: db

  @doc """
  Drops a column family with `name` from the `db`.

  # WARNING - this causes data loss of the column family for the specified `name`
  """
  def destroy(cf) do
    Native.drop_cf(db_ref(cf), name(cf))
  end

  @doc """
  Puts an kv-entry in the `db` at the column family with `name`.
  """
  def put(cf, key, val) do
    Native.put_cf(db_ref(cf), name(cf), key, val)
  end

  @doc """
  Fetches the binary value of the `key` in the `db` at the column family
  with `name`.
  """
  def fetch(cf, key) do
    Native.fetch_cf(db_ref(cf), name(cf), key)
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
    Native.delete_cf(db_ref(cf), name(cf), key)
  end

  @doc """
  Gets binary value or nil for a list of {cf, key} pairs.
  """
  def multi_get(cf, keys) do
    cf_name = name(cf)
    pairs = Enum.map(keys, fn k -> {cf_name, k} end)
    Native.multi_get_cf(db_ref(cf), pairs)
  end
end
