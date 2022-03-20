defmodule Soy.DBCol do
  @moduledoc """
  For dealing with a column family.
  """

  alias Soy.{DBCol, DB, Native, OpenOpts}

  @doc """
  Creates a column family for with `name` and `opts` in the `db`
  """
  def create_new(cf, opts \\ [])

  def create_new(cf, opts) when is_list(opts) do
    create_new(cf, struct!(OpenOpts, opts))
  end

  def create_new({DBCol, {db, name}}, %OpenOpts{} = open_config) do
    Native.db_create_cf(DB.to_ref(db), name, open_config)
  end

  @doc """
  Builds a tuple with the `db` and `name`.
  """
  def build(db, name) do
    {DBCol, {DB.to_ref(db), name}}
  end

  @doc """
  The name of the column family.
  """
  def name({DBCol, {_, name}}), do: name
  def name(name) when is_binary(name), do: name

  @doc """
  The db of the column family.
  """
  def db({DBCol, {db_ref, _name}}), do: {DB, db_ref}

  @doc """
  The db ref of the column family.
  """
  def to_ref({DBCol, {db, _name}}), do: db

  @doc """
  Drops a column family with `name` from the `db`.

  # WARNING - this causes data loss of the column family for the specified `name`
  """
  def destroy(cf) do
    Native.db_drop_cf(to_ref(cf), name(cf))
  end

  @doc """
  Puts an kv-entry in the `db` at the column family with `name`.
  """
  def put(cf, key, val) do
    Native.db_put_cf(to_ref(cf), name(cf), key, val)
  end

  @doc """
  Fetches the binary value of the `key` in the `db` at the column family
  with `name`.
  """
  def fetch(cf, key) do
    Native.db_fetch_cf(to_ref(cf), name(cf), key)
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
  Returns `true` or `false` based on the existence of a `key` in the `col`.
  """
  def has_key?(col, key) do
    Native.db_key_exists_cf(to_ref(col), name(col), key)
  end

  @doc """
  Gets the kv-entry with `key` in the `db` at the column family
  with `name`.
  """
  def delete(cf, key) do
    Native.db_delete_cf(to_ref(cf), name(cf), key)
  end

  @doc """
  Gets binary value or nil for a list of {cf, key} pairs.
  """
  def multi_get(cf, keys) do
    cf_name = name(cf)
    pairs = Enum.map(keys, fn k -> {cf_name, k} end)
    Native.db_multi_get_cf(to_ref(cf), pairs)
  end
end
