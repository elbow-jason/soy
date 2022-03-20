defmodule Soy.DBCol do
  @moduledoc """
  For dealing with a column family.
  """

  alias Soy.{DBCol, DB, Native, OpenOpts}

  @doc """
  Creates a column family for with `name` and `opts` in the `db`
  """
  def create_new(db, name, opts \\ []) do
    open_opts = OpenOpts.new(opts)
    db_ref = DB.to_ref(db)

    case Native.db_create_new_cf(db_ref, name, open_opts) do
      cf_ref when is_reference(cf_ref) -> {:ok, {DBCol, cf_ref}}
      {:error, _} = err -> err
    end
  end

  @doc """
  Builds a tuple with the `db` and `name`.
  """
  def open(db, name) do
    db_ref = DB.to_ref(db)

    case Native.db_open_existing_cf(db_ref, name) do
      db_cf_ref when is_reference(db_cf_ref) -> {:ok, {DBCol, db_cf_ref}}
      {:error, _} = err -> err
    end
  end

  @doc """
  The name of the column family.
  """
  def name({DBCol, db_cf_ref}) do
    Native.db_cf_name(db_cf_ref)
  end

  # @doc """
  # The db of the column family.
  # """
  # def db({DBCol, {db_ref, _name}}), do: {DB, db_ref}

  @doc """
  The ref of the db column family.
  """
  def to_ref({DBCol, ref}), do: ref

  @doc """
  Drops a column family with `name` from the `db`.

  # WARNING - this causes data loss of the column family for the specified `name`
  """
  def destroy(cf) do
    db_cf_ref = to_ref(cf)
    db_ref = Native.db_cf_into_db(db_cf_ref)
    name = Native.db_cf_name(db_cf_ref)
    Native.db_drop_cf(db_ref, name)
  end

  @doc """
  Puts an kv-entry in the `db` at the column family with `name`.
  """
  def put(cf, key, val) do
    Native.db_cf_put(to_ref(cf), key, val)
  end

  @doc """
  Fetches the binary value of the `key` in the `db` at the column family
  with `name`.
  """
  def fetch(cf, key) do
    Native.db_cf_fetch(to_ref(cf), key)
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
    Native.db_cf_has_key(to_ref(col), key)
  end

  @doc """
  Gets the kv-entry with `key` in the `db` at the column family
  with `name`.
  """
  def delete(cf, key) do
    Native.db_cf_delete(to_ref(cf), key)
  end

  @doc """
  Gets binary value or nil for a list of {cf, key} pairs.
  """
  def multi_get(cf, keys) do
    ref = to_ref(cf)
    pairs = Enum.map(keys, fn k -> {ref, k} end)
    Native.db_cf_multi_get(pairs)
  end

  @doc """
  Gets binary value or nil for a list of {cf, key} pairs.
  """
  def multi_get(pairs) do
    pairs
    |> Enum.map(fn
      {{DBCol, ref}, key} -> {ref, key}
      {ref, key} when is_reference(ref) -> {ref, key}
    end)
    |> Native.db_cf_multi_get()
  end
end
