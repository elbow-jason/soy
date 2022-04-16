defmodule Soy.DBCol do
  @moduledoc """
  For dealing with a column family.
  """

  alias Soy.{DBCol, DB}

  defstruct [:name, :cf_ref, :db_ref]

  @doc """
  Builds a Soy.DBCol struct.
  """
  def build(name, cf_ref, db_ref) do
    %DBCol{name: name, cf_ref: cf_ref, db_ref: db_ref}
  end

  @doc """
  Creates a column family for with `name` and `opts` in the `db`
  """
  def create_new(%DB{db_ref: db_ref}, name, opts \\ []) do
    case :rocksdb.create_column_family(db_ref, to_charlist(name), opts) do
      {:ok, cf_ref} when is_reference(cf_ref) -> {:ok, build(name, cf_ref, db_ref)}
      {:error, _} = err -> err
    end
  end

  # can we get this?
  # @doc """
  # Builds a tuple with the `db` and `name`.
  # """
  # def open(db, name) do
  #   db_ref = DB.to_ref(db)

  #   case Native.db_open_existing_cf(db_ref, name) do
  #     db_cf_ref when is_reference(db_cf_ref) -> {:ok, {DBCol, db_cf_ref}}
  #     {:error, _} = err -> err
  #   end
  # end

  @doc """
  The db of the column family.
  """
  def db(%DBCol{db_ref: db_ref}), do: DB.build(db_ref)

  @doc """
  The db ref of the column family.
  """
  def db_ref(%DBCol{db_ref: r}), do: r

  @doc """
  The ref of the column family.
  """
  def to_ref(%DBCol{cf_ref: ref}), do: ref

  @doc """
  The name of the column family.
  """
  def name(%DBCol{name: name}), do: name

  @doc """
  Drops a column family with `name` from the `db`.

  # WARNING - this causes data loss of the column family for the specified `name`
  """
  def destroy(%DBCol{db_ref: db_ref, cf_ref: cf_ref}) do
    :rocksdb.destroy_column_family(db_ref, cf_ref)
  end

  @doc """
  Puts an kv-entry in the `db` at the column family with `name`.
  """
  def put(%DBCol{db_ref: db_ref, cf_ref: cf_ref}, key, val, opts \\ []) do
    :rocksdb.put(db_ref, cf_ref, key, val, opts)
  end

  @doc """
  Flushes the column family.
  """
  def flush(%DBCol{db_ref: db_ref, cf_ref: cf_ref}) do
    :rocksdb.flush(db_ref, cf_ref)
  end

  @doc """
  Fetches the binary value of the `key` in the `db` at the column family
  with `name`.
  """
  def fetch(%DBCol{db_ref: db_ref, cf_ref: cf_ref}, key) do
    case :rocksdb.get(db_ref, cf_ref, key, []) do
      :not_found -> :error
      {:ok, val} when is_binary(val) -> {:ok, val}
    end
  end

  @doc """
  Gets the binary value of `key` in the `db` at the column family
  with `name`.
  """
  def get(%DBCol{db_ref: db_ref, cf_ref: cf_ref}, key, default \\ nil) do
    case :rocksdb.get(db_ref, cf_ref, key, []) do
      :not_found -> default
      {:ok, val} when is_binary(val) -> val
    end
  end

  # @doc """
  # Returns `true` or `false` based on the existence of a `key` in the `col`.
  # """
  # def has_key?(col, key) do
  #   Native.db_cf_has_key(to_ref(col), key)
  # end

  @doc """
  Gets the kv-entry with `key` in the `db` at the column family
  with `name`.
  """
  def delete(%DBCol{db_ref: db_ref, cf_ref: cf_ref}, key, opts \\ []) do
    :rocksdb.single_delete(db_ref, cf_ref, key, opts)
  end

  # @doc """
  # Gets binary value or nil for a list of {cf, key} pairs.
  # """
  # def multi_get(cf, keys) do
  #   ref = to_ref(cf)
  #   pairs = Enum.map(keys, fn k -> {ref, k} end)
  #   Native.db_cf_multi_get(pairs)
  # end

  # @doc """
  # Gets binary value or nil for a list of {cf, key} pairs.
  # """
  # def multi_get(pairs) do
  #   pairs
  #   |> Enum.map(fn
  #     {{DBCol, ref}, key} -> {ref, key}
  #     {ref, key} when is_reference(ref) -> {ref, key}
  #   end)
  #   |> Native.db_cf_multi_get()
  # end
end
