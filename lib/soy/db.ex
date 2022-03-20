defmodule Soy.DB do
  alias Soy.{DB, Native, OpenOpts, Snapshot, DBCol}

  @doc """
  Opens a db at the given path with the given options list or
  Soy.OpenOpts struct.

  See Soy.OpenOpts for specific more information.

  ## Examples

      iex> {DB, db} = DB.open(tmp_dir())
      iex> is_reference(db)
      true

      iex> {DB, db} = DB.open(tmp_dir(), %OpenOpts{prefix_length: 3})
      iex> is_reference(db)
      true

      iex> {DB, db} = DB.open(tmp_dir(), [prefix_length: 3])
      iex> is_reference(db)
      true

  """
  def open(path, options \\ [])

  def open(path, opts) when is_list(opts) do
    open(path, struct!(OpenOpts, opts))
  end

  def open(path, %OpenOpts{} = open_config) do
    {DB, Native.path_open_db(path, open_config)}
  end

  @doc """
  Returns the reference of a tagged db or a reference itself.
  """
  def to_ref({DB, ref}) when is_reference(ref), do: ref
  def to_ref(ref) when is_reference(ref), do: ref

  @doc """
  Lists the column families of the DB or path.

  ## Examples

      iex> path = tmp_dir()
      iex> db = Soy.open(path)
      iex> Soy.list_columns(db)
      ["default"]

      iex> path = tmp_dir()
      iex> db = Soy.open(path)
      iex> {:ok, _cf} = Soy.DBCol.create_new(db, "fam")
      iex> Soy.list_columns(db)
      ["default", "fam"]
      iex> Soy.list_columns(path)
      ["default", "fam"]

  """
  def list_columns(db) do
    Native.path_list_cf(DB.path(db))
  end

  @doc """
  Creates a column family in the db.
  """

  def create_new_cf(db, name, opts \\ []) do
    DBCol.create_new(db, name, opts)
  end

  @doc """
  Destroys the db at the given path.

  # WARNING - causes data loss.

  ## Examples

      iex> path = tmp_dir()
      iex> db = Soy.open(path)
      iex> :ok = Soy.put(db, "hello", "world")
      iex> "world" = Soy.get(db, "hello")
      iex> magically_drop_db_from_scope()
      iex> :ok = Soy.destroy(path)
      iex> db = Soy.open(path)
      iex> Soy.get(db, "hello")
      nil

  """
  def destroy(path) do
    Native.path_destroy(path)
  end

  @doc """
  Repairs the database at the given path

  ## Examples
      iex> path = tmp_dir()
      iex> _db = Soy.open(path)
      iex> magically_drop_db_from_scope()
      iex> :ok = Soy.repair(path)
      iex> {DB, db} = Soy.open(path)
      iex> is_reference(db)
      true

  """
  def repair(path) do
    Native.path_repair(path)
  end

  @doc """
  The path of the db.

  # Examples

      iex> path = tmp_dir()
      iex> db = Soy.open(path)
      iex> Soy.path(db) == path
      true

  """
  def path(db) do
    Native.db_path(to_ref(db))
  end

  @doc """
  Returns the live files of the database.

  When the DB is not processing files in the background
  this function will return an empty list. If the DB is compacting
  then the active SST files will be listed.
  """
  def live_files(db) do
    Native.db_live_files(to_ref(db))
  end

  @doc """
  Stores a value in the DB.

  ## Examples

    iex> db = Soy.open(tmp_dir())
    iex> nil = Soy.get(db, "hello")
    iex> :ok = Soy.put(db, "hello", "world")
    iex> "world" = Soy.get(db, "hello")
  """
  def put(db, key, val) do
    Native.db_put(to_ref(db), key, val)
  end

  @doc """
  Returns `true` or `false` based on the existence of a `key` in the `db`.

  ## Examples

      iex> db = Soy.open(tmp_dir())
      iex> :ok = Soy.put(db, "hello", "world")
      iex> DB.has_key?(db, "hello")
      true
      iex> DB.has_key?(db, "other")
      false

  """
  def has_key?(db, key) do
    Native.db_has_key(to_ref(db), key)
  end

  @doc """
  Fetches a value from the DB.

  Returns `:error` for a missing key and `{:ok, binary}` for
  a found key.

  ## Examples

  For a missing key:

      iex> db = Soy.open(tmp_dir())
      iex> :error = Soy.fetch(db, "name")

  For an existing key:

      iex> db = Soy.open(tmp_dir())
      iex> :ok = Soy.put(db, "hello", "world")
      iex> {:ok, "world"} = Soy.fetch(db, "hello")
  """
  def fetch(db, key) do
    Native.db_fetch(to_ref(db), key)
  end

  @doc """
  Gets a value from the DB.

  Returns `default` (the default of `default` is `nil`) for a missing key and `binary` for
  a found key. The `default` can be overridden with get/3.

  ## Examples

  For a missing key:

      iex> tmp_dir() |> Soy.open() |> Soy.get("name")
      nil

  For an existing key:

      iex> db = Soy.open(tmp_dir())
      iex> :ok = Soy.put(db, "hello", "world")
      iex> "world" = Soy.get(db, "hello")

  For a missing key with a given `default`:

      iex> tmp_dir() |> Soy.open() |> Soy.get("name", "you")
      "you"

  """
  def get(db, key, default \\ nil) do
    case fetch(db, key) do
      {:ok, got} -> got
      :error -> default
    end
  end

  @doc """
  Returns the matching binary value from the DB or raises.

  ## Examples

  For a missing key:

      iex> db = DB.open(tmp_dir())
      iex> DB.fetch!(db, "name")
      ** (KeyError) key "name" not found in db

  For an existing key:

      iex> db = Soy.open(tmp_dir())
      iex> :ok = Soy.put(db, "hello", "world")
      iex> Soy.fetch!(db, "hello")
      "world"

  """
  def fetch!(db, key) do
    case fetch(db, key) do
      {:ok, got} ->
        got

      :error ->
        raise KeyError, message: "key #{inspect(key)} not found in db"
    end
  end

  @doc """
  Removes a key and value from the db.

  ## Examples

  For a missing key:

      iex> db = Soy.open(tmp_dir())
      iex> Soy.delete(db, "name")
      :ok

  For an existing key:

      iex> db = DB.open(tmp_dir())
      iex> :ok = DB.put(db, "hello", "world")
      iex> DB.delete(db, "hello")
      :ok
      iex> DB.get(db, "hello")
      nil

  """
  def delete(db, key) do
    Native.db_delete(to_ref(db), key)
  end

  @doc """
  Run batch mutations on the db its column families

  See Soy.Batch for more info.

  ## Examples

    iex> db = Soy.open(tmp_dir())
    iex> {:ok, cf} = DBCol.create_new(db, "ages")
    iex> ops = [{:put, "name", "bill"}, {:put_cf, Soy.DBCol.to_ref(cf), "bill", "28"}]
    iex> 2 = Soy.batch(db, ops)
    iex> Soy.get(db, "name")
    "bill"
    iex> DBCol.get(cf, "bill")
    "28"

  """
  def batch(db, ops) when is_list(ops) do
    Native.db_batch(to_ref(db), ops)
  end

  @doc """
  Gets multiple keys from the db.
  """
  def multi_get(db, keys) do
    Native.db_multi_get(to_ref(db), keys)
  end

  @doc """
  Creates a immutable snapshot of the DB in memory.
  """
  def snapshot(db) do
    Snapshot.new(to_ref(db))
  end
end
