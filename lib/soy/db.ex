defmodule Soy.DB do
  alias Soy.{DB, Iter, Snapshot, DBCol, Batch}

  defstruct db_ref: nil

  @doc """
  Opens a db at the given `path` with the given `opts`.

  ## Examples

      iex> {:ok, db, [default]} = DB.open(tmp_dir())
      iex> %DB{} = db
      iex> %DBCol{} = default
      iex> default.name
      "default"
      iex> is_reference(db.db_ref)
      true

      iex> {:ok, db, [_]} = DB.open(tmp_dir(), [prefix_length: 3])
      iex> is_reference(db.db_ref)
      true

  """
  def open(path, opts \\ []) do
    opts = Keyword.put_new(opts, :create_if_missing, true)
    charpath = to_charlist(path)
    cols = list_columns_chars(charpath)
    cols_and_opts = Enum.map(cols, fn col -> {col, []} end)
    cols_and_opts = [{'default', []} | cols_and_opts]

    case :rocksdb.open(charpath, opts, cols_and_opts) do
      {:ok, db_ref, col_refs} ->
        db_cols =
          cols_and_opts
          |> Enum.zip(col_refs)
          |> Enum.map(fn {{name, []}, col_ref} ->
            DBCol.build(to_string(name), col_ref, db_ref)
          end)

        {:ok, build(db_ref), db_cols}

      {:error, _} = err ->
        err
    end
  end

  @doc """
  Builds a Soy.DB struct.
  """
  def build(db_ref) when is_reference(db_ref), do: %DB{db_ref: db_ref}

  @doc """
  Lists the column families of given `path`.

  ## Examples

      iex> path = tmp_dir()
      iex> Soy.list_columns(path)
      []

      iex> path = tmp_dir()
      iex> {:ok, db, _} = Soy.open(path)
      iex> {:ok, _cf} = Soy.DBCol.create_new(db, "fam")
      iex> Soy.list_columns(path)
      ["default", "fam"]

  """
  def list_columns(path) do
    Enum.map(list_columns_chars(path), fn col -> to_string(col) end)
  end

  defp list_columns_chars(path) do
    case :rocksdb.list_column_families(to_charlist(path), []) do
      {:ok, cols} -> cols
      {:error, {:db_open, _}} -> []
    end
  end

  @doc """
  Creates a column family in the db.
  """
  def create_new_cf(%DB{db_ref: db_ref}, name, opts \\ []) do
    case :rocksdb.create_column_family(db_ref, to_charlist(name), opts) do
      {:ok, cf_ref} -> {:ok, DBCol.build(name, cf_ref, db_ref)}
      {:error, _} = err -> err
    end
  end

  @doc """
  Destroys the db at the given `path`.

  # WARNING - causes data loss.

  ## Examples

      iex> path = tmp_dir()
      iex> {:ok, db, [_]} = Soy.open(path)
      iex> :ok = Soy.put(db, "hello", "world")
      iex> "world" = Soy.get(db, "hello")
      iex> magically_drop_db_from_scope()
      iex> :ok = Soy.destroy(path)
      iex> {:ok, db, [_]} = Soy.open(path)
      iex> Soy.get(db, "hello")
      nil

  """
  def destroy(path, opts \\ []) do
    :rocksdb.destroy(to_charlist(path), opts)
  end

  @doc """
  Repairs the database at the given path

  ## Examples
      iex> path = tmp_dir()
      iex> {:ok, _db, [_]} = Soy.open(path)
      iex> magically_drop_db_from_scope()
      iex> :ok = Soy.repair(path)
      iex> {:ok, db, [%{name: "default"}, %{name: "default"}]} = Soy.open(path)
      iex> is_reference(db.db_ref)
      true

  ## TODO: why are there 2 "default" column families after repairing?
  """
  def repair(path, opts \\ []) do
    :rocksdb.repair(to_charlist(path), opts)
  end

  # @doc """
  # The path of the db.

  # # Examples

  #     iex> path = tmp_dir()
  #     iex> {:ok, db, [_]} = Soy.open(path)
  #     iex> Soy.path(db) == path
  #     true

  # """
  # def path(db) do
  #   Native.db_path(to_ref(db))
  # end

  # @doc """
  # Returns the live files of the database.

  # When the DB is not processing files in the background
  # this function will return an empty list. If the DB is compacting
  # then the active SST files will be listed.
  # """
  # def live_files(db) do
  #   Native.db_live_files(to_ref(db))
  # end

  @doc """
  Stores a value in the DB.

  ## Examples

    iex> {:ok, db, [_]} = Soy.open(tmp_dir())
    iex> nil = Soy.get(db, "hello")
    iex> :ok = Soy.put(db, "hello", "world")
    iex> "world" = Soy.get(db, "hello")
  """
  def put(%DB{db_ref: db_ref}, key, val, opts \\ []) do
    :rocksdb.put(db_ref, key, val, opts)
  end

  # @doc """
  # Returns `true` or `false` based on the existence of a `key` in the `db`.

  # ## Examples

  #     iex> {:ok, db, [_]} = Soy.open(tmp_dir())
  #     iex> :ok = Soy.put(db, "hello", "world")
  #     iex> DB.has_key?(db, "hello")
  #     true
  #     iex> DB.has_key?(db, "other")
  #     false

  # """
  # def has_key?(db, key) do
  #   Native.db_has_key(to_ref(db), key)
  # end

  @doc """
  Fetches a value from the DB.

  Returns `:error` for a missing key and `{:ok, binary}` for
  a found key.

  ## Examples

  For a missing key:

      iex> {:ok, db, [_]} = Soy.open(tmp_dir())
      iex> :error = Soy.fetch(db, "name")

  For an existing key:

      iex> {:ok, db, [_]} = Soy.open(tmp_dir())
      iex> :ok = Soy.put(db, "hello", "world")
      iex> {:ok, "world"} = Soy.fetch(db, "hello")
  """
  def fetch(%DB{db_ref: db_ref}, key) do
    case :rocksdb.get(db_ref, key, []) do
      :not_found -> :error
      {:ok, _} = okay -> okay
    end
  end

  @doc """
  Gets a value from the DB.

  Returns `default` (the default of `default` is `nil`) for a missing key and `binary` for
  a found key. The `default` can be overridden with get/3.

  ## Examples

  For a missing key:

      iex> {:ok, db, [_]} = Soy.open(tmp_dir())
      iex> Soy.get(db, "name")
      nil

  For an existing key:

      iex> {:ok, db, [_]} = Soy.open(tmp_dir())
      iex> :ok = Soy.put(db, "hello", "world")
      iex> "world" = Soy.get(db, "hello")

  For a missing key with a given `default`:

      iex> {:ok, db, [_]} = Soy.open(tmp_dir())
      iex> Soy.get(db, "name", "you")
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

      iex> {:ok, db, [_]} = DB.open(tmp_dir())
      iex> DB.fetch!(db, "name")
      ** (KeyError) key "name" not found in db

  For an existing key:

      iex> {:ok, db, [_]} = Soy.open(tmp_dir())
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

      iex> {:ok, db, [_]} = Soy.open(tmp_dir())
      iex> Soy.delete(db, "name")
      :ok

  For an existing key:

      iex> {:ok, db, [_]} = DB.open(tmp_dir())
      iex> :ok = DB.put(db, "hello", "world")
      iex> DB.delete(db, "hello")
      :ok
      iex> DB.get(db, "hello")
      nil

  """
  def delete(%DB{db_ref: db_ref}, key, opts \\ []) do
    :rocksdb.single_delete(db_ref, key, opts)
  end

  @doc """
  Run batch mutations on the db its column families

  See Soy.Batch for more info.

  ## Examples

    iex> {:ok, db, [_]} = Soy.open(tmp_dir())
    iex> {:ok, cf} = DBCol.create_new(db, "ages")
    iex> ops = [{:put, "name", "bill"}, {:put, Soy.DBCol.to_ref(cf), "bill", "28"}]
    iex> :ok = Soy.batch(db, ops)
    iex> Soy.get(db, "name")
    "bill"
    iex> Soy.get(cf, "bill")
    "28"

  """
  def write_batch(db, batch, opts \\ [])

  def write_batch(%DB{db_ref: db_ref}, %Batch{ref: batch_ref}, opts) do
    :rocksdb.write_batch(db_ref, batch_ref, opts)
  end

  def write_batch(%DB{} = db, ops, opts) when is_list(ops) do
    b = Batch.from_list(ops)
    write_batch(db, b, opts)
  end

  @doc """
  Flushes/Synces the db to disk.
  """
  def flush(%DB{db_ref: db_ref}, opts \\ []) do
    :rocksdb.flush(db_ref, opts)
  end

  # @doc """
  # Gets multiple keys from the db.
  # """
  # def multi_get(db, keys) do
  #   Native.db_multi_get(to_ref(db), keys)
  # end

  @doc """
  Creates a immutable snapshot of the DB in memory.
  """
  def snapshot(%DB{db_ref: db_ref}) do
    Snapshot.new(db_ref)
  end

  @doc """
  Creates an iter for the `db`.
  """
  def iter(%DB{db_ref: db_ref}) do
    {:ok, it_ref} = :rocksdb.iterator(db_ref, [])
    Iter.new(it_ref)
  end

  def reduce_keys(%DB{db_ref: db_ref}, acc, opts \\ [], func) when is_function(func, 2) do
    :rocksdb.fold_keys(db_ref, func, acc, opts)
  end

  def reduce(%DB{db_ref: db_ref}, acc, opts \\ [], func) when is_function(func, 2) do
    :rocksdb.fold(db_ref, func, acc, opts)
  end

  def db_ref(%DB{db_ref: db_ref}), do: db_ref
end
