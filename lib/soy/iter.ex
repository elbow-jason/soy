defmodule Soy.Iter do
  alias Soy.{SnapshotCol, DBCol, DB, Iter, Native, Snapshot}

  @doc """
  Gets a key-ordered-iterator for the db.

  # Examples

  Iterate through the rows in the `db`:

      iex> db = Soy.open(tmp_dir())
      iex> ops = [{:put, "k2", "v2"}, {:put, "k3", "v3"}, {:put, "k1", "v1"}]
      iex> 3 = Soy.batch(db, ops)
      iex> it = Iter.new(db)
      iex> Iter.next(it)
      {"k1", "v1"}
      iex> Iter.next(it)
      {"k2", "v2"}
      iex> Iter.next(it)
      {"k3", "v3"}
      iex> Iter.next(it)
      nil

  An empty `db` iter returns `nil` for `Iter.next/1`:

      iex> db = Soy.open(tmp_dir())
      iex> it = Iter.new(db)
      iex> Iter.next(it)
      nil
      iex> Iter.next(it)
      nil
      iex> Iter.next(it)
      nil
  """
  def new(store)
  def new({DB, db}), do: {Iter, Native.db_iter(db)}
  def new({Snapshot, ss}), do: {Iter, Native.ss_iter(ss)}
  def new({SnapshotCol, ss}), do: {Iter, Native.ss_cf_iter(ss)}
  def new({DBCol, cf}), do: {Iter, Native.db_cf_iter(cf)}

  # def new(store, cf_name) do
  #   case store do
  #     {DB, db} ->
  #     {Snapshot, ss} -> {Iter, {DBCol, cf_name}, Native.ss_iter_cf(ss, cf_name)}
  #   end
  # end

  # def prefix({DB, db}, prefix), do: new(db, {:prefix, prefix})

  # def prefix({DBCol, {db, name}}, prefix), do: new(db, {:prefix, name, prefix})

  def seek(it, kind), do: Soy.Native.iter_seek(to_ref(it), kind)

  def first(it), do: seek(it, :first)

  def last(it), do: seek(it, :last)

  def next(it), do: seek(it, :next)

  def next(it, key), do: seek(it, {:next, key})

  def prev(it), do: seek(it, :prev)

  def prev(it, key), do: seek(it, {:prev, key})

  def key(it), do: Soy.Native.iter_key(to_ref(it))

  def value(it), do: Soy.Native.iter_value(to_ref(it))

  def key_value(it), do: Soy.Native.iter_key_value(to_ref(it))

  def valid?(it), do: Soy.Native.iter_valid(to_ref(it))

  def to_ref({Iter, ref}) when is_reference(ref), do: ref
  def to_ref(ref) when is_reference(ref), do: ref
end
