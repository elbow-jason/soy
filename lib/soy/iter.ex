defmodule Soy.Iter do
  alias Soy.{ColFam, DB, Iter, Native, Snapshot}

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
  def new({DB, db}), do: {Iter, DB, Native.db_iter(db)}
  def new({Snapshot, ss}), do: {Iter, Snapshot, Native.ss_iter(ss)}

  # def prefix({DB, db}, prefix), do: new(db, {:prefix, prefix})

  # def prefix({ColFam, {db, name}}, prefix), do: new(db, {:prefix, name, prefix})

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

  def set_mode(_it, {:prefix, _}) do
    raise "Iter.set_mode/2 does not support :prefix mode"
  end

  def set_mode(_it, {:prefix, _, _}) do
    raise "Iter.set_mode/2 does not support cf :prefix mode"
  end

  def to_ref({Iter, _, ref}) when is_reference(ref), do: ref
  def to_ref(ref) when is_reference(ref), do: ref
end
