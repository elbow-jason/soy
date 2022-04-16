defmodule Soy.Iter do
  alias Soy.Iter

  defstruct it_ref: nil

  @doc """
  Gets a key-ordered-iterator for the db.

  # Examples

  Iterate through the rows in the `db`:

      iex> {:ok, db, [_]} = Soy.open(tmp_dir())
      iex> ops = [{:put, "k2", "v2"}, {:put, "k3", "v3"}, {:put, "k1", "v1"}]
      iex> :ok = Soy.batch(db, ops)
      iex> it = Iter.new(db)
      iex> Iter.first(it)
      {"k1", "v1"}
      iex> Iter.next(it)
      {"k2", "v2"}
      iex> Iter.next(it)
      {"k3", "v3"}
      iex> Iter.next(it)
      nil

  An empty `db` iter returns `nil` for `Iter.next/1`:

      iex> {:ok, db, [_]} = Soy.open(tmp_dir())
      iex> it = Iter.new(db)
      iex> Iter.next(it)
      nil
      iex> Iter.next(it)
      nil
      iex> Iter.next(it)
      nil
  """
  def new(it_ref) when is_reference(it_ref) do
    %Iter{it_ref: it_ref}
  end

  def new(%impl{} = store) do
    impl.iter(store)
  end

  def refresh(%Iter{it_ref: r} = it) do
    :ok = :rocksdb.iterator_refresh(r)
    it
  end

  def close(%Iter{it_ref: r}) do
    :rocksdb.iterator_close(r)
  end

  def seek(%Iter{it_ref: it_ref}, kind) do
    case :rocksdb.iterator_move(it_ref, kind) do
      {:ok, k, v} -> {k, v}
      {:error, :invalid_iterator} -> nil
    end
  end

  def first(it), do: seek(it, :first)

  def last(it), do: seek(it, :last)

  def next(it), do: seek(it, :next)

  def next(it, key), do: seek(it, key)

  def prev(it), do: seek(it, :prev)

  def prev(it, key), do: seek(it, {:seek_for_prev, key})

  def it_ref(%Iter{it_ref: it_ref}) when is_reference(it_ref), do: it_ref
  def it_ref(ref) when is_reference(ref), do: ref
end
