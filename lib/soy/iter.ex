defmodule Soy.Iter do
  alias Soy.{ColFam, DB, Iter}

  # because native module was interferring with import?...
  defmacrop new(db, mode) do
    quote do
      {Soy.Iter, Soy.Native.iter(DB.to_ref(unquote(db)), unquote(mode))}
    end
  end

  @doc """
  Gets a key-ordered-iterator for the db where keys are ascending in value.

  # Examples

  Iterate through the rows in the `db`:

      iex> db = Soy.open(tmp_dir())
      iex> ops = [{:put, "k2", "v2"}, {:put, "k3", "v3"}, {:put, "k1", "v1"}]
      iex> 3 = Soy.batch(db, ops)
      iex> it = Iter.forward(db)
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
      iex> it = Iter.forward(db)
      iex> Iter.next(it)
      nil
      iex> Iter.next(it)
      nil
      iex> Iter.next(it)
      nil
  """
  def forward(db), do: new(db, :first)

  def forward(db, cf_name), do: new(db, {:first, cf_name})

  def reverse(db), do: new(db, :last)

  def reverse(db, cf_name), do: new(db, {:last, cf_name})

  def forward_from(db, key), do: new(db, {:forward, key})

  def forward_from(db, cf_name, key), do: new(db, {:forward, cf_name, key})

  def reverse_from(db, key), do: new(db, {:reverse, key})

  def reverse_from(db, cf_name, key), do: new(db, {:reverse, cf_name, key})

  def prefix({DB, db}, prefix), do: new(db, {:prefix, prefix})

  def prefix({ColFam, {db, name}}, prefix), do: new(db, {:prefix, name, prefix})

  def next(it), do: Soy.Native.iter_next(to_ref(it))

  def valid?(it), do: Soy.Native.iter_valid(to_ref(it))

  def set_mode(_it, {:prefix, _}) do
    raise "Iter.set_mode/2 does not support :prefix mode"
  end

  def set_mode(_it, {:prefix, _, _}) do
    raise "Iter.set_mode/2 does not support cf :prefix mode"
  end

  # TODO: protect better against unsupported cf iter modes

  def set_mode(it, mode), do: Soy.Native.iter_set_mode(to_ref(it), mode)

  def to_ref({Iter, ref}) when is_reference(ref), do: ref
  def to_ref(ref) when is_reference(ref), do: ref
end
