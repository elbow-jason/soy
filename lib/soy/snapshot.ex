defmodule Soy.Snapshot do
  alias Soy.{Iter, DB, Snapshot}

  defstruct db_ref: nil, ss_ref: nil

  def new(%DB{db_ref: db_ref}) do
    case :rocksdb.snapshot(db_ref) do
      {:ok, ss_ref} -> {:ok, %Snapshot{ss_ref: ss_ref, db_ref: db_ref}}
      {:error, _} = err -> err
    end
  end

  def to_ref(%Snapshot{ss_ref: ss_ref}) when is_reference(ss_ref), do: ss_ref
  def to_ref(ss_ref) when is_reference(ss_ref), do: ss_ref

  def db_ref(%Snapshot{db_ref: db_ref}) when is_reference(db_ref), do: db_ref

  def fetch(%Snapshot{ss_ref: ss_ref, db_ref: db_ref}, key) do
    case :rocksdb.get(db_ref, key, snapshot: ss_ref) do
      {:ok, v} -> {:ok, v}
      :not_found -> :error
    end
  end

  def release(%Snapshot{ss_ref: ss_ref}) do
    :rocksdb.release_snapshot(ss_ref)
  end

  # def multi_get(ss, keys), do: Native.ss_multi_get(to_ref(ss), keys)

  def iter(%Snapshot{db_ref: db_ref, ss_ref: ss_ref}) do
    {:ok, it_ref} = :rocksdb.iterator(db_ref, snapshot: ss_ref)
    Iter.new(it_ref)
  end

  def reduce_keys(
        %Snapshot{db_ref: db_ref, ss_ref: ss_ref},
        acc,
        opts \\ [],
        func
      )
      when is_function(func, 2) do
    :rocksdb.fold_keys(db_ref, func, acc, [{:snapshot, ss_ref} | opts])
  end

  def reduce(%Snapshot{db_ref: db_ref, ss_ref: ss_ref}, acc, opts \\ [], func)
      when is_function(func, 2) do
    :rocksdb.fold(db_ref, func, acc, [{:snapshot, ss_ref} | opts])
  end
end
