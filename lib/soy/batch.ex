defmodule Soy.Batch do
  alias Soy.{Batch, DBCol}
  defstruct ref: nil

  def new do
    {:ok, ref} = :rocksdb.batch()
    %Batch{ref: ref}
  end

  def clear(%Batch{ref: ref} = b) do
    :ok = :rocksdb.batch_clear(ref)
    b
  end

  def put(%Batch{ref: batch_ref} = b, %DBCol{cf_ref: cf_ref}, key, val) do
    :ok = :rocksdb.batch_put(batch_ref, cf_ref, key, val)
    b
  end

  def put(%Batch{ref: batch_ref} = b, key, val) do
    :ok = :rocksdb.batch_put(batch_ref, key, val)
    b
  end

  def delete(%Batch{ref: batch_ref} = b, %DBCol{cf_ref: cf_ref}, key) do
    :ok = :rocksdb.batch_delete(batch_ref, cf_ref, key)
    b
  end

  def delete(%Batch{ref: batch_ref} = b, key) do
    :ok = :rocksdb.batch_delete(batch_ref, key)
    b
  end

  def merge(%Batch{ref: batch_ref} = b, %DBCol{cf_ref: cf_ref}, key) do
    :ok = :rocksdb.batch_merge(batch_ref, cf_ref, key)
    b
  end

  def merge(%Batch{ref: batch_ref} = b, key, val) do
    :ok = :rocksdb.batch_merge(batch_ref, key, val)
    b
  end

  def count(%Batch{ref: ref}) do
    :rocksdb.batch_count(ref)
  end

  def data_size(%Batch{ref: ref}) do
    :rocksdb.batch_data_size(ref)
  end

  def release(%Batch{ref: batch_ref}) do
    :rocksdb.release_batch(batch_ref)
  end

  def to_list(%Batch{ref: batch_ref}) do
    :rocksdb.batch_tolist(batch_ref)
  end

  def from_list(list) do
    {:ok, ref} = :rocksdb.batch()

    Enum.each(list, fn item ->
      add(ref, item)
    end)

    %Batch{ref: ref}
  end

  defp ref(%Batch{ref: r}) when is_reference(r), do: r
  defp ref(r) when is_reference(r), do: r

  defp add(b, {:put, key, val}), do: :rocksdb.batch_put(ref(b), key, val)

  defp add(b, {:put, %DBCol{cf_ref: cf_ref}, key, val}),
    do: :rocksdb.batch_put(ref(b), cf_ref, key, val)

  defp add(b, {:put, cf_ref, key, val}), do: :rocksdb.batch_put(ref(b), cf_ref, key, val)
end
