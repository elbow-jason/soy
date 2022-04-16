defmodule Soy.Txn do
  @moduledoc """
  For working with transactions.
  """
  alias Soy.{Txn, Iter}

  defstruct txn_ref: nil, db_ref: nil

  def new(%_{db_ref: db_ref}, opts \\ []) do
    {:ok, txn_ref} = :rocksdb.transaction(db_ref, opts)
    %Txn{txn_ref: txn_ref, db_ref: db_ref}
  end

  def commit(%Txn{txn_ref: txn_ref}) do
    :rocksdb.transaction_commit(txn_ref)
  end

  def rollback(%Txn{txn_ref: txn_ref}) do
    :rocksdb.transaction_rollback(txn_ref)
  end

  defp ref(%Txn{txn_ref: txn_ref}), do: txn_ref

  def put(txn, key, val) do
    :rocksdb.transaction_put(ref(txn), key, val)
  end

  def put(txn, %_{cf_ref: cf_ref}, key, val) do
    :rocksdb.transaction_put(ref(txn), cf_ref, key, val)
  end

  def fetch(txn, key) when is_binary(key) do
    do_fetch(txn, nil, key, [])
  end

  def fetch(txn, key, opts) when is_binary(key) and is_list(opts) do
    do_fetch(txn, nil, key, opts)
  end

  def fetch(txn, %_{cf_ref: cf_ref}, key) when is_binary(key) do
    do_fetch(txn, cf_ref, key, [])
  end

  def fetch(txn, %_{cf_ref: cf_ref}, key, opts) when is_binary(key) and is_list(opts) do
    do_fetch(txn, cf_ref, key, opts)
  end

  defp do_fetch(txn, cf_ref, key, opts) do
    txn_ref = ref(txn)

    res =
      case cf_ref do
        nil ->
          :rocksdb.transaction_get(txn_ref, key, opts)

        c when is_reference(c) ->
          :rocksdb.transaction_get(ref(txn), c, key, opts)
      end

    case res do
      :not_found -> :error
      {:ok, _} = okay -> okay
    end
  end

  def get(txn, key), do: do_get(txn, nil, key, [])
  def get(txn, %{cf_ref: cf_ref}, key), do: do_get(txn, cf_ref, key, [])
  def get(txn, key, opts) when is_binary(key) and is_list(opts), do: do_get(txn, nil, key, opts)
  def get(txn, %{cf_ref: cf_ref}, key, opts), do: do_get(txn, cf_ref, key, opts)

  def get(txn, %_{cf_ref: cf_ref}, key, opts) when is_binary(key) and is_list(opts) do
    case :rocksdb.transaction_get(ref(txn), cf_ref, key) do
      :not_found -> nil
      {:ok, val} -> val
    end
  end

  defp do_get(txn, cf_ref, key, opts) do
    case do_fetch(txn, cf_ref, key, opts) do
      {:ok, val} -> val
      :error -> nil
    end
  end

  def delete(txn, key) do
    :rocksdb.transaction_delete(ref(txn), key)
  end

  def delete(txn, %_{cf_ref: cf_ref}, key) do
    :rocksdb.transaction_delete(ref(txn), cf_ref, key)
  end

  def iter(txn), do: do_iter(txn, nil, [])
  def iter(txn, opts) when is_list(opts), do: do_iter(txn, nil, opts)
  def iter(txn, %_{cf_ref: cf_ref}), do: do_iter(txn, cf_ref, [])
  def iter(txn, %_{cf_ref: cf_ref}, opts) when is_list(opts), do: do_iter(txn, cf_ref, opts)

  defp do_iter(%Txn{txn_ref: txn_ref, db_ref: db_ref}, cf_ref, opts) do
    res =
      case cf_ref do
        nil ->
          :rocksdb.transaction_iterator(db_ref, txn_ref, opts)

        c when is_reference(c) ->
          :rocksdb.transaction_iterator(db_ref, txn_ref, c, opts)
      end

    case res do
      {:ok, it} -> Iter.new(it)
      {:error, reason} -> raise "failed to create transaction iter - reason: #{inspect(reason)}"
    end
  end
end
