defmodule Soy.TestHelpers do
  defmacro magically_drop_db_from_scope() do
    quote do
      # remove `db` and `_db` from scope
      var!(db) = nil
      # and use var `db` after assigning to it
      _db = var!(db)
      # no need to use `_db` it does not warn
      var!(_db) = nil
      ref = make_ref()
      this_process = self()
      :erlang.garbage_collect(this_process, type: :major, async: ref)

      receive do
        {:garbage_collect, ^ref, true} ->
          :ok
      after
        100 ->
          # best effort
          :ok
      end

      spawn(fn ->
        _ = :sys.get_state(this_process)
        send(this_process, {:you_are_not_busy, ref})
      end)

      receive do
        {:you_are_not_busy, ^ref} ->
          :ok
      after
        100 ->
          # best effort
          :ok
      end
    end
  end

  def tmp_dir do
    Briefly.create!(directory: true)
  end

  def test_dir(extra_path) do
    :soy
    |> :code.priv_dir()
    |> to_string()
    |> Path.join("soy_test")
    |> Path.join(extra_path)
  end
end
