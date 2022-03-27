import Soy.TestHelpers

nums = for i <- 1..1000 do
  to_string(i)
end
db = Soy.Native.path_open_db(tmp_dir(), %Soy.OpenOpts{})
:ok = Soy.Native.db_put(db, "1", "1")


Benchee.run(%{
  "put_10k"   => fn -> Enum.map(nums, fn k -> Soy.Native.db_put(db, k, k) end) end,
  "fetch_10k" => fn -> Enum.map(nums, fn k -> Soy.Native.db_fetch(db, k) end) end,
  "batch_10k" => fn ->
    batch = Enum.map(nums, fn k -> {:put, k, k} end)
    Soy.Native.db_batch(db, batch)
  end,
})
