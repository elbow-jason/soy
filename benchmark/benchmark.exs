import Soy.TestHelpers

nums = for i <- 1..1000 do
  to_string(i)
end
db = Soy.Native.open(tmp_dir(), %Soy.OpenOpts{})
:ok = Soy.Native.put(db, "1", "1")


Benchee.run(%{
  "put_10k"   => fn -> Enum.map(nums, fn k -> Soy.Native.put(db, k, k) end) end,
  "fetch_10k" => fn -> Enum.map(nums, fn k -> Soy.Native.fetch(db, k) end) end
})
