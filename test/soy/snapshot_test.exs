defmodule Soy.SnapshotTest do
  use ExUnit.Case
  import Soy.TestHelpers
  alias Soy.{Iter, Snapshot, DB}

  describe "new/1" do
    test "returns a reference" do
      {:ok, db, [_]} = Soy.open(tmp_dir())
      assert {:ok, %Snapshot{} = ss} = Snapshot.new(db)
      assert is_reference(ss.ss_ref) == true
    end
  end

  describe "fetch/2" do
    test "can fetch entries that exist when it is created" do
      {:ok, db, [_]} = Soy.open(tmp_dir())
      :ok = Soy.put(db, "k1", "v1")
      :ok = Soy.put(db, "k2", "v2")
      :ok = Soy.put(db, "k3", "v3")
      assert {:ok, "v1"} = Soy.fetch(db, "k1")
      assert {:ok, "v2"} = Soy.fetch(db, "k2")
      assert {:ok, "v3"} = Soy.fetch(db, "k3")
      assert {:ok, ss} = Snapshot.new(db)
      assert {:ok, "v1"} = Snapshot.fetch(ss, "k1")
      assert {:ok, "v2"} = Snapshot.fetch(ss, "k2")
      assert {:ok, "v3"} = Snapshot.fetch(ss, "k3")
    end

    test "cannot fetch entries made after the ss is created" do
      {:ok, db, [_]} = Soy.open(tmp_dir())
      :ok = Soy.put(db, "k1", "v1")
      :ok = Soy.put(db, "k2", "v2")
      assert {:ok, ss} = Snapshot.new(db)
      :ok = Soy.put(db, "k3", "v3")
      assert {:ok, "v1"} = Soy.fetch(db, "k1")
      assert {:ok, "v2"} = Soy.fetch(db, "k2")
      assert {:ok, "v3"} = Soy.fetch(db, "k3")
      assert {:ok, "v1"} = Snapshot.fetch(ss, "k1")
      assert {:ok, "v2"} = Snapshot.fetch(ss, "k2")
      assert :error = Snapshot.fetch(ss, "k3")
    end
  end

  describe "iter/1" do
    test "iterates through snapshotted entries" do
      {:ok, db, [_]} = Soy.open(tmp_dir())
      :ok = Soy.put(db, "k1", "v1")
      :ok = Soy.put(db, "k3", "v3")
      assert {:ok, ss} = Snapshot.new(db)
      :ok = Soy.put(db, "k2", "v2")
      ss_it = Snapshot.iter(ss)
      db_it = DB.iter(db)

      assert Iter.first(db_it) == {"k1", "v1"}
      assert Iter.next(db_it) == {"k2", "v2"}
      assert Iter.next(db_it) == {"k3", "v3"}
      assert Iter.next(db_it) == nil

      assert Iter.first(ss_it) == {"k1", "v1"}
      assert Iter.next(ss_it) == {"k3", "v3"}
      assert Iter.next(ss_it) == nil
    end
  end

  # describe "multi_get/2" do
  #   test "returns existing values and nil for entries before creation" do
  #     {:ok, db, [_]} = Soy.open(tmp_dir())
  #     :ok = Soy.put(db, "k1", "v1")
  #     :ok = Soy.put(db, "k3", "v3")
  #     assert {:ok, ss} = Snapshot.new(db)
  #     :ok = Soy.put(db, "k2", "v2")
  #     keys = ["k4", "k1", "k2", "k3"]
  #     assert Soy.multi_get(db, keys) == [nil, "v1", "v2", "v3"]
  #     assert Snapshot.multi_get(ss, keys) == [nil, "v1", nil, "v3"]
  #   end
  # end
end
