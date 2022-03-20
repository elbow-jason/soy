defmodule Soy.SnapshotTest do
  use ExUnit.Case
  import Soy.TestHelpers
  alias Soy.{Iter, Snapshot}

  describe "new/1" do
    test "returns a reference" do
      db = Soy.open(tmp_dir())
      assert {Snapshot, ss} = Snapshot.new(db)
      assert is_reference(ss) == true
    end
  end

  describe "fetch/2" do
    test "can fetch entries that exist when it is created" do
      db = Soy.open(tmp_dir())
      :ok = Soy.put(db, "k1", "v1")
      :ok = Soy.put(db, "k2", "v2")
      :ok = Soy.put(db, "k3", "v3")
      assert {:ok, "v1"} = Soy.fetch(db, "k1")
      assert {:ok, "v2"} = Soy.fetch(db, "k2")
      assert {:ok, "v3"} = Soy.fetch(db, "k3")
      ss = Snapshot.new(db)
      assert {:ok, "v1"} = Snapshot.fetch(ss, "k1")
      assert {:ok, "v2"} = Snapshot.fetch(ss, "k2")
      assert {:ok, "v3"} = Snapshot.fetch(ss, "k3")
    end

    test "cannot fetch entries made after the ss is created" do
      db = Soy.open(tmp_dir())
      :ok = Soy.put(db, "k1", "v1")
      :ok = Soy.put(db, "k2", "v2")
      ss = Snapshot.new(db)
      :ok = Soy.put(db, "k3", "v3")
      assert {:ok, "v1"} = Soy.fetch(db, "k1")
      assert {:ok, "v2"} = Soy.fetch(db, "k2")
      assert {:ok, "v3"} = Soy.fetch(db, "k3")
      assert {:ok, "v1"} = Snapshot.fetch(ss, "k1")
      assert {:ok, "v2"} = Snapshot.fetch(ss, "k2")
      assert :error = Snapshot.fetch(ss, "k3")
    end
  end

  describe "fetch_cf" do
    # test "can fetch entries that exist when the ss is created" do
    #   db = Soy.open(tmp_dir())
    #   cf = DBCol.build(db, "items")
    #   :ok = DBCol.create_new(cf)
    #   :ok = DBCol.put(cf, "k1", "v1")
    #   :ok = DBCol.put(cf, "k2", "v2")
    #   :ok = DBCol.put(cf, "k3", "v3")
    #   assert {:ok, "v1"} = DBCol.fetch(cf, "k1")
    #   assert {:ok, "v2"} = DBCol.fetch(cf, "k2")
    #   assert {:ok, "v3"} = DBCol.fetch(cf, "k3")
    #   ss = Snapshot.new(db)
    #   assert {:ok, "v1"} = Snapshot.fetch_cf(ss, cf, "k1")
    #   assert {:ok, "v2"} = Snapshot.fetch_cf(ss, cf, "k2")
    #   assert {:ok, "v3"} = Snapshot.fetch_cf(ss, cf, "k3")
    # end

    # test "cannot fetch entries made after the ss is created" do
    #   db = Soy.open(tmp_dir())
    #   cf = DBCol.build(db, "items")
    #   :ok = DBCol.create_new(cf)
    #   :ok = DBCol.put(cf, "k1", "v1")
    #   :ok = DBCol.put(cf, "k2", "v2")
    #   ss = Snapshot.new(db)
    #   :ok = DBCol.put(cf, "k3", "v3")
    #   assert {:ok, "v1"} = DBCol.fetch(cf, "k1")
    #   assert {:ok, "v2"} = DBCol.fetch(cf, "k2")
    #   assert {:ok, "v3"} = DBCol.fetch(cf, "k3")
    #   assert {:ok, "v1"} = Snapshot.fetch_cf(ss, cf, "k1")
    #   assert {:ok, "v2"} = Snapshot.fetch_cf(ss, cf, "k2")
    #   assert :error = Snapshot.fetch_cf(ss, cf, "k3")
    # end
  end

  describe "iter/2" do
    test "iterates through snapshotted entries" do
      db = Soy.open(tmp_dir())
      :ok = Soy.put(db, "k1", "v1")
      :ok = Soy.put(db, "k3", "v3")
      ss = Snapshot.new(db)
      :ok = Soy.put(db, "k2", "v2")
      ss_it = Iter.new(ss)
      db_it = Iter.new(db)

      assert Iter.next(db_it) == {"k1", "v1"}
      assert Iter.next(db_it) == {"k2", "v2"}
      assert Iter.next(db_it) == {"k3", "v3"}
      assert Iter.next(db_it) == nil

      assert Iter.next(ss_it) == {"k1", "v1"}
      assert Iter.next(ss_it) == {"k3", "v3"}
      assert Iter.next(ss_it) == nil
    end
  end

  describe "multi_get/2" do
    test "returns existing values and nil for entries before creation" do
      db = Soy.open(tmp_dir())
      :ok = Soy.put(db, "k1", "v1")
      :ok = Soy.put(db, "k3", "v3")
      ss = Snapshot.new(db)
      :ok = Soy.put(db, "k2", "v2")
      keys = ["k4", "k1", "k2", "k3"]
      assert Soy.multi_get(db, keys) == [nil, "v1", "v2", "v3"]
      assert Snapshot.multi_get(ss, keys) == [nil, "v1", nil, "v3"]
    end
  end

  describe "multi_get_cf/2" do
    # test "returns existing values and nil for entries before creation" do
    #   db = Soy.open(tmp_dir())
    #   cf1 = DBCol.build(db, "cf1")
    #   cf2 = DBCol.build(db, "cf2")
    #   :ok = DBCol.create_new(cf1)
    #   :ok = DBCol.create_new(cf2)

    #   :ok = DBCol.put(cf1, "k1", "v1")
    #   :ok = DBCol.put(cf2, "k3", "v3")
    #   ss = Snapshot.new(db)
    #   :ok = DBCol.put(cf1, "k2", "v2")
    #   :ok = DBCol.put(cf2, "k4", "v4")
    #   keys = ["k1", "k2", "k3", "k4"]

    #   pairs =
    #     for cf <- [cf1, cf2] do
    #       name = DBCol.name(cf)

    #       for k <- keys do
    #         {name, k}
    #       end
    #     end
    #     |> List.flatten()

    #   assert DBCol.multi_get(cf1, keys) == ["v1", "v2", nil, nil]
    #   assert DBCol.multi_get(cf2, keys) == [nil, nil, "v3", "v4"]
    #   assert Snapshot.multi_get_cf(ss, pairs) == ["v1", nil, nil, nil, nil, nil, "v3", nil]
    # end
  end
end
