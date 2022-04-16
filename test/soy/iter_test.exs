defmodule Soy.IterTest do
  use ExUnit.Case
  import Soy.TestHelpers
  alias Soy.{Iter, DBCol}
  doctest Soy.Iter

  setup do
    {:ok, db, [_]} = Soy.open(tmp_dir())
    :ok = Soy.put(db, "a", "1")
    :ok = Soy.put(db, "k1", "v1")
    :ok = Soy.put(db, "k3", "v3")
    :ok = Soy.put(db, "k2", "v2")
    :ok = Soy.put(db, "z", "1000")
    {:ok, cf} = DBCol.create_new(db, "things")
    :ok = DBCol.put(cf, "tk2", "tv2")
    :ok = DBCol.put(cf, "tk1", "tv1")
    :ok = DBCol.put(cf, "tk3", "tv3")
    {:ok, %{db: db, cf: cf}}
  end

  describe "next/1" do
    test "iterates forward through all the keys in the db", %{db: db} do
      it = Iter.new(db)
      assert Iter.first(it) == {"a", "1"}
      assert Iter.next(it) == {"k1", "v1"}
      assert Iter.next(it) == {"k2", "v2"}
      assert Iter.next(it) == {"k3", "v3"}
      assert Iter.next(it) == {"z", "1000"}
      assert Iter.next(it) == nil
    end
  end

  describe "next/2" do
    test "seeks to the key if exists", %{db: db} do
      it = Iter.new(db)
      assert Iter.next(it, "k3") == {"k3", "v3"}
    end

    test "seeks to the key next-greatest key if the key does not exist", %{db: db} do
      it = Iter.new(db)
      assert Iter.next(it, "k4") == {"z", "1000"}
    end
  end

  describe "prev/1" do
    test "iterates in reverse through all the keys in the db", %{db: db} do
      it = Iter.new(db)
      assert Iter.last(it) == {"z", "1000"}
      assert Iter.prev(it) == {"k3", "v3"}
      assert Iter.prev(it) == {"k2", "v2"}
      assert Iter.prev(it) == {"k1", "v1"}
      assert Iter.prev(it) == {"a", "1"}
      assert Iter.prev(it) == nil
    end
  end

  describe "prev/2" do
    test "seeks to the key if exists", %{db: db} do
      it = Iter.new(db)
      assert Iter.prev(it, "k1") == {"k1", "v1"}
    end

    test "seeks to the key next-lesser key if the key does not exist", %{db: db} do
      it = Iter.new(db)
      assert Iter.prev(it, "k0") == {"a", "1"}
    end
  end

  describe "last/1" do
    test "seeks the last entry in the db", %{db: db} do
      it = Iter.new(db)
      assert Iter.last(it) == {"z", "1000"}
      assert Iter.prev(it) == {"k3", "v3"}
      assert Iter.next(it) == {"z", "1000"}
      assert Iter.next(it) == nil
    end
  end

  describe "first/1" do
    test "seeks the first entry in the db", %{db: db} do
      it = Iter.new(db)
      assert Iter.first(it) == {"a", "1"}
      assert Iter.next(it) == {"k1", "v1"}
      assert Iter.prev(it) == {"a", "1"}
      assert Iter.prev(it) == nil
    end
  end

  # describe "key/1" do
  #   test "returns the current key", %{db: db} do
  #     it = Iter.new(db)
  #     assert Iter.first(it) == {"a", "1"}
  #     assert Iter.key(it) == "a"
  #     assert Iter.key(it) == "a"
  #     assert Iter.last(it) == {"z", "1000"}
  #     assert Iter.key(it) == "z"
  #     assert Iter.key(it) == "z"
  #   end

  #   test "returns nil with a new iter", %{db: db} do
  #     it = Iter.new(db)
  #     assert Iter.key(it) == nil
  #   end

  #   test "returns nil after becoming invalid", %{db: db} do
  #     it = DB.iter(db)
  #     assert Iter.first(it) == {"a", "1"}
  #     assert Iter.prev(it) == nil
  #     assert Iter.next(it) == nil
  #   end
  # end

  # describe "value/1" do
  #   test "returns the current key", %{db: db} do
  #     it = Iter.new(db)
  #     assert  == true
  #     assert Iter.first(it) == {"a", "1"}
  #     assert Iter.value(it) == "1"
  #     assert Iter.value(it) == "1"
  #     # assert  == true
  #     assert Iter.last(it) == {"z", "1000"}
  #     assert Iter.value(it) == "1000"
  #     assert Iter.value(it) == "1000"
  #   end

  #   test "returns nil with a new iter", %{db: db} do
  #     it = Iter.new(db)
  #     assert  == true
  #     assert Iter.value(it) == nil
  #   end

  #   test "returns nil after becoming invalid", %{db: db} do
  #     it = Iter.new(db)
  #     assert Iter.first(it) == {"a", "1"}
  #     assert Iter.prev(it) == nil
  #     assert  == false
  #     assert Iter.value(it) == nil
  #   end
  # end

  # describe "key_value/1" do
  #   test "returns the current {key, value}", %{db: db} do
  #     it = Iter.new(db)
  #     assert Iter.first(it) == {"a", "1"}
  #     assert Iter.key_value(it) == {"a", "1"}
  #     assert Iter.key_value(it) == {"a", "1"}
  #     assert Iter.last(it) == {"z", "1000"}
  #     assert Iter.key_value(it) == {"z", "1000"}
  #     assert Iter.key_value(it) == {"z", "1000"}
  #   end

  #   test "returns nil with a new iter", %{db: db} do
  #     it = Iter.new(db)
  #     assert Iter.value(it) == nil
  #   end

  #   test "returns nil after becoming invalid", %{db: db} do
  #     it = Iter.new(db)
  #     assert Iter.first(it) == {"a", "1"}
  #     assert Iter.prev(it) == nil
  #     assert Iter.value(it) == nil
  #   end

  #   test "returns a db cf iter for a DBCol", %{cf: cf} do
  #     it = Iter.new(cf)
  #     assert Iter.next(it) == {"tk1", "tv1"}
  #     assert Iter.next(it) == {"tk2", "tv2"}
  #     assert Iter.next(it) == {"tk3", "tv3"}
  #     assert Iter.next(it) == nil
  #   end
  # end

  # describe "new/" do
  # test "returns a cf iter for a snapshot", %{db: db} do
  #   assert {:ok, ss} = Snapshot.new(db)
  #   it = Iter.new(ss, "things")
  #   cf = DBCol.build(db, "things")
  #   :ok = Soy.put(cf, "beep", "boop")
  #   assert Iter.next(it) == {"tk1", "tv1"}
  #   assert Iter.next(it) == {"tk2", "tv2"}
  #   assert Iter.next(it) == {"tk3", "tv3"}
  #   assert Iter.next(it) == nil
  #   cf = DBCol.build(db, "things")
  #   :ok = Soy.put(cf, "beep", "boop")
  # end
  # end

  # describe "prefix/2" do
  #   @tag :skip
  #   test "used with Iter.valid?/2 can be used to probe for presence of keys in the db" do
  #     my_name = "jason"
  #     {:ok, db, [_]} = Soy.open(tmp_dir(), prefix_length: 3)
  #     my_prefix = "jas"
  #     invalid_it_because_no_jasons = Iter.prefix(db, my_prefix)
  #     assert Iter.valid?(invalid_it_because_no_jasons) == false
  #     :ok = Soy.put(db, my_name, "yep")
  #     it = Iter.prefix(db, my_prefix)
  #     assert  == true
  #   end

  #   @tag :skip
  #   test "iterator prefix length must match the OpenConfig's :prefix_length" do
  #     {:ok, db, [_]} = Soy.open(tmp_dir(), prefix_length: 4)
  #     bad_it = Iter.prefix(db, "prefix_longer_than_4")
  #     assert Iter.valid?(bad_it) == false

  #     bad_it = Iter.prefix(db, "four")
  #     assert Iter.valid?(bad_it) == false

  #     :ok = Soy.put(db, "five:1", "51")
  #     :ok = Soy.put(db, "five:2", "52")

  #     bad_it = Iter.prefix(db, "four")
  #     assert Iter.valid?(bad_it) == false
  #   end

  #   @tag :skip
  #   test "iterates the prefix in the db" do
  #     {:ok, db, [_]} = Soy.open(tmp_dir(), prefix_length: 3)
  #     :ok = Soy.put(db, "jason::1", "a")
  #     :ok = Soy.put(db, "jason::2", "b")
  #     :ok = Soy.put(db, "jason::3", "c")
  #     it = Iter.prefix(db, "jas")
  #     assert  == true
  #     assert Iter.next(it) == {"jason::1", "a"}
  #     assert Iter.next(it) == {"jason::2", "b"}
  #     assert Iter.next(it) == {"jason::3", "c"}
  #     assert Iter.next(it) == nil
  #   end
  # end

  # describe "prefix/3" do
  #   @tag :skip
  #   test "used with Iter.valid?/2 can be used to probe for presence of keys in the column family" do
  #     my_name = "jason"
  #     {:ok, db, [_]} = Soy.open(tmp_dir(), prefix_length: 3)
  #     cf = DBCol.build(db, "items")
  #     :ok = DBCol.create_new(cf, prefix_length: 3)
  #     my_prefix = "jas"
  #     invalid_it_because_no_jasons = Iter.prefix(cf, my_prefix)
  #     assert Iter.valid?(invalid_it_because_no_jasons) == false
  #     :ok = DBCol.put(cf, my_name, "yep")
  #     it = Iter.prefix(cf, my_prefix)
  #     assert  == true
  #   end

  #   @tag :skip
  #   test "iterator prefix length must match the OpenConfig's :prefix_length" do
  #     {:ok, db, [_]} = Soy.open(tmp_dir(), prefix_length: 4)
  #     bad_it = Iter.prefix(db, "prefix_longer_than_4")
  #     assert Iter.valid?(bad_it) == false

  #     bad_it = Iter.prefix(db, "four")
  #     assert Iter.valid?(bad_it) == false

  #     :ok = Soy.put(db, "five:1", "51")
  #     :ok = Soy.put(db, "five:2", "52")

  #     bad_it = Iter.prefix(db, "four")
  #     assert Iter.valid?(bad_it) == false
  #   end

  #   @tag :skip
  #   test "iterates the prefix in the db" do
  #     {:ok, db, [_]} = Soy.open(tmp_dir(), prefix_length: 3)
  #     :ok = Soy.put(db, "jason::1", "a")
  #     :ok = Soy.put(db, "jason::2", "b")
  #     :ok = Soy.put(db, "jason::3", "c")

  #     it = Iter.prefix(db, "jas")
  #     assert  == true
  #     assert Iter.next(it) == {"jason::1", "a"}
  #     assert Iter.next(it) == {"jason::2", "b"}
  #     assert Iter.next(it) == {"jason::3", "c"}
  #     assert Iter.next(it) == nil
  #   end
  # end
end
