defmodule Soy.IterTest do
  use ExUnit.Case
  import Soy.TestHelpers
  alias Soy.{Iter, ColFam}
  doctest Soy.Iter

  setup do
    db = Soy.open(tmp_dir())
    :ok = Soy.put(db, "k1", "v1")
    :ok = Soy.put(db, "k3", "v3")
    :ok = Soy.put(db, "k2", "v2")
    cf = ColFam.build(db, "things")
    :ok = ColFam.create(cf)
    :ok = ColFam.put(cf, "tk2", "tv2")
    :ok = ColFam.put(cf, "tk1", "tv1")
    :ok = ColFam.put(cf, "tk3", "tv3")
    {:ok, %{db: db}}
  end

  describe "forward/1" do
    test "iterates forward through all the keys in the db", %{db: db} do
      it = Iter.forward(db)
      assert Iter.valid?(it) == true
      assert Iter.next(it) == {"k1", "v1"}
      assert Iter.next(it) == {"k2", "v2"}
      assert Iter.next(it) == {"k3", "v3"}
      assert Iter.next(it) == nil
    end
  end

  describe "forward/2" do
    test "iterates forward through all the keys in the column family", %{db: db} do
      it = Iter.forward(db, "things")
      assert Iter.valid?(it) == true
      assert Iter.next(it) == {"tk1", "tv1"}
      assert Iter.next(it) == {"tk2", "tv2"}
      assert Iter.next(it) == {"tk3", "tv3"}
      assert Iter.next(it) == nil
    end
  end

  describe "reverse/1" do
    test "iterates in reverse through all the keys in the db", %{db: db} do
      it = Iter.reverse(db)
      assert Iter.valid?(it) == true
      assert Iter.next(it) == {"k3", "v3"}
      assert Iter.next(it) == {"k2", "v2"}
      assert Iter.next(it) == {"k1", "v1"}
      assert Iter.next(it) == nil
    end
  end

  describe "reverse/2" do
    test "iterates in reverse through all the keys the column family", %{db: db} do
      it = Iter.reverse(db, "things")
      assert Iter.valid?(it) == true
      assert Iter.next(it) == {"tk1", "tv1"}
      assert Iter.next(it) == {"tk2", "tv2"}
      assert Iter.next(it) == {"tk3", "tv3"}
      assert Iter.next(it) == nil
    end
  end

  describe "forward_from/2" do
    test "iterates forward in the db skipping the lesser keys", %{db: db} do
      it = Iter.forward_from(db, "k2")
      assert Iter.valid?(it) == true
      assert Iter.next(it) == {"k2", "v2"}
      assert Iter.next(it) == {"k3", "v3"}
      assert Iter.next(it) == nil
    end
  end

  describe "reverse_from/2" do
    test "iterates the db in reverse skipping the greater keys", %{db: db} do
      it = Iter.reverse_from(db, "k2")
      assert Iter.valid?(it) == true
      assert Iter.next(it) == {"k2", "v2"}
      assert Iter.next(it) == {"k1", "v1"}
      assert Iter.next(it) == nil
    end
  end

  describe "forward_from/3" do
    test "iterates forward in the column family skipping the lesser keys", %{db: db} do
      it = Iter.forward_from(db, "things", "tk2")
      assert Iter.valid?(it) == true
      assert Iter.next(it) == {"tk2", "tv2"}
      assert Iter.next(it) == {"tk3", "tv3"}
      assert Iter.next(it) == nil
    end
  end

  describe "reverse_from/3" do
    test "iterates in reverse through the column family skipping the lesser keys", %{db: db} do
      it = Iter.reverse_from(db, "things", "tk2")
      assert Iter.valid?(it) == true
      assert Iter.next(it) == {"tk2", "tv2"}
      assert Iter.next(it) == {"tk1", "tv1"}
      assert Iter.next(it) == nil
    end
  end

  describe "prefix/2" do
    test "used with Iter.valid?/2 can be used to probe for presence of keys in the db" do
      my_name = "jason"
      db = Soy.open(tmp_dir(), prefix_length: 3)
      my_prefix = "jas"
      invalid_it_because_no_jasons = Iter.prefix(db, my_prefix)
      assert Iter.valid?(invalid_it_because_no_jasons) == false
      :ok = Soy.put(db, my_name, "yep")
      it = Iter.prefix(db, my_prefix)
      assert Iter.valid?(it) == true
    end

    test "iterator prefix length must match the OpenConfig's :prefix_length" do
      db = Soy.open(tmp_dir(), prefix_length: 4)
      bad_it = Iter.prefix(db, "prefix_longer_than_4")
      assert Iter.valid?(bad_it) == false

      bad_it = Iter.prefix(db, "four")
      assert Iter.valid?(bad_it) == false

      :ok = Soy.put(db, "five:1", "51")
      :ok = Soy.put(db, "five:2", "52")

      bad_it = Iter.prefix(db, "four")
      assert Iter.valid?(bad_it) == false
    end

    test "iterates the prefix in the db" do
      db = Soy.open(tmp_dir(), prefix_length: 3)
      :ok = Soy.put(db, "jason::1", "a")
      :ok = Soy.put(db, "jason::2", "b")
      :ok = Soy.put(db, "jason::3", "c")
      it = Iter.prefix(db, "jas")
      assert Iter.valid?(it) == true
      assert Iter.next(it) == {"jason::1", "a"}
      assert Iter.next(it) == {"jason::2", "b"}
      assert Iter.next(it) == {"jason::3", "c"}
      assert Iter.next(it) == nil
    end
  end

  describe "prefix/3" do
    test "used with Iter.valid?/2 can be used to probe for presence of keys in the column family" do
      my_name = "jason"
      db = Soy.open(tmp_dir(), prefix_length: 3)
      cf = ColFam.build(db, "items")
      :ok = ColFam.create(cf, prefix_length: 3)
      my_prefix = "jas"
      invalid_it_because_no_jasons = Iter.prefix(cf, my_prefix)
      assert Iter.valid?(invalid_it_because_no_jasons) == false
      :ok = ColFam.put(cf, my_name, "yep")
      it = Iter.prefix(cf, my_prefix)
      assert Iter.valid?(it) == true
    end

    test "iterator prefix length must match the OpenConfig's :prefix_length" do
      db = Soy.open(tmp_dir(), prefix_length: 4)
      bad_it = Iter.prefix(db, "prefix_longer_than_4")
      assert Iter.valid?(bad_it) == false

      bad_it = Iter.prefix(db, "four")
      assert Iter.valid?(bad_it) == false

      :ok = Soy.put(db, "five:1", "51")
      :ok = Soy.put(db, "five:2", "52")

      bad_it = Iter.prefix(db, "four")
      assert Iter.valid?(bad_it) == false
    end

    test "iterates the prefix in the db" do
      db = Soy.open(tmp_dir(), prefix_length: 3)
      :ok = Soy.put(db, "jason::1", "a")
      :ok = Soy.put(db, "jason::2", "b")
      :ok = Soy.put(db, "jason::3", "c")
      it = Iter.prefix(db, "jas")
      assert Iter.valid?(it) == true
      assert Iter.next(it) == {"jason::1", "a"}
      assert Iter.next(it) == {"jason::2", "b"}
      assert Iter.next(it) == {"jason::3", "c"}
      assert Iter.next(it) == nil
    end
  end
end
