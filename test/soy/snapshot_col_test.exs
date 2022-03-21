defmodule Soy.SnapshotColTest do
  use ExUnit.Case
  import Soy.TestHelpers
  alias Soy.{SnapshotCol, DBCol, Snapshot}

  doctest Soy.SnapshotCol

  setup do
    db = Soy.open(tmp_dir())
    assert {:ok, cf} = DBCol.create_new(db, "feet")
    assert :ok = DBCol.put(cf, "me", "2")
    assert :ok = DBCol.put(cf, "ruby", "4")
    ss = Snapshot.new(db)
    assert {:ok, ss_cf} = SnapshotCol.new(ss, "feet")
    {:ok, %{db: db, cf: cf, ss: ss, ss_cf: ss_cf}}
  end

  describe "new/2" do
    test "works" do
      db = Soy.open(tmp_dir())
      assert {:ok, cf} = DBCol.create_new(db, "feet")
      assert :ok = DBCol.put(cf, "me", "2")
      assert :ok = DBCol.put(cf, "ruby", "4")
      ss = Snapshot.new(db)
      assert {:ok, ss_cf} = SnapshotCol.new(ss, "feet")
      assert {SnapshotCol, ss_cf_ref} = ss_cf
      assert is_reference(ss_cf_ref) == true
    end

    test "errors for non-existent column" do
      db = Soy.open(tmp_dir())
      ss = Snapshot.new(db)
      assert SnapshotCol.new(ss, "beeeeeep") == {:error, "column family does not exist: beeeeeep"}
    end
  end

  describe "name/1" do
    test "works", %{ss_cf: ss_cf} do
      assert SnapshotCol.name(ss_cf) == "feet"
    end
  end

  describe "to_ref/1" do
    test "works", %{ss_cf: ss_cf} do
      ref = SnapshotCol.to_ref(ss_cf)
      assert is_reference(ref)
    end
  end

  describe "fetch/2" do
    test "works", %{ss_cf: ss_cf} do
      assert SnapshotCol.fetch(ss_cf, "me") == {:ok, "2"}
      assert SnapshotCol.fetch(ss_cf, "ruby") == {:ok, "4"}
    end
  end

  describe "multi_get/1" do
    test "works" do
      db = Soy.open(tmp_dir())
      assert {:ok, name} = DBCol.create_new(db, "name")
      assert {:ok, age} = DBCol.create_new(db, "age")
      assert :ok = DBCol.put(name, "user:1", "jason")
      assert :ok = DBCol.put(age, "user:1", "38")

      assert :ok = DBCol.put(name, "user:2", "mary")
      assert :ok = DBCol.put(age, "user:2", "35")
      ss = Snapshot.new(db)

      assert :ok = DBCol.put(age, "user:2", "36")
      assert :ok = DBCol.put(name, "user:3", "ruby")
      assert :ok = DBCol.put(age, "user:3", "8")

      assert {:ok, name_ss} = SnapshotCol.new(ss, "name")
      assert {:ok, age_ss} = SnapshotCol.new(ss, "age")

      values =
        SnapshotCol.multi_get([
          {name_ss, "user:1"},
          {age_ss, "user:1"},
          {name_ss, "user:2"},
          {age_ss, "user:2"},
          {name_ss, "user:3"},
          {age_ss, "user:3"}
        ])

      assert values == ["jason", "38", "mary", "35", nil, nil]
    end
  end

  describe "multi_get/2" do
    test "works" do
      db = Soy.open(tmp_dir())
      assert {:ok, name} = DBCol.create_new(db, "name")
      assert :ok = DBCol.put(name, "user:1", "jason")
      assert :ok = DBCol.put(name, "user:2", "mary")

      ss = Snapshot.new(db)
      assert {:ok, ss_name} = SnapshotCol.new(ss, "name")
      assert :ok = DBCol.put(name, "user:3", "ruby")
      assert :ok = DBCol.put(name, "user:2", "not_mary")
      keys = ["user:1", "user:2", "user:3"]

      assert SnapshotCol.multi_get(ss_name, keys) == [
               "jason",
               "mary",
               nil
             ]

      assert DBCol.multi_get(name, keys) == [
               "jason",
               "not_mary",
               "ruby"
             ]
    end
  end
end
