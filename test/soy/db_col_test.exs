defmodule Soy.DBColTest do
  use ExUnit.Case
  import Soy.TestHelpers
  alias Soy.DBCol

  doctest Soy.DBCol

  setup do
    {:ok, db, [_]} = Soy.open(tmp_dir())
    assert {:ok, cf} = DBCol.create_new(db, "feet")
    {:ok, %{db: db, cf: cf}}
  end

  describe "create_new/3" do
    test "works" do
      {:ok, db, [_]} = Soy.open(tmp_dir())
      assert {:ok, cf} = DBCol.create_new(db, "feet")
      assert %DBCol{cf_ref: cf_ref, db_ref: db_ref, name: "feet"} = cf
      assert is_reference(cf_ref)
      assert is_reference(db_ref)
    end
  end

  # describe "open/2" do
  #   test "works" do
  #     {:ok, db, [_]} = Soy.open(tmp_dir())
  #     assert {:ok, cf} = DBCol.create_new(db, "feet")
  #     assert {:ok, cf2} = DBCol.open(db, "feet")
  #     assert {:ok, cf3} = DBCol.open(db, "feet")
  #     assert DBCol.name(cf) == DBCol.name(cf2)
  #     assert DBCol.name(cf) == DBCol.name(cf3)
  #   end
  # end

  describe "name/1" do
    test "works", %{cf: cf} do
      assert DBCol.name(cf) == "feet"
    end
  end

  describe "to_ref/1" do
    test "works", %{cf: cf} do
      ref = DBCol.to_ref(cf)
      assert is_reference(ref)
    end
  end

  describe "destroy/1" do
    test "works", %{cf: cf} do
      assert :ok = DBCol.destroy(cf)
      assert_raise(ArgumentError, fn -> DBCol.destroy(cf) end)
    end
  end

  describe "put/3" do
    test "works", %{cf: cf} do
      assert :ok = DBCol.put(cf, "me", "2")
      assert :ok = DBCol.put(cf, "ruby", "4")
      assert DBCol.get(cf, "me") == "2"
      assert DBCol.get(cf, "ruby") == "4"
    end
  end

  describe "fetch/2" do
    test "works", %{cf: cf} do
      assert :ok = DBCol.put(cf, "me", "2")
      assert :ok = DBCol.put(cf, "ruby", "4")
      assert DBCol.fetch(cf, "me") == {:ok, "2"}
      assert DBCol.fetch(cf, "ruby") == {:ok, "4"}
    end
  end

  # describe "has_key?/2" do
  #   test "works", %{cf: cf} do
  #     assert :ok = DBCol.put(cf, "me", "2")
  #     assert DBCol.has_key?(cf, "me") == true
  #     assert DBCol.has_key?(cf, "you") == false
  #   end
  # end

  describe "delete/2" do
    test "works", %{cf: cf} do
      assert :ok = DBCol.put(cf, "me", "2")
      assert DBCol.get(cf, "me") == "2"
      assert :ok = DBCol.delete(cf, "me")
      assert DBCol.get(cf, "me") == nil
    end
  end

  # describe "multi_get/1" do
  #   test "works" do
  #     {:ok, db, [_]} = Soy.open(tmp_dir())
  #     assert {:ok, name} = DBCol.create_new(db, "name")
  #     assert {:ok, age} = DBCol.create_new(db, "age")
  #     assert :ok = DBCol.put(name, "user:1", "jason")
  #     assert :ok = DBCol.put(name, "user:2", "mary")
  #     assert :ok = DBCol.put(age, "user:1", "38")
  #     assert :ok = DBCol.put(age, "user:2", "36")

  #     values =
  #       DBCol.multi_get([
  #         {name, "user:1"},
  #         {age, "user:1"},
  #         {name, "user:2"},
  #         {age, "user:2"},
  #         {name, "user:3"},
  #         {age, "user:3"}
  #       ])

  #     assert values == ["jason", "38", "mary", "36", nil, nil]
  #   end
  # end

  # describe "multi_get/2" do
  #   test "works" do
  #     {:ok, db, [_]} = Soy.open(tmp_dir())
  #     assert {:ok, name} = DBCol.create_new(db, "name")
  #     assert :ok = DBCol.put(name, "user:1", "jason")
  #     assert :ok = DBCol.put(name, "user:2", "mary")
  #     values = DBCol.multi_get(name, ["user:1", "user:2", "user:3"])

  #     assert values == [
  #              "jason",
  #              "mary",
  #              nil
  #            ]
  #   end
  # end
end
