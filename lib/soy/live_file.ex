defmodule Soy.LiveFile do
  defstruct [
    :column_family_name,
    :name,
    :size,
    :level,
    :start_key,
    :end_key,
    :num_entries,
    :num_deletions
  ]
end
