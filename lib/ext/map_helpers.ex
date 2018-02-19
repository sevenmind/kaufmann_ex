defmodule Map.Helpers do
  @moduledoc """
  Functions to transform maps
  Source: https://gist.github.com/kipcole9/0bd4c6fb6109bfec9955f785087f53fb
  """

  @doc """
  Convert map string camelCase keys to underscore_keys
  """
  def underscore_keys(nil), do: nil

  def underscore_keys(%{} = map) do
    map
    |> Enum.map(fn {k, v} -> {Macro.underscore(k), underscore_keys(v)} end)
    |> Enum.map(fn {k, v} -> {String.replace(k, "-", "_"), v} end)
    |> Enum.into(%{})
  end

  # Walk the list and atomize the keys of
  # of any map members
  def underscore_keys([head | rest]) do
    [underscore_keys(head) | underscore_keys(rest)]
  end

  def underscore_keys(not_a_map) do
    not_a_map
  end

  @doc """
  Convert map string keys to :atom keys
  """
  def atomize_keys(nil), do: nil

  # Structs don't do enumerable and anyway the keys are already
  # atoms
  def atomize_keys(%{__struct__: _} = struct) do
    struct
  end

  def atomize_keys(%{} = map) do
    map
    |> Enum.map(fn {k, v} -> {atomize_string(k), atomize_keys(v)} end)
    |> Enum.into(%{})
  end

  # Walk the list and atomize the keys of
  # of any map members
  def atomize_keys([head | rest]) do
    [atomize_keys(head) | atomize_keys(rest)]
  end

  def atomize_keys(not_a_map) do
    not_a_map
  end

  def atomize_string(string) when is_binary(string), do: String.to_atom(string)

  def atomize_string(not_a_string) do
    not_a_string
  end

  @doc """
  Convert map atom keys to strings
  """
  def stringify_keys(nil), do: nil

  def stringify_keys(%{} = map) do
    map
    |> Enum.map(fn {k, v} -> {stringify_atom(k), stringify_keys(v)} end)
    |> Enum.into(%{})
  end

  def stringify_keys(%{__meta__: _} = schema), do: schema
  def stringify_keys(%{struct: _} = struct), do: struct

  # Walk the list and stringify the keys of
  # of any map members
  def stringify_keys([head | rest]) do
    [stringify_keys(head) | stringify_keys(rest)]
  end

  def stringify_keys(not_a_map) do
    not_a_map
  end

  def stringify_atom(atom) when is_atom(atom) do
    Atom.to_string(atom)
  end

  def stringify_atom(not_a_atom), do: not_a_atom

  @doc """
  Deep merge two maps
  """
  def deep_merge(left, right) do
    Map.merge(left, right, &deep_resolve/3)
  end

  # Key exists in both maps, and both values are maps as well.
  # These can be merged recursively.
  defp deep_resolve(_key, %{} = left, %{} = right) do
    deep_merge(left, right)
  end

  # Key exists in both maps, but at least one of the values is
  # NOT a map. We fall back to standard merge behavior, preferring
  # the value on the right.
  defp deep_resolve(_key, _left, right) do
    right
  end
end
