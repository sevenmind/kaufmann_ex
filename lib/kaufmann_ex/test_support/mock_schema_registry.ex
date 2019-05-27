defmodule KaufmannEx.TestSupport.MockSchemaRegistry do
  @moduledoc """
  A simple immitation schema registry that verifies Test Events against the schemas we have saved to file

  Looks for Schemas at `Application.get_env(:kaufmann_ex, :schema_path)`
  """

  import ExUnit.Assertions

  @spec defined_event?(String.t()) :: boolean
  def defined_event?(schema_name) do
    case find_schema(all_schemas(), schema_name) do
      nil -> false
      _ -> true
    end
  end

  @spec find_schema(any, any) :: any
  def find_schema(paths, schema_name) do
    Enum.find(paths, &(Path.rootname(Path.basename(&1)) == schema_name))
  end

  @spec encodable?(String.t(), any) :: boolean
  def encodable?(schema_name, payload) do
    # find schema file
    schema_path = find_schema(all_schemas(), schema_name)
    # identify transcoder

    assert schema_path != nil, "no valid schema found for event #{schema_name}"

    file_extension = Path.extname(schema_path)

    transcoder =
      Enum.find(KaufmannEx.Config.transcoders(), &(&1.schema_extension == file_extension))

    assert transcoder != nil, "no valid transcoder found for schema #{file_extension}"

    # validate schema

    schema_path
    |> transcoder.read_schema
    |> transcoder.encodable?(payload)
  end

  defp all_schemas do
    KaufmannEx.Config.schema_path()
    |> List.flatten()
    |> Enum.flat_map(&Path.wildcard([&1, "/**"]))
  end
end
