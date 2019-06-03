defmodule KaufmannEx.TestSupport.MockSchemaRegistry do
  @moduledoc """
  A simple immitation schema registry that verifies Test Events against the schemas we have saved to file

  Looks for Schemas at `Application.get_env(:kaufmann_ex, :schema_path)`
  """

  alias KaufmannEx.Publisher.Request
  alias KaufmannEx.Schemas.Event

  @spec defined_event?(Event.t() | Request.t() | String.t()) :: boolean
  def defined_event?(%Event{name: name}), do: name
  def defined_event?(%Request{event_name: name}), do: name

  def defined_event?(schema_name) do
    Enum.any?(
      KaufmannEx.Config.transcoders(),
      &apply(&1, :defined_event?, [schema_name])
    )
  end

  @spec encodable?(Request.t()) :: boolean
  def encodable?(publish_request) do
    Enum.any?(
      KaufmannEx.Config.transcoders(),
      &apply(&1, :encodable?, [publish_request])
    )
  end

  defp all_schemas do
    KaufmannEx.Config.schema_path()
    |> List.flatten()
    |> Enum.flat_map(&Path.wildcard([&1, "/**"]))
  end
end
