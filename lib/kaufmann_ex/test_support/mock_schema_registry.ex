defmodule KaufmannEx.TestSupport.MockSchemaRegistry do
  @moduledoc """
  A simple immitation schema registry that verifies Test Events against the schemas we have saved to file

  Looks for Schemas at `Application.get_env(:kaufmann_ex, :schema_path)`
  """

  alias KaufmannEx.Config
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

  @spec encode_decode(Request.t()) :: Event.t()
  def encode_decode(%Request{} = request) do
    %Request{encoded: encoded, event_name: event_name, metadata: meta} = brute_encode(request)

    brute_decode(%Event{raw_event: %{key: event_name, value: encoded, offset: 0}, meta: meta})
  end

  defp all_schemas do
    KaufmannEx.Config.schema_path()
    |> List.flatten()
    |> Enum.flat_map(&Path.wildcard([&1, "/**"]))
  end

  defp brute_decode(%Event{raw_event: %{key: _, value: _}} = event) do
    # when in doubt try all the transcoders
    Enum.map(
      Config.transcoders(),
      fn tr ->
        try do
          tr.decode_event(event)
        rescue
          _ -> []
        end
      end
    )
    |> Enum.find(event, fn
      %Event{} = _ -> true
      _ -> false
    end)
  end

  defp brute_encode(%Request{} = request) do
    Enum.map(Config.transcoders(), fn tr ->
      try do
        tr.encode_event(request)
      rescue
        _ -> []
      end
    end)
    |> Enum.find(request, fn
      %Request{} = _ -> true
      _ -> false
    end)
  end
end
