defmodule KaufmannEx.TestSupport.MockSchemaRegistry do
  @moduledoc """
  A simple immitation schema registry that verifies Test Events against the schemas we have saved to file

  Looks for Schemas at `Application.get_env(:kaufmann_ex, :schema_path)`
  """

  import ExUnit.Assertions

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
  def encode_decode(%Request{format: nil} = request),
    do: encode_decode(%Request{request | format: :default})

  def encode_decode(%Request{format: format} = request) do
    transcoder = Config.transcoder(format)

    assert transcoder, """
      undefined format: #{inspect(format)}
      Format must match a transcoder specified in Application env `:kaufmann_ex, :transcoder`
    """

    %Request{encoded: encoded, event_name: event_name, metadata: meta} =
      transcoder.encode_event(request)

    %Event{} =
      transcoder.decode_event(%Event{
        raw_event: %{key: event_name, value: encoded, offset: 0},
        meta: meta
      })
  end
end
