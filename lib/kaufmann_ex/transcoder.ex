defmodule KaufmannEx.Transcoder do
  @moduledoc """
  Behaviour for a kaufmann transcoder.

  Transcoder may
  """
  alias KaufmannEx.Publisher.Request
  alias KaufmannEx.Schemas.Event

  @callback decode_event(Event.t()) :: Event.t() | {:error, any}
  @callback encode_event(Request.t()) :: Request.t()

  # Used in tests to validate test events match schema expectations
  @callback schema_extension() :: binary
  @callback encodable?(Request.t()) :: boolean
  @callback encodable?(map, map) :: boolean
  @callback read_schema(binary) :: any()

  @callback defined_event?(binary) :: boolean

  @optional_callbacks [
    encodable?: 2,
    encode_event: 1,
    read_schema: 1,
    schema_extension: 0
  ]
end
