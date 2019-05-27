defmodule KaufmannEx.Transcode do
  @moduledoc """
  Behaviour for a kaufmann transcoder.

  Transcode may
  """
  alias KaufmannEx.Publisher.Request
  alias KaufmannEx.Schemas.Event

  @callback decode_event(Event.t()) :: Event.t() | {:error, any}
  @callback encode_event(Request.t()) :: Request.t()

  # Used in tests to validate test events match schema expectations
  @callback schema_extension :: binary
  @callback encodable?(map, map) :: boolean
  @callback read_schema(binary) :: any()
end
