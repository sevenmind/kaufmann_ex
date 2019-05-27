defmodule KaufmannEx.Transcode do
  @moduledoc """
  Behaviour for a kaufmann transcoder.

  Transcode may
  """
  alias KaufmannEx.Publisher.Request
  alias KaufmannEx.Schemas.Event

  @callback decode_event(Event.t()) :: {:ok, Event.t()} | {:error, any}
  @callback encode_event(Request.t()) :: Request.t()
  @callback sniff_format(binary) :: boolean

  # Used in tests to validate test events and effects
  # match schema expectations
  @callback schema_extension :: binary
  @callback valid_schema(map, map) :: boolean
  @callback read_schema(binary) :: any()
end
