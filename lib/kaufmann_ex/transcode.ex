defmodule KaufmannEx.Transcode do
  alias KaufmannEx.Publisher.Request
  alias KaufmannEx.Schemas.Event

  @callback decode_event(Event.t()) :: {:ok, Event.t()} | {:error, any}
  @callback encode_event(Request.t()) :: Request.t()
  @callback sniff_format(binary) :: [atom]
end
