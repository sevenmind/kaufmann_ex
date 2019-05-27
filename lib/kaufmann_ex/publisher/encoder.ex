defmodule KaufmannEx.Publisher.Encoder do
  alias KaufmannEx.Config
  alias KaufmannEx.Publisher.Request

  @spec encode_event(KaufmannEx.Publisher.Request.t()) :: any()
  def encode_event(%Request{format: nil} = request),
    do: encode_event(%Request{request | format: :default})

  def encode_event(%Request{format: format} = request) do
    Config.transcoder(format).encode_event(request)
  end
end
