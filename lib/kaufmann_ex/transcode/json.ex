defmodule KaufmannEx.Transcode.Json do
  @moduledoc """
  avro encoding & serialization in use in sevenmind
  """

  alias KaufmannEx.Publisher.Request
  alias KaufmannEx.Schemas.Event

  @behaviour KaufmannEx.Transcode

  @impl true
  def decode_event(%Event{raw_event: %{key: key, value: encoded}} = event) do
    case Jason.decode(encoded) do
      {:ok, %{meta: meta, payload: payload}} ->
        %KaufmannEx.Schemas.Event{
          event
          | name: key,
            meta: meta,
            payload: payload
        }

      {:ok, payload} ->
        %KaufmannEx.Schemas.Event{
          event
          | name: key,
            payload: payload
        }

      other ->
        other
    end
  end

  @impl true
  def encode_event(%Request{format: :json, payload: payload} = request) do
    %Request{request | encoded: Jason.encode!(payload)}
  end

  @impl true
  def sniff_format("{" <> _), do: true
  def sniff_format("[" <> _), do: true
  def sniff_format(_), do: false
end
