defmodule KaufmannEx.TestSupport.Transcoder.SevenAvro do
  alias KaufmannEx.Publisher.Request
  alias KaufmannEx.Transcoder.SevenAvro

  @behaviour KaufmannEx.Transcoder

  def decode_event(event) do
    SevenAvro.decode_event(event)
  end

  def encode_event(event) do
    SevenAvro.decode_event(event)
  end

  @impl true
  def defined_event?(event_name) do
    event_name
    |> SevenAvro.find_local_schema()
  end

  @impl true
  def encodable?(%Request{event_name: event_name, payload: payload, metadata: meta}) do
    case SevenAvro.read_local_schema(event_name) do
      nil ->
        nil

      schema ->
        %{payload: payload, meta: meta} =
          case payload do
            %{payload: payload, meta: meta} -> %{payload: payload, meta: meta}
            payload -> %{payload: payload, meta: meta}
          end

        SevenAvro.encodable?(schema, %{payload: payload, meta: meta})
    end
  end
end
