defmodule KaufmannEx.TestSupport.Transcoder.SevenAvro do
  require Logger
  import Map.Helpers, only: [atomize_keys: 1]
  alias KaufmannEx.Publisher.Request
  alias KaufmannEx.Schemas.Event
  alias KaufmannEx.Transcoder.SevenAvro
  alias KaufmannEx.Transcoder.SevenAvro.Schema

  @behaviour KaufmannEx.Transcoder

  def decode_event(%Event{raw_event: %{key: key, value: encoded}} = event) do
    schema = SevenAvro.read_local_schema(key)

    with {:ok, decoded} <- Schema.decode(schema, encoded) do
      %{meta: meta, payload: payload} =
        case atomize_keys(decoded) do
          %{meta: meta, payload: payload} -> %{meta: meta, payload: payload}
          %{metadata: meta, payload: payload} -> %{meta: meta, payload: payload}
          payload -> %{meta: %{}, payload: payload}
        end

      %KaufmannEx.Schemas.Event{
        event
        | name: key,
          meta: meta,
          payload: payload
      }
    else
      {:error, error} ->
        err_event =
          event
          |> Map.from_struct()
          |> Map.merge(%{name: key, error: error})

        struct(KaufmannEx.Schemas.ErrorEvent, err_event)
    end
  end

  def encode_event(
        %Request{format: _, payload: payload, event_name: event_name, metadata: meta} = request
      ) do
    %{payload: payload, meta: meta} =
      case payload do
        %{payload: payload, meta: meta} -> %{payload: payload, meta: meta}
        payload -> %{payload: payload, meta: meta}
      end

    schema = SevenAvro.read_local_schema(event_name)

    {:ok, encoded} =
      with {:ok, encoded} <- Schema.encode(schema, %{payload: payload, meta: meta}) do
        {:ok, encoded}
      else
        {:error, error_message, to_encode, schema} ->
          Logger.warn(fn ->
            "Error Encoding #{event_name}, #{inspect(to_encode)} \n #{inspect(schema)}"
          end)

          {:error, {:schema_encoding_error, error_message}}

        {:error, error_message} ->
          Logger.warn(fn -> "Error Encoding #{event_name}, #{inspect(payload)}" end)

          {:error, {:schema_encoding_error, error_message}}
      end

    %Request{request | encoded: encoded}
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
