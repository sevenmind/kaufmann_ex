defmodule KaufmannEx.Telemetry.Logger do
  @moduledoc """
  Logs telemetry events to the console. Start by adding to a supervision tree.

  Doesn't start any processed, just registerest itself with the Telemetry supervisor
  """
  require Logger

  def child_spec(arg) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [arg]}
    }
  end

  def start_link(_) do
    :telemetry.attach_many(
      "KaufmannEx.Telemetry.Logger",
      [
        [:kaufmann_ex, :schema, :decode],
        [:kaufmann_ex, :event_handler, :handle_event],
        [:kaufmann_ex, :schema, :encode],
        [:kaufmann_ex, :publisher, :publish]
      ],
      &KaufmannEx.Telemetry.Logger.handle_event/4,
      nil
    )

    :ignore
  end

  def handle_event(
        [:kaufmann_ex, :schema, :decode],
        metrics,
        %{event: event, topic: topic, partition: partition},
        _config
      ) do
    time = :erlang.convert_time_unit(metrics.duration, :native, :microsecond)

    Logger.debug(
      "[#{event}] #{metrics.size}B from #{topic}##{partition}@#{metrics.offset} decode took #{
        time
      }μs"
    )
  end

  def handle_event(
        [:kaufmann_ex, :event_handler, :handle_event],
        %{duration: duration},
        %{event: event, topic: topic, partition: partition, handler: handler},
        _config
      ) do
    time = :erlang.convert_time_unit(duration, :native, :microsecond)
    Logger.debug("[#{event}] #{handler}#handle_event/1 took #{time}μs from #{topic}##{partition}")
  end

  def handle_event(
        [:kaufmann_ex, :schema, :encode],
        %{duration: duration, size: size},
        %{event: event},
        _config
      ) do
    time = :erlang.convert_time_unit(duration, :native, :microsecond)
    Logger.debug("[#{event}] encode took #{time}μs")
  end

  def handle_event(
        [:kaufmann_ex, :publisher, :publish],
        %{duration: duration, size: size},
        %{event: event, topic: topic, partition: partition},
        _config
      ) do
    time = :erlang.convert_time_unit(duration, :native, :microsecond)
    Logger.debug("[#{event}] #{size}B publish to #{topic}##{partition} took #{time}μs")
  end

  def report_decode_time(start_time: start_time, event: event) do
    event_name =
<<<<<<< HEAD
      (event.raw_event.key || "") |> String.split("#") |> Enum.at(0) |> String.split(":") |> Enum.at(0)
=======
      event.raw_event.key |> String.split("#") |> Enum.at(0) |> String.split(":") |> Enum.at(0)
>>>>>>> e223c65d92b03b7523fe7c5728dcd126eb52487f

    :telemetry.execute(
      [:kaufmann_ex, :schema, :decode],
      %{
        duration: System.monotonic_time() - start_time,
        offset: event.raw_event.offset,
        size: byte_size(event.raw_event.value)
      },
      %{event: event_name, topic: event.topic, partition: event.partition}
    )
  end

  def report_encode_duration(
        start_time: start_time,
        encoded: encoded,
        message_name: message_name
      ) do
    event_name =
<<<<<<< HEAD
      (message_name || "") |> String.split("#") |> Enum.at(0) |> String.split(":") |> Enum.at(0)
=======
      message_name |> String.split("#") |> Enum.at(0) |> String.split(":") |> Enum.at(0)
>>>>>>> e223c65d92b03b7523fe7c5728dcd126eb52487f

    :telemetry.execute(
      [:kaufmann_ex, :schema, :encode],
      %{
        duration: System.monotonic_time() - start_time,
        size: byte_size(encoded)
      },
      %{event: event_name}
    )
  end
end
