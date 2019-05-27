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
      "KaufmannEx.TelemetryLogger",
      [
        [:kaufmann_ex, :schema, :decode],
        [:kaufmann_ex, :event_handler, :handle_event],
        [:kaufmann_ex, :schema, :encode],
        [:kaufmann_ex, :publisher, :publish]
      ],
      &KaufmannEx.TelemetryLogger.handle_event/4,
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
end
