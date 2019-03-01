defmodule KaufmannEx.Consumer.Stage.EventHandler do
  @moduledoc """
  A consumer will be a consumer supervisor that will
  Subscriber tasks for each event.
  """

  require Logger
  use GenStage
  alias KaufmannEx.StageSupervisor

  def start_link(opts \\ []) do
    GenStage.start_link(__MODULE__, opts, opts)
  end

  def init(opts) do
    {:producer_consumer, %{}, Keyword.drop(opts, [:name])}
  end

  @spec handle_events(any(), any(), any()) :: {:noreply, [any()], any()}
  def handle_events(events, _from, state) do
    publish_events =
      events
      |> Enum.map(&handle_event/1)

      # Old event handler implementation calls publish from handler rather than
      # return messages to be published. We filter out that behavior. Those
      # messages are piped back into the publish portion of the pipeline via
      # KaufmannEx.Publisher.Producer

      |> Enum.reject(fn
        :ok -> true
        {:ok, z} when is_pid(z) -> true
        nil -> true
        _ -> false
      end)

    {:noreply, publish_events, state}
  end

  def handle_event(event) do
    handler = KaufmannEx.Config.event_handler()

    event
    |> handler.given_event()
  rescue
    error ->
      Logger.warn("Error Consuming #{inspect(event)} #{inspect(error)}")
      handler = KaufmannEx.Config.event_handler()

      event
      |> error_from_event(error)
      |> handler.given_event()

      # reraise error, __STACKTRACE__
  end

  # if loop of error events, just emit whatever we got
  defp error_from_event(%KaufmannEx.Schemas.ErrorEvent{} = event, _error) do
    event
  end

  defp error_from_event(event, error) do
    %KaufmannEx.Schemas.ErrorEvent{
      name: event.name,
      error: inspect(error),
      message_payload: event.payload,
      meta: event.meta
    }
  end

  # Callbacks

  # def init({topic, partition}) do
  #   children = [
  #     %{
  #       id: KaufmannEx.Consumer.Stage.EventHandler,
  #       start: {KaufmannEx.Consumer.Stage.EventHandler, :start_link, []},
  #       restart: :temporary
  #     }
  #   ]

  #   opts = [
  #     strategy: :one_for_one,
  #     subscribe_to: [
  #       {:via, Registry,
  #        {Registry.ConsumerRegistry,
  #         StageSupervisor.stage_name(KaufmannEx.Consumer.Stage.Decoder, topic, partition)}}
  #     ]
  #   ]

  #   ConsumerSupervisor.init(children, opts)
  # end
end
