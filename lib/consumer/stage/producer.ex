defmodule KaufmannEx.Consumer.Stage.Producer do
  @moduledoc """
  `GenStage` Producer to introduce backpressure between `KafkaEx.GenConsumer`
  and `Flow` stage in `KaufmannEx.Subscriber`.

  The core function of this module is to handle a `GenServer.call` from a
  `KafkaEx.GenConsumer` module. Using a blocking call provides a back pressure
  signal to constrain consumption.
  """

  require Logger
  use GenStage
  alias KaufmannEx.StageSupervisor

  def start_link(opts: opts, stage_opts: stage_opts) do
    GenStage.start_link(__MODULE__, stage_opts, opts)
  end

  def start_link(opts: opts), do: start_link(opts: opts, stage_opts: [])
  def start_link([opts, args]), do: start_link(opts: opts, stage_opts: args)
  def start_link([]), do: start_link(opts: [], stage_opts: [])
  def start_link(opts), do: start_link(opts: opts, stage_opts: [])

  def init(stage_opts \\ []) do
    {:producer, %{message_set: [], demand: 0}, stage_opts}
  end

  @spec stop_self({binary(), integer()}) :: :ok
  def stop_self({topic, partition}) do
    GenServer.stop(StageSupervisor.stage_name(__MODULE__, topic, partition))
  end

  def notify(message_set, topic, partition) do
    GenStage.call(
      StageSupervisor.stage_name(__MODULE__, topic, partition),
      {:notify, message_set}
    )
  end

  # def notify(message_set, timeout \\ 50_000) do
  #   GenStage.call(__MODULE__, {:notify, message_set}, timeout)
  # end

  # When no messages to meet demand, nothing to do
  def handle_demand(demand, %{message_set: []} = state) when demand > 0 do
    {:noreply, [], %{state | demand: demand}}
  end

  # when more messages than demand, no need to request more messages
  def handle_demand(demand, %{message_set: message_set} = state)
      when demand > 0 and length(message_set) > demand do
    {to_dispatch, remaining} = Enum.split(message_set, demand)

    {:noreply, Enum.map(to_dispatch, &send_reply/1), %{state | message_set: remaining, demand: 0}}
  end

  # When demand & messages
  def handle_demand(demand, %{message_set: message_set} = state) when demand > 0 do
    new_state = %{
      state
      | message_set: [],
        demand: demand - length(message_set)
    }

    Enum.each(message_set, &send_reply/1)

    {:noreply, Enum.map(message_set, &Enum.at(&1, 0)), new_state}
  end

  # When no demand, wait for demand
  def handle_demand(0, state) do
    {:noreply, [], %{state | demand: 0}}
  end

  # When no demand, save messages to state, wait.
  def handle_call({:notify, message_set}, from, %{demand: 0} = state) do
    message_set = Enum.map(message_set, fn m -> [m, from] end)

    {:noreply, [],
     %{
       state
       | message_set: Enum.concat(state[:message_set], message_set)
     }}
  end

  # When more messages than demand, dispatch to meet demand, wait for more demand
  def handle_call(
        {:notify, message_set},
        from,
        %{demand: demand, message_set: existing_message_set} = state
      )
      when length(message_set) + length(existing_message_set) > demand do
    message_set = Enum.map(message_set, fn m -> [m, from] end)
    message_set = Enum.concat(existing_message_set, message_set)

    {to_dispatch, remaining} = Enum.split(message_set, demand)

    new_state = %{
      state
      | message_set: remaining,
        demand: demand - length(to_dispatch)
    }

    {:noreply, Enum.map(to_dispatch, &send_reply/1), new_state}
  end

  # When demand greater than message count, reply for more messages
  def handle_call(
        {:notify, message_set},
        _from,
        %{demand: demand, message_set: existing_message_set} = state
      ) do
    existing_message_set = Enum.map(existing_message_set, &send_reply/1)
    message_set = Enum.concat(existing_message_set, message_set)

    new_state = %{
      state
      | message_set: [],
        demand: demand - length(message_set)
    }

    {:reply, :ok, message_set, new_state}
  end

  def handle_subscribe(_producer_or_consumer, _subscription_options, _from, state) do
    {:automatic, state}
  end

  def send_reply([message, from]) do
    GenStage.reply(from, :ok)

    message
  end
end
