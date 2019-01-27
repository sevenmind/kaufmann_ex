defmodule KaufmannEx.Consumer.Stage.Producer do
  @moduledoc """
  `GenStage` Producer to introduce backpressure between `KafkaEx.GenConsumer` and `Flow` stage in `KaufmannEx.Subscriber`
  """

  require Logger
  use GenStage
  alias KaufmannEx.Consumer.StageSupervisor

  def start_link({topic, partition}) do
    :ok = Logger.info(fn -> "#{__MODULE__} #{topic}@#{partition} Starting" end)

    name =
      {:via, Registry,
       {Registry.ConsumerRegistry, StageSupervisor.stage_name(__MODULE__, topic, partition)}}

    GenStage.start_link(__MODULE__, {topic, partition}, name: name)
  end

  def init({topic, partition}) do
    {:producer,
     %{message_set: [], demand: 0, from: MapSet.new(), topic: topic, partition: partition}}
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
    {:noreply, to_dispatch, %{state | message_set: remaining, demand: 0}}
  end

  # When demand & messages
  def handle_demand(demand, %{message_set: message_set, from: from} = state) when demand > 0 do
    new_state = %{
      state
      | from: MapSet.new(),
        message_set: [],
        demand: demand - length(message_set)
    }

    Enum.map(state.from, &GenStage.reply(&1, :ok))

    {:noreply, message_set, new_state}
  end

  # When no demand, wait for demand
  def handle_demand(demand, %{message_set: message_set} = state) when demand == 0 do
    {:noreply, [], %{state | demand: demand}}
  end

  # When no demand, save messages to state, wait.
  def handle_call({:notify, message_set}, from, %{demand: 0} = state) do
    {:noreply, [],
     %{
       state
       | message_set: Enum.concat(state[:message_set], message_set),
         from: MapSet.put(state.from, from)
     }}
  end

  # When more messages than demand, dispatch to meet demand, wait for more demand
  def handle_call({:notify, message_set}, from, %{demand: demand} = state)
      when length(message_set) > demand do
    {to_dispatch, remaining} = Enum.split(message_set, demand)

    new_state = %{
      state
      | message_set: remaining,
        demand: demand - length(to_dispatch),
        from: MapSet.put(state.from, from)
    }

    {:noreply, to_dispatch, new_state}
  end

  # When demand greater than message count, reply for more messages
  def handle_call({:notify, message_set}, _from, %{demand: demand} = state) do
    new_state = %{
      state
      | message_set: [],
        demand: demand - length(message_set),
        from: MapSet.new()
    }

    {:reply, :ok, message_set, new_state}
  end

  def handle_subscribe(producer_or_consumer, subscription_options, from, state) do
    {:automatic, state}
  end
end
