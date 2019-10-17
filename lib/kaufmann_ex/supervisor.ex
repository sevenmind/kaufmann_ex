defmodule KaufmannEx.Supervisor do
  @moduledoc """
  Supervisor that coordinates Kafka Subscription and event consumption

  Accepts `KafkaEx.ConsumerGroup` options
  """

  require Logger
  use Supervisor

  @spec start_link(any()) :: :ignore | {:error, any()} | {:ok, pid()}
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    :ok = Logger.info(fn -> "#{name} Starting" end)
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  def init(opts \\ []) do
    consumer_group_name =
      Keyword.get(opts, :consumer_group_name, KaufmannEx.Config.consumer_group())

    topics = Keyword.get(opts, :topics, KaufmannEx.Config.subscription_topics())

    consumer_group_id = Keyword.get(opts, :id, KaufmannEx.ConsumerGroup)
    manager_name = Keyword.get(opts, :manager_name, KaufmannEx.ConsumerGroup.Manager)
    gen_server_opts = Keyword.get(opts, :gen_server_opts, [])
    extra_consumer_args = Keyword.get(opts, :extra_consumer_args, [])

    children = [
      %{
        id: consumer_group_id,
        start:
          {KafkaEx.ConsumerGroup, :start_link,
           [
             {KafkaExGenStageConsumer, KaufmannEx.Consumer.Flow},
             consumer_group_name,
             topics,
             [
               heartbeat_interval: 1_000,
               commit_interval: 10_000,
               # Probably inadvisable in many uses
               auto_offset_reset: :latest,
               fetch_options: [
                 max_bytes: 1_971_520,
                 wait_time: 100,
                 auto_commit: false
               ],
               commit_strategy: :async_commit,
               # passed through to the ConsumerGroup.Manager
               name: manager_name,
               gen_server_opts: gen_server_opts,
               extra_consumer_args: extra_consumer_args
             ]
           ]},
        type: :supervisor
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
