defmodule KaufmannEx.Consumer.Stage.ProducerBench do
  @topic "topic"
  @partition 0

  defmodule ConsumerStage do
    use GenStage

    def start_link({topic, partition} = args) do
      GenStage.start_link(__MODULE__, args)
    end

    # Callbacks

    def init({topic, partition} = args) do
      {:consumer, %{partition: partition, topic: topic},
       subscribe_to: [
         KaufmannEx.Consumer.Stage.Producer.stage_name(
           topic,
           partition
         )
       ]}
    end

    def handle_events(events, _from, state) do
      for pid <- events do
        send(pid, :ack)
      end

      {:noreply, [], state}
    end
  end

  def run do
    Benchee.run(
      %{
        "producer" => fn _ ->
          KaufmannEx.Consumer.Stage.Producer.notify([self()], @topic, @partition)
          # apply subject to input, await callback
          receive do
            :ack -> :ok
          after
            1000 ->
              IO.warn("ack timeout")
          end,
          "decoder" => fn _ ->

          end
        end
      },
      before_scenario: fn _ ->
        Registry.start_link(keys: :unique, name: Registry.ConsumerRegistry)

        KaufmannEx.Consumer.Stage.Producer.start_link({@topic, @partition})

        KaufmannEx.Consumer.Stage.ProducerBench.ConsumerStage.start_link({@topic, @partition})
      end,
      after_scenario: fn _ ->
        KaufmannEx.Consumer.Stage.Producer.stop_self({@topic, @partition})

        Registry.unregister_match(
          Registry.ConsumerRegistry,
          KaufmannEx.Consumer.Stage.Producer.stage_name(
            @topic,
            @partition
          )
        )
      end,
      warmup: 5
    )
  end
end

KaufmannEx.Consumer.Stage.ProducerBench.run()
