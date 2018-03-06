# Kaufmann

Kaufmann is a library tieing together `KafkaEx`, `AvroEx`, and `Schemex`

Kaufmann exists to make it easy to decode Avro encoded messages off of a kafka broker.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `kaufmann` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:kaufmann, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/kaufmann](https://hexdocs.pm/kaufmann).

## Usage

Kaufmann is under _very_ active development. So it's a little more complicated than is ideal at the moment.

Kaufmann needs:

* to be in `mix.exs`
* to be in your Supervision Tree
* `config.exs`
* `event_handler_mod`
* schemas

### Supervision Tree

To Use Kaufmann start by adding it to your supervision tree

```elixir
defmodule Sample.Application do
  use Application
  require Logger

  def start(_type, _args) do
    children = [
      Kaufmann.Supervisor
    ]

    opts = [strategy: :one_for_one, name: Sample.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
```

### Config.exs

Kaufmann expects configuration in your Application Config.

Kaufman depends on KafkaEx which must also be configured

A `config.exs` may include something like this:

```elixir
config :kaufmann,
  consumer_group: "Consumer-Group",
  default_topic: "default-topic",
  event_handler_mod: Service.Subscriber,
  producer_mod: Kaufmann.Publisher,
  schema_path: "priv/schemas",
  schema_registry_uri: "http://localhost:8081"

config :kafka_ex,
  brokers: [
    {
      localhost,
      9092
    }
  ],
  consumer_group: "Consumer-Group"
```

### `event_handler_mod`

Kaufmann expects an event handler module with the callback `given_event/1`

```elixir
defmodule ExampleEventHandler do
    alias Kaufmann.Schemas.Event
    alias Kaufmann.Schemas.ErrorEvent

    def given_event(%Event{name: :"event_name", payload: payload} = event) do
      # ... event logic
    end
  end
```

## Events

Kaufmann assumes every event has a matching event Avro Event Schema.

All events are expected to include a `meta` metadata key.

If an Event causes an exception it will emit an error event with `"event.error.#{event.name}"` as the `event_name`. Events that raise exceptions are not retried or put in a deadletter queue. If you want to do that, write a deadletter queue service.

## Internals

Kaufmann uses `KafkaEx.ConsumerGroup` to subscribe to kafka topic/s. Events are consumed by a `GenStage` producer stage which in turn is used as the entrypoint for a `GenStage`/`Flow` flow.

```
Kafka
  |
  V
KafkaEx.ConsumerGroup
   |   |   | (Per Partition)
   V   V   V
Kaufmann.GenConsumer
  |   |   |
  V   V   V
Kaufmann.Stages.Producer
    |  (Flow.from_stage/1)
    V
Subscriber.handle_messages/0
  |   |   | (parallelized by Flow)
  V   V   V
Application.EventHandler.given_event/1
```
