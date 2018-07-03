# KaufmannEx

[![Build Status](https://travis-ci.org/sevenmind/kaufmann_ex.svg?branch=master)](https://travis-ci.org/sevenmind/kaufmann_ex) [![Hex.pm](https://img.shields.io/hexpm/v/kaufmann_ex.svg)](https://hex.pm/packages/kaufmann_ex) [![Inline docs](http://inch-ci.org/github/sevenmind/kaufmann_ex.svg)](http://inch-ci.org/github/sevenmind/kaufmann_ex)

Check out [our blog post about KaufmannEx](https://medium.com/@7mind_dev/kaufmann-ex-317415c27978)

The goal of KaufmannEx is to provide a simple to use library for building kafka based microservices.

It should be simple and fast to write new microservices with [Avro](https://avro.apache.org/docs/current/) event schemas backed by [schema Registry](https://docs.confluent.io/current/schema-registry/docs/index.html).

Tieing `KafkaEx`, `AvroEx`, and `Schemex`.

KaufmannEx exists to make it easy to consume Avro encoded messages off of a kafka broker in a parallel, controlled, manner. 

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `kaufmann_ex` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:kaufmann_ex, "~> 0.1.2"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/kaufmann_ex](https://hexdocs.pm/kaufmann_ex).

## Usage

KaufmannEx is under _very_ active development. So it's a little more complicated than is ideal at the moment.

KaufmannEx needs:

* to be in `mix.exs`
* to be in your Supervision Tree
* `config.exs`
* `event_handler_mod`
* schemas

### Supervision Tree

To Use KaufmannEx start by adding it to your supervision tree

```elixir
defmodule Sample.Application do
  use Application
  require Logger

  def start(_type, _args) do
    children = [
      KaufmannEx.Supervisor
    ]

    opts = [strategy: :one_for_one, name: Sample.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
```

### Config.exs

KaufmannEx expects configuration in your Application Config.

Kaufman depends on KafkaEx which must also be configured

A `config.exs` may include something like this:

```elixir
config :kaufmann_ex,
  consumer_group: "Consumer-Group",
  default_topic: "default-topic",
  event_handler_mod: Service.Subscriber,
  producer_mod: KaufmannEx.Publisher,
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

KaufmannEx expects an event handler module with the callback `given_event/1`

```elixir
defmodule ExampleEventHandler do
    alias KaufmannEx.Schemas.Event
    alias KaufmannEx.Schemas.ErrorEvent

    def given_event(%Event{name: :"event_name", payload: payload} = event) do
      # ... event logic
    end
  end
```

## Events

KaufmannEx assumes every event has a matching event Avro Event Schema.

All events are expected to include a `meta` metadata key.

If an Event causes an exception it will emit an error event with `"event.error.#{event.name}"` as the `event_name`. Events that raise exceptions are not retried or persisted beyond emitting this error event. If specific handling of failing events is important to you, implement a dead-letter queue service or similar.

## Internals
KaufmannEx uses `KafkaEx.ConsumerGroup` to subscribe to kafka topic/s. Events are consumed by a `GenStage` producer stage to a `GenStage.ConsumerSupervisor` which has a limited demand (default 50) and spawns a task to process each event.

```
Kafka
  |
  V
KafkaEx.ConsumerGroup
   |   |   | (Per Partition)
   V   V   V
KaufmannEx.Stages.GenConsumer
   |
   V
KaufmannEx.Stages.Producer
    |
    V
KaufmannEx.Stages.Consumer
  |   |   | (ConsumerSupervisor spawns workers for each event received)
  V   V   V
KaufmannEx.Stages.EventHandler
  |   |   |
  V   V   V
Application.EventHandler.given_event/1
```

## Release Tasks

There are a few release tasks intended for use as [Distillery custom commands](https://hexdocs.pm/distillery/custom-commands.html). Distillery's custom commands don't provide the environment we're used to with mix tasks, so extra configuration and care is needed. 

#### `migrate_schemas`

Migrate Schemas will attempt to register all schemas in the implementing project's `priv/schemas` directory.

#### `reinit_service`

This task is intended to be used to recover idempotent services from a catastrophic failure or substantial architectural change. 

ReInit Service will reset the configured consumer group to the earliest available Kafka Offset. It will then consume all events from the Kafka broker until the specified offset is reached (Or all events are consumed).

By default message publication is disabled during reinitialization. This can be overridden in `KaufmannEx.ReleaseTasks.reinit_service/4`.

### Configuration & Use

These tasks are intended for use with Distillery in a release environment.  In these examples the application is named `Sample`. 

#### `release_tasks.ex` 

```elixir
defmodule Sample.ReleaseTasks do
  def migrate_schemas do
    Application.load(:kaufmann_ex)

    KaufmannEx.ReleaseTasks.migrate_schemas(:sample)
  end

  def reinit_service do
    Application.load(:kaufmann_ex)
    KaufmannEx.ReleaseTasks.reinit_service(:sample)  
  end
end
```
#### `rel/config.exs

```elixir
...

release :sample do
  set(
      commands: [
        migrate_schemas: "rel/commands/migrate_schemas.sh",
        reinit_service: "rel/commands/reinit_service.sh"
      ]
    )
end
```

#### `rel/commands/migrate_schemas.sh`

```
#!/bin/sh

$RELEASE_ROOT_DIR/bin/sample command Elixir.Sample.ReleaseTasks migrate_schemas
```

