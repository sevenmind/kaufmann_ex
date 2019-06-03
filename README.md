# KaufmannEx

[![Build
Status](https://travis-ci.org/sevenmind/kaufmann_ex.svg?branch=master)](https://travis-ci.org/sevenmind/kaufmann_ex)
[![Hex.pm](https://img.shields.io/hexpm/v/kaufmann_ex.svg)](https://hex.pm/packages/kaufmann_ex)
[![Inline
docs](http://inch-ci.org/github/sevenmind/kaufmann_ex.svg)](http://inch-ci.org/github/sevenmind/kaufmann_ex)
[![Ebert](https://ebertapp.io/github/sevenmind/kaufmann_ex.svg)](https://ebertapp.io/github/sevenmind/kaufmann_ex)
[![codebeat badge](https://codebeat.co/badges/5a95d37f-8087-4d99-8df3-758991d602ff)](https://codebeat.co/projects/github-com-sevenmind-kaufmann_ex-master)

Check out [our blog post about
KaufmannEx](https://medium.com/@7mind_dev/kaufmann-ex-317415c27978)

The goal of KaufmannEx is to provide a simple to use library for building kafka
based microservices.

It should be simple and fast to write new microservices with
[Avro](https://avro.apache.org/docs/current/) or JSON event schemas  (or
whatever).

Tieing `KafkaEx`, `AvroEx`, and `Schemex`.

KaufmannEx exists to make it easy to consume Avro encoded messages off of a
kafka broker in a parallel, controlled, manner. 

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `kaufmann_ex` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:kaufmann_ex, "~> 0.4.0-dev"}
  ]
end
```

Documentation can be generated with
[ExDoc](https://github.com/elixir-lang/ex_doc) and published on
[HexDocs](https://hexdocs.pm). Once published, the docs can be found at
[https://hexdocs.pm/kaufmann_ex](https://hexdocs.pm/kaufmann_ex).

## Usage

KaufmannEx is under _very_ active development. So it's a little more complicated
than is ideal at the moment.

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
  default_topics: ["default-topic"],
  event_handler_mod: MyApp.EventHandler,
  producer_mod: KaufmannEx.Publisher,
  schema_path: "priv/schemas",
  schema_registry_uri: "http://localhost:8081",
  transcoder: [
    default: KaufmannEx.Transcoder.SevenAvro,
    json: KaufmannEx.Transcoder.Json
  ]

config :kafka_ex,
  brokers: [
    {
      localhost,
      9092
    }
  ],
  consumer_group: "Consumer-Group",
  commit_threshold: 10,
  commit_interval: 100,
  sync_timeout: 10_000
```

### `event_handler_mod`

KaufmannEx expects an event handler module with the callback `given_event/1`

```elixir
defmodule MyApp.EventHandler do
  use KaufmannEx.EventHandler
  alias KaufmannEx.Schemas.Event

  @behaviour KaufmannEx.EventHandler

  @impl true
  def given_event(%Event{name: "test.command", payload: payload}) do
    message_body = do_some_work(payload)

    {:reply, [{"test.event", message_body, topic}]}
  end

  # In the event of an error a ErrorEvent is emitted
  def given_event(%Event{name: "this.event.returns.error", payload: payload}) do
    {:error, :unhandled_event}
  end
end
```

## Events

KaufmannEx assumes every event has a matching event Avro or JSON Event Schema.

All AVRO events are expected to include a `meta` metadata key.

If an Event causes an exception it will emit an error event with
`"event.error.#{event.name}"` as the `event_name`. Events that raise exceptions
are not retried or persisted beyond emitting this error event. If specific
handling of failing events is important to you, implement a dead-letter service
or similar.

## Internals

KaufmannEx uses `KafkaEx.ConsumerGroup` to subscribe to kafka topic/s. Events
are consumed by a `kafka_ex_gen_stage_consumer` stage to a `Flow` event handler
`KaufmannEx.Consumer.Flow`.

## Release Tasks

There are a few release tasks intended for use as [Distillery custom
commands](https://hexdocs.pm/distillery/custom-commands.html). Distillery's
custom commands don't provide the environment we're used to with mix tasks, so
extra configuration and care is needed. 

#### `migrate_schemas`

Migrate Schemas will attempt to register all schemas in the implementing
project's `priv/schemas` directory.

#### `reinit_service`

This task is intended to be used to recover idempotent services from a
catastrophic failure or substantial architectural change. 

ReInit Service will reset the configured consumer group to the earliest
available Kafka Offset. It will then consume all events from the Kafka broker
until the specified offset is reached (Or all events are consumed).

By default message publication is disabled during reinitialization. This can be
overridden in `KaufmannEx.ReleaseTasks.reinit_service/4`.

### Configuration & Use

These tasks are intended for use with Distillery in a release environment.  In
these examples the application is named `Sample`. 

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

#### `rel/config.exs`

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


## Common questions

### When are offsets commited? In case of a node going down, will it lose messages?

It is possible to lose events when a node goes down. But we try to prevent that
from happening.

1. The backpressure in Kaufmann prevents pulling more events than processing capacity. 
2. KuafmannEx uses the default [KafkaEx GenConsumer asynchronous offset commit
   behavior](https://hexdocs.pm/kafka_ex/KafkaEx.GenConsumer.html#module-asynchronous-offset-commits).
   Offsets are committed asynchronously on a timer or event count.

Ideally the `kafka_ex :commit_threshold` should be set somewhat larger than
`kaufmann_ex :max_demand` (the default is 100 and 50, respectively). This should
make it less likely that the node will be processing already-committed messages
when it goes down.

### Can order of delivery be guaranteed?

Kafka can only guarantee event ordering within a single partition. KaufmannEx
will consume events in the order published, but event processing is not
guaranteed to be sequential. KaufmannEx (as with kafka in general) does
cannot provide strong consistency.


## Telemetry

Kaufmann provides some internal
[:temeletry](https://github.com/beam-telemetry/telemetry) events

```elixir
[:kaufmann_ex, :schemas, :decode]
[:kaufmann_ex, :event_handler, :handle_event]
[:kaufmann_ex, :schema, :encode]
[:kaufmann_ex, :publisher, :publish]
```

Kaufmann provides an optional logger demonstrating consumption of these events
in `KaufmannEx.TelemetryLogger`. This logger can be started by adding
`KaufmannEx.TelemetryLogger` to your supervision tree.
