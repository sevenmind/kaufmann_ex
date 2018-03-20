defmodule Mix.Tasks.Schemas.Migrate do
  @moduledoc """
    Mix task to update schemas in the schema registry.

    Assumes env `:auth, :schema_registry_uri` is defined and points to running 
    Confluent Schema Registry instance

    NB: This module will probably need refinement for production use. Currently it only registers a schema if the previously defined one does not exist.

    run with `mix schemas.migrate`

    reset all registered schemas to the version specified `priv/schemas` (useful for testing)
    
    ```
    mix schemas.migrate --reset
    ```
  """
  use Mix.Task
  alias KaufmannEx.ReleaseTasks

  @switches [reset: :boolean]

  # Accept arguments at some point, like an schema path or name.
  def run(args) do
    {opts, _} = OptionParser.parse!(args, strict: @switches)

    case opts do
      [reset: true] -> ReleaseTasks.reset_schemas()
      _ -> ReleaseTasks.migrate_schemas()
    end
  end
end
