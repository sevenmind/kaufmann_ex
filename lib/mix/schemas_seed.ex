defmodule Mix.Tasks.Schemas.Seed do
  @moduledoc """
  Mix task for seeding schemas in local dev/test environment

  Run with the path to the local schema directory:

  `mix schemas.seed priv/schemas`
  """
  use Mix.Task

  def run([app | _]) do
    app =
      case app do
        ":" <> rest -> String.to_atom(rest)
        _ -> app
      end

    KaufmannEx.ReleaseTasks.MigrateSchemas.migrate_schemas(app)
  end
end
