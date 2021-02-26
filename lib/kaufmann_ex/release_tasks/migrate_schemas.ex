defmodule KaufmannEx.ReleaseTasks.MigrateSchemas do
  @moduledoc """
  Task for registering all schemas in `priv/schemas` with the schema registry.

  Expects
   - schemas to be defined in `priv/schemas`.
   - an `event_metadata.avsc` schema should be defined and required by all events

  Can be called in a production attached console, or via a release task. Should not have any requirements beyont itself.

  This script will load all required dependencies and should not need further configuration.

  ```
  # Attempt to create or update all schemas in `priv/schemas`
  KaufmannEx.ReleaseTasks.MigrateSchemas.migrate_schemas(:app_name)

  # delete and recreate all schemas
  KaufmannEx.ReleaseTasks.MigrateSchemas.reset_schemas(:app_name)
  ```
  """

  # credo:disable-for-this-file Credo.Check.Warning.IoInspect

  alias KaufmannEx.Transcoder.SevenAvro.Schema.Registry

  defp ensure_startup do
    :ok = Application.ensure_started(:logger)
    {:ok, _} = Application.ensure_all_started(:httpoison)
    {:ok, _} = Application.ensure_all_started(:kaufmann_ex)
  end

  defp priv_dir(app) do
    app
    |> :code.priv_dir()
    |> to_string
  end

  @doc """
  Attempts to update all schemas defined in `app/priv/schemas`.

  Expects a `event_metadata.avsc` metadata scheme to be defined for all other schemas.
  """
  def migrate_schemas(app \\ :kaufmann_ex)

  def migrate_schemas(path) when is_binary(path) do
    ensure_startup()
    true = File.exists?(path)
    _ = load_metadata(path)

    path
    |> read_schemas()
    |> Enum.map(&register_schema/1)
    |> Enum.map(&IO.inspect/1)
  end

  def migrate_schemas(app) do
    ensure_startup()
    IO.puts("Migrating Schemas")

    meta_data_schema = load_metadata(app)

    app
    |> priv_dir()
    |> Path.join("schemas")
    |> scan_dir()
    |> Enum.map(&load_and_parse_schema/1)
    |> Enum.map(&inject_metadata(&1, meta_data_schema))
    |> Enum.map(&register_schema/1)
    |> Enum.map(&IO.inspect/1)
  end

  def read_schemas(path) do
    meta_data_schema = load_metadata(path)

    path
    |> scan_dir()
    |> Enum.map(&load_and_parse_schema/1)
    |> Enum.map(&inject_metadata(&1, meta_data_schema))
  end

  @doc """
  Attempts to delete and recreate all schemas defined in `app/priv/schemas`

  Expects a `event_metadata.avsc` metadata scheme to be defined for all other schemas.
  """
  def reset_schemas(app \\ :kaufmann_ex) do
    ensure_startup()
    IO.puts("Resetting Schemas")
    meta_data_schema = load_metadata(app)

    app
    |> priv_dir()
    |> Path.join("schemas")
    |> scan_dir()
    |> Enum.map(&load_and_parse_schema/1)
    |> Enum.map(&inject_metadata(&1, meta_data_schema))
    |> Enum.map(&reset_schema/1)
    |> IO.inspect()

    # |> Enum.map(&IO.inspect/1)
  end

  def load_metadata(path) when is_binary(path) do
    meta_data_schema =
      Path.wildcard([path, "/**/event_metadata.avsc"])
      |> Enum.at(0)
      |> load_and_parse_schema()

    {:ok, _, _} = register_schema(meta_data_schema)

    meta_data_schema
  end

  def load_metadata(app) do
    app
    |> priv_dir()
    |> Path.join("schemas")
    |> load_metadata()
  end

  @spec register_schema({String.t(), map}) :: {atom, String.t(), any}
  def register_schema({event_name, schema}) do
    with {:ok, status} <- update_schema({event_name, schema}) do
      {:ok, event_name, status}
    else
      {:error, error} ->
        {:error, event_name, error}
    end
  end

  defp update_schema({event_name, schema}) do
    case Registry.register(event_name, schema) do
      {:ok, _} ->
        {:ok, "Schema updated"}

      {:error, %{"error_code" => 409}} ->
        {:error, "Incompatible schema"}

      {:error, error} ->
        {:error, error}
    end
  end

  def reset_schema({event_name, schema}) do
    _ = Registry.delete(event_name)
    {:ok, _} = Registry.register(event_name, schema)
  end

  @spec load_and_parse_schema(Path.t()) :: {String.t(), map}
  defp load_and_parse_schema(schema_path) do
    {:ok, schema} =
      schema_path
      |> File.read!()
      |> Jason.decode()

    schema_name = schema_path |> Path.basename() |> String.trim(".avsc")

    {schema_name, schema}
  end

  def inject_metadata({event_name, event_schema}, {_, meta_data_schema}) do
    # Only inject metadata into event-type schemas
    if String.match?(event_name, ~r/command\.|event\.|query\./) do
      {event_name, [meta_data_schema, event_schema]}
    else
      {event_name, event_schema}
    end
  end

  def scan_dir(dir) do
    Path.wildcard([dir, "/**/*.avsc"])
  end
end
