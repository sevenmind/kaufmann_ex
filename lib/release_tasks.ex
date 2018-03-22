defmodule KaufmannEx.ReleaseTasks do
  alias KaufmannEx.Schemas

  # @schema_path 'priv/schemas'

  defp ensure_startup do
    :ok = Application.ensure_started(:logger)
    {:ok, _} = Application.ensure_all_started(:httpoison)
    {:ok, _} = Application.ensure_all_started(:kaufmann_ex)
  end

  defp priv_dir(app) do
    "#{:code.priv_dir(app)}"
  end

  def migrate_schemas(app \\ :kaufman_ex) do
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
    |> Enum.map(&pretty_print_tuple/1)
  end

  def reset_schemas(app \\ :kaufman_ex) do
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
    |> Enum.map(&pretty_print_tuple/1)
  end

  def log_and(x) do
    IO.inspect(x)
    x
  end

  def pretty_print_tuple(tup) do
    IO.puts(inspect(tup))
  end

  def load_metadata(app) do
    meta_data_schema =
      app
      |> priv_dir()
      |> Path.join("schemas")
      |> Path.join("event_metadata.avsc")
      |> load_and_parse_schema()

    {:ok, _, _} = register_schema(meta_data_schema)

    meta_data_schema
  end

  def schema_registered({schema_name, schema}) do
    case Schemas.test(schema_name, schema) do
      {:ok, res} -> {:ok, res}
      {:error, %{"error_code" => 40_401}} -> {:ok, %{"is_compatible" => false}}
    end
  rescue
    exception -> {:error, exception}
  end

  @spec register_schema({String.t(), map}) :: {atom, String.t(), any}
  def register_schema({event_name, _} = schema) do
    with {:ok, status} <- update_schema(schema) do
      {:ok, event_name, status}
    else
      {:error, error} ->
        {:error, event_name, error}
    end
  end

  defp update_schema(schema) do
    case Schemas.register(schema) do
      {:ok, _} ->
        {:ok, "Schema updated"}

      {:error, %{"error_code" => 409}} ->
        {:error, "Incompatible schema"}

      {:error, error} ->
        {:error, error}
    end
  end

  def reset_schema({event_name, _} = schema) do
    _ = Schemas.delete(event_name)
    {:ok, _} = Schemas.register(schema)
  end

  @spec load_and_parse_schema(Path.t()) :: {String.t(), map}
  defp load_and_parse_schema(schema_path) do
    {:ok, schema} =
      schema_path
      |> File.read()
      |> ok_and()
      |> Poison.decode()

    schema_name = schema_path |> Path.basename() |> String.trim(".avsc")

    {schema_name, schema}
  end

  defp inject_metadata({event_name, event_schema}, {_, meta_data_schema}) do
    # Only inject metadata into event-type schemas
    if String.match?(event_name, ~r/command\.|event\.|query\./) do
      {event_name, [meta_data_schema, event_schema]}
    else
      {event_name, event_schema}
    end
  end

  defp scan_dir(dir) do
    files = File.ls!(dir)

    child_schemas =
      files
      |> Enum.map(&Path.join(dir, &1))
      |> Enum.filter(&File.dir?/1)
      |> Enum.map(&scan_dir/1)

    files
    |> Enum.filter(&String.match?(&1, ~r/\.avsc/))
    |> Enum.map(&Path.join(dir, &1))
    |> Enum.concat(child_schemas)
    |> List.flatten()
  end

  defp ok_and({:ok, right}) do
    right
  end
end
