defmodule Kaufmann.Schemas.Event do
  @moduledoc false
  defstruct [:name, :meta, :payload]
end

defmodule Kaufmann.Schemas.ErrorEvent do
  @moduledoc false
  defstruct [:name, :error, :message_payload]
end
