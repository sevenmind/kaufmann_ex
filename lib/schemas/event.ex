defmodule Kaufmann.Schemas.Event do
  @type t :: %Kaufmann.Schemas.Event{
          name: atom,
          meta: map,
          payload: term
        }
  @moduledoc false
  defstruct [:name, :meta, :payload]
end

defmodule Kaufmann.Schemas.ErrorEvent do
  @type t :: %Kaufmann.Schemas.ErrorEvent{
          name: atom,
          error: term,
          message_payload: term
        }

  @moduledoc false
  defstruct [:name, :error, :message_payload]
end
