defmodule KaufmannEx.Schemas.Event do
  @type t :: %KaufmannEx.Schemas.Event{
          name: atom,
          meta: map,
          payload: term
        }
  @moduledoc false
  defstruct [:name, :meta, :payload]
end

defmodule KaufmannEx.Schemas.ErrorEvent do
  @type t :: %KaufmannEx.Schemas.ErrorEvent{
          name: atom,
          error: term,
          message_payload: term,
          meta: term | nil
        }

  @moduledoc false
  defstruct [:name, :error, :message_payload, meta: %{}]
end
