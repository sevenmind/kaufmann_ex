defmodule KaufmannEx.Publisher.Request do
  @moduledoc """
  A Struct wrapping a publish request
  """
  defstruct [:event_name, :payload, :metadata, :context, :topic, :format, :partition, :encoded]

  @type t :: %__MODULE__{
          event_name: atom | binary,
          payload: Map,
          metadata: Map,
          context: Map,
          topic: binary | atom | map,
          partition: non_neg_integer | nil,
          format: atom,
          encoded: binary | nil
        }
end
