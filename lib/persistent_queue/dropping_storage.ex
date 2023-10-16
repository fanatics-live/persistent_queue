defmodule PersistentQueue.DroppingStorage do
  @moduledoc """
  Storage which stores nothing and just drops
  existing events
  """

  defstruct []

  defimpl PersistentQueue.Storage do
    def dequeue(s), do: {:empty, s}
    def dequeue_n(s, _), do: {[], s}
    def size(_), do: 0
    def enqueue(s, _), do: {:drop, s}
  end
end
