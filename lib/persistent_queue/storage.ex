defprotocol PersistentQueue.Storage do
  @moduledoc """
  Storage layer interface for PersistentQueue.
  It must implement the queue semantics
  """

  @doc """
  Stores a single entry into the tail of the storage queue.
  Returns `:ok` if the entry is stored and returns `:drop` if the entry is dropped
  """
  @spec enqueue(t(), term()) :: {:ok | :drop, t()}
  def enqueue(storage, entry)

  @doc """
  Loads a single entry from the head of the storage queue
  """
  @spec dequeue(t()) :: {{:value, term()} | :empty, t()}
  def dequeue(storage)

  @doc """
  Returns amount of entries in the storage.

  This function is used only upon queue creation
  """
  @spec size(t()) :: non_neg_integer()
  def size(storage)

  @doc """
  Loads multiple events from the storage.

  This function is used only upon queue creation
  """
  @spec dequeue_n(t(), pos_integer()) :: {[term()], t()}
  def dequeue_n(storage, n)
end

defmodule PersistentQueue.Storage.ImplHelper do
  @moduledoc """
  Simple helper with default implementation for `dequeue_n` function
  """

  defmacro __using__(_opts) do
    quote do
      @doc """
      Pops N items from the head of the queue
      """
      @spec dequeue_n(t(), non_neg_integer()) :: {[entry :: term()], t()}
      def dequeue_n(storage, 0), do: {[], storage}

      def dequeue_n(storage, n) do
        case dequeue(storage) do
          {:empty, storage} ->
            {[], storage}

          {{:value, entry}, storage} ->
            {entries, storage} = dequeue_n(storage, n - 1)
            {[entry | entries], storage}
        end
      end

      defoverridable dequeue_n: 2
    end
  end
end
