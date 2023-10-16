defmodule PersistentQueue do
  @moduledoc """
  Queue with hard-limits and possible persistence adaptation.
  It always maintains full in-memory queue when possible to speed up access to the data.
  """

  use PersistentQueue.Storage.ImplHelper

  @typedoc """
  Options for a function `new/1`. Every described option is required.
  - `:limit` (positive integer) - maximum amount of entries stored in memory.
  - `:storage` (module) - implementation of storage layer which handles overflow situations.
  """
  @type new_option ::
          {:limit, pos_integer()}
          | {:storage, module()}

  @type t(entry) :: %__MODULE__{
          ins: [entry],
          outs: [entry],
          size: non_neg_integer(),
          limit: pos_integer(),
          storage: module()
        }

  @type t :: t(term())

  alias PersistentQueue.Storage

  @enforce_keys [:limit, :storage]
  defstruct [ins: [], outs: [], size: 0] ++ @enforce_keys

  @spec new([new_option()]) :: t()
  def new(options) do
    limit = Keyword.fetch!(options, :limit)
    storage = Keyword.fetch!(options, :storage)

    size = Storage.size(storage)
    {entries, storage} = Storage.dequeue_n(storage, limit)

    %__MODULE__{
      limit: limit,
      storage: storage,
      ins: entries,
      size: size
    }
  end

  @doc """
  Returns whole size (in number of entries) of a queue.
  It even accounts the amount of entries in the storage.

  ## Example:

      iex> queue = new(limit: 3, storage: %PersistentQueue.DroppingStorage{})
      iex> queue =
      ...>   queue
      ...>   |> enqueue(:hello)
      ...>   |> enqueue(:world)
      iex> size(queue)
      2
      iex> queue =
      ...>   queue
      ...>   |> enqueue(:hello)
      ...>   |> enqueue(:another_world) # This one will be dropped
      iex> size(queue)
      3
  """
  @spec size(t()) :: non_neg_integer()
  def size(%__MODULE__{size: size}), do: size

  @doc """
  Puts the entry into the tail of the queue.

  ## Example:

      iex> queue = new(limit: 3, storage: %PersistentQueue.DroppingStorage{})
      iex> queue =
      ...>   queue
      ...>   |> enqueue(:hello)
      ...>   |> enqueue(:world)
      iex> {{:value, :hello}, queue} = dequeue(queue)
      iex> {{:value, entry}, _} = dequeue(queue)
      iex> entry
      :world
  """
  @spec enqueue(t(entry), entry) :: t(entry)
        when entry: term()

  # Queue has nothing in storage
  def enqueue(%__MODULE__{ins: ins, size: size, limit: limit} = queue, entry) when size < limit do
    %{queue | ins: [entry | ins], size: size + 1}
  end

  # Queue has something in storage
  def enqueue(%__MODULE__{size: size, storage: storage} = queue, entry) do
    case Storage.enqueue(storage, entry) do
      {:ok, storage} -> %{queue | size: size + 1, storage: storage}
      {:drop, storage} -> %{queue | storage: storage}
    end
  end

  @doc """
  Gets the entry from the head of the queue

  ## Example:

      iex> queue = new(limit: 3, storage: %PersistentQueue.DroppingStorage{})
      iex> queue =
      ...>   queue
      ...>   |> enqueue(:hello)
      ...>   |> enqueue(:world)
      iex> {{:value, :hello}, queue} = dequeue(queue)
      iex> {{:value, :world}, queue} = dequeue(queue)
      iex> {:empty, _} = dequeue(queue)
  """
  @spec dequeue(t(entry)) :: {{:value, entry} | :empty, t(entry)}
        when entry: term()

  # Queue is definitely empty
  def dequeue(%__MODULE__{size: 0} = queue) do
    {:empty, queue}
  end

  # Queue has nothing in storage
  def dequeue(%__MODULE__{size: size, limit: limit, ins: ins, outs: outs} = queue)
      when size <= limit do
    case outs do
      [] ->
        [entry | outs] = :lists.reverse(ins)
        {{:value, entry}, %{queue | size: size - 1, outs: outs, ins: []}}

      [entry | outs] ->
        {{:value, entry}, %{queue | size: size - 1, outs: outs}}
    end
  end

  # Queue has some data in storage
  def dequeue(
        %__MODULE__{ins: ins, outs: outs, storage: storage, size: size, limit: limit} = queue
      ) do
    {entry, outs, ins} =
      case outs do
        [] ->
          [entry | outs] = :lists.reverse(ins)
          {entry, outs, []}

        [entry | outs] ->
          {entry, outs, ins}
      end

    case size do
      ^limit ->
        {{:value, entry}, %{queue | ins: ins, outs: outs, size: size - 1}}

      _ ->
        {{:value, last_entry}, storage} = Storage.dequeue(storage)
        ins = [last_entry | ins]
        {{:value, entry}, %{queue | ins: ins, outs: outs, size: size - 1, storage: storage}}
    end
  end
end
