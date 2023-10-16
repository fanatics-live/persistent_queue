defmodule PersistentQueue.FSStorage do
  @moduledoc """
  Extremely simple filesystem based storage.
  Requires a directory with `rw` permissions to work.

  Expects filesystems with ability to make atomic write (`sync`).
  """

  @enforce_keys [:directory, :last_index, :first_index, :limit]
  defstruct [size: 0] ++ @enforce_keys

  @type t :: %__MODULE__{
          directory: Path.t(),
          last_index: non_neg_integer() | nil,
          first_index: non_neg_integer() | nil,
          limit: non_neg_integer() | nil,
          size: non_neg_integer()
        }

  @typedoc """
  - `:directory` (path) - Path to directory with `rw` permissions to use as a persistent storage
  - `:limit` (positive integer of bytes) - Maximum memory limit, which, when hit, makes the queue drop the entries.
  """
  @type new_option ::
          {:directory, Path.t()}
          | {:limit, pos_integer()}

  import :erlang, only: [term_to_binary: 2, binary_to_term: 1]

  @doc """
  Use this function to initialize FSStorage
  """
  @spec new([new_option()]) :: t()
  def new(options) do
    [directory: directory, limit: limit] = Enum.sort(options)
    File.mkdir_p!(directory)

    {first, last, size} =
      directory
      |> File.ls!()
      |> Stream.filter(&String.ends_with?(&1, ".data"))
      |> Enum.reduce({nil, nil, 0}, fn filename, {mn, mx, size} ->
        index =
          filename
          |> String.trim_trailing(".data")
          |> String.to_integer()

        size =
          directory
          |> Path.join(filename)
          |> File.stat!()
          |> Map.fetch!(:size)
          |> Kernel.+(size)

        if mn do
          {min(mn, index), max(mx, index), size}
        else
          {index, index, size}
        end
      end)

    %__MODULE__{
      first_index: first,
      last_index: last,
      directory: directory,
      limit: limit,
      size: size
    }
  end

  defimpl PersistentQueue.Storage do
    @type t :: PersistentQueue.FSStorage.t()
    use PersistentQueue.Storage.ImplHelper

    alias PersistentQueue.FSStorage

    def size(%FSStorage{first_index: nil}), do: 0

    def size(%FSStorage{first_index: first_index, last_index: last_index}),
      do: last_index - first_index + 1

    def enqueue(
          %FSStorage{
            directory: directory,
            last_index: last,
            first_index: first,
            limit: limit,
            size: size
          } = storage,
          entry
        ) do
      binary = term_to_binary(entry, [:compressed])
      size = byte_size(binary) + size

      if limit >= size do
        last = (last && last + 1) || 1
        first = first || 1

        directory
        |> Path.join("#{last}.data")
        |> File.write!(binary, [:sync])

        {:ok, %{storage | size: size, last_index: last, first_index: first}}
      else
        {:drop, storage}
      end
    end

    def dequeue(%FSStorage{first_index: nil} = storage), do: {:empty, storage}

    def dequeue(
          %FSStorage{directory: directory, first_index: first, last_index: last, size: size} =
            storage
        ) do
      filepath = Path.join(directory, "#{first}.data")
      binary = File.read!(filepath)
      File.rm!(filepath)

      entry_size = byte_size(binary)
      entry = binary_to_term(binary)

      storage =
        if first == last do
          %{storage | first_index: nil, last_index: nil, size: 0}
        else
          %{storage | first_index: first + 1, size: size - entry_size}
        end

      {{:value, entry}, storage}
    end
  end
end
