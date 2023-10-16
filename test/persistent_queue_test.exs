defmodule PersistentQueueTest do
  use ExUnit.Case, async: true
  doctest PersistentQueue, import: true
  import PersistentQueue
  alias PersistentQueue.FSStorage

  describe "Dropping queue" do
    setup tags do
      limit = Map.get(tags, :limit, 100)
      storage = Map.get(tags, :storage, %PersistentQueue.DroppingStorage{})
      queue = PersistentQueue.new(limit: limit, storage: storage)

      {:ok, queue: queue}
    end

    @tag limit: 3
    test "drops new when overflows", %{queue: queue} do
      queue =
        queue
        |> enqueue(1)
        |> enqueue(2)
        |> enqueue(3)

      assert 3 == size(queue)
      assert {{:value, 1}, queue} = dequeue(queue)
      assert {{:value, 2}, queue} = dequeue(queue)
      assert {{:value, 3}, queue} = dequeue(queue)
      assert {:empty, queue} = dequeue(queue)

      queue =
        queue
        |> enqueue(1)
        |> enqueue(2)
        |> enqueue(3)
        |> enqueue(4)
        |> enqueue(5)

      assert 3 == size(queue)
      assert {{:value, 1}, queue} = dequeue(queue)
      assert {{:value, 2}, queue} = dequeue(queue)
      assert {{:value, 3}, queue} = dequeue(queue)
      assert {:empty, _queue} = dequeue(queue)
    end

    test "Puts non-unique", %{queue: queue} do
      queue =
        queue
        |> enqueue(1)
        |> enqueue(2)
        |> enqueue(1)
        |> enqueue(1)
        |> enqueue(2)

      assert 5 == size(queue)
      assert {{:value, 1}, queue} = dequeue(queue)
      assert 4 == size(queue)
      assert {{:value, 2}, queue} = dequeue(queue)
      assert {{:value, 1}, queue} = dequeue(queue)
      assert {{:value, 1}, queue} = dequeue(queue)
      assert 1 == size(queue)
      assert {{:value, 2}, queue} = dequeue(queue)
      assert {:empty, _queue} = dequeue(queue)
    end
  end

  describe "FS queue" do
    setup tags do
      limit = Map.get(tags, :limit, 100)
      disk_limit = Map.get(tags, :disk_limit, 100)
      filename = "persistent-queue-test-#{:erlang.unique_integer([:positive])}"
      directory = Path.join(System.tmp_dir!(), filename)
      storage = Map.get(tags, :storage, FSStorage.new(directory: directory, limit: disk_limit))
      queue = PersistentQueue.new(limit: limit, storage: storage)

      on_exit(fn -> File.rm_rf!(directory) end)

      {:ok, queue: queue}
    end

    @tag limit: 1
    test "Stores overflow on disk", %{queue: queue} do
      queue =
        queue
        |> enqueue(1)
        |> enqueue(2)
        |> enqueue(3)
        |> enqueue(4)

      assert 4 == size(queue)
      assert {{:value, 1}, queue} = dequeue(queue)
      assert {{:value, 2}, queue} = dequeue(queue)
      assert {{:value, 3}, queue} = dequeue(queue)
      assert {{:value, 4}, queue} = dequeue(queue)
      assert {:empty, _queue} = dequeue(queue)
    end

    # As of OTP26, each integer is stored as 3 bytes in term_to_binary compressed format
    @tag limit: 1, disk_limit: 6
    test "Overflows are not stored", %{queue: queue} do
      queue =
        queue
        |> enqueue(1)
        |> enqueue(2)
        |> enqueue(3)
        |> enqueue(4)

      assert 3 == size(queue)
      assert {{:value, 1}, queue} = dequeue(queue)
      assert {{:value, 2}, queue} = dequeue(queue)
      assert {{:value, 3}, queue} = dequeue(queue)
      assert {:empty, _queue} = dequeue(queue)
    end

    @tag limit: 1, disk_limit: 10_000_000_000_000
    test "Strange access pattern", %{queue: queue} do
      {erlqueue, queue} =
        Enum.reduce(1..10_000, {:queue.new(), queue}, fn i, {erlqueue, queue} ->
          if :rand.uniform(11) <= 5 do
            {result, erlqueue} = :queue.out(erlqueue)
            assert {^result, queue} = dequeue(queue)
            {erlqueue, queue}
          else
            erlqueue = :queue.in(i, erlqueue)
            queue = enqueue(queue, i)
            {erlqueue, queue}
          end
        end)

      Enum.reduce(1..(size(queue) + 1), {erlqueue, queue}, fn _, {erlqueue, queue} ->
        {result, erlqueue} = :queue.out(erlqueue)
        assert {^result, queue} = dequeue(queue)
        {erlqueue, queue}
      end)
    end

    @tag limit: 1, disk_limit: 10_000_000_000_000
    test "Recovery", %{queue: queue} do
      {erlqueue, queue} =
        Enum.reduce(1..10_000, {:queue.new(), queue}, fn i, {erlqueue, queue} ->
          if :rand.uniform(11) <= 5 do
            {result, erlqueue} = :queue.out(erlqueue)
            assert {^result, queue} = dequeue(queue)
            {erlqueue, queue}
          else
            erlqueue = :queue.in(i, erlqueue)
            queue = enqueue(queue, i)
            {erlqueue, queue}
          end
        end)

      # Here we drop the first element, since it is stored in memory
      # for the PersistentQueue
      {_, erlqueue} = :queue.out(erlqueue)

      storage = FSStorage.new(directory: queue.storage.directory, limit: queue.storage.limit)
      queue = new(storage: storage, limit: queue.limit)

      {erlqueue, queue} =
        Enum.reduce(1..10_000, {erlqueue, queue}, fn i, {erlqueue, queue} ->
          if :rand.uniform(11) <= 5 do
            {result, erlqueue} = :queue.out(erlqueue)
            assert {^result, queue} = dequeue(queue)
            {erlqueue, queue}
          else
            erlqueue = :queue.in(i, erlqueue)
            queue = enqueue(queue, i)
            {erlqueue, queue}
          end
        end)

      Enum.reduce(1..(size(queue) + 1), {erlqueue, queue}, fn _, {erlqueue, queue} ->
        {result, erlqueue} = :queue.out(erlqueue)
        assert {^result, queue} = dequeue(queue)
        {erlqueue, queue}
      end)
    end
  end
end
