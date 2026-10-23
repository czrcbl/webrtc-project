import asyncio


class Broadcaster:
    def __init__(self) -> None:
        self._subscribers = []
        self._closed = False

    def subscribe(
        self,
        maxsize: int = 0,
    ) -> asyncio.Queue:
        """Return a queue receiving every accepted item (a new one unless `queue` is given)."""
        queue = asyncio.Queue(maxsize=maxsize)
        self._subscribers.append(queue)
        return queue

    async def put(self, item) -> None:
        self.put_nowait(item)

    def put_nowait(self, item) -> None:
        if self._closed:
            raise asyncio.QueueShutDown
        for queue in self._subscribers:
            self._deliver(queue, item)

    def shutdown(self) -> None:
        self._closed = True
        for sub in self._subscribers:
            sub.shutdown()

    @staticmethod
    def _deliver(queue: asyncio.Queue, item) -> None:
        try:
            try:
                queue.put_nowait(item)
            except asyncio.QueueFull:
                queue.get_nowait()
                queue.put_nowait(item)
        except asyncio.QueueShutDown:
            pass  # that consumer is gone; the others still get the item
