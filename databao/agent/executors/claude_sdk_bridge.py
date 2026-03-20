import asyncio
import concurrent.futures
import queue
import threading
from collections.abc import Generator
from typing import Any

from claude_agent_sdk import ClaudeSDKClient
from claude_agent_sdk.types import Message as ClaudeMessage


class ClaudeSDKBridge:
    """Manages a ClaudeSDKClient on a background event loop with a sync generator interface.

    Works in both regular Python scripts and Jupyter notebooks because it always
    creates its own event loop on a daemon thread, never touching the caller's loop.

    Usage::

        client = ClaudeSDKClient(options=...)
        with ClaudeSDKBridge(client) as bridge:
            for message in bridge.query_sync("What tables exist?"):
                print(message)
    """

    def __init__(self, client: ClaudeSDKClient) -> None:
        self._client = client
        self._loop: asyncio.AbstractEventLoop
        self._thread: threading.Thread
        self._exit_event: asyncio.Event
        self._lifecycle_task: concurrent.futures.Future[None]

    def __enter__(self) -> "ClaudeSDKBridge":
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True, name="claude-sdk-bridge")
        self._thread.start()

        ready = threading.Event()

        async def _lifecycle() -> None:
            self._exit_event = asyncio.Event()
            async with self._client:
                ready.set()
                await self._exit_event.wait()

        self._lifecycle_task = asyncio.run_coroutine_threadsafe(_lifecycle(), self._loop)
        ready.wait()
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any) -> None:
        self._loop.call_soon_threadsafe(self._exit_event.set)
        self._lifecycle_task.result()
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join()

    def query_sync(self, prompt: str) -> Generator[ClaudeMessage, None, None]:
        """Send a query and yield response messages synchronously.

        Messages are produced asynchronously on the background loop and bridged
        to the caller via a thread-safe queue.
        """
        _sentinel = object()
        q: queue.Queue[Any] = queue.Queue()

        async def _produce() -> None:
            await self._client.query(prompt=prompt)
            async for message in self._client.receive_response():
                q.put(message)
            q.put(_sentinel)

        future = asyncio.run_coroutine_threadsafe(_produce(), self._loop)

        try:
            while (message := q.get()) is not _sentinel:
                yield message
        finally:
            future.cancel()
