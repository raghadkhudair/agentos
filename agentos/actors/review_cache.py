from __future__ import annotations

import asyncio
import copy
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from typing import Any


class CriterionReviewCache:
    """Bound provider concurrency and coalesce identical revision-bound review calls."""

    def __init__(self, max_concurrency: int, max_entries: int):
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._max_entries = max_entries
        self._cache: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._inflight: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._lock = asyncio.Lock()

    async def get_or_run(
        self, key: str, operation: Callable[[], Awaitable[dict[str, Any]]]
    ) -> tuple[dict[str, Any], bool]:
        owner = False
        async with self._lock:
            cached = self._cache.get(key)
            if cached is not None:
                self._cache.move_to_end(key)
                return copy.deepcopy(cached), True
            future = self._inflight.get(key)
            if future is None:
                future = asyncio.get_running_loop().create_future()
                self._inflight[key] = future
                owner = True
        if not owner:
            return copy.deepcopy(await future), True
        try:
            async with self._semaphore:
                result = await operation()
            if result.get("run_status") == "OK":
                async with self._lock:
                    self._cache[key] = copy.deepcopy(result)
                    self._cache.move_to_end(key)
                    while len(self._cache) > self._max_entries:
                        self._cache.popitem(last=False)
            future.set_result(copy.deepcopy(result))
            return result, False
        except BaseException as error:
            future.set_exception(error)
            # Consume the exception when no coalesced waiter exists.
            future.exception()
            raise
        finally:
            async with self._lock:
                self._inflight.pop(key, None)
