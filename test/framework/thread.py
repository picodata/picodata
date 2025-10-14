import inspect
import os
import threading
from typing import Any, Callable

from framework.log import log


class ThreadWhichReturnsAValue(threading.Thread):
    func: Callable
    args: tuple[Any, ...]
    result: Any

    def __init__(
        self, func: Callable, args: tuple[Any, ...] = (), kwargs: dict[str, Any] | None = None, name: str | None = None
    ):
        """Consider using the `spawn_thread` standalone function."""

        self.func = func
        self.args = args
        self.kwargs = kwargs or dict()
        self.result = None
        if not name:
            name = "<unnamed>"

        super().__init__(target=self._trampoline, name=name, daemon=False)

    def _trampoline(self):
        log.info(f"thread {self.name} started")
        try:
            self.result = self.func(*self.args, **self.kwargs)
            log.info(f"thread {self.name} finished")
        except Exception as e:
            log.info(f"thread {self.name} failed")
            self.result = e

    def start(self):
        super().start()

    def wait_finished(self, timeout: int | float = 10) -> bool:
        """Wait until the thread finishes. Returns `False` if it's not finished.
        The result is not returned, you can use `self.join` to get the result
        if the thread is finished.
        """
        super().join(timeout)
        return not self.is_alive()

    def join(self, timeout: float | None = 10) -> Any:
        """Wait until the thread finishes and return the result.
        If thread doesn't finish within `timeout` an exception is raised.
        If the thread function raised an expection it will be re-raised from here.
        """
        super().join(timeout)

        if self.is_alive():
            raise TimeoutError

        if isinstance(self.result, Exception):
            raise self.result from self.result

        return self.result


def spawn_thread(
    func: Callable, args: tuple[Any, ...] = (), kwargs: dict[str, Any] | None = None, name: str | None = None
) -> ThreadWhichReturnsAValue:
    """Spawn a thread which can return a value like in rust."""
    if not name:
        caller = inspect.stack()[1]
        file = os.path.basename(caller.filename)
        name = f"[{file}:{caller.lineno}]"

    thread = ThreadWhichReturnsAValue(func, args, kwargs=kwargs, name=name)

    thread.start()

    return thread
