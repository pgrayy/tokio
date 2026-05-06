"""Python asyncio integration with Tokio.

Platform-specific strategies:
- Unix: Single-threaded. Python's asyncio drives Tokio via poll_once().
  Python monitors Tokio's pipe + kqueue/epoll fds with add_reader.
  No background threads, no polling.
- Windows: Tokio runs its own event loop on a background thread.
  When tasks complete, Tokio writes to a notification socket.
  Python monitors the socket via ProactorEventLoop's sock_recv (IOCP).
  One background thread (managed by Rust), no polling.
"""

import asyncio
import ctypes
import os
import socket
import sys
import time

# Load the shared library
if sys.platform == "win32":
    lib_ext = ".dll"
elif sys.platform == "darwin":
    lib_ext = ".dylib"
else:
    lib_ext = ".so"

lib_path = os.path.join(
    os.path.dirname(__file__), "target", "release",
    f"{'tokio_driven' if sys.platform == 'win32' else 'libtokio_driven'}{lib_ext}"
)
lib = ctypes.CDLL(lib_path)

# Define function signatures
lib.runtime_notify_fd.restype = ctypes.c_int64
lib.runtime_notify_fd.argtypes = []

lib.runtime_io_fd.restype = ctypes.c_int64
lib.runtime_io_fd.argtypes = []

lib.runtime_tick.restype = ctypes.c_int64
lib.runtime_tick.argtypes = []

lib.runtime_spawn_http_get.restype = ctypes.c_uint64
lib.runtime_spawn_http_get.argtypes = [ctypes.c_char_p]

lib.runtime_spawn_sleep.restype = ctypes.c_uint64
lib.runtime_spawn_sleep.argtypes = [ctypes.c_uint64]

lib.runtime_response_ready.restype = ctypes.c_int
lib.runtime_response_ready.argtypes = [ctypes.c_uint64]

lib.runtime_get_response.restype = ctypes.c_char_p
lib.runtime_get_response.argtypes = [ctypes.c_uint64]

if sys.platform == "win32":
    lib.runtime_set_notify_socket.restype = None
    lib.runtime_set_notify_socket.argtypes = [ctypes.c_uint64]

    lib.runtime_start_background.restype = None
    lib.runtime_start_background.argtypes = []


class TokioDriver:
    """Integrates Tokio with Python's asyncio event loop.

    Platform-aware:
    - Unix: monitors notify_fd + io_fd via add_reader, calls runtime_tick()
    - Windows: Tokio runs on a background thread, Python monitors a
      notification socket via sock_recv for task completions
    """

    def __init__(self):
        self._loop = None
        self._waiters: dict[int, asyncio.Future] = {}
        self._started = False
        self._timer_handle = None
        self._reader_task = None  # Windows: sock_recv task

        if sys.platform == "win32":
            # Create notification socket pair. Python keeps the read end.
            # Rust gets the write end and uses it to signal task completions.
            self._notify_read, notify_write = socket.socketpair(
                socket.AF_INET, socket.SOCK_STREAM
            )
            self._notify_read.setblocking(False)
            lib.runtime_set_notify_socket(notify_write.fileno())
            notify_write.detach()
            # Start Tokio's event loop on a background thread
            lib.runtime_start_background()
            self._notify_fd = None
            self._io_fd = -1
        else:
            # Unix: Python drives Tokio directly (no background thread)
            self._notify_read = None
            self._notify_fd = lib.runtime_notify_fd()
            self._io_fd = lib.runtime_io_fd()

    def start(self, loop: asyncio.AbstractEventLoop):
        """Register with the event loop."""
        self._loop = loop
        if not self._started:
            if sys.platform == "win32":
                self._reader_task = loop.create_task(self._windows_reader())
            else:
                loop.add_reader(self._notify_fd, self._on_unix_event)
                if self._io_fd >= 0:
                    loop.add_reader(self._io_fd, self._on_unix_event)
            self._started = True

    def stop(self):
        """Unregister from the event loop."""
        if self._started and self._loop:
            if sys.platform == "win32":
                if self._reader_task:
                    self._reader_task.cancel()
                    self._reader_task = None
            else:
                self._loop.remove_reader(self._notify_fd)
                if self._io_fd >= 0:
                    self._loop.remove_reader(self._io_fd)
            self._started = False
            if self._timer_handle:
                self._timer_handle.cancel()
                self._timer_handle = None

    async def _windows_reader(self):
        """Windows: wait for task completion notifications from Tokio's thread.

        Tokio's background thread writes to the notification socket when a
        task completes. sock_recv wakes us via IOCP.
        """
        try:
            while self._waiters:
                await self._loop.sock_recv(self._notify_read, 64)
                self._check_waiters()
        except asyncio.CancelledError:
            pass

    def _on_unix_event(self):
        """Unix: called by asyncio when a notification fd is readable."""
        result = lib.runtime_tick()
        self._check_waiters()

        if not self._waiters:
            return

        if result > 0:
            if self._timer_handle:
                self._timer_handle.cancel()
            self._timer_handle = self._loop.call_later(
                result / 1000.0, self._on_unix_event
            )

    def wait_for(self, request_id: int) -> asyncio.Future:
        """Return a future that resolves when the given request completes."""
        future = self._loop.create_future()
        self._waiters[request_id] = future
        return future

    def _check_waiters(self):
        """Check if any pending requests have completed."""
        done = []
        for req_id, future in self._waiters.items():
            if future.done():
                done.append(req_id)
                continue
            if lib.runtime_response_ready(req_id):
                raw = lib.runtime_get_response(req_id)
                if raw:
                    future.set_result(raw.decode("utf-8", errors="replace"))
                else:
                    future.set_exception(RuntimeError("request failed"))
                done.append(req_id)
        for req_id in done:
            del self._waiters[req_id]

        if not self._waiters:
            self.stop()


# Global driver instance
driver = TokioDriver()


async def fetch_via_tokio(url: str) -> str:
    """Make an HTTP request using Tokio, driven by Python's asyncio."""
    loop = asyncio.get_running_loop()
    driver.start(loop)

    request_id = lib.runtime_spawn_http_get(url.encode("utf-8"))
    print(f"[fetch] Spawned request {request_id} for {url}")

    return await driver.wait_for(request_id)


async def sleep_via_tokio(ms: int) -> str:
    """Sleep using Tokio's timer, driven by Python's asyncio."""
    loop = asyncio.get_running_loop()
    driver.start(loop)

    start = time.monotonic()
    request_id = lib.runtime_spawn_sleep(ms)
    print(f"[sleep] Spawned {ms}ms sleep (task {request_id})")

    result = await driver.wait_for(request_id)
    elapsed = (time.monotonic() - start) * 1000
    return f"{result} (actual: {elapsed:.0f}ms)"


async def main():
    if sys.platform == "win32":
        print("=== Windows: Tokio background thread + IOCP notification ===")
    else:
        print("=== Unix: Single-threaded, Python asyncio drives Tokio ===")
        notify_fd = lib.runtime_notify_fd()
        io_fd = lib.runtime_io_fd()
        print(f"    notify_fd={notify_fd}, io_fd={io_fd}")
    print()

    # Run both concurrently
    http_result, sleep_result = await asyncio.gather(
        fetch_via_tokio("http://httpbin.org/get"),
        sleep_via_tokio(150),
    )

    # Print results
    lines = http_result.split("\n")
    print(f"\n[result:http] Got {len(http_result)} bytes:")
    for line in lines[:5]:
        print(f"  {line}")

    print(f"\n[result:sleep] {sleep_result}")


asyncio.run(main())
