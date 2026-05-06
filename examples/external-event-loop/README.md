# External Event Loop Integration

Proof of concept: integrating Tokio with Python's asyncio event loop.

## What this demonstrates

- Tokio as a library: no standalone Tokio event loop on Unix
- Python's asyncio is the primary event loop
- Tokio handles HTTP (TCP, protocol parsing) and timers
- Cross-platform with platform-appropriate strategies

## Platform strategies

### Unix (macOS, Linux)

Single-threaded. Python drives Tokio directly via `poll_once()`.

- Python monitors two fds with `add_reader()`:
  - Notification pipe (readable when tasks are woken)
  - kqueue/epoll fd (readable when I/O events are pending)
- When either fd fires, Python calls `runtime_tick()` which runs one
  non-blocking iteration of Tokio's scheduler and I/O driver
- No background threads, no polling

This works because kqueue/epoll fds are composable: Python's event loop
can monitor Tokio's I/O reactor fd directly. The kernel propagates
readiness signals through nested kqueue/epoll instances automatically.

### Windows

One background thread runs Tokio's event loop. Python monitors a
notification socket via ProactorEventLoop's `sock_recv()`.

- Python creates a socket pair and passes the write end to Rust
- Tokio runs `block_on()` on a background thread, driving IOCP polling,
  timers, and task execution
- When a task completes, it writes to the notification socket
- Python's `sock_recv` completes via IOCP, resolving the asyncio future

A background thread is required because Windows IOCP is completion-based:
completions only arrive when `GetQueuedCompletionStatus` is called on the
specific IOCP handle. Unlike Unix (where kqueue/epoll fds are nestable),
there is no way to monitor a foreign IOCP from Python's event loop without
actively dequeuing from it. The background thread provides this active
dequeuing as part of Tokio's normal event loop.

## How it works

### Rust (`src/lib.rs`)

Exposes a C API:

- `runtime_notify_fd()` — pipe read fd (Unix only)
- `runtime_io_fd()` — kqueue/epoll fd (Unix only)
- `runtime_tick()` — one non-blocking iteration (Unix only)
- `runtime_set_notify_socket(handle)` — set notification write handle (Windows)
- `runtime_start_background()` — start Tokio's event loop thread (Windows)
- `runtime_spawn_http_get(url)` — spawn an HTTP request, returns request ID
- `runtime_spawn_sleep(ms)` — spawn a timer task, returns request ID
- `runtime_response_ready(id)` — check if a response is available
- `runtime_get_response(id)` — retrieve the response

### Python (`host.py`)

Integrates with asyncio:

- Unix: `add_reader()` on both fds, calls `runtime_tick()` on event
- Windows: `sock_recv()` on notification socket, checks results on wake
- Both: `asyncio.Future` per request, resolved when response is ready

## Building and running

```bash
cargo build --release
python host.py
```

## Tokio changes required

This example depends on methods added to Tokio's `Runtime` (see the
`external-event-loop` branch):

- `Runtime::io_fd()` — exposes the mio registry's raw fd (Unix)
- `Runtime::poll_once()` — runs one non-blocking tick, returns timer deadline
- `Runtime::set_wake_callback()` — notifies host when tasks are woken
