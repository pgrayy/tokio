//! A thin C API that exposes Tokio's current_thread runtime to external event loops.
//!
//! Platform-specific integration strategy:
//! - Unix: Python drives Tokio via poll_once(). Rust creates a pipe for
//!   notifications. Python monitors the pipe + kqueue/epoll fd with add_reader.
//! - Windows: Tokio runs its own event loop on a background thread. When tasks
//!   complete, Rust writes to a notification socket (provided by the host).
//!   Python monitors the socket via ProactorEventLoop's sock_recv.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use tokio::runtime::Runtime;

// Platform-specific fd type
#[cfg(unix)]
type RawHandle = std::os::unix::io::RawFd;
#[cfg(windows)]
type RawHandle = std::os::windows::io::RawSocket;

struct State {
    runtime: Runtime,
    /// Readable end of the notification pipe (Unix only).
    #[cfg(unix)]
    notify_read: RawHandle,
    /// Writable end of the notification channel.
    /// Unix: pipe write end. Windows: socket handle provided by host.
    notify_write: Mutex<RawHandle>,
    /// Pending responses (shared between Tokio tasks and the host)
    responses: Mutex<HashMap<u64, Result<Vec<u8>, String>>>,
    /// Next request ID
    next_id: std::sync::atomic::AtomicU64,
}

static STATE: OnceLock<State> = OnceLock::new();

fn get_state() -> &'static State {
    STATE.get_or_init(|| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to create tokio runtime");

        #[cfg(unix)]
        {
            runtime.set_wake_callback(Some(on_wake));
            let (notify_read, notify_write) = create_notify_pair();
            State {
                runtime,
                notify_read,
                notify_write: Mutex::new(notify_write),
                responses: Mutex::new(HashMap::new()),
                next_id: std::sync::atomic::AtomicU64::new(1),
            }
        }

        #[cfg(windows)]
        {
            // On Windows, the runtime is created here but driven by a
            // background thread via runtime_start_background().
            State {
                runtime,
                notify_write: Mutex::new(0),
                responses: Mutex::new(HashMap::new()),
                next_id: std::sync::atomic::AtomicU64::new(1),
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Notification helpers
// ---------------------------------------------------------------------------

/// Notify the host that something happened (task wake or task completion).
fn notify_host_if_ready() {
    if let Some(state) = STATE.get() {
        let handle = *state.notify_write.lock().unwrap();
        if handle != 0 {
            do_notify(handle);
        }
    }
}

#[cfg(unix)]
fn do_notify(write_handle: RawHandle) {
    unsafe {
        libc::write(write_handle, b"!" as *const u8 as *const _, 1);
    }
}

#[cfg(windows)]
fn do_notify(write_handle: RawHandle) {
    unsafe {
        windows_sys::Win32::Networking::WinSock::send(
            write_handle as usize,
            b"!" as *const u8 as *const _,
            1,
            0,
        );
    }
}

/// Called by Tokio's waker whenever a task is woken (Unix only).
/// On Windows, Tokio runs its own loop so we don't need external notification
/// for task wakes; we only notify on task completion.
#[cfg(unix)]
fn on_wake() {
    notify_host_if_ready();
}

// ---------------------------------------------------------------------------
// Platform-specific: Unix (pipe creation + drain)
// ---------------------------------------------------------------------------

#[cfg(unix)]
fn create_notify_pair() -> (RawHandle, RawHandle) {
    let mut fds = [0 as libc::c_int; 2];
    unsafe {
        assert_eq!(libc::pipe(fds.as_mut_ptr()), 0);
        libc::fcntl(fds[0], libc::F_SETFL, libc::O_NONBLOCK);
        libc::fcntl(fds[1], libc::F_SETFL, libc::O_NONBLOCK);
    }
    (fds[0], fds[1])
}

#[cfg(unix)]
fn drain_notifications(read_handle: RawHandle) {
    let mut buf = [0u8; 64];
    unsafe {
        while libc::read(read_handle, buf.as_mut_ptr() as *mut _, buf.len()) > 0 {}
    }
}

// ---------------------------------------------------------------------------
// C API
// ---------------------------------------------------------------------------

/// Returns the notification fd (Unix only: pipe read end).
/// On Windows, returns -1.
#[no_mangle]
pub extern "C" fn runtime_notify_fd() -> i64 {
    #[cfg(unix)]
    {
        get_state().notify_read as i64
    }
    #[cfg(windows)]
    {
        -1
    }
}

/// Returns the I/O reactor's kqueue/epoll fd (Unix only).
/// On Windows, returns -1.
#[no_mangle]
pub extern "C" fn runtime_io_fd() -> i64 {
    #[cfg(unix)]
    {
        get_state().runtime.io_fd() as i64
    }
    #[cfg(windows)]
    {
        -1
    }
}

/// (Windows only) Set the notification socket write handle.
/// The host creates a socket pair and passes the write end here.
/// Rust will send(1 byte) to this handle when tasks complete.
#[no_mangle]
pub extern "C" fn runtime_set_notify_socket(handle: u64) {
    let state = get_state();
    let mut w = state.notify_write.lock().unwrap();
    *w = handle as RawHandle;
}

/// (Windows only) Start Tokio's event loop on a background thread.
/// Tasks spawned via runtime_spawn_* will be driven by this thread.
/// When a task completes, the thread writes to the notification socket.
#[no_mangle]
pub extern "C" fn runtime_start_background() {
    #[cfg(windows)]
    {
        // Ensure state is initialized (creates the runtime)
        get_state();

        std::thread::spawn(|| {
            let state = get_state();
            // block_on drives the event loop (IOCP polling, timers, etc.)
            // We give it a future that parks forever; actual work happens
            // via spawned tasks.
            state.runtime.block_on(std::future::pending::<()>());
        });
    }
}

/// Run one non-blocking iteration of the Tokio event loop (Unix only).
/// On Windows this is a no-op (the background thread drives Tokio).
#[no_mangle]
pub extern "C" fn runtime_tick() -> i64 {
    #[cfg(unix)]
    {
        let state = get_state();
        drain_notifications(state.notify_read);
        state.runtime.poll_once()
    }
    #[cfg(windows)]
    {
        // No-op: background thread handles everything
        -1
    }
}

/// Spawn an HTTP GET request. Returns a request ID.
#[no_mangle]
pub extern "C" fn runtime_spawn_http_get(url_ptr: *const libc::c_char) -> u64 {
    use std::sync::atomic::Ordering;

    let state = get_state();
    let url = unsafe { std::ffi::CStr::from_ptr(url_ptr) }
        .to_str()
        .expect("invalid utf-8 url")
        .to_string();

    let id = state.next_id.fetch_add(1, Ordering::Relaxed);
    let _guard = state.runtime.enter();

    tokio::spawn(async move {
        let result = async {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            use tokio::net::TcpStream;

            let without_scheme = url.strip_prefix("http://").unwrap_or(&url);
            let (host, path) = match without_scheme.find('/') {
                Some(i) => (&without_scheme[..i], &without_scheme[i..]),
                None => (without_scheme, "/"),
            };
            let addr = format!("{}:80", host);

            let mut stream = TcpStream::connect(&addr).await?;
            let request = format!(
                "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
                path, host
            );
            stream.write_all(request.as_bytes()).await?;

            let mut response = Vec::new();
            stream.read_to_end(&mut response).await?;
            Ok::<Vec<u8>, std::io::Error>(response)
        }
        .await;

        let state = get_state();
        let mut responses = state.responses.lock().unwrap();
        match result {
            Ok(data) => responses.insert(id, Ok(data)),
            Err(e) => responses.insert(id, Err(e.to_string())),
        };
        drop(responses);

        // Notify host that a response is ready
        notify_host_if_ready();
    });

    // On Unix, notify host that a task was spawned (so it ticks)
    #[cfg(unix)]
    notify_host_if_ready();

    id
}

/// Spawn a sleep task. Returns a request ID.
#[no_mangle]
pub extern "C" fn runtime_spawn_sleep(ms: u64) -> u64 {
    use std::sync::atomic::Ordering;

    let state = get_state();
    let id = state.next_id.fetch_add(1, Ordering::Relaxed);
    let _guard = state.runtime.enter();

    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;

        let state = get_state();
        let mut responses = state.responses.lock().unwrap();
        responses.insert(id, Ok(format!("slept {}ms", ms).into_bytes()));
        drop(responses);

        // Notify host that a response is ready
        notify_host_if_ready();
    });

    // On Unix, notify host that a task was spawned (so it ticks)
    #[cfg(unix)]
    notify_host_if_ready();

    id
}

/// Check if a response is ready.
#[no_mangle]
pub extern "C" fn runtime_response_ready(id: u64) -> libc::c_int {
    let state = get_state();
    let map = state.responses.lock().unwrap();
    if map.contains_key(&id) { 1 } else { 0 }
}

/// Get the response body. Returns a null-terminated string pointer.
/// Caller must free with `runtime_free_string`.
#[no_mangle]
pub extern "C" fn runtime_get_response(id: u64) -> *mut libc::c_char {
    let state = get_state();
    let mut map = state.responses.lock().unwrap();
    match map.remove(&id) {
        Some(Ok(data)) => {
            let s = String::from_utf8_lossy(&data).to_string();
            let c_str = std::ffi::CString::new(s).unwrap_or_default();
            c_str.into_raw()
        }
        Some(Err(_)) | None => std::ptr::null_mut(),
    }
}

/// Free a string returned by `runtime_get_response`.
#[no_mangle]
pub extern "C" fn runtime_free_string(ptr: *mut libc::c_char) {
    if !ptr.is_null() {
        unsafe {
            let _ = std::ffi::CString::from_raw(ptr);
        }
    }
}
