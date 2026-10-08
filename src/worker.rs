use crate::config::{
    CALLBACK_FRAME_BUDGET, CALLBACK_QUEUE_CAPACITY, DEFAULT_CONNECT_TIMEOUT,
    DEFAULT_REQUEST_TIMEOUT, MAX_BUFFERED_BODY_BYTES, MAX_CALLBACKS_PER_FRAME,
    MAX_CONCURRENT_REQUESTS, MAX_IN_FLIGHT_REQUESTS, MAX_REDIRECTS, MAX_RESPONSE_HEADERS,
    MAX_RESPONSE_HEADER_BYTES, POOL_IDLE_TIMEOUT, POOL_MAX_IDLE_PER_HOST, SHUTDOWN_GRACE,
    TCP_KEEPALIVE, USER_AGENT,
};
use bytes::Bytes;
use gmod::lua::{LuaReference, State};
use gmod::lua_string;
use reqwest::header::HeaderMap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

pub enum CallbackTask {
    Success(LuaReference, u16, Bytes, HeaderMap, BodyBudget),
    Failed(LuaReference, String),
    DropRef(LuaReference),
}

pub struct BodyBudget {
    permits: Vec<OwnedSemaphorePermit>,
    reserved: usize,
}

impl BodyBudget {
    pub fn new() -> Self {
        Self {
            permits: Vec::new(),
            reserved: 0,
        }
    }

    pub fn reserve(&mut self, permit: OwnedSemaphorePermit, bytes: usize) {
        self.permits.push(permit);
        self.reserved += bytes;
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn reserved(&self) -> usize {
        self.reserved
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn is_empty(&self) -> bool {
        self.permits.is_empty()
    }
}

impl Default for BodyBudget {
    fn default() -> Self {
        Self::new()
    }
}

pub struct RequestCallbacks {
    pub success: Option<LuaReference>,
    pub failed: Option<LuaReference>,
}

pub struct HttpStats {
    submitted: AtomicU64,
    in_flight: AtomicU64,
    succeeded: AtomicU64,
    failed: AtomicU64,
    retried: AtomicU64,
    cancelled: AtomicU64,
}

#[derive(Default)]
pub struct StatsSnapshot {
    pub submitted: u64,
    pub in_flight: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub retried: u64,
    pub cancelled: u64,
}

impl HttpStats {
    fn new() -> Self {
        Self {
            submitted: AtomicU64::new(0),
            in_flight: AtomicU64::new(0),
            succeeded: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            retried: AtomicU64::new(0),
            cancelled: AtomicU64::new(0),
        }
    }

    pub fn try_request_started(self: &Arc<Self>) -> Option<InFlightRequest> {
        let mut in_flight = self.in_flight.load(Ordering::Relaxed);
        loop {
            if in_flight >= MAX_IN_FLIGHT_REQUESTS {
                return None;
            }
            match self.in_flight.compare_exchange_weak(
                in_flight,
                in_flight + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(current) => in_flight = current,
            }
        }
        self.submitted.fetch_add(1, Ordering::Relaxed);
        Some(InFlightRequest {
            stats: self.clone(),
        })
    }

    pub fn succeeded(&self) {
        self.succeeded.fetch_add(1, Ordering::Relaxed);
    }

    pub fn failed(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn retried(&self) {
        self.retried.fetch_add(1, Ordering::Relaxed);
    }

    pub fn cancelled(&self) {
        self.cancelled.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            submitted: self.submitted.load(Ordering::Relaxed),
            in_flight: self.in_flight.load(Ordering::Relaxed),
            succeeded: self.succeeded.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            retried: self.retried.load(Ordering::Relaxed),
            cancelled: self.cancelled.load(Ordering::Relaxed),
        }
    }
}

pub struct InFlightRequest {
    stats: Arc<HttpStats>,
}

impl Drop for InFlightRequest {
    fn drop(&mut self) {
        self.stats.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

pub struct RequestRegistry {
    next_id: AtomicU64,
    requests: Mutex<HashMap<u64, RequestEntry>>,
}

struct RequestEntry {
    token: CancellationToken,
    callbacks: Option<RequestCallbacks>,
    detached: bool,
}

impl RequestRegistry {
    fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            requests: Mutex::new(HashMap::new()),
        }
    }

    pub fn register(
        self: &Arc<Self>,
        success: Option<LuaReference>,
        failed: Option<LuaReference>,
    ) -> RegisteredRequest {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let token = CancellationToken::new();

        let mut requests = lock(&self.requests);
        requests.insert(
            id,
            RequestEntry {
                token: token.clone(),
                callbacks: Some(RequestCallbacks { success, failed }),
                detached: false,
            },
        );
        drop(requests);

        RegisteredRequest {
            id,
            token,
            registry: self.clone(),
        }
    }

    pub fn finish(&self, id: u64) {
        lock(&self.requests).remove(&id);
    }

    pub fn cancel(&self, id: u64) -> bool {
        let Some(token) = ({
            let requests = lock(&self.requests);
            requests.get(&id).and_then(|entry| {
                if entry.detached {
                    None
                } else {
                    Some(entry.token.clone())
                }
            })
        }) else {
            return false;
        };

        token.cancel();
        true
    }

    pub fn take_callbacks(&self, id: u64) -> Option<RequestCallbacks> {
        lock(&self.requests).get_mut(&id)?.callbacks.take()
    }

    fn cancel_all(&self) -> Vec<RequestCallbacks> {
        let entries: Vec<RequestEntry> = {
            let mut requests = lock(&self.requests);
            requests.drain().map(|(_, entry)| entry).collect()
        };

        let mut callbacks = Vec::with_capacity(entries.len());
        for mut entry in entries {
            entry.token.cancel();
            if let Some(callbacks_for_request) = entry.callbacks.take() {
                callbacks.push(callbacks_for_request);
            }
        }
        callbacks
    }
}

pub struct RegisteredRequest {
    pub id: u64,
    pub token: CancellationToken,
    registry: Arc<RequestRegistry>,
}

impl Drop for RegisteredRequest {
    fn drop(&mut self) {
        self.registry.finish(self.id);
    }
}

impl RegisteredRequest {
    pub fn take_callbacks(&self) -> Option<RequestCallbacks> {
        self.registry.take_callbacks(self.id)
    }

    pub fn release(&self) -> Option<RequestCallbacks> {
        let mut requests = lock(&self.registry.requests);
        let entry = requests.get_mut(&self.id)?;
        entry.detached = true;
        entry.callbacks.take()
    }
}

struct WorkerState {
    callback_tx: Option<tokio::sync::mpsc::Sender<CallbackTask>>,
    callback_rx: Option<tokio::sync::mpsc::Receiver<CallbackTask>>,
    orphan_refs: Option<Arc<Mutex<Vec<LuaReference>>>>,
    runtime: Option<tokio::runtime::Runtime>,
    client: Option<reqwest::Client>,
    concurrency_limit: Option<Arc<Semaphore>>,
    body_budget: Option<Arc<Semaphore>>,
    stats: Option<Arc<HttpStats>>,
    requests: Option<Arc<RequestRegistry>>,
}

impl WorkerState {
    const fn new() -> Self {
        Self {
            callback_tx: None,
            callback_rx: None,
            orphan_refs: None,
            runtime: None,
            client: None,
            concurrency_limit: None,
            body_budget: None,
            stats: None,
            requests: None,
        }
    }
}

const THINK_HOOK_NAME: &str = "rhttpThink";
const LEGACY_THINK_HOOK_NAME: &str = "FetchRsW";

static WORKER: OnceLock<Mutex<WorkerState>> = OnceLock::new();

pub struct WorkerResources {
    pub callback_tx: tokio::sync::mpsc::Sender<CallbackTask>,
    pub orphan_refs: Arc<Mutex<Vec<LuaReference>>>,
    pub client: reqwest::Client,
    pub concurrency_limit: Arc<Semaphore>,
    pub body_budget: Arc<Semaphore>,
    pub stats: Arc<HttpStats>,
    pub requests: Arc<RequestRegistry>,
    handle: tokio::runtime::Handle,
}

impl WorkerResources {
    pub fn handle(&self) -> tokio::runtime::Handle {
        self.handle.clone()
    }
}

fn extra_root_certs() -> Vec<reqwest::Certificate> {
    let path = match std::env::var("RHTTP_CAINFO") {
        Ok(path) if !path.trim().is_empty() => path,
        _ => return Vec::new(),
    };
    let bundle = match std::fs::read(&path) {
        Ok(bundle) => bundle,
        Err(e) => {
            eprintln!("[rhttp] RHTTP_CAINFO: cannot read {path}: {e}");
            return Vec::new();
        }
    };
    match reqwest::Certificate::from_pem_bundle(&bundle) {
        Ok(certs) => certs,
        Err(e) => {
            eprintln!("[rhttp] RHTTP_CAINFO: cannot parse {path}: {e}");
            Vec::new()
        }
    }
}

pub fn init(lua: State) {
    let worker = WORKER.get_or_init(|| Mutex::new(WorkerState::new()));
    let mut state = lock(worker);

    if state.runtime.is_none() {
        let (callback_tx, callback_rx) = tokio::sync::mpsc::channel(CALLBACK_QUEUE_CAPACITY);
        let redirect_policy = reqwest::redirect::Policy::custom(|attempt| {
            if attempt.previous().len() > MAX_REDIRECTS {
                attempt.error("too many redirects")
            } else if !matches!(attempt.url().scheme(), "http" | "https") {
                attempt.error("redirect to unsupported scheme")
            } else {
                attempt.follow()
            }
        });
        let mut builder = reqwest::Client::builder()
            .user_agent(USER_AGENT)
            .connect_timeout(DEFAULT_CONNECT_TIMEOUT)
            .timeout(DEFAULT_REQUEST_TIMEOUT)
            .redirect(redirect_policy)
            .tcp_keepalive(TCP_KEEPALIVE)
            .pool_max_idle_per_host(POOL_MAX_IDLE_PER_HOST)
            .pool_idle_timeout(POOL_IDLE_TIMEOUT)
            .no_proxy();
        for cert in extra_root_certs() {
            builder = builder.add_root_certificate(cert);
        }
        let client = builder.build().expect("Failed to build reqwest client");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to build Tokio runtime");

        state.callback_tx = Some(callback_tx);
        state.callback_rx = Some(callback_rx);
        state.orphan_refs = Some(Arc::new(Mutex::new(Vec::new())));
        state.client = Some(client);
        state.concurrency_limit = Some(Arc::new(Semaphore::new(MAX_CONCURRENT_REQUESTS)));
        state.body_budget = Some(Arc::new(Semaphore::new(MAX_BUFFERED_BODY_BYTES as usize)));
        state.stats = Some(Arc::new(HttpStats::new()));
        state.requests = Some(Arc::new(RequestRegistry::new()));
        state.runtime = Some(runtime);
    }
    drop(state);

    unsafe {
        lua.get_global(lua_string!("hook"));
        lua.get_field(-1, lua_string!("Add"));
        lua.push_string("Think");
        lua.push_string(THINK_HOOK_NAME);
        lua.push_function(think);
        lua.call(3, 0);
        lua.pop();
        remove_think_hook(lua, LEGACY_THINK_HOOK_NAME);
        lua.get_global(lua_string!("timer"));
        lua.get_field(-1, lua_string!("Create"));
        lua.push_string(THINK_HOOK_NAME);
        lua.push_number(0.0);
        lua.push_integer(0);
        lua.push_function(think);
        lua.call(4, 0);
        lua.pop();
    }
}

unsafe fn remove_think_hook(lua: State, name: &str) {
    lua.get_global(lua_string!("hook"));
    lua.get_field(-1, lua_string!("Remove"));
    lua.push_string("Think");
    lua.push_string(name);
    lua.call(2, 0);
    lua.pop();
}

unsafe fn remove_think_timer(lua: State, name: &str) {
    lua.get_global(lua_string!("timer"));
    lua.get_field(-1, lua_string!("Remove"));
    lua.push_string(name);
    lua.call(1, 0);
    lua.pop();
}

pub fn resources() -> Option<WorkerResources> {
    let worker = WORKER.get()?;
    let state = lock(worker);

    Some(WorkerResources {
        callback_tx: state.callback_tx.clone()?,
        orphan_refs: state.orphan_refs.clone()?,
        client: state.client.clone()?,
        concurrency_limit: state.concurrency_limit.clone()?,
        body_budget: state.body_budget.clone()?,
        stats: state.stats.clone()?,
        requests: state.requests.clone()?,
        handle: state.runtime.as_ref()?.handle().clone(),
    })
}

pub fn stats() -> StatsSnapshot {
    WORKER
        .get()
        .and_then(|worker| lock(worker).stats.as_ref().map(|stats| stats.snapshot()))
        .unwrap_or_default()
}

pub fn cancel_request(id: u64) -> bool {
    WORKER
        .get()
        .and_then(|worker| lock(worker).requests.clone())
        .is_some_and(|requests| requests.cancel(id))
}

pub fn park_orphan_ref(orphan_refs: &Mutex<Vec<LuaReference>>, reference: LuaReference) {
    lock(orphan_refs).push(reference);
}

pub fn spawn_task<F: std::future::Future<Output = ()> + Send + 'static>(
    handle: tokio::runtime::Handle,
    future: F,
) {
    handle.spawn(future);
}

unsafe extern "C-unwind" fn think(lua: State) -> i32 {
    let mut budget = CALLBACK_FRAME_BUDGET;
    let mut processed = 0usize;

    loop {
        let task = WORKER.get().and_then(|worker| {
            let mut state = lock(worker);
            state.callback_rx.as_mut()?.try_recv().ok()
        });

        let Some(task) = task else {
            break;
        };

        processed += 1;

        match task {
            CallbackTask::Success(cb, status, body, headers, body_budget) => {
                lua.from_reference(cb);
                lua.push_integer(status as _);
                lua.push_binary_string(&body);
                let mut header_bytes = 0usize;
                lua.create_table(0, headers.len().min(MAX_RESPONSE_HEADERS) as _);
                for (name, value) in headers.iter().take(MAX_RESPONSE_HEADERS) {
                    let entry_len = name.as_str().len() + value.as_bytes().len();
                    if header_bytes + entry_len > MAX_RESPONSE_HEADER_BYTES {
                        break;
                    }
                    header_bytes += entry_len;
                    lua.push_string(name.as_str());
                    lua.push_binary_string(value.as_bytes());
                    lua.set_table(-3);
                }
                lua.pcall_ignore(3, 0);
                lua.dereference(cb);
                drop(body_budget);
                budget = budget.saturating_sub(body.len() + header_bytes);
            }
            CallbackTask::Failed(cb, err) => {
                lua.from_reference(cb);
                lua.push_string(&err);
                lua.pcall_ignore(1, 0);
                lua.dereference(cb);
                budget = budget.saturating_sub(err.len());
            }
            CallbackTask::DropRef(cb) => {
                lua.dereference(cb);
                budget = budget.saturating_sub(64);
            }
        }

        if processed >= MAX_CALLBACKS_PER_FRAME || budget == 0 {
            break;
        }
    }

    let orphans: Vec<LuaReference> = WORKER
        .get()
        .map(|worker| {
            lock(worker)
                .orphan_refs
                .clone()
                .map(|refs| std::mem::take(&mut *lock(&refs)))
                .unwrap_or_default()
        })
        .unwrap_or_default();
    for reference in orphans {
        lua.dereference(reference);
    }

    0
}

pub fn shutdown(lua: State) {
    unsafe {
        remove_think_hook(lua, THINK_HOOK_NAME);
        remove_think_hook(lua, LEGACY_THINK_HOOK_NAME);
        remove_think_timer(lua, THINK_HOOK_NAME);
    }

    let Some((runtime, mut callback_rx, callbacks, orphan_refs)) = WORKER.get().map(|worker| {
        let mut state = lock(worker);
        let callbacks = state
            .requests
            .take()
            .map(|requests| requests.cancel_all())
            .unwrap_or_default();
        state.callback_tx = None;
        let callback_rx = state.callback_rx.take();
        let orphan_refs = state.orphan_refs.take();
        state.client = None;
        state.concurrency_limit = None;
        state.body_budget = None;
        state.stats = None;
        (state.runtime.take(), callback_rx, callbacks, orphan_refs)
    }) else {
        return;
    };

    if let Some(rx) = callback_rx.as_mut() {
        rx.close();
    }

    if let Some(rt) = runtime {
        rt.shutdown_timeout(SHUTDOWN_GRACE);
    }

    if let Some(mut rx) = callback_rx {
        while let Ok(task) = rx.try_recv() {
            unsafe { discard_callback_task(lua, task) };
        }
    }
    for callbacks_for_request in callbacks {
        unsafe { discard_callbacks(lua, callbacks_for_request) };
    }
    if let Some(refs) = orphan_refs {
        for reference in std::mem::take(&mut *lock(&refs)) {
            unsafe { lua.dereference(reference) };
        }
    }
}

unsafe fn discard_callback_task(lua: State, task: CallbackTask) {
    match task {
        CallbackTask::Success(cb, _, _, _, budget) => {
            lua.dereference(cb);
            drop(budget);
        }
        CallbackTask::Failed(cb, _) | CallbackTask::DropRef(cb) => lua.dereference(cb),
    }
}

unsafe fn discard_callbacks(lua: State, callbacks: RequestCallbacks) {
    if let Some(cb) = callbacks.success {
        lua.dereference(cb);
    }
    if let Some(cb) = callbacks.failed {
        lua.dereference(cb);
    }
}

#[cfg(test)]
mod tests {
    use super::{extra_root_certs, BodyBudget, HttpStats, RequestRegistry};
    use std::sync::Arc;
    use tokio::sync::Semaphore;

    #[test]
    fn extra_root_certs_skips_bad_input() {
        std::env::remove_var("RHTTP_CAINFO");
        assert!(extra_root_certs().is_empty());

        std::env::set_var("RHTTP_CAINFO", "   ");
        assert!(extra_root_certs().is_empty());

        let missing = std::env::temp_dir().join("rhttp-test-missing-ca.pem");
        let _ = std::fs::remove_file(&missing);
        std::env::set_var("RHTTP_CAINFO", &missing);
        assert!(extra_root_certs().is_empty());

        let garbage = std::env::temp_dir().join("rhttp-test-garbage-ca.pem");
        std::fs::write(&garbage, b"not a pem bundle").unwrap();
        std::env::set_var("RHTTP_CAINFO", &garbage);
        assert!(extra_root_certs().is_empty());
        let _ = std::fs::remove_file(&garbage);

        std::env::remove_var("RHTTP_CAINFO");
    }

    #[test]
    fn request_lifecycle_updates_stats() {
        let stats = Arc::new(HttpStats::new());
        let request = stats
            .try_request_started()
            .expect("request should be admitted");
        assert_eq!(stats.snapshot().submitted, 1);
        assert_eq!(stats.snapshot().in_flight, 1);
        drop(request);
        assert_eq!(stats.snapshot().in_flight, 0);
    }

    #[test]
    fn in_flight_is_capped() {
        let stats = Arc::new(HttpStats::new());
        let mut held = Vec::new();
        for _ in 0..crate::config::MAX_IN_FLIGHT_REQUESTS {
            held.push(stats.try_request_started().expect("should be admitted"));
        }
        assert!(stats.try_request_started().is_none());
        assert_eq!(
            stats.snapshot().submitted,
            crate::config::MAX_IN_FLIGHT_REQUESTS,
            "rejected requests are not counted as submitted"
        );
        drop(held);
        assert!(stats.try_request_started().is_some());
    }

    #[test]
    fn cancelled_request_is_removed_after_completion() {
        let registry = Arc::new(RequestRegistry::new());
        let request = registry.register(None, None);
        assert!(registry.cancel(request.id));
        assert!(request.token.is_cancelled());
        drop(request);
        assert!(!registry.cancel(1));
    }

    #[test]
    fn release_makes_request_invisible_to_cancel_early() {
        let registry = Arc::new(RequestRegistry::new());
        let request = registry.register(None, None);
        assert!(registry.cancel(request.id));
        let callbacks = request.release().expect("release must hand over callbacks");
        assert!(callbacks.success.is_none() && callbacks.failed.is_none());
        assert!(
            !registry.cancel(request.id),
            "released requests are not cancelable"
        );
        assert!(
            request.take_callbacks().is_none(),
            "release must take the callbacks exactly once, so nothing leaks"
        );
    }

    #[test]
    fn poisoned_registry_still_stores_callbacks() {
        let registry = Arc::new(RequestRegistry::new());

        let poisoner = registry.clone();
        let _ = std::thread::spawn(move || {
            let _guard = poisoner.requests.lock().unwrap();
            panic!("poison");
        })
        .join();

        let request = registry.register(Some(10), Some(11));
        let callbacks = request
            .take_callbacks()
            .expect("callbacks must survive poisoning");
        assert_eq!(callbacks.success, Some(10));
        assert_eq!(callbacks.failed, Some(11));
        assert!(registry.cancel(request.id));
    }

    #[test]
    fn shutdown_collects_callback_references() {
        let registry = Arc::new(RequestRegistry::new());
        let request = registry.register(Some(10), Some(11));

        let callbacks = registry.cancel_all();
        assert_eq!(callbacks.len(), 1);
        assert_eq!(callbacks[0].success, Some(10));
        assert_eq!(callbacks[0].failed, Some(11));
        assert!(request.token.is_cancelled());
    }

    #[test]
    fn body_budget_keeps_bytes_reserved_until_callback_is_dropped() {
        let budget = Arc::new(Semaphore::new(256));
        let mut body_budget = BodyBudget::new();
        assert!(body_budget.is_empty());
        assert_eq!(body_budget.reserved(), 0);

        for chunk in [1usize, 4, 16, 64] {
            let permit = budget
                .clone()
                .try_acquire_many_owned(chunk as u32)
                .expect("budget should accept the growth");
            body_budget.reserve(permit, chunk);
        }

        assert_eq!(body_budget.reserved(), 85);
        assert_eq!(budget.available_permits(), 171, "all growth must be held");
        drop(body_budget);
        assert_eq!(budget.available_permits(), 256, "all permits return");
    }

    #[test]
    fn repeated_growths_do_not_release_earlier_permits() {
        let budget = Arc::new(Semaphore::new(64));
        let mut body_budget = BodyBudget::new();

        for _ in 0..10 {
            let permit = budget
                .clone()
                .try_acquire_many_owned(4)
                .expect("budget should accept each growth");
            body_budget.reserve(permit, 4);
        }

        assert_eq!(body_budget.reserved(), 40);
        assert_eq!(
            budget.available_permits(),
            24,
            "each growth must stay reserved until the body is delivered"
        );
    }
}
