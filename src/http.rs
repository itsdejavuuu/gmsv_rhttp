use crate::config::{BODY_BUDGET_ERROR, MAX_IN_FLIGHT_REQUESTS, MIN_RETRY_DELAY};
use crate::executor::{retry_delay, run_attempt, AttemptOutcome, RequestPlan};
use crate::options::RequestOptions;
use crate::worker::{park_orphan_ref, resources, spawn_task, CallbackTask, RequestCallbacks};
use gmod::lua::{LuaReference, State, LUA_TNUMBER, LUA_TTABLE};
use gmod::lua_string;
use std::sync::Mutex;
use std::time::Instant;

unsafe fn fail_request(
    lua: State,
    success_cb: Option<LuaReference>,
    failed_cb: Option<LuaReference>,
    message: &str,
) {
    if let Some(cb) = failed_cb {
        lua.from_reference(cb);
        lua.push_string(message);
        lua.pcall_ignore(1, 0);
        lua.dereference(cb);
    }
    if let Some(cb) = success_cb {
        lua.dereference(cb);
    }
    lua.push_boolean(false);
}

pub unsafe extern "C-unwind" fn request_lua(lua: State) -> i32 {
    if lua.lua_type(1) != LUA_TTABLE {
        lua.error("rhttp: Expected table as first argument");
    }

    lua.get_field(1, lua_string!("success"));
    let success_cb = if lua.is_function(-1) {
        Some(lua.reference())
    } else {
        lua.pop_n(1);
        None
    };

    lua.get_field(1, lua_string!("failed"));
    let failed_cb = if lua.is_function(-1) {
        Some(lua.reference())
    } else {
        lua.pop_n(1);
        None
    };

    let opts = match RequestOptions::parse(lua, success_cb, failed_cb) {
        Ok(opts) => opts,
        Err(error) => {
            fail_request(lua, error.success, error.failed, &error.message);
            return 1;
        }
    };

    let Some(worker) = resources() else {
        fail_request(lua, opts.success, opts.failed, "rhttp is not initialized");
        return 1;
    };

    let request_body_budget = match opts.body.as_ref() {
        Some(body) if !body.is_empty() => match worker
            .body_budget
            .clone()
            .try_acquire_many_owned(body.len() as u32)
        {
            Ok(permit) => Some(permit),
            Err(_) => {
                fail_request(lua, opts.success, opts.failed, BODY_BUDGET_ERROR);
                return 1;
            }
        },
        _ => None,
    };
    let Some(in_flight) = worker.stats.try_request_started() else {
        fail_request(
            lua,
            opts.success,
            opts.failed,
            &format!("rhttp request queue is full (limit: {MAX_IN_FLIGHT_REQUESTS})"),
        );
        return 1;
    };
    let registered_request = worker.requests.register(opts.success, opts.failed);
    let request_id = registered_request.id;

    let plan = RequestPlan {
        method: opts.method,
        url: opts.url,
        headers: opts.headers,
        body: opts.body,
        deadline: Instant::now() + opts.timeout,
        collect_body: opts.success.is_some(),
        body_budget: worker.body_budget.clone(),
    };
    let retries = opts.retries;
    let retry_base_delay = opts.retry_base_delay;

    let stats = worker.stats.clone();
    let concurrency_limit = worker.concurrency_limit.clone();
    let client = worker.client.clone();
    let callback_tx = worker.callback_tx.clone();
    let orphan_refs = worker.orphan_refs.clone();
    let token = registered_request.token.clone();
    let handle = worker.handle();

    spawn_task(handle, async move {
        let _in_flight = in_flight;
        let _request_body_budget = request_body_budget;
        let stats = &stats;
        let orphan_refs = &orphan_refs;

        for attempt in 0..=retries {
            if token.is_cancelled() {
                stats.cancelled();
                deliver_failure(
                    &callback_tx,
                    orphan_refs,
                    registered_request.take_callbacks(),
                    "Request cancelled".to_string(),
                )
                .await;
                return;
            }

            let wait_budget = plan.deadline.saturating_duration_since(Instant::now());
            if wait_budget.is_zero() {
                stats.failed();
                deliver_failure(
                    &callback_tx,
                    orphan_refs,
                    registered_request.take_callbacks(),
                    "Request timed out".to_string(),
                )
                .await;
                return;
            }
            let permit = tokio::select! {
                biased;
                _ = token.cancelled() => {
                    stats.cancelled();
                    deliver_failure(
                        &callback_tx,
                        orphan_refs,
                        registered_request.take_callbacks(),
                        "Request cancelled".to_string(),
                    )
                    .await;
                    return;
                }
                permit = concurrency_limit.clone().acquire_owned() => match permit {
                    Ok(permit) => permit,
                    Err(_) => {
                        deliver_failure(
                            &callback_tx,
                            orphan_refs,
                            registered_request.take_callbacks(),
                            "rhttp concurrency limiter is closed".to_string(),
                        )
                        .await;
                        return;
                    }
                },
                _ = tokio::time::sleep(wait_budget) => {
                    stats.failed();
                    deliver_failure(
                        &callback_tx,
                        orphan_refs,
                        registered_request.take_callbacks(),
                        "Request timed out".to_string(),
                    )
                    .await;
                    return;
                }
            };

            match run_attempt(&client, &plan, &token, permit).await {
                AttemptOutcome::Complete {
                    status,
                    headers,
                    body,
                    budget,
                } => {
                    stats.succeeded();
                    if let Some(callbacks) = registered_request.release() {
                        if let Some(cb) = callbacks.success {
                            send_or_park(
                                &callback_tx,
                                orphan_refs,
                                CallbackTask::Success(
                                    cb,
                                    status,
                                    body.unwrap_or_default(),
                                    headers,
                                    budget,
                                ),
                            )
                            .await;
                        } else {
                            drop(body);
                            drop(budget);
                        }
                        if let Some(cb) = callbacks.failed {
                            send_or_park(&callback_tx, orphan_refs, CallbackTask::DropRef(cb))
                                .await;
                        }
                    }
                    return;
                }
                AttemptOutcome::Cancelled => {
                    stats.cancelled();
                    deliver_failure(
                        &callback_tx,
                        orphan_refs,
                        registered_request.take_callbacks(),
                        "Request cancelled".to_string(),
                    )
                    .await;
                    return;
                }
                AttemptOutcome::Transient {
                    message,
                    retry_after,
                } => {
                    let remaining = plan.deadline.saturating_duration_since(Instant::now());
                    if attempt == retries || remaining <= MIN_RETRY_DELAY {
                        stats.failed();
                        deliver_failure(
                            &callback_tx,
                            orphan_refs,
                            registered_request.take_callbacks(),
                            message,
                        )
                        .await;
                        return;
                    }

                    stats.retried();
                    let wait =
                        retry_delay(retry_after.as_ref(), attempt, retry_base_delay).min(remaining);
                    let cancelled = tokio::select! {
                        biased;
                        _ = token.cancelled() => true,
                        _ = tokio::time::sleep(wait) => false,
                    };
                    if cancelled {
                        stats.cancelled();
                        deliver_failure(
                            &callback_tx,
                            orphan_refs,
                            registered_request.take_callbacks(),
                            "Request cancelled".to_string(),
                        )
                        .await;
                        return;
                    }
                }
                AttemptOutcome::Fatal(message) => {
                    stats.failed();
                    deliver_failure(
                        &callback_tx,
                        orphan_refs,
                        registered_request.take_callbacks(),
                        message,
                    )
                    .await;
                    return;
                }
            }
        }
    });

    lua.push_boolean(true);
    lua.push_integer(request_id as _);
    2
}

async fn send_or_park(
    tx: &tokio::sync::mpsc::Sender<CallbackTask>,
    orphan_refs: &Mutex<Vec<LuaReference>>,
    task: CallbackTask,
) {
    let reference = match &task {
        CallbackTask::Success(cb, ..) | CallbackTask::Failed(cb, _) | CallbackTask::DropRef(cb) => {
            *cb
        }
    };

    if tx.send(task).await.is_err() {
        park_orphan_ref(orphan_refs, reference);
    }
}

async fn deliver_failure(
    tx: &tokio::sync::mpsc::Sender<CallbackTask>,
    orphan_refs: &Mutex<Vec<LuaReference>>,
    callbacks: Option<RequestCallbacks>,
    message: String,
) {
    let Some(callbacks) = callbacks else {
        return;
    };
    if let Some(cb) = callbacks.failed {
        send_or_park(tx, orphan_refs, CallbackTask::Failed(cb, message)).await;
    }
    if let Some(cb) = callbacks.success {
        send_or_park(tx, orphan_refs, CallbackTask::DropRef(cb)).await;
    }
}

pub unsafe extern "C-unwind" fn cancel_lua(lua: State) -> i32 {
    let cancelled = lua.lua_type(1) == LUA_TNUMBER
        && crate::worker::cancel_request(lua.to_integer(1).max(0) as u64);
    lua.push_boolean(cancelled);
    1
}

pub unsafe extern "C-unwind" fn stats_lua(lua: State) -> i32 {
    let stats = crate::worker::stats();
    lua.create_table(0, 6);
    lua.push_integer(stats.submitted as _);
    lua.set_field(-2, lua_string!("submitted"));
    lua.push_integer(stats.in_flight as _);
    lua.set_field(-2, lua_string!("in_flight"));
    lua.push_integer(stats.succeeded as _);
    lua.set_field(-2, lua_string!("succeeded"));
    lua.push_integer(stats.failed as _);
    lua.set_field(-2, lua_string!("failed"));
    lua.push_integer(stats.retried as _);
    lua.set_field(-2, lua_string!("retried"));
    lua.push_integer(stats.cancelled as _);
    lua.set_field(-2, lua_string!("cancelled"));
    1
}
