// SPDX-License-Identifier: CC0-1.0

//! A `Task` state whose Lambda function runs in a WORKER: Step Functions and Lambda on ONE
//! engine ([`Chain`]), with the function covered by Lambda's worker table
//! (`state::execution::container` of that crate), so its invocations are run by a worker
//! container instead of by an embedded engine.
//!
//! RULE: where a function's handler runs changes nothing about what a definition decides.
//! A handler that THREW in its worker is a failed task named by the error it threw, so the
//! state's `Retry` and `Catch` act on it; and a Distributed Map's items each invoke the
//! function, with the worker handed one invocation at a time.
//!
//! The worker is a STAND-IN: a listener of this suite's own that speaks the worker agent's
//! protocol (`docker/worker/agent.mjs`) — it takes the function it is pushed, and answers
//! an invocation with what a row says the run came to: a result, or the document
//! `public.ecr.aws/lambda/nodejs:22` wrote for a handler that threw.

use std::io::{Read as _, Write as _};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use serde_json::{json, Value};
use service_com_amazonaws_lambda_awsgirapiservice::state::execution::container;

use crate::lambda_task_control_flow::Chain;

/// The runtime the functions here declare. Their stored code is never run.
const RUNTIME: &str = "nodejs22.x";

/// The module the functions here store, and its text: a handler that answers something no
/// stand-in does, so an execution whose output carries it ran the function in process.
const MODULE: &str = "index.js";

/// See [`MODULE`].
const STORED_SOURCE: &str = "exports.handler = async () => ({ ran: 'in-process' });";

/// The name the handler in the worker throws its error under, which is what a `Retry`
/// or a `Catch` names to match it.
const THROWN_ERROR: &str = "OrderRejected";

/// The message of that error.
const THROWN_MESSAGE: &str = "order 7 was rejected";

/// The document the runtime writes for a handler that threw that error: recorded from
/// `public.ecr.aws/lambda/nodejs:22`.
const THREW: &str = r#"{"errorType":"OrderRejected","errorMessage":"order 7 was rejected","trace":["OrderRejected: order 7 was rejected","    at Runtime.handler (file:///var/task/index.mjs:13:19)","    at Runtime.handleOnceNonStreaming (file:///var/runtime/index.mjs:1207:29)"]}"#;

/// A frame of the worker protocol: the head's length, the head as JSON, then the tails.
fn frame(head: &Value, tails: &[&[u8]]) -> Vec<u8> {
    let json = head.to_string().into_bytes();
    let mut framed = u32::try_from(json.len())
        .expect("a head fits its length")
        .to_be_bytes()
        .to_vec();
    framed.extend_from_slice(&json);
    for tail in tails {
        framed.extend_from_slice(tail);
    }
    framed
}

/// The head and the tail of a frame, or `None` for a body that is not one.
fn unframe(body: &[u8]) -> Option<(Value, &[u8])> {
    let length = u32::from_be_bytes(body.get(..4)?.try_into().ok()?) as usize;
    let head = serde_json::from_slice(body.get(4..4 + length)?).ok()?;
    Some((head, &body[4 + length..]))
}

/// What a stand-in worker says one run came to.
enum Run {
    /// The handler returned this.
    Returned(String),
    /// The handler threw, and the runtime wrote this document for it.
    Threw(&'static str),
}

/// A listener standing in for one worker, and what it has seen.
struct StandIn {
    /// The base address the worker table names it by.
    address: String,
    /// The event of every invocation it ran, in order.
    events: Arc<Mutex<Vec<String>>>,
    /// The most invocations it ever had running at one instant.
    most_at_once: Arc<AtomicUsize>,
}

impl StandIn {
    fn events(&self) -> Vec<String> {
        self.events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// Stand a worker up on a port the host chooses. `answer` is asked what each run came to,
/// with how many invocations arrived before it and the event it carried. Each run takes
/// `run` to answer.
///
/// Every connection is served on a thread of its own, so the stand-in never makes two
/// invocations take turns: two in it together show in [`StandIn::most_at_once`].
fn stand_in(run: Duration, answer: impl Fn(usize, &str) -> Run + Send + Sync + 'static) -> StandIn {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a stand-in worker binds");
    let address = format!(
        "http://{}",
        listener.local_addr().expect("the stand-in has an address")
    );
    let events = Arc::new(Mutex::new(Vec::new()));
    let most_at_once = Arc::new(AtomicUsize::new(0));
    let running = Arc::new(AtomicUsize::new(0));
    // The revision the worker was pushed last.
    let holding: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let answer = Arc::new(answer);
    {
        let events = Arc::clone(&events);
        let most_at_once = Arc::clone(&most_at_once);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let events = Arc::clone(&events);
                let most_at_once = Arc::clone(&most_at_once);
                let running = Arc::clone(&running);
                let holding = Arc::clone(&holding);
                let answer = Arc::clone(&answer);
                std::thread::spawn(move || {
                    let Some((path, body)) = read_request(&mut stream) else {
                        return;
                    };
                    let refused = |status: u16, error: &str| {
                        (status, json!({ "error": error }).to_string().into_bytes())
                    };
                    let (status, body) = match (path.as_str(), unframe(&body)) {
                        ("/function", Some((head, _))) => {
                            *holding.lock().unwrap_or_else(PoisonError::into_inner) =
                                head["revision"].as_str().map(str::to_string);
                            (200, b"{}".to_vec())
                        }
                        ("/invoke", Some((head, event))) => {
                            let held = holding
                                .lock()
                                .unwrap_or_else(PoisonError::into_inner)
                                .clone();
                            if held.is_none() || held.as_deref() != head["revision"].as_str() {
                                refused(409, "not-loaded")
                            } else {
                                let event = String::from_utf8_lossy(event).to_string();
                                let ordinal = {
                                    let mut events =
                                        events.lock().unwrap_or_else(PoisonError::into_inner);
                                    events.push(event.clone());
                                    events.len() - 1
                                };
                                let now_running = running.fetch_add(1, Ordering::SeqCst) + 1;
                                most_at_once.fetch_max(now_running, Ordering::SeqCst);
                                let (outcome, payload) = match answer(ordinal, &event) {
                                    Run::Returned(payload) => ("returned", payload),
                                    Run::Threw(document) => ("failed", document.to_string()),
                                };
                                std::thread::sleep(run);
                                // The run is over before the answer is written, so the
                                // next invocation is not counted as arriving on top of it.
                                running.fetch_sub(1, Ordering::SeqCst);
                                let head = json!({
                                    "outcome": outcome,
                                    "payloadBytes": payload.len(),
                                    "logBytes": 0,
                                });
                                (200, frame(&head, &[payload.as_bytes()]))
                            }
                        }
                        _ => refused(404, "not-found"),
                    };
                    let head = format!(
                        "HTTP/1.1 {status} Answered\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = stream.write_all(head.as_bytes());
                    let _ = stream.write_all(&body);
                    let _ = stream.flush();
                });
            }
        });
    }
    StandIn {
        address,
        events,
        most_at_once,
    }
}

/// The path and the body of one HTTP/1.1 request with a `Content-Length`, read off
/// `stream`; `None` for a connection that sent none.
fn read_request(stream: &mut TcpStream) -> Option<(String, Vec<u8>)> {
    const HEAD_END: &[u8] = b"\r\n\r\n";
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 8192];
    let head_end = loop {
        if let Some(at) = buffer
            .windows(HEAD_END.len())
            .position(|window| window == HEAD_END)
        {
            break at;
        }
        let read = stream.read(&mut chunk).ok()?;
        if read == 0 {
            return None;
        }
        buffer.extend_from_slice(&chunk[..read]);
    };
    let head = String::from_utf8_lossy(&buffer[..head_end]).to_string();
    let path = head.split("\r\n").next()?.split(' ').nth(1)?.to_string();
    let length = head
        .split("\r\n")
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.trim().eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.trim().parse::<usize>().ok())
        .unwrap_or(0);
    let mut body = buffer[head_end + HEAD_END.len()..].to_vec();
    while body.len() < length {
        let read = stream.read(&mut chunk).ok()?;
        if read == 0 {
            return None;
        }
        body.extend_from_slice(&chunk[..read]);
    }
    Some((path, body))
}

/// The `Timeout` a function here is given when its row is not about how long it runs.
const AMPLE_TIMEOUT_SECONDS: u64 = 30;

/// The function `name`, created in Lambda with `timeout_seconds` to run in and run by
/// `worker`; answers its ARN.
async fn forwarded_function(
    chain: &Chain,
    name: &str,
    worker: &StandIn,
    timeout_seconds: u64,
) -> String {
    container::forward(name, &[&worker.address]);
    chain
        .given_function_in(name, RUNTIME, MODULE, STORED_SOURCE, Some(timeout_seconds))
        .await
}

/// A member of a described execution, as text.
fn member<'a>(described: &'a Value, name: &str) -> Option<&'a str> {
    described.get(name).and_then(Value::as_str)
}

/// The `output` of a described execution, parsed.
fn output(case: &str, described: &Value) -> Value {
    member(described, "output")
        .map(|text| serde_json::from_str(text).expect("the output is JSON"))
        .unwrap_or_else(|| panic!("case {case:?}: expected an output in {described}"))
}

/// A handler that throws in its worker is retried under the name it threw, until the
/// run that returns: the execution succeeds with that run's result.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retry_naming_the_thrown_error_runs_the_worker_again() {
    let case = "two throws, then a result";
    let chain = Chain::new();
    let worker = stand_in(Duration::ZERO, |ordinal, _| match ordinal {
        0 | 1 => Run::Threw(THREW),
        _ => Run::Returned(r#"{"orderId":7,"written":true}"#.to_string()),
    });
    let function_arn =
        forwarded_function(&chain, "container-retried", &worker, AMPLE_TIMEOUT_SECONDS).await;
    let definition = json!({
        "StartAt": "Write",
        "States": {
            "Write": {
                "Type": "Task",
                "Resource": function_arn,
                "Retry": [{
                    "ErrorEquals": [THROWN_ERROR],
                    "IntervalSeconds": 1,
                    "MaxAttempts": 3,
                    "BackoffRate": 1.0,
                }],
                "End": true,
            },
        },
    });

    let described = chain
        .ran(case, "container-retried", &definition, &json!({ "orderId": 7 }))
        .await;

    assert_eq!(
        member(&described, "status"),
        Some("SUCCEEDED"),
        "case {case:?}: got {described}"
    );
    assert_eq!(
        output(case, &described),
        json!({ "orderId": 7, "written": true }),
        "case {case:?}: the output is the result of the run that returned"
    );
    assert_eq!(
        worker.events(),
        [r#"{"orderId":7}"#; 3],
        "case {case:?}: the task ran in the worker once and again for each retry, with the \
         state's input as its event"
    );
}

/// A handler that always throws: a `Catch` naming the error routes to its state, and with
/// nothing to answer for it the execution fails as the error the handler threw, carrying
/// the runtime's own document.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_thrown_error_is_caught_by_name_or_fails_the_execution_as_itself() {
    let chain = Chain::new();
    let worker = stand_in(Duration::ZERO, |_, _| Run::Threw(THREW));
    let function_arn =
        forwarded_function(&chain, "container-thrower", &worker, AMPLE_TIMEOUT_SECONDS).await;

    let case = "a Catch naming the thrown error";
    let caught = chain
        .ran(
            case,
            "container-caught",
            &json!({
                "StartAt": "Write",
                "States": {
                    "Write": {
                        "Type": "Task",
                        "Resource": function_arn,
                        "Catch": [{ "ErrorEquals": [THROWN_ERROR], "Next": "Compensate" }],
                        "End": true,
                    },
                    "Compensate": {
                        "Type": "Pass",
                        "Result": { "compensated": true },
                        "End": true,
                    },
                },
            }),
            &json!({ "orderId": 7 }),
        )
        .await;
    assert_eq!(
        member(&caught, "status"),
        Some("SUCCEEDED"),
        "case {case:?}: got {caught}"
    );
    assert_eq!(
        output(case, &caught),
        json!({ "compensated": true }),
        "case {case:?}: the catcher's state decided the output"
    );

    let case = "nothing answers for the thrown error";
    let failed = chain
        .ran(
            case,
            "container-uncaught",
            &json!({
                "StartAt": "Write",
                "States": {
                    "Write": { "Type": "Task", "Resource": function_arn, "End": true },
                },
            }),
            &json!({ "orderId": 7 }),
        )
        .await;
    assert_eq!(
        member(&failed, "status"),
        Some("FAILED"),
        "case {case:?}: got {failed}"
    );
    assert_eq!(
        member(&failed, "error"),
        Some(THROWN_ERROR),
        "case {case:?}: the execution fails as the error the handler threw, got {failed}"
    );
    let document: Value = member(&failed, "cause")
        .and_then(|cause| serde_json::from_str(cause).ok())
        .unwrap_or_else(|| panic!("case {case:?}: the cause is the error document, got {failed}"));
    assert_eq!(
        document.get("errorMessage").and_then(Value::as_str),
        Some(THROWN_MESSAGE),
        "case {case:?}: the cause carries the message the handler threw, got {document}"
    );
}

/// A Distributed Map over a function a worker runs: every item is run by the worker, with
/// the item as its event and its answer as that item's result — and the worker is handed
/// one invocation at a time.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_distributed_map_runs_every_item_in_the_worker() {
    let case = "five items";
    let chain = Chain::new();
    // Each run takes a while, so two items in the worker together would show.
    let worker = stand_in(Duration::from_millis(100), |_, event| {
        let item: Value = serde_json::from_str(event).unwrap_or(Value::Null);
        Run::Returned(json!({ "written": item["orderId"] }).to_string())
    });
    let function_arn =
        forwarded_function(&chain, "container-mapped", &worker, AMPLE_TIMEOUT_SECONDS).await;
    let definition = json!({
        "StartAt": "EachOrder",
        "States": {
            "EachOrder": {
                "Type": "Map",
                "ItemsPath": "$.orders",
                "ItemProcessor": {
                    "ProcessorConfig": { "Mode": "DISTRIBUTED", "ExecutionType": "STANDARD" },
                    "StartAt": "Write",
                    "States": {
                        "Write": { "Type": "Task", "Resource": function_arn, "End": true },
                    },
                },
                "End": true,
            },
        },
    });
    let orders: Vec<Value> = (1..=5).map(|id| json!({ "orderId": id })).collect();

    let described = chain
        .ran(case, "container-mapped", &definition, &json!({ "orders": orders }))
        .await;

    assert_eq!(
        member(&described, "status"),
        Some("SUCCEEDED"),
        "case {case:?}: got {described}"
    );
    assert_eq!(
        output(case, &described),
        json!([
            { "written": 1 },
            { "written": 2 },
            { "written": 3 },
            { "written": 4 },
            { "written": 5 },
        ]),
        "case {case:?}: each item's result is what the worker answered for it, in order"
    );
    let mut events: Vec<Value> = worker
        .events()
        .iter()
        .map(|event| serde_json::from_str(event).unwrap_or(Value::Null))
        .collect();
    events.sort_by_key(|event| event["orderId"].as_u64());
    assert_eq!(
        events, orders,
        "case {case:?}: the worker ran once per item, with the item as its event"
    );
    assert_eq!(
        worker.most_at_once.load(Ordering::SeqCst),
        1,
        "case {case:?}: the worker must never be sent an item while one is running in it"
    );
}

/// A run that takes longer than the ceilings UPSTREAM holds a call to is still the task's
/// own answer here. Upstream gives up on a function after 30 seconds whatever its
/// `Timeout`, and on one task's call after 10; this fork raises both so that a handler
/// doing real work in its worker is bounded by its function's `Timeout` — so a run
/// just past the longer of the two, inside a `Timeout` that allows it, must succeed.
///
/// This test takes as long as that run does. It is the only way to see the ceilings are
/// where the fork puts them: a shorter run would pass under upstream's as well.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_run_longer_than_the_upstream_ceilings_is_still_the_tasks_answer() {
    /// Just past the 30 seconds upstream waits for a function.
    const RUN: Duration = Duration::from_secs(32);
    /// A `Timeout` that allows that run.
    const TIMEOUT_SECONDS: u64 = 60;
    let case = "a 32 second run";
    let chain = Chain::new();
    let worker = stand_in(RUN, |_, _| Run::Returned(r#"{"finished":true}"#.to_string()));
    let function_arn =
        forwarded_function(&chain, "container-slow", &worker, TIMEOUT_SECONDS).await;
    let definition = json!({
        "StartAt": "Write",
        "States": {
            "Write": { "Type": "Task", "Resource": function_arn, "End": true },
        },
    });

    let described = chain
        .ran(case, "container-slow", &definition, &json!({}))
        .await;

    assert_eq!(
        member(&described, "status"),
        Some("SUCCEEDED"),
        "case {case:?}: got {described}"
    );
    assert_eq!(
        output(case, &described),
        json!({ "finished": true }),
        "case {case:?}: the output is what the worker answered when its run ended"
    );
}
