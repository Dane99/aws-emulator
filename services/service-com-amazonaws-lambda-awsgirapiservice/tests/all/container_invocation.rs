// SPDX-License-Identifier: CC0-1.0

//! End-to-end tests for RUNNING a function in a worker container
//! (`state::execution::container`): `Invoke` (`POST
//! /2015-03-31/functions/{FunctionName}/invocations`) on a Node.js function the worker table
//! covers is run by a worker — which this emulator first PUSHES the function to — and on any
//! other function by the embedded engine.
//!
//! The worker is a STAND-IN: a listener of this suite's own that speaks the worker agent's
//! protocol (`docker/worker/agent.mjs`) — `PUT /function` takes a function, `POST /invoke`
//! runs one invocation of the revision it holds and refuses any other with `409` — and
//! answers an invocation with whatever a row says the run came to. So what is under test is
//! this emulator's half of the exchange. The rules, each over what it generalises over:
//!
//! * a worker is pushed what the function IS before it is invoked — its deployment package,
//!   handler, timeout and environment — once, and again whenever any of that changes;
//! * it is then sent the event and the invocation's own identity, and what it answers is
//!   the invocation's payload;
//! * what a run came to in its worker is what the invocation reports: a handler that threw,
//!   a runtime that exited, a run past the timeout and a response past the quota are each
//!   the failure Lambda reports for it, and a RESULT is a result whatever it looks like;
//! * what the run wrote is the invocation's log;
//! * a worker runs ONE invocation at a time, so invocations queue for it — across its whole
//!   exchange, including one whose caller already stopped waiting — a pool of workers runs
//!   that many at once, and a function is run by the worker that already holds it;
//! * only a Node.js function the table covers is forwarded;
//! * a worker that cannot be reached, or that answers nothing a run produces, is the
//!   service's failure and never a function result.
//!
//! Every row drives real HTTP requests through the router.

use std::io::{Read as _, Write as _};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use base64::Engine as _;
use serde_json::{json, Value};
use service_com_amazonaws_lambda_awsgirapiservice::state::execution::container;
use service_com_amazonaws_lambda_awsgirapiservice::state::{ACTIVATION_DELAY, UPDATE_DELAY};
use service_com_amazonaws_lambda_awsgirapiservice::EmulatedService;
use smithy_server_runtime::MultiServiceRouter;

use crate::invoke_request_id::{is_host_bound, is_request_id, one_invocation_stream_at_a_time};
// The one multi-entry deployment-archive writer this target has.
use crate::node_invoke_run::package;

/// Where `CreateFunction` is bound.
const FUNCTIONS_URI: &str = "/2015-03-31/functions";

/// A SigV4 credential scope naming this service's signing name (`lambda`) in `us-east-1`.
/// Nothing in it is verified: the harness turns request authentication off.
const AUTHORIZATION: &str = "AWS4-HMAC-SHA256 \
     Credential=emulator-test-key/20260101/us-east-1/lambda/aws4_request, \
     SignedHeaders=host, Signature=not-verified";

/// A well-formed execution role ARN, satisfying the model's `RoleArn` pattern.
const ROLE: &str = "arn:aws:iam::000000000000:role/service-role/lambda-exec";

/// The header AWS reports a response's request id in, which for an invocation is the
/// invocation's own id.
const REQUEST_ID_HEADER: &str = "x-amzn-requestid";

/// The header an invocation whose function failed is marked with.
const FUNCTION_ERROR_HEADER: &str = "x-amz-function-error";

/// What that header carries for a run that failed.
const UNHANDLED: &str = "Unhandled";

/// How long past a due instant this suite waits before calling.
const SLACK: Duration = Duration::from_millis(150);

/// The `Runtime` a forwarded function is created with.
const NODE: &str = "nodejs22.x";

/// The `Timeout` a function gets when its row is not about the timeout, in seconds.
const AMPLE_TIMEOUT_SECONDS: i64 = 30;

/// The code every Node.js function here STORES. It runs in the embedded engine and answers
/// something no stand-in worker does, so an answer that carries it was run in process.
const STORED_MODULE: (&str, &str) = (
    "index.js",
    "exports.handler = async () => ({ ran: 'in-process' });",
);

/// What the stored code answers.
const RAN_IN_PROCESS: &str = r#"{"ran":"in-process"}"#;

/// The document a Node.js runtime reports for a handler that threw, as
/// `public.ecr.aws/lambda/nodejs:22` wrote it.
const THREW: &str = r#"{"errorType":"OrderRejected","errorMessage":"boom","trace":["OrderRejected: boom","    at Runtime.handler (file:///var/task/index.mjs:8:31)","    at Runtime.handleOnceNonStreaming (file:///var/runtime/index.mjs:1207:29)"]}"#;

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

/// One function a stand-in worker was pushed.
#[derive(Debug, Clone)]
struct Pushed {
    head: Value,
    /// The archives the push carried, in the order its head lists them.
    archives: Vec<Vec<u8>>,
}

impl Pushed {
    /// The value the pushed environment gives `name`.
    fn variable(&self, name: &str) -> Option<&str> {
        self.head["environment"][name].as_str()
    }
}

/// One invocation a stand-in worker was sent.
#[derive(Debug, Clone)]
struct Called {
    head: Value,
    event: Vec<u8>,
}

impl Called {
    fn event(&self) -> String {
        String::from_utf8_lossy(&self.event).to_string()
    }
}

/// What a stand-in worker answers one invocation with.
struct Reply {
    /// What the run came to, as the agent names it.
    outcome: &'static str,
    detail: Option<&'static str>,
    payload: String,
    log: String,
    /// The most memory the run held, in MB, where the worker measured it.
    max_memory_mb: Option<u64>,
    /// How long the run takes.
    after: Duration,
    /// An answer that is no frame at all, in place of everything above.
    raw: Option<(u16, &'static str)>,
}

impl Reply {
    fn of(outcome: &'static str, payload: &str) -> Self {
        Self {
            outcome,
            detail: None,
            payload: payload.to_string(),
            log: String::new(),
            max_memory_mb: None,
            after: Duration::ZERO,
            raw: None,
        }
    }

    /// A handler that returned `payload`.
    fn returned(payload: &str) -> Self {
        Self::of("returned", payload)
    }

    fn detail(mut self, detail: &'static str) -> Self {
        self.detail = Some(detail);
        self
    }

    fn log(mut self, log: &str) -> Self {
        self.log = log.to_string();
        self
    }

    fn max_memory_mb(mut self, megabytes: u64) -> Self {
        self.max_memory_mb = Some(megabytes);
        self
    }

    fn after(mut self, after: Duration) -> Self {
        self.after = after;
        self
    }

    fn raw(status: u16, body: &'static str) -> Self {
        Self {
            raw: Some((status, body)),
            ..Self::returned("")
        }
    }
}

/// A listener standing in for one worker, and what it has seen.
struct StandIn {
    /// The base address the worker table names it by.
    address: String,
    pushed: Arc<Mutex<Vec<Pushed>>>,
    called: Arc<Mutex<Vec<Called>>>,
    /// The most invocations it ever had running at one instant.
    most_at_once: Arc<AtomicUsize>,
}

impl StandIn {
    fn pushed(&self) -> Vec<Pushed> {
        self.pushed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn called(&self) -> Vec<Called> {
        self.called
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn most_at_once(&self) -> usize {
        self.most_at_once.load(Ordering::SeqCst)
    }
}

/// Stand a worker up on a port the host chooses. `answer` is asked for each invocation's
/// reply, with how many invocations arrived before it.
fn stand_in(answer: impl Fn(usize, &Called) -> Reply + Send + Sync + 'static) -> StandIn {
    stand_in_taking(None, answer)
}

/// [`stand_in`], with every push answered `refusal` instead of being taken when it names
/// one.
///
/// Every connection is served on a thread of its own, so the stand-in itself never makes
/// two invocations take turns: if two are ever in it together, [`StandIn::most_at_once`]
/// says so.
fn stand_in_taking(
    refusal: Option<(u16, &'static str)>,
    answer: impl Fn(usize, &Called) -> Reply + Send + Sync + 'static,
) -> StandIn {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a stand-in worker binds");
    let address = format!(
        "http://{}",
        listener.local_addr().expect("the stand-in has an address")
    );
    let pushed = Arc::new(Mutex::new(Vec::new()));
    let called = Arc::new(Mutex::new(Vec::new()));
    let most_at_once = Arc::new(AtomicUsize::new(0));
    let serving = Arc::new(Serving {
        refusal,
        answer: Box::new(answer),
        holding: Mutex::new(None),
        running: AtomicUsize::new(0),
        pushed: Arc::clone(&pushed),
        called: Arc::clone(&called),
        most_at_once: Arc::clone(&most_at_once),
    });
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let serving = Arc::clone(&serving);
            std::thread::spawn(move || {
                let Some((method, path, body)) = read_request(&mut stream) else {
                    return;
                };
                let (status, body) = serving.serve(&method, &path, &body);
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
    StandIn {
        address,
        pushed,
        called,
        most_at_once,
    }
}

/// What a stand-in worker's connections share.
struct Serving {
    refusal: Option<(u16, &'static str)>,
    answer: Box<dyn Fn(usize, &Called) -> Reply + Send + Sync>,
    /// The revision it was pushed last.
    holding: Mutex<Option<String>>,
    running: AtomicUsize,
    pushed: Arc<Mutex<Vec<Pushed>>>,
    called: Arc<Mutex<Vec<Called>>>,
    most_at_once: Arc<AtomicUsize>,
}

impl Serving {
    fn serve(&self, method: &str, path: &str, body: &[u8]) -> (u16, Vec<u8>) {
        let refused = |status: u16, error: &str| (status, json!({ "error": error }).to_string().into_bytes());
        let Some((head, tail)) = unframe(body) else {
            return refused(400, "malformed");
        };
        match (method, path) {
            ("PUT", "/function") => {
                if let Some((status, body)) = self.refusal {
                    return (status, body.as_bytes().to_vec());
                }
                let mut archives = Vec::new();
                let mut rest = tail;
                for archive in head["archives"].as_array().into_iter().flatten() {
                    let bytes = archive["bytes"].as_u64().unwrap_or(0) as usize;
                    let (taken, left) = rest.split_at(bytes.min(rest.len()));
                    archives.push(taken.to_vec());
                    rest = left;
                }
                let revision = head["revision"].as_str().map(str::to_string);
                self.pushed
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(Pushed { head, archives });
                *self.holding.lock().unwrap_or_else(PoisonError::into_inner) = revision.clone();
                (200, json!({ "revision": revision }).to_string().into_bytes())
            }
            ("POST", "/invoke") => {
                let holding = self
                    .holding
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .clone();
                if holding.is_none() || holding.as_deref() != head["revision"].as_str() {
                    return refused(409, "not-loaded");
                }
                let invocation = Called {
                    head,
                    event: tail.to_vec(),
                };
                let ordinal = {
                    let mut called = self.called.lock().unwrap_or_else(PoisonError::into_inner);
                    called.push(invocation.clone());
                    called.len() - 1
                };
                let now_running = self.running.fetch_add(1, Ordering::SeqCst) + 1;
                self.most_at_once.fetch_max(now_running, Ordering::SeqCst);
                let reply = (self.answer)(ordinal, &invocation);
                std::thread::sleep(reply.after);
                // The run is over BEFORE the answer is written, so a caller that sends its
                // next invocation the instant it reads this one is not counted as
                // overlapping it.
                self.running.fetch_sub(1, Ordering::SeqCst);
                if let Some((status, body)) = reply.raw {
                    return (status, body.as_bytes().to_vec());
                }
                let head = json!({
                    "outcome": reply.outcome,
                    "detail": reply.detail,
                    "payloadBytes": reply.payload.len(),
                    "logBytes": reply.log.len(),
                    "maxMemoryUsedMb": reply.max_memory_mb,
                });
                (
                    200,
                    frame(&head, &[reply.payload.as_bytes(), reply.log.as_bytes()]),
                )
            }
            _ => refused(404, "not-found"),
        }
    }
}

/// Read one HTTP/1.1 request with a `Content-Length` body off `stream` — its method, its
/// path and its body — or `None` for a connection that sent none.
fn read_request(stream: &mut TcpStream) -> Option<(String, String, Vec<u8>)> {
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
    let mut lines = head.split("\r\n");
    let mut request_line = lines.next()?.split(' ');
    let method = request_line.next()?.to_string();
    let path = request_line.next()?.to_string();
    let length = lines
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
    Some((method, path, body))
}

/// An address nothing listens on: a port the host handed out and this took back.
fn unreachable_address() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a port to give back");
    let address = listener.local_addr().expect("the port has an address");
    drop(listener);
    format!("http://{address}")
}

/// What a dispatched request answered.
struct Answer {
    status: u16,
    headers: http::HeaderMap,
    body: bytes::Bytes,
}

impl Answer {
    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap_or(Value::Null)
    }

    fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).to_string()
    }

    fn header(&self, name: &str) -> Option<String> {
        self.headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string)
    }

    /// The id this answer names its request by.
    fn request_id(&self) -> String {
        self.header(REQUEST_ID_HEADER).unwrap_or_default()
    }

    /// The wire error code of a refusal, without the namespace an SDK strips.
    fn error_code(&self) -> String {
        let code = self
            .header("x-amzn-errortype")
            .or_else(|| {
                self.json()
                    .get("__type")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .unwrap_or_default();
        let code = code.split(':').next().unwrap_or_default();
        code.rsplit('#').next().unwrap_or_default().to_string()
    }
}

/// One service mounted on its own engine, reached the way the production router reaches it.
fn router() -> MultiServiceRouter {
    let engine = Arc::new(engine::InMemoryEngine::new());
    let mut router = MultiServiceRouter::new();
    router.set_request_authentication(false);
    router.register_service(EmulatedService::new(engine));
    router
}

/// Dispatch one request, with the headers `headers` names.
async fn call(
    router: &MultiServiceRouter,
    method: &str,
    uri: &str,
    body: &[u8],
    headers: &[(&str, &str)],
) -> Answer {
    let mut builder = http::Request::builder()
        .method(method)
        .uri(format!("http://lambda.us-east-1.amazonaws.com{uri}"))
        .header("host", "lambda.us-east-1.amazonaws.com")
        .header("authorization", AUTHORIZATION)
        .header("content-type", "application/json");
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    let request = builder
        .body(bytes::Bytes::from(body.to_vec()))
        .expect("the test request must build");
    let response = router.dispatch(request).await;
    let (parts, body) = response.into_parts();
    Answer {
        status: parts.status.as_u16(),
        headers: parts.headers,
        body,
    }
}

/// A deployment package as `Code.ZipFile` carries it.
fn encoded(archive: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(archive)
}

/// Create the Node.js function `name` holding [`STORED_MODULE`], with `timeout` seconds to
/// run in, and wait out the provisioning window the create documents.
async fn create_function(router: &MultiServiceRouter, name: &str, timeout: i64) {
    create(router, name, NODE, &[STORED_MODULE], timeout, json!({})).await;
}

/// Create the function `name` of `runtime` holding `entries`, with `variables` as its
/// environment, and wait out the provisioning window the create documents.
async fn create(
    router: &MultiServiceRouter,
    name: &str,
    runtime: &str,
    entries: &[(&str, &str)],
    timeout: i64,
    variables: Value,
) {
    let created = call(
        router,
        "POST",
        FUNCTIONS_URI,
        &serde_json::to_vec(&json!({
            "FunctionName": name,
            "Role": ROLE,
            "Runtime": runtime,
            "Handler": "index.handler",
            "Timeout": timeout,
            "Environment": { "Variables": variables },
            "Code": { "ZipFile": encoded(&package(entries)) },
        }))
        .expect("the create body serializes"),
        &[],
    )
    .await;
    assert_eq!(
        created.status,
        201,
        "CreateFunction {name:?} must succeed, got {} {}",
        created.status,
        created.text()
    );
    tokio::time::sleep(ACTIVATION_DELAY + SLACK).await;
}

/// Change the function `name` through `member` of its resource (`configuration` or `code`)
/// and wait out the window the update is applied in.
async fn update(router: &MultiServiceRouter, name: &str, member: &str, change: &Value) {
    let updated = call(
        router,
        "PUT",
        &format!("{FUNCTIONS_URI}/{name}/{member}"),
        &serde_json::to_vec(change).expect("the update body serializes"),
        &[],
    )
    .await;
    assert_eq!(
        updated.status,
        200,
        "the update of {name:?}'s {member} must succeed, got {} {}",
        updated.status,
        updated.text()
    );
    tokio::time::sleep(UPDATE_DELAY + SLACK).await;
}

/// One `Invoke` on the function `name`.
async fn invoke(
    router: &MultiServiceRouter,
    name: &str,
    payload: &[u8],
    headers: &[(&str, &str)],
) -> Answer {
    call(
        router,
        "POST",
        &format!("{FUNCTIONS_URI}/{name}/invocations"),
        payload,
        headers,
    )
    .await
}

/// Assert that `answered` is an invocation whose function FAILED, and report its document.
fn function_error(case: &str, answered: &Answer) -> Value {
    assert_eq!(
        answered.status,
        200,
        "case {case}: a function error is still an answered invocation, got {} {}",
        answered.status,
        answered.text()
    );
    assert_eq!(
        answered.header(FUNCTION_ERROR_HEADER).as_deref(),
        Some(UNHANDLED),
        "case {case}: the invocation must be marked as one whose function failed, got {}",
        answered.text()
    );
    answered.json()
}

/// Assert that `answered` is an invocation whose function RETURNED exactly `payload`.
fn assert_result(case: &str, answered: &Answer, payload: &str) {
    assert_eq!(
        answered.status,
        200,
        "case {case}: the invocation must be answered, got {} {}",
        answered.status,
        answered.text()
    );
    assert_eq!(
        answered.header(FUNCTION_ERROR_HEADER),
        None,
        "case {case}: the function returned, so nothing marks the invocation failed, got {}",
        answered.text()
    );
    assert_eq!(
        answered.text(),
        payload,
        "case {case}: the payload must be exactly what the worker answered"
    );
}

/// A worker is pushed what the function IS before it runs it — the package the function
/// stores, its handler, its timeout and its environment — and is then sent the event with
/// the invocation's own identity. What it answers is the payload.
#[tokio::test]
async fn a_worker_is_pushed_the_function_and_then_sent_the_event() {
    let worker = stand_in(|_, _| Reply::returned(r#"{"ok":true,"rows":3}"#));
    container::forward("container-returns", &[&worker.address]);
    let router = router();
    create(
        &router,
        "container-returns",
        NODE,
        &[STORED_MODULE],
        AMPLE_TIMEOUT_SECONDS,
        json!({ "PGHOST": "postgres", "STAGE": "local" }),
    )
    .await;

    let answered = invoke(&router, "container-returns", br#"{"order":7}"#, &[]).await;

    assert_result("a handler that returned", &answered, r#"{"ok":true,"rows":3}"#);
    let pushed = worker.pushed();
    assert_eq!(pushed.len(), 1, "the function is pushed once, got {pushed:?}");
    let function = &pushed[0];
    assert_eq!(function.head["functionName"], "container-returns");
    assert_eq!(function.head["handler"], "index.handler");
    assert_eq!(
        function.head["timeoutMs"],
        AMPLE_TIMEOUT_SECONDS * 1_000,
        "the worker enforces the function's own timeout"
    );
    assert_eq!(
        function.archives,
        vec![package(&[STORED_MODULE])],
        "the worker is pushed the deployment package the function stores"
    );
    assert_eq!(
        (function.variable("PGHOST"), function.variable("STAGE")),
        (Some("postgres"), Some("local")),
        "the function's own variables reach the worker"
    );
    assert_eq!(
        (
            function.variable("AWS_LAMBDA_FUNCTION_NAME"),
            function.variable("AWS_LAMBDA_FUNCTION_VERSION"),
            function.variable("AWS_REGION"),
        ),
        (Some("container-returns"), Some("$LATEST"), Some("us-east-1")),
        "and so do the variables a runtime sets"
    );
    assert!(
        function
            .variable("AWS_ENDPOINT_URL")
            .is_some_and(|endpoint| endpoint.starts_with("https://")),
        "the handler's own AWS SDK is pointed back at this emulator, got {:?}",
        function.variable("AWS_ENDPOINT_URL")
    );
    assert_eq!(
        function.head["emulatorEndpoint"].as_str(),
        function.variable("AWS_ENDPOINT_URL"),
        "and the worker is told that address, as where the handler's signed AWS requests go"
    );
    assert!(
        function.head["caPem"]
            .as_str()
            .is_some_and(|pem| pem.starts_with("-----BEGIN CERTIFICATE-----")),
        "the worker is given the certificate this emulator serves that endpoint with"
    );

    let called = worker.called();
    assert_eq!(called.len(), 1, "one invocation is one run, got {called:?}");
    assert_eq!(called[0].event(), r#"{"order":7}"#, "the worker is sent the event");
    assert_eq!(
        called[0].head["revision"], function.head["revision"],
        "the invocation names the revision that was pushed"
    );
    let sent_id = called[0].head["requestId"].as_str().unwrap_or_default();
    assert!(
        is_request_id(sent_id),
        "the worker is told the invocation's id, got {sent_id:?}"
    );
    assert_eq!(
        sent_id,
        answered.request_id(),
        "the id the handler reads as its request id is the one the caller is answered with"
    );
    assert_eq!(
        called[0].head["invokedFunctionArn"],
        "arn:aws:lambda:us-east-1:000000000000:function:container-returns",
        "the handler reads the ARN the invocation addressed"
    );
}

/// A function a worker already holds is not pushed again — and is pushed again as soon as
/// what it is changes: its environment, or its code.
#[tokio::test]
async fn a_function_is_pushed_once_and_again_when_it_changes() {
    let worker = stand_in(|_, _| Reply::returned("null"));
    container::forward("container-changes", &[&worker.address]);
    let router = router();
    create(
        &router,
        "container-changes",
        NODE,
        &[STORED_MODULE],
        AMPLE_TIMEOUT_SECONDS,
        json!({ "STAGE": "one" }),
    )
    .await;

    invoke(&router, "container-changes", b"{}", &[]).await;
    invoke(&router, "container-changes", b"{}", &[]).await;
    assert_eq!(
        (worker.pushed().len(), worker.called().len()),
        (1, 2),
        "two invocations of an unchanged function are one push and two runs"
    );

    update(
        &router,
        "container-changes",
        "configuration",
        &json!({ "Environment": { "Variables": { "STAGE": "two" } } }),
    )
    .await;
    assert_result(
        "after a configuration update",
        &invoke(&router, "container-changes", b"{}", &[]).await,
        "null",
    );
    let pushed = worker.pushed();
    assert_eq!(pushed.len(), 2, "a changed environment is pushed, got {pushed:?}");
    assert_eq!(pushed[1].variable("STAGE"), Some("two"));
    assert_ne!(
        pushed[1].head["revision"], pushed[0].head["revision"],
        "what changed is a revision of its own"
    );

    let second_module = ("index.js", "exports.handler = async () => 'second build';");
    update(
        &router,
        "container-changes",
        "code",
        &json!({ "ZipFile": encoded(&package(&[second_module])) }),
    )
    .await;
    assert_result(
        "after a code update",
        &invoke(&router, "container-changes", b"{}", &[]).await,
        "null",
    );
    let pushed = worker.pushed();
    assert_eq!(pushed.len(), 3, "changed code is pushed, got {} pushes", pushed.len());
    assert_eq!(
        pushed[2].archives,
        vec![package(&[second_module])],
        "the worker is pushed the package the function stores NOW"
    );
    assert_eq!(pushed[2].variable("STAGE"), Some("two"));
}

/// What travels with an event: the client context the caller sent; and an invocation that
/// carried no payload is the empty event a runtime hands a handler.
#[tokio::test]
async fn the_worker_is_sent_what_the_invocation_carried() {
    let worker = stand_in(|_, _| Reply::returned("null"));
    container::forward("container-carries", &[&worker.address]);
    let router = router();
    create_function(&router, "container-carries", AMPLE_TIMEOUT_SECONDS).await;
    let client_context = encoded(br#"{"custom":{"k":"v"}}"#);

    let with_context = invoke(
        &router,
        "container-carries",
        br#"{"a":1}"#,
        &[("x-amz-client-context", &client_context)],
    )
    .await;
    let without_payload = invoke(&router, "container-carries", b"", &[]).await;

    assert_result("a client context", &with_context, "null");
    assert_result("no payload", &without_payload, "null");
    let called = worker.called();
    assert_eq!(called.len(), 2, "two invocations, got {called:?}");
    let sent_context = called[0].head["clientContext"]
        .as_str()
        .map(|text| serde_json::from_str::<Value>(text).unwrap_or(Value::Null));
    assert_eq!(
        sent_context,
        Some(json!({ "custom": { "k": "v" } })),
        "the client context travels as the document the caller encoded"
    );
    assert!(
        called[1].head["clientContext"].is_null(),
        "an invocation that carried no client context sends none"
    );
    assert_eq!(
        called[1].event(),
        "{}",
        "an invocation with no payload is the empty event"
    );
}

/// What a run came to in its worker is the failure Lambda reports for it — one row per way
/// a run ends without a result.
#[tokio::test]
async fn a_run_that_failed_in_its_worker_is_a_function_error() {
    let worker = stand_in(|_, invocation| match invocation.event().as_str() {
        r#"{"case":"threw"}"# => Reply::of("failed", THREW),
        r#"{"case":"exited"}"# => Reply::of("exited", "").detail("exit status 3"),
        r#"{"case":"timed out"}"# => Reply::of("timedOut", ""),
        _ => Reply::of("tooLarge", ""),
    });
    container::forward("container-fails", &[&worker.address]);
    let router = router();
    create_function(&router, "container-fails", AMPLE_TIMEOUT_SECONDS).await;

    let threw = invoke(&router, "container-fails", br#"{"case":"threw"}"#, &[]).await;
    function_error("the handler threw", &threw);
    assert_eq!(
        threw.text(),
        THREW,
        "the payload is the document the runtime wrote for the throw"
    );

    let exited = invoke(&router, "container-fails", br#"{"case":"exited"}"#, &[]).await;
    let document = function_error("the runtime exited", &exited);
    assert_eq!(document["errorType"], "Runtime.ExitError", "got {document}");
    assert_eq!(
        document["errorMessage"],
        format!(
            "RequestId: {} Error: Runtime exited with error: exit status 3",
            exited.request_id()
        ),
    );

    let timed_out = invoke(&router, "container-fails", br#"{"case":"timed out"}"#, &[]).await;
    let document = function_error("the run outlasted its timeout", &timed_out);
    assert_eq!(document["errorType"], "Sandbox.Timedout", "got {document}");
    assert_eq!(
        document["errorMessage"],
        format!("{} Task timed out after 30.00 seconds", timed_out.request_id()),
        "the message names the invocation and the function's own timeout"
    );

    let too_large = invoke(&router, "container-fails", br#"{"case":"too large"}"#, &[]).await;
    let document = function_error("the response was past the quota", &too_large);
    assert_eq!(
        document["errorType"], "Function.ResponseSizeTooLarge",
        "got {document}"
    );
}

/// A run in a worker is metered where every run is: Lambda publishes a `Duration` datapoint
/// for each invocation that ran and an `Errors` datapoint for each that failed, on the
/// `AWS/Lambda` series CloudWatch answers for, whichever of this emulator's two ways of
/// running a handler ran it.
///
/// The names are the AWS Lambda metric reference's, written as literals, and the series is
/// read where a client reads it: CloudWatch mounted beside Lambda on one engine.
#[tokio::test]
async fn a_run_in_a_worker_is_metered_like_any_other() {
    const FUNCTION: &str = "container-metered";
    let worker = stand_in(|_, invocation| match invocation.event().as_str() {
        r#"{"case":"threw"}"# => Reply::of("failed", THREW),
        event => Reply::returned(event),
    });
    container::forward(FUNCTION, &[&worker.address]);
    let engine = Arc::new(engine::InMemoryEngine::new());
    let mut router = MultiServiceRouter::new();
    router.set_request_authentication(false);
    router.register_service(EmulatedService::new(Arc::clone(&engine)));
    router.register_service(
        service_com_amazonaws_cloudwatch_graniteserviceversion20100801::EmulatedService::new(
            Arc::clone(&engine),
        ),
    );
    create_function(&router, FUNCTION, AMPLE_TIMEOUT_SECONDS).await;

    let returned = invoke(&router, FUNCTION, br#"{"case":"returned"}"#, &[]).await;
    assert_result("the handler returned", &returned, r#"{"case":"returned"}"#);
    let threw = invoke(&router, FUNCTION, br#"{"case":"threw"}"#, &[]).await;
    function_error("the handler threw", &threw);
    assert_eq!(worker.called().len(), 2, "both invocations ran in the worker");

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    let dimensions = json!([{ "Name": "FunctionName", "Value": FUNCTION }]);
    let mut totals = Vec::new();
    for (metric, unit, statistic) in [
        ("Duration", "Milliseconds", "SampleCount"),
        ("Errors", "Count", "Sum"),
    ] {
        let (status, stats) = crate::kinesis_event_source_delivery::cloudwatch(
            &router,
            "GetMetricStatistics",
            &json!({
                "Namespace": "AWS/Lambda",
                "MetricName": metric,
                "Dimensions": dimensions,
                "StartTime": now - 600.0,
                "EndTime": now + 600.0,
                "Period": 60,
                "Statistics": ["Sum", "SampleCount"],
                "Unit": unit,
            }),
        )
        .await;
        assert_eq!(status, 200, "GetMetricStatistics({metric}) answered {stats}");
        let total: f64 = stats["Datapoints"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|point| point[statistic].as_f64())
            .sum();
        totals.push((metric, total));
    }
    assert_eq!(
        totals,
        [("Duration", 2.0), ("Errors", 1.0)],
        "two runs were measured and one of them failed"
    );
}

/// The worker says whether a handler returned or threw, so a RESULT is a result whatever
/// it looks like — the very document a runtime writes for a throw included.
#[tokio::test]
async fn a_result_is_a_result_whatever_it_looks_like() {
    let worker = stand_in(|_, invocation| Reply::returned(&invocation.event()));
    container::forward("container-lookalike", &[&worker.address]);
    let router = router();
    create_function(&router, "container-lookalike", AMPLE_TIMEOUT_SECONDS).await;

    for (case, payload) in [
        ("the document of a throw", THREW),
        (
            "an error's two members",
            r#"{"errorType":"NotReally","errorMessage":"returned"}"#,
        ),
        ("a line of text", r#""Task timed out after 3.00 seconds""#),
        ("nothing", "null"),
    ] {
        let answered = invoke(&router, "container-lookalike", payload.as_bytes(), &[]).await;
        assert_result(case, &answered, payload);
    }
}

/// What a run wrote in its worker is the invocation's log, between the lines Lambda frames
/// a run's log with — the last of which closes with the memory the worker measured the run
/// holding, where it measured any.
#[tokio::test]
async fn what_a_run_wrote_is_the_invocations_log() {
    let worker = stand_in(|ordinal, _| {
        let run = Reply::returned("null").log("INFO\tconnected to postgres\nINFO\twrote 3 rows\n");
        match ordinal {
            0 => run.max_memory_mb(87),
            _ => run,
        }
    });
    container::forward("container-logs", &[&worker.address]);
    let router = router();
    create_function(&router, "container-logs", AMPLE_TIMEOUT_SECONDS).await;

    let answered = invoke(
        &router,
        "container-logs",
        b"{}",
        &[("x-amz-log-type", "Tail")],
    )
    .await;

    assert_result("a run that wrote two lines", &answered, "null");
    let tail = answered
        .header("x-amz-log-result")
        .map(|encoded| {
            base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .unwrap_or_default()
        })
        .map(|decoded| String::from_utf8_lossy(&decoded).to_string())
        .unwrap_or_default();
    let lines: Vec<&str> = tail.lines().collect();
    let id = answered.request_id();
    assert_eq!(
        lines.len(),
        5,
        "the log is the run's two lines inside Lambda's three, got {lines:?}"
    );
    assert_eq!(lines[0], format!("START RequestId: {id} Version: $LATEST"));
    assert_eq!(
        lines[1..3],
        ["INFO\tconnected to postgres", "INFO\twrote 3 rows"]
    );
    assert_eq!(lines[3], format!("END RequestId: {id}"));
    let report: Vec<&str> = lines[4].split('\t').collect();
    assert_eq!(report.len(), 5, "the closing line states five things, got {report:?}");
    assert_eq!(report[0], format!("REPORT RequestId: {id}"));
    for (member, opens) in report[1..4]
        .iter()
        .zip(["Duration: ", "Billed Duration: ", "Memory Size: "])
    {
        assert!(member.starts_with(opens), "got {report:?}");
    }
    assert_eq!(
        report[4], "Max Memory Used: 87 MB",
        "the memory the worker measured closes the line"
    );

    let unmeasured = invoke(
        &router,
        "container-logs",
        b"{}",
        &[("x-amz-log-type", "Tail")],
    )
    .await;
    let tail = unmeasured
        .header("x-amz-log-result")
        .map(|encoded| {
            base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .unwrap_or_default()
        })
        .map(|decoded| String::from_utf8_lossy(&decoded).to_string())
        .unwrap_or_default();
    assert!(
        !tail.contains("Max Memory Used"),
        "a run nobody measured states no memory figure, got {tail:?}"
    );
}

/// Invocations of one function that arrive together take TURNS in its one worker, which
/// is pushed the function once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invocations_that_arrive_together_take_turns_in_one_worker() {
    let worker =
        stand_in(|ordinal, _| Reply::returned(&ordinal.to_string()).after(Duration::from_millis(200)));
    container::forward("container-queued", &[&worker.address]);
    let router = router();
    create_function(&router, "container-queued", AMPLE_TIMEOUT_SECONDS).await;

    let (a, b, c, d) = tokio::join!(
        invoke(&router, "container-queued", b"{}", &[]),
        invoke(&router, "container-queued", b"{}", &[]),
        invoke(&router, "container-queued", b"{}", &[]),
        invoke(&router, "container-queued", b"{}", &[]),
    );

    let mut answers: Vec<String> = [&a, &b, &c, &d]
        .iter()
        .map(|answered| {
            assert_eq!(answered.status, 200, "got {}", answered.text());
            assert_eq!(answered.header(FUNCTION_ERROR_HEADER), None);
            answered.text()
        })
        .collect();
    answers.sort();
    assert_eq!(
        answers,
        ["0", "1", "2", "3"],
        "each invocation is answered by a run of its own"
    );
    assert_eq!(
        worker.most_at_once(),
        1,
        "a worker must never be sent an invocation while one is running in it"
    );
    assert_eq!(worker.pushed().len(), 1, "and it is pushed the function once");
}

/// A pool of SEVERAL workers runs that many invocations at once: each worker still takes
/// one at a time, and none of them idles while an invocation waits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pool_of_workers_runs_that_many_invocations_at_once() {
    /// How long one run takes. Four of them in turn would take four times this.
    const RUN: Duration = Duration::from_millis(400);
    let first = stand_in(|_, _| Reply::returned(r#""first""#).after(RUN));
    let second = stand_in(|_, _| Reply::returned(r#""second""#).after(RUN));
    container::forward("container-pooled", &[&first.address, &second.address]);
    let router = router();
    create_function(&router, "container-pooled", AMPLE_TIMEOUT_SECONDS).await;

    let began = Instant::now();
    let (a, b, c, d) = tokio::join!(
        invoke(&router, "container-pooled", b"{}", &[]),
        invoke(&router, "container-pooled", b"{}", &[]),
        invoke(&router, "container-pooled", b"{}", &[]),
        invoke(&router, "container-pooled", b"{}", &[]),
    );
    let took = began.elapsed();

    for answered in [&a, &b, &c, &d] {
        assert_eq!(answered.status, 200, "got {}", answered.text());
        assert_eq!(answered.header(FUNCTION_ERROR_HEADER), None);
    }
    assert_eq!(
        (first.called().len(), second.called().len()),
        (2, 2),
        "four invocations over two workers are two runs in each"
    );
    assert_eq!(first.most_at_once(), 1, "the first worker takes one at a time");
    assert_eq!(second.most_at_once(), 1, "the second worker takes one at a time");
    assert!(
        took < RUN * 4,
        "two workers must run two invocations at once: four runs of {RUN:?} took {took:?}"
    );
}

/// Workers are generic: one pool serves every function its pattern covers, and a function
/// is run by the worker that already HOLDS it, so two functions invoked in turn are each
/// pushed once rather than displacing one another on every invocation.
#[tokio::test]
async fn a_function_is_run_by_the_worker_that_already_holds_it() {
    let first = stand_in(|_, _| Reply::returned("null"));
    let second = stand_in(|_, _| Reply::returned("null"));
    container::forward("container-kept-*", &[&first.address, &second.address]);
    let router = router();
    create_function(&router, "container-kept-orders", AMPLE_TIMEOUT_SECONDS).await;
    create_function(&router, "container-kept-billing", AMPLE_TIMEOUT_SECONDS).await;

    for _ in 0..3 {
        for name in ["container-kept-orders", "container-kept-billing"] {
            assert_result(name, &invoke(&router, name, b"{}", &[]).await, "null");
        }
    }

    let held = |worker: &StandIn| -> Vec<String> {
        worker
            .pushed()
            .iter()
            .map(|pushed| pushed.head["functionName"].as_str().unwrap_or_default().to_string())
            .collect()
    };
    let mut pushes = [held(&first), held(&second)].concat();
    pushes.sort();
    assert_eq!(
        pushes,
        ["container-kept-billing", "container-kept-orders"],
        "six invocations of two functions are one push of each"
    );
    assert_eq!(
        (first.called().len(), second.called().len()),
        (3, 3),
        "each worker ran the function it holds"
    );
}

/// A run its caller stopped waiting for is still that worker's run until the worker
/// answers: the caller is answered the sandbox timeout, and the next invocation waits for
/// the worker rather than arriving on top of the run still in it.
#[tokio::test]
async fn a_run_its_caller_gave_up_on_keeps_its_worker_until_it_answers() {
    /// The function's own `Timeout`, in seconds.
    const TIMEOUT_SECONDS: u64 = 1;
    // How long this emulator waits for a worker's answer: the function's timeout, which the
    // worker enforces, and the allowance on top of it.
    let caller_waits = Duration::from_secs(TIMEOUT_SECONDS) + container::ANSWER_ALLOWANCE;
    // A worker that has stopped answering holds its run for longer than that.
    let first_run = caller_waits + Duration::from_secs(2);
    let worker = stand_in(move |ordinal, _| match ordinal {
        0 => Reply::returned(r#"{"n":1}"#).after(first_run),
        _ => Reply::returned(r#"{"n":2}"#),
    });
    container::forward("container-outlasted", &[&worker.address]);
    let router = router();
    create_function(&router, "container-outlasted", TIMEOUT_SECONDS as i64).await;

    let began = Instant::now();
    let first = invoke(&router, "container-outlasted", b"{}", &[]).await;
    let first_answered_after = began.elapsed();
    let second = invoke(&router, "container-outlasted", b"{}", &[]).await;
    let second_answered_after = began.elapsed();

    let document = function_error("the caller stopped waiting", &first);
    assert_eq!(document["errorType"], "Sandbox.Timedout", "got {document}");
    assert_eq!(
        document["errorMessage"],
        format!("{} Task timed out after 1.00 seconds", first.request_id()),
    );
    assert!(
        first_answered_after < first_run,
        "the caller is answered when its wait runs out, not when the worker gets round to \
         it: waited {first_answered_after:?}"
    );
    assert_result("the invocation after it", &second, r#"{"n":2}"#);
    assert!(
        second_answered_after >= first_run,
        "the next invocation must wait for the run still in the worker: it was answered \
         {second_answered_after:?} after a run of {first_run:?} began"
    );
    assert_eq!(
        worker.most_at_once(),
        1,
        "a worker must never be sent an invocation while one is running in it"
    );
}

/// An asynchronous invocation is run by a worker too: every way of invoking a forwarded
/// function reaches one.
#[tokio::test]
async fn an_asynchronous_invocation_is_run_by_a_worker_too() {
    let worker = stand_in(|_, _| Reply::returned("null"));
    container::forward("container-async", &[&worker.address]);
    let router = router();
    create_function(&router, "container-async", AMPLE_TIMEOUT_SECONDS).await;

    let accepted = invoke(
        &router,
        "container-async",
        br#"{"queued":true}"#,
        &[("x-amz-invocation-type", "Event")],
    )
    .await;

    assert_eq!(
        accepted.status,
        202,
        "an asynchronous invocation is accepted, got {} {}",
        accepted.status,
        accepted.text()
    );
    let deadline = Instant::now() + Duration::from_secs(20);
    while worker.called().is_empty() && Instant::now() < deadline {
        tokio::time::sleep(SLACK).await;
    }
    let called = worker.called();
    assert_eq!(
        called.first().map(Called::event).as_deref(),
        Some(r#"{"queued":true}"#),
        "the queued event must reach the worker, got {called:?}"
    );
}

/// Only a Node.js function the worker table covers is forwarded: a function it does not
/// name, and a function of another runtime that it does, are run by the embedded engine as
/// they were.
#[tokio::test]
async fn only_a_node_function_the_table_covers_is_forwarded() {
    let _stream = one_invocation_stream_at_a_time().await;
    let worker = stand_in(|_, _| Reply::returned(r#""ran in a worker""#));
    container::forward("container-python-*", &[&worker.address]);
    let router = router();
    create_function(&router, "container-unlisted", AMPLE_TIMEOUT_SECONDS).await;
    create(
        &router,
        "container-python-fn",
        "python3.12",
        &[(
            "index.py",
            "def handler(event, context):\n    return {'ran': 'in-process'}\n",
        )],
        AMPLE_TIMEOUT_SECONDS,
        json!({}),
    )
    .await;

    for (case, name, expected) in [
        ("a function the table does not name", "container-unlisted", RAN_IN_PROCESS),
        (
            "a Python function the table names",
            "container-python-fn",
            r#"{"ran": "in-process"}"#,
        ),
    ] {
        let mut answered = invoke(&router, name, b"{}", &[]).await;
        for _ in 0..5 {
            if !is_host_bound(answered.status, &answered.text()) {
                break;
            }
            tokio::time::sleep(SLACK).await;
            answered = invoke(&router, name, b"{}", &[]).await;
        }
        assert_result(case, &answered, expected);
    }
    assert!(
        worker.pushed().is_empty() && worker.called().is_empty(),
        "nothing reaches a worker for either"
    );
}

/// A worker that cannot run a function — nothing listens at its address, it refuses the
/// push, or what it answers an invocation with is nothing a run produces — is the
/// SERVICE's failure: no payload is invented for it, and nothing marks a function as
/// having failed.
#[tokio::test]
async fn a_worker_that_cannot_answer_is_the_services_failure() {
    let router = router();

    let nowhere = unreachable_address();
    container::forward("container-unreachable", &[&nowhere]);
    create_function(&router, "container-unreachable", AMPLE_TIMEOUT_SECONDS).await;
    let unreachable = invoke(&router, "container-unreachable", b"{}", &[]).await;

    let refusing = stand_in_taking(Some((500, "out of disk")), |_, _| Reply::returned("null"));
    container::forward("container-refused", &[&refusing.address]);
    create_function(&router, "container-refused", AMPLE_TIMEOUT_SECONDS).await;
    let refused = invoke(&router, "container-refused", b"{}", &[]).await;

    let broken = stand_in(|_, _| Reply::raw(500, "InternalError"));
    container::forward("container-broken", &[&broken.address]);
    create_function(&router, "container-broken", AMPLE_TIMEOUT_SECONDS).await;
    let unusable = invoke(&router, "container-broken", b"{}", &[]).await;

    for (case, answered) in [
        ("nothing listens", &unreachable),
        ("the push is refused", &refused),
        ("no run's answer", &unusable),
    ] {
        assert_eq!(
            answered.status,
            500,
            "case {case}: the service failed, got {} {}",
            answered.status,
            answered.text()
        );
        assert_eq!(
            answered.error_code(),
            "ServiceException",
            "case {case}: got {}",
            answered.text()
        );
        assert_eq!(
            answered.header(FUNCTION_ERROR_HEADER),
            None,
            "case {case}: no function ran, so none failed"
        );
    }
    assert!(
        unreachable.text().contains(&nowhere),
        "the refusal names the worker that could not be reached, got {}",
        unreachable.text()
    );
    assert!(
        refusing.called().is_empty(),
        "a worker that did not take the function is not sent its invocation"
    );
}

/// A package a worker cannot unpack is the PACKAGE's failure, as it is for a package the
/// embedded engine cannot read.
#[tokio::test]
async fn a_package_a_worker_cannot_unpack_is_the_packages_failure() {
    let worker = stand_in_taking(
        Some((422, r#"{"error":"package","message":"'index.js' is encrypted"}"#)),
        |_, _| Reply::returned("null"),
    );
    container::forward("container-unpackable", &[&worker.address]);
    let router = router();
    create_function(&router, "container-unpackable", AMPLE_TIMEOUT_SECONDS).await;

    let answered = invoke(&router, "container-unpackable", b"{}", &[]).await;

    assert_eq!(
        answered.error_code(),
        "InvalidZipFileException",
        "got {} {}",
        answered.status,
        answered.text()
    );
    assert!(
        answered.text().contains("'index.js' is encrypted"),
        "the refusal says what the worker could not read, got {}",
        answered.text()
    );
}
