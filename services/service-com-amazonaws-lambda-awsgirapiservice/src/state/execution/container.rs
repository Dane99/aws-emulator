// SPDX-License-Identifier: CC0-1.0

//! RUNNING a function in a WORKER: a container that runs its handler for real.
//!
//! FORK-ONLY and OPT-IN. The engines this emulator embeds ([`super::python`],
//! [`super::node`]) run a handler in this process with no host access, so a handler cannot
//! open a network connection of its own — to a database, say. Where the operator names
//! WORKERS ([`WORKERS_ENV`]), a Node.js function they cover is not run by an embedded engine
//! at all: it is run by the real Node.js runtime, in a container built from the AWS Lambda
//! base image and fronted by the worker agent (`docker/worker/agent.mjs`). A function no
//! worker covers, and a function of any other runtime, is run in process exactly as before.
//!
//! This KNOWINGLY BREAKS the containment rule the shared engine states for execution
//! (`vendor/engine/src/lib.rs`: "no outbound connection to any other host"): the emulator
//! makes a connection to the worker, and the handler in it reaches whatever its own network
//! reaches. That is the point of the fork, and why nothing is forwarded unless the operator
//! names the workers.
//!
//! # A worker is generic
//!
//! A worker holds no function of its own. Before a function is invoked in one, this
//! emulator PUSHES the worker what the function is — the deployment package and the layers
//! it stores, the handler, the timeout and the environment — and the worker runs that until
//! it is pushed something else. Whether a push is needed is the WORKER's to say: every
//! invocation names the [revision](Function::revision) it is an invocation of, a worker that
//! holds another answers that it does not, and the function is then pushed and the
//! invocation made again. So what runs is always what the function is NOW — a deploy, an
//! `UpdateFunctionConfiguration`, a new session of the execution role each make a new
//! revision — and a worker that was restarted is simply pushed again.
//!
//! Everything AROUND the run stays this emulator's: resolving the function and its version,
//! authorizing the caller, the concurrency it is admitted under, the asynchronous queue and
//! the event source poller all happen before an invocation reaches here, and every caller
//! of [`super::run_invocation`] is served alike. The log a run wrote comes back with its
//! answer and is delivered where every run's log is.
//!
//! # What the handler is given to call AWS with
//!
//! A handler in a worker is outside this process, so its own AWS SDK reaches this emulator
//! the way any client does — over the data port, with a signed request:
//!
//! * `AWS_ENDPOINT_URL` names the data port ([`HANDLER_ENDPOINT_ENV`] where the worker
//!   reaches it at another address than the published one), and the worker is handed the
//!   certificate the port is served with, which the agent makes the runtime trust;
//! * the worker is told that address a second time, as the address every SIGNED request
//!   the handler addresses to an AWS endpoint is connected to instead
//!   (`docker/worker/aws-transport.cjs`). A queue URL is reported under the service's
//!   regional endpoint, and the SDK for JavaScript sends to a queue URL's own host, so
//!   without this a handler's `SendMessage` would leave for the real service;
//! * `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN` are the SESSION of
//!   the function's execution role ([`super::execution_role_session`]) — the credential an
//!   embedded run's calls are signed with by host code, and the one Lambda itself puts in a
//!   function's environment. This is the one place that credential leaves the process: it
//!   is a created credential of the role and never this emulator's own, and it is in no log.
//!   A function whose execution role this emulator holds no record of has no session, and
//!   its handler is given no credential at all.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError, RwLock};
use std::time::{Duration, Instant, SystemTime};

use super::{Budget, Completion, Engine, Finished, Invocation, Invoked, Unrunnable};

/// The environment variable that names the workers and the functions each set of them
/// runs: `<pattern>=<url>[|<url>…]`, entries separated by commas.
///
/// A pattern is a function name in which `*` stands for any run of characters; the FIRST
/// entry whose pattern covers a function is the one that runs it, and an entry with no
/// pattern at all (`http://127.0.0.1:8081|http://127.0.0.1:8082`) covers every function. A
/// URL is a worker's base address. The workers of one entry are a pool: they run that many
/// invocations at once, and any of them runs any function the entry covers.
pub const WORKERS_ENV: &str = "EMULATOR_LAMBDA_WORKERS";

/// The environment variable that names the address a HANDLER reaches this emulator's data
/// port at, where that is not the address the port is published at — a worker on another
/// network than the emulator's own. Unset, a handler is given the published address.
pub const HANDLER_ENDPOINT_ENV: &str = "EMULATOR_LAMBDA_HANDLER_ENDPOINT_URL";

/// The variable an earlier form of this module read, which named one container PER
/// FUNCTION, each running code built into its image. Workers replaced it, and the two do
/// not speak the same protocol, so a process that still sets it is told rather than left
/// running its functions somewhere it did not ask for.
const RETIRED_CONTAINERS_ENV: &str = "EMULATOR_LAMBDA_CONTAINERS";

/// The path a worker takes a function on.
pub const FUNCTION_PATH: &str = "/function";

/// The path a worker runs one invocation on.
pub const INVOKE_PATH: &str = "/invoke";

/// What separates one entry from the next in [`WORKERS_ENV`].
const ENTRY_SEPARATOR: char = ',';

/// What separates an entry's pattern from its workers.
const PATTERN_SEPARATOR: char = '=';

/// What separates the workers of one entry.
const WORKER_SEPARATOR: char = '|';

/// What stands for any run of characters in a pattern.
const WILDCARD: char = '*';

/// The pattern of an entry that names none.
const EVERY_FUNCTION: &str = "*";

/// The `Runtime` values a worker runs: its image is the Node.js one, so a function of any
/// other runtime is not a function it could run.
const WORKER_RUNTIME_PREFIX: &str = "nodejs";

/// How long past the function's own timeout a worker's answer is waited for.
///
/// The WORKER ends a run at the function's timeout and answers that it did, so this
/// emulator's own wait is only for a worker that has stopped answering: the timeout, the
/// span a runtime may spend initializing first — which the worker gives a runtime that is
/// still initializing when its invocation arrives, and which is the span an embedded run's
/// initialization is waited for ([`super::INIT_PHASE_LIMIT`]) — and the grace an embedded
/// run is given on top of its budget.
pub const ANSWER_ALLOWANCE: Duration =
    super::INIT_PHASE_LIMIT.saturating_add(super::ABANDON_GRACE);

/// The longest a push of one function lasts: the package is sent and unpacked in it.
const PUSH_CEILING: Duration = Duration::from_secs(120);

/// How long a connection to a worker may take to open. A worker is on this host or beside
/// it, so one that has not accepted by now is not there.
const CONNECT_WAIT: Duration = Duration::from_secs(5);

/// The event a runtime hands a handler for an invocation that carried no payload, as the
/// embedded engines hand it.
const EMPTY_EVENT: &[u8] = b"{}";

/// How much of a worker's answer is read beyond the response and the log it may carry.
const ANSWER_HEAD_CEILING: usize = 64 * 1024;

/// One worker of a pool.
#[derive(Debug)]
struct Worker {
    /// Its base URL.
    url: String,
    /// Whether an exchange with it is in flight.
    busy: bool,
    /// The revision it was last known to hold. Only ever a HINT for choosing a worker and
    /// for whether to push before invoking: the worker itself is what says what it holds.
    holding: Option<String>,
    /// When it last ran something, in the pool's own count of uses.
    last_used: u64,
}

/// The workers of one entry, and which of them are idle.
///
/// A worker runs one invocation at a time, as a Lambda execution environment does, so it
/// is HELD for the whole of one exchange and handed to nobody else until that exchange is
/// over — including an exchange whose caller has stopped waiting for it — and a caller that
/// finds every worker held queues for one.
#[derive(Debug)]
pub struct Pool {
    workers: Mutex<Vec<Worker>>,
    /// One permit per worker: what a caller queues on.
    slots: Arc<::tokio::sync::Semaphore>,
    /// How many exchanges this pool has started, which orders its workers by use.
    uses: AtomicU64,
}

impl Pool {
    /// A pool of the workers at `urls`, all of them idle and none known to hold anything.
    fn of(urls: Vec<String>) -> Self {
        Self {
            slots: Arc::new(::tokio::sync::Semaphore::new(urls.len())),
            workers: Mutex::new(
                urls.into_iter()
                    .map(|url| Worker {
                        url,
                        busy: false,
                        holding: None,
                        last_used: 0,
                    })
                    .collect(),
            ),
            uses: AtomicU64::new(0),
        }
    }

    /// Take one of this pool's workers for an invocation of `revision`, queueing for it
    /// while they are all held.
    ///
    /// The wait is the one an invocation of an embedded engine waits for its run memory
    /// ([`super::PERMIT_WAIT`]) and ends the same way: a function whose workers are all
    /// still busy after it is answered with the modelled throttle.
    async fn held(self: &Arc<Self>, revision: &str) -> Result<Held, Unrunnable> {
        let slot = ::tokio::time::timeout(
            super::PERMIT_WAIT,
            Arc::clone(&self.slots).acquire_owned(),
        )
        .await
        .map_err(|_| {
            ::tracing::warn!(
                waited_seconds = super::PERMIT_WAIT.as_secs(),
                "every worker that runs this function was still running an invocation after \
                 the caller's own wait, so this invocation was throttled rather than started"
            );
            Unrunnable::Overloaded(
                super::super::execution_slot::THROTTLED_INVOCATION_MESSAGE.to_string(),
            )
        })?
        .map_err(|error| Unrunnable::Internal(format!("the worker slots are gone: {error}")))?;
        let mut workers = self.workers.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(index) = chosen(&workers, revision) else {
            return Err(Unrunnable::Internal(
                "a worker slot came free with no worker idle behind it".to_string(),
            ));
        };
        let worker = &mut workers[index];
        worker.busy = true;
        worker.last_used = self.uses.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(Held {
            pool: Arc::clone(self),
            index,
            url: worker.url.clone(),
            holding: worker.holding.clone(),
            _slot: slot,
        })
    }
}

/// Which idle worker runs an invocation of `revision`: one that already holds it, so
/// nothing is pushed and the handler keeps what it has open; failing that one that holds
/// nothing; failing that the one that has gone longest without running anything, whose
/// function is displaced.
fn chosen(workers: &[Worker], revision: &str) -> Option<usize> {
    workers
        .iter()
        .enumerate()
        .filter(|(_, worker)| !worker.busy)
        .min_by_key(|(_, worker)| {
            let preference = match worker.holding.as_deref() {
                Some(held) if held == revision => 0,
                None => 1,
                Some(_) => 2,
            };
            (preference, worker.last_used)
        })
        .map(|(index, _)| index)
}

/// One worker, for as long as one exchange with it lasts.
struct Held {
    /// The pool the worker goes back to.
    pool: Arc<Pool>,
    /// Which of the pool's workers this is.
    index: usize,
    /// The worker's base URL.
    url: String,
    /// The revision the worker is known to hold, as the exchange leaves it.
    holding: Option<String>,
    /// The pool's permit for it, released after the worker is marked idle.
    _slot: ::tokio::sync::OwnedSemaphorePermit,
}

impl Drop for Held {
    fn drop(&mut self) {
        // Idle BEFORE the permit is released — the fields drop after this body — so whoever
        // the permit wakes always finds a worker to take.
        let mut workers = self.pool.workers.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(worker) = workers.get_mut(self.index) {
            worker.busy = false;
            worker.holding = self.holding.take();
        }
    }
}

/// Where an invocation of one function is run.
#[derive(Debug, Clone)]
pub enum Routed {
    /// By the engine this emulator embeds for the function's runtime.
    InProcess,
    /// By a worker of this pool.
    Worker(Arc<Pool>),
    /// Nowhere: the worker table is set and cannot be acted on, and this is why.
    ///
    /// A table that cannot be read routes NOTHING rather than falling back to the embedded
    /// engines: the operator asked for workers, and a handler quietly run in process
    /// instead would fail somewhere far from the cause.
    Misconfigured(String),
}

/// One entry of the worker table: the functions it covers and the workers that run them.
#[derive(Debug)]
struct Entry {
    pattern: String,
    pool: Arc<Pool>,
}

/// The worker table: its entries in the order they are tried.
#[derive(Debug, Default)]
struct Table {
    /// Why the environment's table could not be acted on, when it could not.
    misconfigured: Option<String>,
    entries: Vec<Entry>,
}

impl Table {
    /// The table the environment states, or an empty one where it states none.
    fn from_environment() -> Self {
        let refused = |why: String| {
            ::tracing::error!(reason = why.as_str(), "no function can be invoked");
            Self {
                misconfigured: Some(why),
                entries: Vec::new(),
            }
        };
        if ::std::env::var_os(RETIRED_CONTAINERS_ENV).is_some() {
            return refused(format!(
                "{RETIRED_CONTAINERS_ENV} is set, and is no longer read: functions are run by \
                 generic workers the emulator pushes each function to, named in {WORKERS_ENV} \
                 (see docker/docker-compose.yml)"
            ));
        }
        let Ok(text) = ::std::env::var(WORKERS_ENV) else {
            return Self::default();
        };
        match parse_workers(&text) {
            Ok(entries) => {
                for (pattern, workers) in &entries {
                    ::tracing::info!(
                        functions = pattern.as_str(),
                        workers = workers.join(" ").as_str(),
                        "Node.js functions this pattern covers are run by these workers"
                    );
                }
                Self {
                    misconfigured: None,
                    entries: entries
                        .into_iter()
                        .map(|(pattern, workers)| Entry {
                            pattern,
                            pool: Arc::new(Pool::of(workers)),
                        })
                        .collect(),
                }
            }
            Err(why) => refused(format!("{WORKERS_ENV} could not be read: {why}")),
        }
    }
}

/// The one table of this process, read from the environment the first time it is asked for.
fn table() -> &'static RwLock<Table> {
    static TABLE: OnceLock<RwLock<Table>> = OnceLock::new();
    TABLE.get_or_init(|| RwLock::new(Table::from_environment()))
}

/// Where an invocation of the function `function_name`, whose `Runtime` is `runtime`, is
/// run.
#[must_use]
pub fn routed(function_name: &str, runtime: Option<&str>) -> Routed {
    let table = table().read().unwrap_or_else(PoisonError::into_inner);
    if let Some(why) = &table.misconfigured {
        return Routed::Misconfigured(why.clone());
    }
    if !runtime.is_some_and(|runtime| runtime.starts_with(WORKER_RUNTIME_PREFIX)) {
        return Routed::InProcess;
    }
    table
        .entries
        .iter()
        .find(|entry| covers(&entry.pattern, function_name))
        .map_or(Routed::InProcess, |entry| {
            Routed::Worker(Arc::clone(&entry.pool))
        })
}

/// Run the functions `pattern` covers in `workers` from now on, ahead of every entry the
/// table already holds and in place of one with the same pattern.
///
/// The in-process way to state one entry of [`WORKERS_ENV`], for a caller that learns a
/// worker's address only once it is running — a test that stands one up on a port the host
/// chose. Nothing is checked here that [`parse_workers`] checks across a whole table: the
/// caller names each worker once.
pub fn forward(pattern: &str, workers: &[&str]) {
    let pool = Pool::of(
        workers
            .iter()
            .map(|worker| worker.trim_end_matches('/').to_string())
            .collect(),
    );
    let mut table = table().write().unwrap_or_else(PoisonError::into_inner);
    table.entries.retain(|entry| entry.pattern != pattern);
    table.entries.insert(
        0,
        Entry {
            pattern: pattern.to_string(),
            pool: Arc::new(pool),
        },
    );
}

/// Whether `pattern` covers the function `name`: the two are equal, with each `*` of the
/// pattern standing for any run of characters, an empty one included.
#[must_use]
pub fn covers(pattern: &str, name: &str) -> bool {
    let mut pieces = pattern.split(WILDCARD);
    let Some(mut rest) = pieces.next().and_then(|first| name.strip_prefix(first)) else {
        return false;
    };
    let pieces: Vec<&str> = pieces.collect();
    // A pattern with no wildcard is a name: nothing may follow what it spells.
    let Some((last, middle)) = pieces.split_last() else {
        return rest.is_empty();
    };
    for piece in middle {
        match rest.find(piece) {
            Some(at) => rest = &rest[at + piece.len()..],
            None => return false,
        }
    }
    rest.ends_with(last)
}

/// Read the worker table `text` states: each entry's pattern with the base URLs of its
/// workers, in the order the entries are tried.
///
/// A table is refused as a whole, with the entry that is wrong, rather than read around
/// it. Naming one worker TWICE — in one entry or in two — is refused too: each entry's pool
/// hands its workers out on its own, so two entries for one worker would let two
/// invocations into it at once.
pub fn parse_workers(text: &str) -> Result<Vec<(String, Vec<String>)>, String> {
    let mut entries: Vec<(String, Vec<String>)> = Vec::new();
    let mut named: BTreeSet<String> = BTreeSet::new();
    let stated = text
        .split(ENTRY_SEPARATOR)
        .map(str::trim)
        .filter(|entry| !entry.is_empty());
    for entry in stated {
        let (pattern, workers) = match entry.split_once(PATTERN_SEPARATOR) {
            Some((pattern, workers)) => (pattern.trim(), workers),
            None => (EVERY_FUNCTION, entry),
        };
        if pattern.is_empty() {
            return Err(format!("'{entry}' names no function before its workers"));
        }
        let mut urls = Vec::new();
        for worker in workers.split(WORKER_SEPARATOR) {
            let url = worker.trim().trim_end_matches('/');
            let authority = url
                .strip_prefix("http://")
                .or_else(|| url.strip_prefix("https://"));
            if !authority.is_some_and(|authority| !authority.is_empty()) {
                return Err(format!(
                    "'{}' is not a worker's address for '{pattern}': expected \
                     http://<host>:<port>",
                    worker.trim()
                ));
            }
            if !named.insert(url.to_string()) {
                return Err(format!(
                    "the worker '{url}' is named more than once, and a worker runs one \
                     invocation at a time"
                ));
            }
            urls.push(url.to_string());
        }
        if entries.iter().any(|(stated, _)| stated == pattern) {
            return Err(format!("the pattern '{pattern}' is stated more than once"));
        }
        entries.push((pattern.to_string(), urls));
    }
    Ok(entries)
}

/// What a worker is pushed for one function, and the revision that names it.
#[derive(Debug)]
struct Function {
    /// The name of everything a worker would run for this invocation: a digest over the
    /// function's identity, handler, timeout and environment — the session it calls AWS
    /// with included — and over every byte of its package and layers. Two invocations name
    /// one revision exactly when a worker that holds it has nothing to be pushed.
    revision: String,
    /// The handler the runtime resolves.
    handler: String,
    /// The environment the runtime is started with.
    environment: BTreeMap<String, String>,
    /// The certificate this emulator's data port is served with, for the handler to trust.
    ca_pem: Option<String>,
    /// The address the handler reaches this emulator's data port at.
    endpoint: Option<String>,
}

impl Function {
    /// What a worker is pushed for `invocation`.
    fn of(invocation: &Invocation, handler: &str, budget: Budget) -> Self {
        let endpoint = ::std::env::var(HANDLER_ENDPOINT_ENV).ok().or_else(|| {
            invocation
                .aws
                .as_ref()
                .and_then(super::AwsAccess::published_endpoint)
                .map(str::to_string)
        });
        let environment = handler_environment(invocation, endpoint.as_deref());
        let ca_pem = invocation
            .aws
            .as_ref()
            .map(super::AwsAccess::certificate_authority_pem);
        // The digest reads the same members in the same order every time, each one framed
        // by its length, so no two functions spell the same bytes.
        let mut digest = <::sha2::Sha256 as ::sha2::Digest>::new();
        let mut read = |member: &[u8]| {
            ::sha2::Digest::update(&mut digest, (member.len() as u64).to_be_bytes());
            ::sha2::Digest::update(&mut digest, member);
        };
        read(invocation.function_name.as_bytes());
        read(invocation.executed_version.as_bytes());
        read(handler.as_bytes());
        read(invocation.runtime.as_deref().unwrap_or_default().as_bytes());
        read(&budget.wall_clock.as_millis().to_be_bytes());
        for (name, value) in &environment {
            read(name.as_bytes());
            read(value.as_bytes());
        }
        read(ca_pem.as_deref().unwrap_or_default().as_bytes());
        read(endpoint.as_deref().unwrap_or_default().as_bytes());
        read(invocation.zip_bytes.as_deref().unwrap_or_default());
        for layer in &invocation.layer_zips {
            read(layer.as_slice());
        }
        Self {
            revision: ::hex::encode(::sha2::Digest::finalize(digest)),
            handler: handler.to_string(),
            environment,
            ca_pem,
            endpoint,
        }
    }
}

/// The environment a handler in a worker runs with, apart from what differs from one
/// invocation to the next: the function's own variables, the ones a runtime sets beside
/// them, and what the handler's own AWS SDK reaches this emulator with — at `endpoint` —
/// (see the module documentation).
fn handler_environment(invocation: &Invocation, endpoint: Option<&str>) -> BTreeMap<String, String> {
    let mut environment = BTreeMap::new();
    // What a sandbox sets and a function may set otherwise.
    environment.insert("TZ".to_string(), ":UTC".to_string());
    if let Some(endpoint) = endpoint {
        environment.insert("AWS_ENDPOINT_URL".to_string(), endpoint.to_string());
    }
    environment.extend(invocation.variables.clone());
    // What a runtime sets and a function may not.
    let reserved: [(&str, String); 6] = [
        ("AWS_LAMBDA_FUNCTION_NAME", invocation.function_name.clone()),
        (
            "AWS_LAMBDA_FUNCTION_VERSION",
            invocation.executed_version.clone(),
        ),
        (
            "AWS_LAMBDA_FUNCTION_MEMORY_SIZE",
            invocation.memory_size.to_string(),
        ),
        (
            "AWS_LAMBDA_LOG_GROUP_NAME",
            invocation.logs.as_ref().map_or_else(
                || super::super::default_log_group(&invocation.function_name),
                |target| target.log_group.clone(),
            ),
        ),
        ("AWS_REGION", invocation.region.clone()),
        ("AWS_DEFAULT_REGION", invocation.region.clone()),
    ];
    for (name, value) in reserved {
        environment.insert(name.to_string(), value);
    }
    // The session of the function's execution role, or no credential at all: this
    // emulator's own credentials are never a handler's.
    const CREDENTIAL_VARIABLES: [&str; 3] = [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
    ];
    for name in CREDENTIAL_VARIABLES {
        environment.remove(name);
    }
    if let Some(session) = invocation.aws.as_ref().and_then(super::AwsAccess::identity) {
        let credential = [
            &session.access_key_id,
            &session.secret_access_key,
            &session.session_token,
        ];
        for (name, value) in CREDENTIAL_VARIABLES.into_iter().zip(credential) {
            environment.insert(name.to_string(), value.clone());
        }
    }
    environment
}

/// The one HTTP client of this process for reaching workers.
///
/// It goes through NO proxy — a worker is addressed directly, whatever the host's
/// environment says about outbound requests — and keeps no connection between exchanges:
/// a worker that was restarted is reached on a fresh connection rather than through one
/// its previous life left behind.
fn client() -> Result<&'static ::reqwest::Client, Unrunnable> {
    static CLIENT: OnceLock<Result<::reqwest::Client, String>> = OnceLock::new();
    CLIENT
        .get_or_init(|| {
            ::reqwest::Client::builder()
                .no_proxy()
                .pool_max_idle_per_host(0)
                .connect_timeout(CONNECT_WAIT)
                .build()
                .map_err(|error| described(&error))
        })
        .as_ref()
        .map_err(|why| {
            Unrunnable::Internal(format!(
                "no HTTP client could be built to reach a worker: {why}"
            ))
        })
}

/// `error` with every cause under it, which is where a transport failure says what
/// happened: the outermost error of a failed request names only the URL.
fn described(error: &dyn std::error::Error) -> String {
    let mut text = error.to_string();
    let mut cause = error.source();
    while let Some(under) = cause {
        text.push_str(": ");
        text.push_str(&under.to_string());
        cause = under.source();
    }
    text
}

/// A frame of the worker protocol: the head's length in four bytes, the head as JSON, then
/// the bytes the head describes.
fn frame(head: &::serde_json::Value, tails: &[&[u8]]) -> Vec<u8> {
    let json = head.to_string().into_bytes();
    let length = u32::try_from(json.len()).unwrap_or(u32::MAX);
    let mut framed = Vec::with_capacity(
        4 + json.len() + tails.iter().map(|tail| tail.len()).sum::<usize>(),
    );
    framed.extend_from_slice(&length.to_be_bytes());
    framed.extend_from_slice(&json);
    for tail in tails {
        framed.extend_from_slice(tail);
    }
    framed
}

/// The head and the tail of a frame, or `None` for bytes that are not one.
fn unframe(body: &[u8]) -> Option<(::serde_json::Value, &[u8])> {
    let length = usize::try_from(u32::from_be_bytes(body.get(..4)?.try_into().ok()?)).ok()?;
    let end = 4usize.checked_add(length)?;
    let head = ::serde_json::from_slice(body.get(4..end)?).ok()?;
    Some((head, body.get(end..)?))
}

/// Why an exchange with a worker produced no answer of a run.
#[derive(Debug)]
enum Fault {
    /// The worker could not be reached at all.
    Unreachable(String),
    /// The worker answered something that is not part of the protocol.
    Unusable(String),
    /// The worker could not unpack the function's package.
    Package(String),
}

/// What a worker said one invocation came to.
#[derive(Debug, PartialEq, Eq)]
struct Answered {
    /// How the run ended, in the worker's own word for it.
    outcome: String,
    /// What the worker adds to that word, where it adds anything.
    detail: Option<String>,
    /// What the runtime answered with.
    payload: Vec<u8>,
    /// What the run wrote.
    log: String,
    /// The most memory the runtime process held, in MB, where the worker measured it.
    max_memory_mb: Option<u64>,
}

/// Read a worker's answer to an invocation, or `None` for bytes that are not one.
fn answered(body: &[u8]) -> Option<Answered> {
    let (head, tail) = unframe(body)?;
    let payload_bytes = usize::try_from(head.get("payloadBytes")?.as_u64()?).ok()?;
    let log_bytes = usize::try_from(head.get("logBytes")?.as_u64()?).ok()?;
    if tail.len() != payload_bytes.checked_add(log_bytes)? {
        return None;
    }
    let (payload, log) = tail.split_at(payload_bytes);
    Some(Answered {
        outcome: head.get("outcome")?.as_str()?.to_string(),
        detail: head
            .get("detail")
            .and_then(::serde_json::Value::as_str)
            .map(str::to_string),
        payload: payload.to_vec(),
        log: String::from_utf8_lossy(log).to_string(),
        max_memory_mb: head
            .get("maxMemoryUsedMb")
            .and_then(::serde_json::Value::as_u64),
    })
}

/// What one request to a worker came back as.
struct Response {
    status: u16,
    body: Vec<u8>,
}

impl Response {
    /// The member `name` of a JSON body, as text.
    fn member(&self, name: &str) -> Option<String> {
        ::serde_json::from_slice::<::serde_json::Value>(&self.body)
            .ok()?
            .get(name)?
            .as_str()
            .map(str::to_string)
    }

    /// The opening of the body as text, for a message about an answer that could not be
    /// used.
    fn excerpt(&self) -> String {
        /// How many characters of an unusable answer a message quotes.
        const EXCERPT_CHARS: usize = 200;
        String::from_utf8_lossy(&self.body)
            .chars()
            .take(EXCERPT_CHARS)
            .collect()
    }
}

/// Send `body` to `url` and read the whole answer, up to `ceiling` bytes of it.
async fn sent(
    client: &::reqwest::Client,
    method: ::reqwest::Method,
    url: &str,
    body: Vec<u8>,
    within: Duration,
    ceiling: usize,
) -> Result<Response, Fault> {
    let unreachable = |error: ::reqwest::Error| Fault::Unreachable(described(&error));
    let mut response = client
        .request(method, url)
        .header("content-type", "application/octet-stream")
        .timeout(within)
        .body(body)
        .send()
        .await
        .map_err(unreachable)?;
    let status = response.status().as_u16();
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(unreachable)? {
        if body.len().saturating_add(chunk.len()) > ceiling {
            return Err(Fault::Unusable(format!(
                "it answered {status} with more than the {ceiling} bytes an answer may carry"
            )));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(Response { status, body })
}

/// Everything one exchange with a worker needs, which outlives the caller that started it.
struct Exchange {
    client: &'static ::reqwest::Client,
    invocation: Arc<Invocation>,
    function: Function,
    budget: Budget,
    started: SystemTime,
}

impl Exchange {
    /// Push the function to the worker at `worker`.
    async fn push(&self, worker: &str) -> Result<(), Fault> {
        let invocation = &self.invocation;
        let mut environment = self.function.environment.clone();
        if !environment.contains_key("AWS_ACCESS_KEY_ID") {
            ::tracing::warn!(
                function = invocation.function_name.as_str(),
                "this emulator holds no record of the function's execution role, so its \
                 handler is given no AWS credentials in its worker"
            );
        }
        // What a runtime states of the execution environment itself, which is the one this
        // push starts: it does not make a function a different one, so it is no part of
        // the revision.
        environment.insert(
            "AWS_LAMBDA_LOG_STREAM_NAME".to_string(),
            super::log_stream_name(
                &invocation.executed_version,
                &invocation.request_id,
                self.started,
            ),
        );
        environment.insert(
            "AWS_LAMBDA_INITIALIZATION_TYPE".to_string(),
            invocation.initialization_type.to_string(),
        );
        let package = invocation.zip_bytes.as_deref().unwrap_or_default();
        let mut archives = vec![::serde_json::json!({ "root": "task", "bytes": package.len() })];
        let mut tails: Vec<&[u8]> = vec![package];
        for layer in &invocation.layer_zips {
            archives.push(::serde_json::json!({ "root": "layer", "bytes": layer.len() }));
            tails.push(layer.as_slice());
        }
        let head = ::serde_json::json!({
            "revision": self.function.revision,
            "functionName": invocation.function_name,
            "handler": self.function.handler,
            "timeoutMs": u64::try_from(self.budget.wall_clock.as_millis()).unwrap_or(u64::MAX),
            "environment": environment,
            "caPem": self.function.ca_pem,
            "emulatorEndpoint": self.function.endpoint,
            "archives": archives,
        });
        let response = sent(
            self.client,
            ::reqwest::Method::PUT,
            &format!("{worker}{FUNCTION_PATH}"),
            frame(&head, &tails),
            PUSH_CEILING,
            ANSWER_HEAD_CEILING,
        )
        .await?;
        match response.status {
            200 => {
                ::tracing::info!(
                    function = invocation.function_name.as_str(),
                    version = invocation.executed_version.as_str(),
                    worker,
                    "the function was pushed to this worker"
                );
                Ok(())
            }
            422 => Err(Fault::Package(
                response.member("message").unwrap_or_else(|| response.excerpt()),
            )),
            status => Err(Fault::Unusable(format!(
                "it answered {status} to the function it was pushed: {}",
                response.excerpt()
            ))),
        }
    }

    /// Invoke the function in the worker at `worker`: what the run came to, or `None`
    /// where the worker does not hold this revision.
    async fn invoke(&self, worker: &str) -> Result<Option<Answered>, Fault> {
        let invocation = &self.invocation;
        let head = ::serde_json::json!({
            "revision": self.function.revision,
            "requestId": invocation.request_id,
            "invokedFunctionArn": invocation.invoked_function_arn,
            "traceId": invocation.trace.as_ref().map(|trace| trace.header.clone()),
            "clientContext": invocation.client_context,
            "tenantId": invocation.tenant_id,
            "responseLimitBytes": self.budget.response_bytes,
        });
        let event: &[u8] = match invocation.payload.is_empty() {
            true => EMPTY_EVENT,
            false => &invocation.payload,
        };
        let response = sent(
            self.client,
            ::reqwest::Method::POST,
            &format!("{worker}{INVOKE_PATH}"),
            frame(&head, &[event]),
            // The worker answers by the function's timeout; this only ends an exchange
            // with one that never does, so that its worker is not held for ever.
            self.budget
                .wall_clock
                .saturating_add(ANSWER_ALLOWANCE)
                .saturating_mul(2),
            self.budget
                .response_bytes
                .saturating_add(self.budget.log_bytes)
                .saturating_add(ANSWER_HEAD_CEILING),
        )
        .await?;
        match response.status {
            200 => answered(&response.body).map(Some).ok_or_else(|| {
                Fault::Unusable(format!(
                    "it answered an invocation with something that is no answer of a run: {}",
                    response.excerpt()
                ))
            }),
            409 if response.member("error").as_deref() == Some("not-loaded") => Ok(None),
            status => Err(Fault::Unusable(format!(
                "it answered {status} to an invocation: {}",
                response.excerpt()
            ))),
        }
    }

    /// Run the invocation in `held`'s worker, pushing the function first wherever the
    /// worker does not hold it, and leave `held` knowing what the worker holds now.
    async fn made(&self, held: &mut Held) -> Result<Answered, Fault> {
        let worker = held.url.clone();
        let revision = &self.function.revision;
        // Whatever the exchange comes to, the worker is known to hold this revision only
        // once the worker has said so.
        let believed = held.holding.take().as_ref() == Some(revision);
        if !believed {
            self.push(&worker).await?;
        }
        let run = match self.invoke(&worker).await? {
            Some(run) => run,
            // The worker holds something else after all: it was restarted, or pushed by
            // another emulator. It is pushed and asked once more.
            None => {
                self.push(&worker).await?;
                self.invoke(&worker).await?.ok_or_else(|| {
                    Fault::Unusable(
                        "it does not hold the function it was just pushed".to_string(),
                    )
                })?
            }
        };
        held.holding = Some(revision.clone());
        Ok(run)
    }
}

/// What an invocation whose run came to `answered` reports: its payload and its
/// `FunctionError`, or what is wrong with an answer no run produces.
fn reported(
    answered: Answered,
    request_id: &str,
    budget: Budget,
    engine: Engine,
) -> Result<(Vec<u8>, Option<String>), String> {
    let failed = |document: Vec<u8>| (document, Some(super::FUNCTION_ERROR_UNHANDLED.to_string()));
    let document = |message: &str, error_type: &str| {
        failed(super::error_document(
            message,
            Some(error_type),
            request_id,
            &[],
            engine,
        ))
    };
    Ok(match answered.outcome.as_str() {
        "returned" => (answered.payload, None),
        // The document the runtime itself wrote for the handler's failure, as it wrote it.
        "failed" => failed(answered.payload),
        "timedOut" => timed_out(request_id, budget, engine),
        "exited" => document(
            &format!(
                "RequestId: {request_id} Error: Runtime exited with error: {}",
                answered.detail.as_deref().unwrap_or("unknown")
            ),
            super::RUNTIME_EXIT_ERROR,
        ),
        "tooLarge" => document(
            &format!(
                "Response payload size exceeded maximum allowed payload size ({} bytes).",
                budget.response_bytes
            ),
            super::RESPONSE_SIZE_TOO_LARGE,
        ),
        other => return Err(format!("it reported a run that ended as '{other}'")),
    })
}

/// What an invocation whose run outlasted the function's own `Timeout` reports: the
/// document a run of an embedded engine past its deadline is reported with.
fn timed_out(request_id: &str, budget: Budget, engine: Engine) -> (Vec<u8>, Option<String>) {
    (
        super::error_document(
            &format!(
                "{request_id} Task timed out after {:.2} seconds",
                budget.wall_clock.as_secs_f64()
            ),
            Some(super::SANDBOX_TIMEDOUT),
            request_id,
            &[],
            engine,
        ),
        Some(super::FUNCTION_ERROR_UNHANDLED.to_string()),
    )
}

/// Run `invocation` in one of `pool`'s workers, and report what it came to and when the
/// run began.
///
/// The exchange is made on a task of its OWN, which holds the worker until the worker has
/// answered. That is what keeps a second invocation out of a worker whose run is still
/// going when its caller is no longer there to wait — this emulator's wait ran out, or the
/// caller's request was cancelled and this future dropped.
pub(super) async fn run(
    pool: &Arc<Pool>,
    invocation: &Arc<Invocation>,
) -> Result<(Invoked, SystemTime), Unrunnable> {
    let Some(handler) = invocation.handler.clone() else {
        return Err(Unrunnable::NoHandler);
    };
    if invocation.zip_bytes.is_none() {
        return Err(Unrunnable::PackageNotHeld);
    }
    let client = client()?;
    let budget = super::run_budget(invocation.timeout, invocation.memory_size);
    let function = Function::of(invocation, &handler, budget);
    // A request answered the modelled throttle is metered as one the service throttled,
    // as it is where an embedded run found no memory to run in.
    let held = pool.held(&function.revision).await.inspect_err(|refused| {
        if matches!(refused, Unrunnable::Overloaded(_)) {
            super::throttled_request(invocation);
        }
    })?;
    let worker = held.url.clone();
    let started = SystemTime::now();
    let began = Instant::now();
    let exchange = Exchange {
        client,
        invocation: Arc::clone(invocation),
        function,
        budget,
        started,
    };
    let (answer, answering) = ::tokio::sync::oneshot::channel();
    ::tokio::spawn(async move {
        let mut held = held;
        // A caller that has stopped waiting cannot be told, and need not be.
        let _ = answer.send(exchange.made(&mut held).await);
    });
    let waited = ::tokio::time::timeout(
        budget.wall_clock.saturating_add(ANSWER_ALLOWANCE),
        answering,
    )
    .await;
    let elapsed = began.elapsed();
    // The documents this emulator writes for a run in a worker carry the members the
    // function's own runtime reports them under.
    let engine = super::engine_for(invocation.runtime.as_deref()).unwrap_or(Engine::Node);
    let request_id = invocation.request_id.as_str();
    let at_fault = |what: String| {
        Unrunnable::Internal(format!(
            "the worker at {worker} that runs function '{}' {what}",
            invocation.function_name
        ))
    };
    let (payload, function_error, log, max_memory_mb) = match waited {
        // The worker ends a run at the function's timeout and says so. One that has said
        // nothing by now has stopped answering, and the run is reported as what it is: one
        // that outlasted the function's timeout.
        Err(_elapsed) => {
            let (payload, function_error) = timed_out(request_id, budget, engine);
            (payload, function_error, String::new(), None)
        }
        Ok(Err(_gone)) => return Err(at_fault("ended its exchange without reporting".to_string())),
        Ok(Ok(Err(Fault::Unreachable(why)))) => {
            return Err(at_fault(format!("could not be reached: {why}")))
        }
        Ok(Ok(Err(Fault::Unusable(what)))) => return Err(at_fault(format!("cannot run it: {what}"))),
        Ok(Ok(Err(Fault::Package(why)))) => return Err(Unrunnable::PackageUnreadable(why)),
        Ok(Ok(Ok(mut answered))) => {
            let log = ::std::mem::take(&mut answered.log);
            let max_memory_mb = answered.max_memory_mb;
            let (payload, function_error) =
                reported(answered, request_id, budget, engine).map_err(&at_fault)?;
            (payload, function_error, log, max_memory_mb)
        }
    };
    // The log is what the run wrote in its worker, inside the lines Lambda frames a run's
    // log with. Only the log and the elapsed time are read off this `Finished`.
    let mut log = super::run_log(
        request_id,
        &invocation.executed_version,
        invocation.memory_size,
        &Finished {
            completion: Completion::Returned(String::new()),
            log,
            elapsed,
        },
    );
    // A worker runs the handler in a process of its own, so what Lambda reports last on
    // the line that closes a run's log — the most memory the run held — is a figure the
    // worker measured, and is reported where there is one. (An embedded run has no process
    // to measure, and its line ends at the memory it was allowed.)
    if let (Some(megabytes), Some(report)) = (max_memory_mb, log.lines.last_mut()) {
        report.push_str(&format!("\tMax Memory Used: {megabytes} MB"));
    }
    Ok((
        Invoked {
            payload,
            function_error,
            log_events: super::log_delivery::events(&log.lines, log.opened, started, elapsed),
            log: log.text(),
            duration: elapsed,
            chunks: Vec::new(),
            stream_failure: None,
        },
        started,
    ))
}

#[cfg(test)]
mod tests {
    use super::{answered, chosen, covers, frame, parse_workers, Answered, Worker};

    /// One entry per pattern, in the order they are tried, each naming the workers that
    /// run what it covers; an entry with no pattern covers every function, and spacing
    /// around the separators and a trailing slash on an address are not part of either.
    #[test]
    fn a_worker_table_names_each_entrys_workers_in_order() {
        let cases: &[(&str, &[(&str, &[&str])])] = &[
            ("", &[]),
            (" , ", &[]),
            ("http://127.0.0.1:8081", &[("*", &["http://127.0.0.1:8081"])]),
            (
                "http://127.0.0.1:8081/ | http://127.0.0.1:8082",
                &[("*", &["http://127.0.0.1:8081", "http://127.0.0.1:8082"])],
            ),
            (
                "orders-* = http://a:8080|http://b:8080 , *=https://c.internal:9000/",
                &[
                    ("orders-*", &["http://a:8080", "http://b:8080"]),
                    ("*", &["https://c.internal:9000"]),
                ],
            ),
        ];
        for (text, expected) in cases {
            let entries = parse_workers(text)
                .unwrap_or_else(|why| panic!("case {text:?}: must be read, got {why}"));
            let read: Vec<(&str, Vec<&str>)> = entries
                .iter()
                .map(|(pattern, workers)| {
                    (
                        pattern.as_str(),
                        workers.iter().map(String::as_str).collect(),
                    )
                })
                .collect();
            let expected: Vec<(&str, Vec<&str>)> = expected
                .iter()
                .map(|(pattern, workers)| (*pattern, workers.to_vec()))
                .collect();
            assert_eq!(read, expected, "case {text:?}");
        }
    }

    /// A table with an entry that cannot be acted on is refused as a whole, and the
    /// refusal quotes what is wrong with it.
    #[test]
    fn a_worker_table_with_an_unusable_entry_is_refused() {
        let cases: &[(&str, &str)] = &[
            ("orders", "is not a worker's address"),
            ("=http://a:8080", "names no function"),
            ("orders=a:8080", "is not a worker's address"),
            ("orders=http://", "is not a worker's address"),
            ("orders=http://a:8080|", "is not a worker's address"),
            ("orders=http://a:8080,orders=http://b:8080", "stated more than once"),
            ("http://a:8080,*=http://b:8080", "stated more than once"),
            ("orders=http://a:8080|http://a:8080/", "named more than once"),
            ("orders=http://a:8080,billing=http://a:8080", "named more than once"),
        ];
        for (text, expected) in cases {
            let refusal = match parse_workers(text) {
                Ok(entries) => panic!("case {text:?}: must be refused, got {entries:?}"),
                Err(refusal) => refusal,
            };
            assert!(
                refusal.contains(expected),
                "case {text:?}: the refusal must say it {expected:?}, got {refusal:?}"
            );
        }
    }

    /// A pattern covers a name it spells, with each `*` standing for any run of
    /// characters.
    #[test]
    fn a_pattern_covers_the_names_it_spells() {
        let cases: &[(&str, &str, bool)] = &[
            ("*", "anything", true),
            ("*", "", true),
            ("orders", "orders", true),
            ("orders", "orders-local", false),
            ("orders", "my-orders", false),
            ("orders-*", "orders-local-write", true),
            ("orders-*", "orders-", true),
            ("orders-*", "orders", false),
            ("*-write", "orders-local-write", true),
            ("*-write", "orders-local-writer", false),
            ("sample-*-write*", "sample-local-writeOrder", true),
            ("sample-*-write*", "sample-write", false),
            ("a*b*c", "abc", true),
            ("a*b*c", "axxbxxc", true),
            ("a*b*c", "acb", false),
            ("**", "anything", true),
            ("", "", true),
            ("", "orders", false),
        ];
        for (pattern, name, expected) in cases {
            assert_eq!(
                covers(pattern, name),
                *expected,
                "pattern {pattern:?} over {name:?}"
            );
        }
    }

    /// An invocation is run by an idle worker that already holds its revision, failing
    /// that by one that holds nothing, failing that by the one idle longest; a busy worker
    /// is never chosen.
    #[test]
    fn an_invocation_is_run_by_the_idle_worker_that_suits_it_best() {
        let worker = |holding: Option<&str>, busy: bool, last_used: u64| Worker {
            url: String::new(),
            busy,
            holding: holding.map(str::to_string),
            last_used,
        };
        let cases: &[(&str, Vec<Worker>, Option<usize>)] = &[
            ("no worker", vec![], None),
            ("every worker busy", vec![worker(Some("r"), true, 1)], None),
            (
                "one that holds it, over one that holds nothing",
                vec![worker(None, false, 0), worker(Some("r"), false, 9)],
                Some(1),
            ),
            (
                "one that holds nothing, over one that holds another",
                vec![worker(Some("other"), false, 1), worker(None, false, 5)],
                Some(1),
            ),
            (
                "the one idle longest, where each holds another",
                vec![worker(Some("a"), false, 7), worker(Some("b"), false, 3)],
                Some(1),
            ),
            (
                "not the busy one that holds it",
                vec![worker(Some("r"), true, 1), worker(Some("other"), false, 2)],
                Some(1),
            ),
            (
                "the one idle longest, where several hold it",
                vec![worker(Some("r"), false, 4), worker(Some("r"), false, 2)],
                Some(1),
            ),
        ];
        for (case, workers, expected) in cases {
            assert_eq!(chosen(workers, "r"), *expected, "case {case}");
        }
    }

    /// A worker's answer is its head, the payload and the log, each as long as the head
    /// says; bytes that do not add up to that are no answer.
    #[test]
    fn a_workers_answer_is_read_as_its_head_says() {
        let head = |payload: usize, log: usize| {
            ::serde_json::json!({
                "outcome": "exited", "detail": "exit status 3",
                "payloadBytes": payload, "logBytes": log, "maxMemoryUsedMb": 87,
            })
        };
        assert_eq!(
            answered(&frame(&head(2, 5), &[b"{}".as_slice(), b"line\n".as_slice()])),
            Some(Answered {
                outcome: "exited".to_string(),
                detail: Some("exit status 3".to_string()),
                payload: b"{}".to_vec(),
                log: "line\n".to_string(),
                max_memory_mb: Some(87),
            }),
        );
        let cases: &[(&str, Vec<u8>)] = &[
            ("nothing", Vec::new()),
            ("a body that is no frame", b"InternalError".to_vec()),
            ("a tail shorter than the head says", frame(&head(2, 5), &[b"{}".as_slice()])),
            ("a tail longer than the head says", frame(&head(2, 0), &[b"{}".as_slice(), b"more".as_slice()])),
            (
                "a head with no outcome",
                frame(&::serde_json::json!({ "payloadBytes": 0, "logBytes": 0 }), &[]),
            ),
        ];
        for (case, body) in cases {
            assert_eq!(answered(body), None, "case {case}");
        }
    }
}
