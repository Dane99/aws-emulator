// The WORKER AGENT: what runs in a `public.ecr.aws/lambda/nodejs:22` container so the
// emulator can run a Lambda function's handler there instead of in its embedded engine.
//
// A worker is generic. It holds no function until the emulator PUSHES one — the deployment
// package, the layers, the handler name, the timeout and the environment — and it runs
// whichever function it was pushed last. The emulator pushes again whenever the function it
// is about to invoke is not the one the worker holds, so what runs is always what was last
// deployed.
//
// The agent is two HTTP servers:
//
//   * the CONTROL port (EMULATOR_WORKER_PORT, default 8080), which the emulator talks to:
//       GET  /state      what this worker holds
//       PUT  /function   take a function (stops the runtime, replaces /var/task and /opt)
//       POST /invoke     run one invocation and answer what it came to
//   * the Lambda RUNTIME API, on a loopback port of its own, which the image's own runtime
//     (/var/runtime/bootstrap) polls for invocations exactly as it does on Lambda. The
//     image's runtime interface emulator is not used.
//
// A request or response body that carries bytes is a FRAME: a 4-byte big-endian length,
// that many bytes of JSON (the head), then the raw bytes the head describes (the tail).
//
// One invocation runs at a time, as in a Lambda execution environment. The emulator
// queues; a second request while one is running is refused with 409.

import { spawn } from 'node:child_process';
import fs from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import zlib from 'node:zlib';

const CONTROL_PORT = Number(process.env.EMULATOR_WORKER_PORT ?? 8080);
const CONTROL_HOST = process.env.EMULATOR_WORKER_HOST ?? '0.0.0.0';
const BOOTSTRAP = process.env.EMULATOR_WORKER_BOOTSTRAP ?? '/var/runtime/bootstrap';
const TASK_ROOT = process.env.LAMBDA_TASK_ROOT ?? '/var/task';
const LAYER_ROOT = '/opt';
const SCRATCH_ROOT = '/tmp';
// Where the emulator's certificate is kept: outside every directory a push replaces.
const STATE_ROOT = '/var/emulator-worker';
const CA_FILE = path.join(STATE_ROOT, 'emulator-ca.pem');
// What makes the emulator the transport of the handler's AWS API calls (see that file).
const AWS_TRANSPORT = path.join(import.meta.dirname, 'aws-transport.cjs');

const RUNTIME_API = '/2018-06-01/runtime';
// How long a runtime that is still initializing when an invocation arrives is waited for,
// which is the span Lambda gives the init phase.
const INIT_ALLOWANCE_MS = 10_000;
// How often the memory a running handler holds is read.
const MEMORY_SAMPLE_MS = 250;
// How much of one invocation's output is kept, from its end.
const LOG_LIMIT_BYTES = 256 * 1024;
const PACKAGE_LIMIT_BYTES = 1024 * 1024 * 1024;
const EVENT_LIMIT_BYTES = 8 * 1024 * 1024;

/** The function this worker holds: { revision, functionName, handler, timeoutMs, environment, emulatorEndpoint }. */
let loaded = null;
/** The runtime process: { child, waiting, initError, exited }. */
let runtime = null;
/** The invocation in flight: { head, event, delivered, timer, sampler, maxMemoryMb, resolve }. */
let current = null;
let logged = [];
let loggedBytes = 0;
let runtimeApiPort = 0;

const say = (message) => process.stdout.write(`[worker] ${message}\n`);

// ---------------------------------------------------------------------------
// Frames
// ---------------------------------------------------------------------------

function frame(head, ...tails) {
  const json = Buffer.from(JSON.stringify(head));
  const length = Buffer.alloc(4);
  length.writeUInt32BE(json.length);
  return Buffer.concat([length, json, ...tails]);
}

function unframe(body) {
  if (body.length < 4) throw new Refusal(400, 'malformed', 'the body is not a frame');
  const length = body.readUInt32BE(0);
  if (4 + length > body.length) throw new Refusal(400, 'malformed', 'the frame head is cut short');
  let head;
  try {
    head = JSON.parse(body.subarray(4, 4 + length).toString('utf8'));
  } catch {
    throw new Refusal(400, 'malformed', 'the frame head is not JSON');
  }
  return { head, tail: body.subarray(4 + length) };
}

class Refusal extends Error {
  constructor(status, code, message) {
    super(message);
    this.status = status;
    this.code = code;
  }
}

function readBody(request, limit) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let bytes = 0;
    request.on('data', (chunk) => {
      bytes += chunk.length;
      if (bytes > limit) {
        reject(new Refusal(413, 'too-large', `the body is larger than ${limit} bytes`));
        request.destroy();
        return;
      }
      chunks.push(chunk);
    });
    request.on('end', () => resolve(Buffer.concat(chunks)));
    request.on('error', reject);
  });
}

function reply(response, status, body) {
  const bytes = Buffer.isBuffer(body) ? body : Buffer.from(JSON.stringify(body));
  response.writeHead(status, {
    'content-type': Buffer.isBuffer(body) ? 'application/octet-stream' : 'application/json',
    'content-length': bytes.length,
  });
  response.end(bytes);
}

// ---------------------------------------------------------------------------
// Reading a .zip archive. The image carries no unzip, so the two compression methods a
// deployment package uses (stored and deflate) are read here.
// ---------------------------------------------------------------------------

const END_OF_DIRECTORY = 0x06054b50;
const ZIP64_LOCATOR = 0x07064b50;
const ZIP64_END_OF_DIRECTORY = 0x06064b50;
const DIRECTORY_ENTRY = 0x02014b50;
const LOCAL_ENTRY = 0x04034b50;
const ZIP64_EXTRA = 0x0001;
const MADE_ON_UNIX = 3;
const FILE_TYPE_MASK = 0o170000;
const SYMBOLIC_LINK = 0o120000;

function archiveEntries(zip) {
  let end = -1;
  for (let at = zip.length - 22; at >= Math.max(0, zip.length - 22 - 0xffff); at -= 1) {
    if (zip.readUInt32LE(at) === END_OF_DIRECTORY) {
      end = at;
      break;
    }
  }
  if (end < 0) throw new Error('not a .zip archive: it has no end-of-central-directory record');
  let count = zip.readUInt16LE(end + 10);
  let offset = zip.readUInt32LE(end + 16);
  if (end >= 20 && zip.readUInt32LE(end - 20) === ZIP64_LOCATOR) {
    const record = Number(zip.readBigUInt64LE(end - 12));
    if (zip.readUInt32LE(record) !== ZIP64_END_OF_DIRECTORY) {
      throw new Error('not a readable .zip archive: its zip64 directory record is missing');
    }
    count = Number(zip.readBigUInt64LE(record + 32));
    offset = Number(zip.readBigUInt64LE(record + 48));
  }
  const entries = [];
  let at = offset;
  for (let index = 0; index < count; index += 1) {
    if (at + 46 > zip.length || zip.readUInt32LE(at) !== DIRECTORY_ENTRY) {
      throw new Error('not a readable .zip archive: its central directory is cut short');
    }
    const madeBy = zip.readUInt16LE(at + 4);
    const flags = zip.readUInt16LE(at + 8);
    const nameLength = zip.readUInt16LE(at + 28);
    const extraLength = zip.readUInt16LE(at + 30);
    const commentLength = zip.readUInt16LE(at + 32);
    const entry = {
      name: zip.subarray(at + 46, at + 46 + nameLength).toString('utf8'),
      encrypted: (flags & 1) === 1,
      method: zip.readUInt16LE(at + 10),
      checksum: zip.readUInt32LE(at + 16),
      compressedSize: zip.readUInt32LE(at + 20),
      size: zip.readUInt32LE(at + 24),
      local: zip.readUInt32LE(at + 42),
      mode: madeBy >>> 8 === MADE_ON_UNIX ? zip.readUInt32LE(at + 38) >>> 16 : 0,
    };
    // A zip64 entry states the sizes and the offset that did not fit, in this order.
    let extra = at + 46 + nameLength;
    const extraEnd = extra + extraLength;
    while (extra + 4 <= extraEnd) {
      const id = zip.readUInt16LE(extra);
      const length = zip.readUInt16LE(extra + 2);
      if (id === ZIP64_EXTRA) {
        let field = extra + 4;
        for (const member of ['size', 'compressedSize', 'local']) {
          if (entry[member] === 0xffffffff) {
            entry[member] = Number(zip.readBigUInt64LE(field));
            field += 8;
          }
        }
      }
      extra += 4 + length;
    }
    entries.push(entry);
    at = extraEnd + commentLength;
  }
  return entries;
}

function entryBytes(zip, entry) {
  if (entry.encrypted) throw new Error(`'${entry.name}' is encrypted`);
  if (entry.local + 30 > zip.length || zip.readUInt32LE(entry.local) !== LOCAL_ENTRY) {
    throw new Error(`'${entry.name}' has no local header`);
  }
  const start =
    entry.local + 30 + zip.readUInt16LE(entry.local + 26) + zip.readUInt16LE(entry.local + 28);
  const stored = zip.subarray(start, start + entry.compressedSize);
  let bytes;
  if (entry.method === 0) bytes = stored;
  else if (entry.method === 8) bytes = zlib.inflateRawSync(stored);
  else throw new Error(`'${entry.name}' uses compression method ${entry.method}, not deflate`);
  if (bytes.length !== entry.size || zlib.crc32(bytes) >>> 0 !== entry.checksum) {
    throw new Error(`'${entry.name}' does not match its recorded size and checksum`);
  }
  return bytes;
}

/** Write every entry of `zip` under `root`, with the permissions the archive records. */
function extract(zip, root) {
  const links = [];
  for (const entry of archiveEntries(zip)) {
    const target = path.resolve(root, entry.name);
    if (target !== root && !target.startsWith(root + path.sep)) {
      throw new Error(`'${entry.name}' names a path outside the archive`);
    }
    if (entry.name.endsWith('/')) {
      fs.mkdirSync(target, { recursive: true });
      continue;
    }
    const bytes = entryBytes(zip, entry);
    // A link is made once every file is in place, so no entry is written THROUGH one.
    if ((entry.mode & FILE_TYPE_MASK) === SYMBOLIC_LINK) {
      links.push([target, bytes.toString('utf8')]);
      continue;
    }
    fs.mkdirSync(path.dirname(target), { recursive: true });
    fs.writeFileSync(target, bytes);
    fs.chmodSync(target, entry.mode & 0o777 || 0o644);
  }
  for (const [target, destination] of links) {
    fs.mkdirSync(path.dirname(target), { recursive: true });
    fs.symlinkSync(destination, target);
  }
}

function emptied(directory) {
  fs.mkdirSync(directory, { recursive: true });
  for (const name of fs.readdirSync(directory)) {
    fs.rmSync(path.join(directory, name), { recursive: true, force: true });
  }
}

// ---------------------------------------------------------------------------
// The runtime process
// ---------------------------------------------------------------------------

function record(chunk) {
  process.stdout.write(chunk);
  logged.push(chunk);
  loggedBytes += chunk.length;
  while (loggedBytes > LOG_LIMIT_BYTES && logged.length > 1) {
    loggedBytes -= logged.shift().length;
  }
}

function takeLog() {
  const taken = Buffer.concat(logged);
  logged = [];
  loggedBytes = 0;
  return taken.length > LOG_LIMIT_BYTES ? taken.subarray(taken.length - LOG_LIMIT_BYTES) : taken;
}

/** What Lambda calls the way a runtime process ended. */
function ending(code, signal) {
  const signals = { SIGKILL: 'killed', SIGTERM: 'terminated', SIGSEGV: 'segmentation fault', SIGABRT: 'aborted' };
  return signal ? `signal: ${signals[signal] ?? signal}` : `exit status ${code}`;
}

function startRuntime() {
  const environment = { ...process.env, ...loaded.environment };
  for (const name of Object.keys(environment)) {
    if (name.startsWith('EMULATOR_WORKER_')) delete environment[name];
  }
  environment.AWS_LAMBDA_RUNTIME_API = `127.0.0.1:${runtimeApiPort}`;
  environment._HANDLER = loaded.handler;
  environment.LAMBDA_TASK_ROOT = TASK_ROOT;
  if (loaded.emulatorEndpoint) {
    environment.EMULATOR_ENDPOINT_URL = loaded.emulatorEndpoint;
    environment.NODE_OPTIONS = [`--require ${AWS_TRANSPORT}`, environment.NODE_OPTIONS].filter(Boolean).join(' ');
  }
  const started = { child: null, waiting: null, initError: null, exited: false };
  // Its own process group, so a run that is stopped takes the processes it started with it.
  started.child = spawn(BOOTSTRAP, [], {
    cwd: TASK_ROOT,
    env: environment,
    stdio: ['ignore', 'pipe', 'pipe'],
    detached: true,
  });
  started.child.stdout.on('data', record);
  started.child.stderr.on('data', record);
  const closed = new Promise((resolve) => started.child.once('close', resolve));
  const ended = async (how) => {
    if (started.exited) return;
    started.exited = true;
    started.waiting = null;
    // What the process wrote last is read before the invocation it ended is answered.
    await Promise.race([closed, new Promise((resolve) => setTimeout(resolve, 250))]);
    if (runtime !== started || !current) return;
    if (!current.delivered && started.initError) finish({ outcome: 'failed', payload: started.initError });
    else finish({ outcome: 'exited', detail: how });
  };
  started.child.once('error', (error) => ended(`the runtime could not be started: ${error.message}`));
  started.child.once('exit', (code, signal) => ended(ending(code, signal)));
  runtime = started;
}

function stopRuntime() {
  const stopped = runtime;
  runtime = null;
  if (!stopped || stopped.exited) return;
  stopped.exited = true;
  stopped.waiting = null;
  try {
    process.kill(-stopped.child.pid, 'SIGKILL');
  } catch {
    stopped.child.kill('SIGKILL');
  }
}

/** Note the most memory the runtime process has held, which Lambda reports for a run. */
function sampleMemory() {
  if (!current || !runtime || runtime.exited) return;
  try {
    const status = fs.readFileSync(`/proc/${runtime.child.pid}/status`, 'utf8');
    const kilobytes = Number(/^VmHWM:\s+(\d+) kB/m.exec(status)?.[1]);
    if (Number.isFinite(kilobytes)) {
      current.maxMemoryMb = Math.max(current.maxMemoryMb ?? 0, Math.ceil(kilobytes / 1024));
    }
  } catch {
    // The process is gone: what was read last stands.
  }
}

function finish(result) {
  const done = current;
  if (!done) return;
  sampleMemory();
  current = null;
  clearTimeout(done.timer);
  clearInterval(done.sampler);
  done.resolve({ ...result, maxMemoryMb: done.maxMemoryMb });
}

/** Hand the invocation in flight to the runtime, once the runtime is asking for one. */
function deliver() {
  if (!current || current.delivered || !runtime?.waiting) return;
  const response = runtime.waiting;
  runtime.waiting = null;
  current.delivered = true;
  clearTimeout(current.timer);
  // The function's own timeout runs from here: a run still going at it is stopped, as the
  // sandbox is on Lambda, and the next invocation starts a fresh runtime.
  current.timer = setTimeout(() => {
    sampleMemory();
    stopRuntime();
    finish({ outcome: 'timedOut' });
  }, loaded.timeoutMs);
  sampleMemory();
  current.sampler = setInterval(sampleMemory, MEMORY_SAMPLE_MS);
  const headers = {
    'content-type': 'application/json',
    'content-length': current.event.length,
    'lambda-runtime-aws-request-id': current.head.requestId,
    'lambda-runtime-deadline-ms': String(Date.now() + loaded.timeoutMs),
    'lambda-runtime-invoked-function-arn': current.head.invokedFunctionArn,
  };
  if (current.head.traceId) headers['lambda-runtime-trace-id'] = current.head.traceId;
  if (current.head.clientContext) headers['lambda-runtime-client-context'] = current.head.clientContext;
  if (current.head.tenantId) headers['lambda-runtime-aws-tenant-id'] = current.head.tenantId;
  response.writeHead(200, headers);
  response.end(current.event);
}

// ---------------------------------------------------------------------------
// The Lambda Runtime API, which the runtime process polls
// ---------------------------------------------------------------------------

async function serveRuntime(request, response) {
  const accepted = () => reply(response, 202, { status: 'OK' });
  if (request.method === 'GET' && request.url === `${RUNTIME_API}/invocation/next`) {
    if (!runtime || runtime.exited) return reply(response, 410, { errorType: 'Gone' });
    runtime.waiting = response;
    response.once('close', () => {
      if (runtime?.waiting === response) runtime.waiting = null;
    });
    return deliver();
  }
  if (request.method === 'POST' && request.url === `${RUNTIME_API}/init/error`) {
    const document = await readBody(request, EVENT_LIMIT_BYTES);
    if (runtime) runtime.initError = document;
    return accepted();
  }
  const answer = request.url.match(/\/runtime\/invocation\/([^/]+)\/(response|error)$/);
  if (request.method === 'POST' && answer) {
    const limit = current?.head.responseLimitBytes ?? EVENT_LIMIT_BYTES;
    let body;
    try {
      body = await readBody(request, limit);
    } catch (error) {
      if (!(error instanceof Refusal)) throw error;
      if (current?.delivered && current.head.requestId === answer[1]) finish({ outcome: 'tooLarge' });
      return reply(response, 413, { errorType: 'RequestEntityTooLarge' });
    }
    if (!current?.delivered || current.head.requestId !== answer[1]) {
      return reply(response, 400, { errorType: 'InvalidRequestID' });
    }
    // A response-streaming handler that fails once it has written says so in a trailer.
    const failure = request.trailers['lambda-runtime-function-error-body'];
    if (answer[2] === 'error') finish({ outcome: 'failed', payload: body });
    else if (failure) finish({ outcome: 'failed', payload: Buffer.from(failure, 'base64') });
    else finish({ outcome: 'returned', payload: body });
    return accepted();
  }
  return reply(response, 404, { errorType: 'NotFound' });
}

// ---------------------------------------------------------------------------
// The control port, which the emulator talks to
// ---------------------------------------------------------------------------

async function takeFunction(request, response) {
  const { head, tail } = unframe(await readBody(request, PACKAGE_LIMIT_BYTES));
  const usable =
    typeof head.revision === 'string' &&
    typeof head.handler === 'string' &&
    Number.isFinite(head.timeoutMs) &&
    head.environment !== null &&
    typeof head.environment === 'object' &&
    Array.isArray(head.archives);
  if (!usable) throw new Refusal(400, 'malformed', 'the function is missing a member');
  if (current) throw new Refusal(409, 'busy', 'an invocation is running');
  stopRuntime();
  loaded = null;
  for (const directory of [TASK_ROOT, LAYER_ROOT, SCRATCH_ROOT]) emptied(directory);
  let offset = 0;
  for (const archive of head.archives) {
    const bytes = tail.subarray(offset, offset + archive.bytes);
    if (bytes.length !== archive.bytes) throw new Refusal(400, 'malformed', 'an archive is cut short');
    offset += archive.bytes;
    try {
      extract(bytes, archive.root === 'layer' ? LAYER_ROOT : TASK_ROOT);
    } catch (error) {
      throw new Refusal(422, 'package', error.message);
    }
  }
  const environment = { ...head.environment };
  if (typeof head.caPem === 'string' && head.caPem !== '') {
    fs.mkdirSync(STATE_ROOT, { recursive: true });
    fs.writeFileSync(CA_FILE, head.caPem);
    // The handler's own HTTPS clients trust the emulator, unless the function says what
    // they trust itself.
    environment.NODE_EXTRA_CA_CERTS ??= CA_FILE;
    environment.AWS_CA_BUNDLE ??= CA_FILE;
  }
  loaded = {
    revision: head.revision,
    functionName: head.functionName,
    handler: head.handler,
    timeoutMs: head.timeoutMs,
    environment,
    emulatorEndpoint: typeof head.emulatorEndpoint === 'string' ? head.emulatorEndpoint : null,
  };
  takeLog();
  // The runtime initializes now, as an execution environment does before its first
  // invocation; what it writes while it does is the first invocation's log.
  startRuntime();
  say(`holding ${head.functionName} (${head.handler}), revision ${head.revision.slice(0, 12)}`);
  reply(response, 200, { revision: head.revision });
}

async function invoke(request, response) {
  const { head, tail } = unframe(await readBody(request, EVENT_LIMIT_BYTES));
  if (typeof head.requestId !== 'string' || typeof head.invokedFunctionArn !== 'string') {
    throw new Refusal(400, 'malformed', 'the invocation is missing a member');
  }
  if (!loaded || loaded.revision !== head.revision) {
    throw new Refusal(409, 'not-loaded', 'this worker does not hold that revision');
  }
  if (current) throw new Refusal(409, 'busy', 'an invocation is running');
  const result = await new Promise((resolve) => {
    current = { head, event: tail, delivered: false, timer: null, sampler: null, resolve };
    // A runtime that ended — it exited, or its last run was stopped at the timeout — is
    // started again for this invocation, which is a cold start.
    if (!runtime || runtime.exited) startRuntime();
    current.timer = setTimeout(() => {
      stopRuntime();
      finish({ outcome: 'timedOut', detail: 'the runtime did not finish initializing' });
    }, INIT_ALLOWANCE_MS);
    deliver();
  });
  // Let what the handler wrote just before it answered be read.
  await new Promise((resolve) => setImmediate(resolve));
  await new Promise((resolve) => setImmediate(resolve));
  const payload = result.payload ?? Buffer.alloc(0);
  const log = takeLog();
  reply(
    response,
    200,
    frame(
      {
        outcome: result.outcome,
        detail: result.detail,
        payloadBytes: payload.length,
        logBytes: log.length,
        maxMemoryUsedMb: result.maxMemoryMb,
      },
      payload,
      log,
    ),
  );
}

async function serveControl(request, response) {
  if (request.method === 'GET' && request.url === '/state') {
    return reply(response, 200, { revision: loaded?.revision ?? null, busy: current !== null });
  }
  if (request.method === 'PUT' && request.url === '/function') return takeFunction(request, response);
  if (request.method === 'POST' && request.url === '/invoke') return invoke(request, response);
  throw new Refusal(404, 'not-found', `${request.method} ${request.url} is not served here`);
}

function served(handler) {
  return (request, response) => {
    handler(request, response).catch((error) => {
      const status = error instanceof Refusal ? error.status : 500;
      if (status === 500) say(`failed: ${error.stack ?? error}`);
      if (!response.headersSent) {
        reply(response, status, { error: error.code ?? 'internal', message: error.message });
      } else {
        response.destroy();
      }
    });
  };
}

const runtimeApi = http.createServer(served(serveRuntime));
// The runtime's poll for its next invocation is held open for as long as the worker idles.
runtimeApi.requestTimeout = 0;
runtimeApi.headersTimeout = 0;
runtimeApi.keepAliveTimeout = 0;
runtimeApi.listen(0, '127.0.0.1', () => {
  runtimeApiPort = runtimeApi.address().port;
  const control = http.createServer(served(serveControl));
  control.requestTimeout = 0;
  control.listen(CONTROL_PORT, CONTROL_HOST, () => {
    say(`ready on ${CONTROL_HOST}:${CONTROL_PORT}, holding no function`);
  });
});

for (const signal of ['SIGTERM', 'SIGINT']) {
  process.on(signal, () => {
    stopRuntime();
    process.exit(0);
  });
}
