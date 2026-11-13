'use strict';

// Loaded into a worker's runtime before the function's own code (`node --require`), by the
// worker agent. It makes the emulator the TRANSPORT of every AWS API call the handler makes:
// an HTTPS request that is SigV4-signed and addressed to an AWS endpoint is connected to the
// emulator instead, with nothing else about it changed.
//
// AWS_ENDPOINT_URL already points a current AWS SDK at the emulator, but not every call
// honours it: the SDK for JavaScript sends an SQS request to the host of its QueueUrl,
// which the emulator reports as `https://sqs.<region>.amazonaws.com/...`, and an SDK
// bundled before that variable existed ignores it altogether. Either would otherwise leave
// for the real service, carrying a session only the emulator knows.
//
// The request keeps the `Host` header it was signed with, so its signature stays valid, and
// the emulator routes it by the service its signature names — which is how the emulator's
// embedded mode serves an SDK that was given no endpoint at all. A request that carries no
// AWS signature is not an API call and is left alone, whatever its host.

const https = require('node:https');
const net = require('node:net');
const { syncBuiltinESMExports } = require('node:module');

const target = process.env.EMULATOR_ENDPOINT_URL;

if (target) {
  const endpoint = new URL(target);
  const port = endpoint.port || '443';
  const awsHost = /(\.amazonaws\.com(\.cn)?|\.api\.aws)$/i;

  const header = (headers, name) => {
    for (const [key, value] of Object.entries(headers ?? {})) {
      if (key.toLowerCase() === name) return Array.isArray(value) ? value[0] : value;
    }
    return undefined;
  };

  /** The options of a request to the emulator in place of `options`, or `null` to leave it. */
  const redirected = (options, host, hostPort) => {
    if (!host || !awsHost.test(host)) return null;
    if (!String(header(options.headers, 'authorization') ?? '').startsWith('AWS4-')) return null;
    const headers = { ...options.headers };
    // What the request was signed for stays what it says it is for.
    if (header(headers, 'host') === undefined) headers.host = hostPort ? `${host}:${hostPort}` : host;
    return {
      ...options,
      protocol: 'https:',
      host: endpoint.hostname,
      hostname: endpoint.hostname,
      port,
      // The certificate is checked against the emulator's address, not the AWS name.
      servername: net.isIP(endpoint.hostname) ? '' : endpoint.hostname,
      headers,
    };
  };

  for (const method of ['request', 'get']) {
    const original = https[method];
    https[method] = function patched(...args) {
      const [first, second] = args;
      if (typeof first === 'string' || first instanceof URL) {
        const url = new URL(first);
        const options = typeof second === 'object' && second !== null ? second : {};
        const replaced = redirected({ ...options, path: url.pathname + url.search }, url.hostname, url.port);
        if (replaced) return original.call(this, replaced, ...args.slice(options === second ? 2 : 1));
      } else if (typeof first === 'object' && first !== null) {
        const replaced = redirected(first, first.hostname ?? first.host, first.port);
        if (replaced) return original.call(this, replaced, ...args.slice(1));
      }
      return original.apply(this, args);
    };
  }
  // A module that imports `request` from 'node:https' by name sees the replacement too.
  syncBuiltinESMExports();
}
