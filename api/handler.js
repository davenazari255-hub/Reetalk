import { Readable } from “node:stream”;
import { pipeline } from “node:stream/promises”;

export const config = {
api: { bodyParser: false },
supportsResponseStreaming: true,
maxDuration: 60,
};

const UPSTREAM_BASE = (process.env.UPSTREAM_ENDPOINT || “”).replace(//$/, “”);

const BLOCKED_HEADERS = new Set([
“host”,
“connection”,
“keep-alive”,
“proxy-authenticate”,
“proxy-authorization”,
“te”,
“trailer”,
“transfer-encoding”,
“upgrade”,
“forwarded”,
“x-forwarded-host”,
“x-forwarded-proto”,
“x-forwarded-port”,
]);

function buildForwardHeaders(reqHeaders) {
const out = {};
let originIp = null;

for (const key of Object.keys(reqHeaders)) {
const lower = key.toLowerCase();
const value = reqHeaders[key];

```
if (BLOCKED_HEADERS.has(lower)) continue;
if (lower.startsWith("x-vercel-")) continue;

if (lower === "x-real-ip") {
  originIp = value;
  continue;
}
if (lower === "x-forwarded-for") {
  if (!originIp) originIp = value;
  continue;
}

out[lower] = Array.isArray(value) ? value.join(", ") : value;
```

}

if (originIp) out[“x-forwarded-for”] = originIp;

return out;
}

async function forwardRequest(req, headers) {
const destination = UPSTREAM_BASE + req.url;
const method = req.method;
const hasBody = method !== “GET” && method !== “HEAD”;

const options = { method, headers, redirect: “manual” };
if (hasBody) {
options.body = Readable.toWeb(req);
options.duplex = “half”;
}

return fetch(destination, options);
}

async function pipeResponse(upstream, res) {
res.statusCode = upstream.status;

for (const [key, value] of upstream.headers) {
if (key.toLowerCase() === “transfer-encoding”) continue;
try {
res.setHeader(key, value);
} catch {}
}

if (upstream.body) {
await pipeline(Readable.fromWeb(upstream.body), res);
} else {
res.end();
}
}

export default async function gatewayHandler(req, res) {
if (!UPSTREAM_BASE) {
res.statusCode = 500;
return res.end(“Misconfigured: UPSTREAM_ENDPOINT is not set”);
}

try {
const headers = buildForwardHeaders(req.headers);
const upstream = await forwardRequest(req, headers);
await pipeResponse(upstream, res);
} catch (err) {
console.error(“gateway error:”, err);
if (!res.headersSent) {
res.statusCode = 502;
res.end(“Bad Gateway: Relay Failed”);
}
}
}
