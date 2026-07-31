import http from "node:http";
import readline from "node:readline";
import {
  CallToolResultSchema,
  JSONRPCMessageSchema,
  ListToolsResultSchema,
} from "@modelcontextprotocol/sdk/types.js";

const PROTOCOL = "2026-07-28";
const TASKS = "io.modelcontextprotocol/tasks";
const SDK = "@modelcontextprotocol/sdk@1.30.0";
const CREATED_AT = "2026-07-30T00:00:00Z";
const UPDATED_AT = "2026-07-30T00:00:01Z";

function invariant(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function validateJsonRpc(message) {
  const parsed = JSONRPCMessageSchema.safeParse(message);
  invariant(parsed.success, `official SDK rejected JSON-RPC message: ${parsed.error}`);
}

function metadata() {
  return {
    "io.modelcontextprotocol/protocolVersion": PROTOCOL,
    "io.modelcontextprotocol/clientInfo": {
      name: "insight-typescript-qualification",
      version: "1.0.0",
    },
    "io.modelcontextprotocol/clientCapabilities": {
      extensions: {
        [TASKS]: {},
      },
    },
  };
}

function task(status = "working") {
  return {
    taskId: "typescript-task-1",
    status,
    createdAt: CREATED_AT,
    lastUpdatedAt: UPDATED_AT,
    ttlMs: 60000,
    pollIntervalMs: 25,
  };
}

function resultFor(request) {
  validateJsonRpc(request);
  invariant(request.jsonrpc === "2.0", "invalid JSON-RPC version");
  invariant(request.params?._meta?.["io.modelcontextprotocol/protocolVersion"] === PROTOCOL,
    "missing modern protocol metadata");

  let result;
  switch (request.method) {
    case "server/discover":
      result = {
        resultType: "complete",
        supportedVersions: [PROTOCOL],
        capabilities: {
          tools: {},
          extensions: {
            [TASKS]: {},
          },
        },
        serverInfo: {
          name: "typescript-reference-fixture",
          version: SDK,
        },
        ttlMs: 1000,
        cacheScope: "private",
      };
      break;
    case "tools/list":
      result = {
        resultType: "complete",
        tools: [
          {
            name: "sdk_echo",
            description: "Echo through the pinned TypeScript SDK fixture.",
            inputSchema: {
              type: "object",
              properties: { value: { type: "string" } },
              required: ["value"],
              additionalProperties: false,
            },
            outputSchema: {
              type: "object",
              properties: { value: { type: "string" } },
              required: ["value"],
              additionalProperties: false,
            },
          },
          {
            name: "sdk_task",
            description: "Create a task through the pinned TypeScript SDK fixture.",
            inputSchema: {
              type: "object",
              additionalProperties: false,
            },
          },
        ],
        ttlMs: 1000,
        cacheScope: "private",
      };
      invariant(ListToolsResultSchema.safeParse(result).success,
        "official SDK rejected fixture tools/list result");
      break;
    case "tools/call":
      if (request.params.name === "sdk_task") {
        result = { resultType: "task", ...task() };
      } else {
        result = {
          resultType: "complete",
          content: [{ type: "text", text: String(request.params.arguments?.value ?? "") }],
          structuredContent: { value: String(request.params.arguments?.value ?? "") },
          isError: false,
        };
        invariant(CallToolResultSchema.safeParse(result).success,
          "official SDK rejected fixture tool result");
      }
      break;
    case "tasks/get":
      invariant(request.params.taskId === "typescript-task-1", "unexpected task id");
      result = {
        resultType: "complete",
        ...task("completed"),
        result: {
          content: [{ type: "text", text: "typescript-task-complete" }],
          isError: false,
        },
      };
      break;
    case "tasks/update":
    case "tasks/cancel":
      invariant(request.params.taskId === "typescript-task-1", "unexpected task id");
      result = { resultType: "complete" };
      break;
    default:
      return {
        jsonrpc: "2.0",
        id: request.id,
        error: { code: -32601, message: "Method not found" },
      };
  }
  const response = { jsonrpc: "2.0", id: request.id, result };
  validateJsonRpc(response);
  return response;
}

function writeLine(value) {
  process.stdout.write(`${JSON.stringify(value)}\n`);
}

async function serveStdio() {
  const lines = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
  for await (const line of lines) {
    if (!line.trim()) continue;
    writeLine(resultFor(JSON.parse(line)));
  }
}

async function serveHttp() {
  const requestedPort = Number(process.argv[3] ?? "0");
  const server = http.createServer(async (request, response) => {
    const chunks = [];
    for await (const chunk of request) chunks.push(chunk);
    const message = JSON.parse(Buffer.concat(chunks).toString("utf8"));
    invariant(request.method === "POST", "expected POST");
    invariant(request.headers["content-type"]?.startsWith("application/json"),
      "missing JSON content type");
    invariant(request.headers["mcp-protocol-version"] === PROTOCOL,
      "protocol header mismatch");
    invariant(request.headers["mcp-method"] === message.method, "method header mismatch");
    const body = JSON.stringify(resultFor(message));
    response.writeHead(200, {
      "content-type": "application/json",
      "cache-control": "no-store",
    });
    response.end(body);
  });
  await new Promise((resolve) => server.listen(requestedPort, "127.0.0.1", resolve));
  const address = server.address();
  writeLine({ ready: `http://127.0.0.1:${address.port}/mcp`, sdk: SDK });
  const stop = () => server.close(() => process.exit(0));
  process.on("SIGTERM", stop);
  process.on("SIGINT", stop);
}

let nextId = 0;
async function callHttp(endpoint, method, params = {}) {
  const id = `typescript-${++nextId}`;
  const request = {
    jsonrpc: "2.0",
    id,
    method,
    params: { ...params, _meta: metadata() },
  };
  validateJsonRpc(request);
  const headers = {
    "content-type": "application/json",
    accept: "application/json, text/event-stream",
    "mcp-protocol-version": PROTOCOL,
    "mcp-method": method,
    authorization: "Bearer qualification-secret",
  };
  if (params.name) headers["mcp-name"] = params.name;
  if (params.taskId) headers["mcp-name"] = params.taskId;
  const response = await fetch(endpoint, {
    method: "POST",
    headers,
    body: JSON.stringify(request),
  });
  invariant(response.ok, `platform HTTP status ${response.status}`);
  const message = await response.json();
  validateJsonRpc(message);
  invariant(message.id === id && !message.error, "platform returned protocol error");
  return message.result;
}

async function runClientHttp() {
  const endpoint = process.argv[3];
  invariant(endpoint, "missing platform endpoint");
  const discover = await callHttp(endpoint, "server/discover");
  invariant(discover.supportedVersions?.includes(PROTOCOL), "platform did not advertise modern MCP");
  invariant(discover.capabilities?.extensions?.[TASKS], "platform did not advertise Tasks");
  const listed = await callHttp(endpoint, "tools/list");
  invariant(listed.tools?.some((tool) => tool.name === "qualified_echo"),
    "platform export missing");
  const complete = await callHttp(endpoint, "tools/call", {
    name: "qualified_echo",
    arguments: { value: "typescript-client" },
  });
  invariant(complete.structuredContent?.value === "typescript-client",
    "platform tool result mismatch");
  const created = await callHttp(endpoint, "tools/call", {
    name: "qualified_task",
    arguments: {},
  });
  invariant(created.resultType === "task", "platform did not create task");
  const completed = await callHttp(endpoint, "tasks/get", { taskId: created.taskId });
  invariant(completed.status === "completed", "platform task did not complete");
  writeLine({ qualified: true, sdk: SDK, transport: "streamable_http", tasks: true });
}

switch (process.argv[2]) {
  case "server-stdio":
    await serveStdio();
    break;
  case "server-http":
    await serveHttp();
    break;
  case "client-http":
    await runClientHttp();
    break;
  default:
    throw new Error("usage: fixture.mjs server-stdio|server-http|client-http [endpoint|port]");
}
