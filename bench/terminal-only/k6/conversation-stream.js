import http from "k6/http";
import { check } from "k6";
import { Counter } from "k6/metrics";

const baseUrl = __ENV.BASE_URL;
const agentId = __ENV.AGENT_ID;
const batchId = __ENV.BATCH_ID;
const outputScale = Number.parseInt(__ENV.OUTPUT_SCALE || "1", 10);
const tenantId = `gate-d-stream-${batchId}`;
const userId = "gate-d-stream-user";
const deltaFrames = new Counter("conversation_stream_delta_frames");
const terminalFrames = new Counter("conversation_stream_terminal_frames");
const messages = new Counter("conversation_stream_persisted_messages");
const calibrated = new Counter("conversation_stream_calibrated");

export const options = {
  scenarios: {
    stream: {executor: "shared-iterations", vus: 1, iterations: 1},
  },
  discardResponseBodies: false,
};

export function handleSummary(data) {
  return {
    [__ENV.SUMMARY_PATH || "stream-summary.json"]: JSON.stringify(data),
  };
}

function parseData(response) {
  try {
    return response.json().data;
  } catch (_) {
    return null;
  }
}

function parseSse(body) {
  const frames = [];
  let parseErrors = 0;
  for (const rawFrame of body.split(/\r?\n\r?\n/)) {
    if (!rawFrame.trim()) {
      continue;
    }
    let event = "message";
    const dataLines = [];
    for (const line of rawFrame.split(/\r?\n/)) {
      if (line.startsWith("event:")) {
        event = line.slice("event:".length).trimStart();
      } else if (line.startsWith("data:")) {
        dataLines.push(line.slice("data:".length).trimStart());
      }
    }
    if (dataLines.length === 0) {
      continue;
    }
    try {
      frames.push({event, data: JSON.parse(dataLines.join("\n"))});
    } catch (_) {
      parseErrors += 1;
    }
  }
  return {frames, parseErrors};
}

export default function () {
  const commonHeaders = {
    "content-type": "application/json",
    "x-tenant-id": tenantId,
    "x-user-id": userId,
  };
  const created = http.post(
    `${baseUrl}/v1/conversations`,
    JSON.stringify({agent_id: agentId}),
    {
      headers: {
        ...commonHeaders,
        "x-request-id": `stream-create-${batchId}`,
      },
      timeout: "10s",
    },
  );
  const conversation = parseData(created);
  const conversationId = conversation && conversation.conversation_id;
  if (!check(created, {
    "stream conversation created": () =>
      created.status === 201 && typeof conversationId === "string",
  })) {
    return;
  }

  const stream = http.post(
    `${baseUrl}/v1/conversations/${conversationId}/messages/stream`,
    JSON.stringify({
      content: {
        prompt: `emit the deterministic qualification fixture at ${outputScale}x scale`,
        output_scale: String(outputScale),
      },
    }),
    {
      headers: {
        ...commonHeaders,
        "x-request-id": `stream-turn-${batchId}`,
      },
      timeout: __ENV.STREAM_TIMEOUT || "60s",
      tags: {endpoint: "gate_d_conversation_stream"},
    },
  );
  const body = String(stream.body || "");
  const parsed = parseSse(body);
  const deltaEvents = parsed.frames.filter(
    (frame) => frame.event === "response.output_text.delta",
  );
  const completedEvents = parsed.frames.filter(
    (frame) => frame.event === "response.completed",
  );
  const failureEvents = parsed.frames.filter(
    (frame) =>
      frame.event === "response.failed" ||
      frame.event === "response.incomplete" ||
      frame.event === "workflow.response.timed_out" ||
      frame.event === "workflow.response.cancelled" ||
      frame.event === "workflow.response.interrupted" ||
      frame.event === "error",
  );
  const deltas = deltaEvents.length;
  const terminals = completedEvents.length + failureEvents.length;
  const concatenatedDeltas = deltaEvents
    .map((frame) => frame.data && frame.data.delta)
    .filter((delta) => typeof delta === "string")
    .join("");
  const terminal =
    completedEvents.length === 1 ? completedEvents[0].data : null;
  const terminalResult =
    terminal && terminal.workflow ? terminal.workflow.result : undefined;
  const runId =
    terminal && terminal.workflow ? terminal.workflow.run_id : undefined;
  const streamContentCalibrated =
    parsed.parseErrors === 0 &&
    deltas > 0 &&
    deltaEvents.every(
      (frame) => frame.data && typeof frame.data.delta === "string",
    ) &&
    completedEvents.length === 1 &&
    failureEvents.length === 0 &&
    typeof runId === "string" &&
    terminalResult === concatenatedDeltas;
  deltaFrames.add(deltas);
  terminalFrames.add(terminals);
  check(stream, {
    "attached stream returned HTTP 200": () => stream.status === 200,
    "attached stream frames are valid JSON": () => parsed.parseErrors === 0,
    "attached stream emitted output deltas": () => deltas > 0,
    "attached stream completed exactly once": () =>
      completedEvents.length === 1 &&
      failureEvents.length === 0 &&
      terminals === 1,
    "terminal result equals concatenated deltas": () =>
      streamContentCalibrated,
  });

  const run = http.get(`${baseUrl}/v1/runs/${runId}`, {
    headers: commonHeaders,
    timeout: "10s",
  });
  const runData = parseData(run);
  const durableOutput =
    runData && runData.output ? runData.output.data : undefined;
  const runCalibrated =
    run.status === 200 &&
    runData &&
    runData.status === "completed" &&
    durableOutput === terminalResult;
  check(run, {
    "Run GET matches terminal stream result": () => runCalibrated,
  });

  const page = http.get(
    `${baseUrl}/v1/conversations/${conversationId}/messages?limit=10`,
    {headers: commonHeaders, timeout: "10s"},
  );
  const pageData = parseData(page);
  const count =
    pageData && Array.isArray(pageData.messages)
      ? pageData.messages.length
      : -1;
  if (count >= 0) {
    messages.add(count);
  }
  const assistant =
    count >= 0
      ? pageData.messages.find(
          (message) => message.role === "assistant" && message.run_id === runId,
        )
      : null;
  const pageCalibrated =
    page.status === 200 &&
    count === 2 &&
    assistant &&
    assistant.content === terminalResult &&
    assistant.content === concatenatedDeltas;
  check(page, {
    "stream persists one user and one assistant message": () =>
      page.status === 200 &&
      count === 2 &&
      pageData.messages[0].role === "assistant" &&
      pageData.messages[1].role === "user",
    "assistant message matches stream and Run GET": () => pageCalibrated,
  });
  if (streamContentCalibrated && runCalibrated && pageCalibrated) {
    calibrated.add(1);
  }
}
