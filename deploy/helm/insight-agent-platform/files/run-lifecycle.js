import http from "k6/http";
import { check, sleep } from "k6";
import { Counter, Rate, Trend } from "k6/metrics";

const baseUrl = __ENV.BASE_URL || "http://insight-agent-platform:3000";
const profile = __ENV.PROFILE || "manual";
const virtualUsers = Number.parseInt(__ENV.VUS || "8", 10);
const duration = __ENV.DURATION || "30s";
const pollIntervalSeconds = Number.parseFloat(__ENV.POLL_INTERVAL_SECONDS || "0.02");
const runTimeoutSeconds = Number.parseFloat(__ENV.RUN_TIMEOUT_SECONDS || "15");
const startDelaySeconds = Number.parseFloat(__ENV.START_DELAY_SECONDS || "0");

const createDuration = new Trend("run_create_duration", true);
const lifecycleDuration = new Trend("run_lifecycle_duration", true);
const pollDuration = new Trend("run_poll_duration", true);
const createSuccess = new Rate("run_create_success");
const terminalSuccess = new Rate("run_terminal_success");
const completedRuns = new Counter("run_completed");
const failedRuns = new Counter("run_failed");
const timedOutRuns = new Counter("run_timed_out");
const pollRequests = new Counter("run_poll_requests");

export const options = {
  scenarios: {
    default: {
      executor: "constant-vus",
      vus: virtualUsers,
      duration,
      gracefulStop: "20s",
    },
  },
  discardResponseBodies: false,
  summaryTrendStats: ["avg", "min", "med", "p(90)", "p(95)", "p(99)", "max"],
};

export function handleSummary(data) {
  const serialized = JSON.stringify(data);
  return {
    stdout: `K6_SUMMARY_JSON_BEGIN\n${serialized}\nK6_SUMMARY_JSON_END\n`,
    "/results/summary.json": serialized,
  };
}

export function setup() {
  const response = http.get(`${baseUrl}/health/ready`, {
    tags: { endpoint: "health_ready" },
    timeout: "5s",
  });
  if (response.status !== 200) {
    throw new Error(`runtime is not ready: HTTP ${response.status}`);
  }
  if (startDelaySeconds > 0) {
    sleep(startDelaySeconds);
  }
  return { startedAt: new Date().toISOString() };
}

export default function () {
  const iterationStartedAt = Date.now();
  const requestId = `k6-${profile}-${__VU}-${__ITER}-${iterationStartedAt}`;
  const payload = JSON.stringify({
    text: `limited resource benchmark ${profile} vu=${__VU} iteration=${__ITER}`,
  });
  const createResponse = http.post(`${baseUrl}/v1/agents/action_demo/runs`, payload, {
    headers: {
      "content-type": "application/json",
      "x-request-id": requestId,
    },
    tags: { endpoint: "create_run" },
    timeout: "10s",
  });
  createDuration.add(createResponse.timings.duration);

  let body;
  try {
    body = createResponse.json();
  } catch (_) {
    body = null;
  }
  const runId =
    createResponse.headers["X-Run-Id"] ||
    createResponse.headers["x-run-id"] ||
    (body && body.data && body.data.run_id);
  const created =
    createResponse.status === 202 &&
    body &&
    body.code === "OK" &&
    typeof runId === "string";
  createSuccess.add(created);
  check(createResponse, {
    "create accepted": () => created,
  });
  if (!created) {
    failedRuns.add(1);
    terminalSuccess.add(false);
    return;
  }

  const deadline = Date.now() + runTimeoutSeconds * 1000;
  while (Date.now() < deadline) {
    const pollResponse = http.get(`${baseUrl}/v1/runs/${runId}`, {
      tags: { endpoint: "get_run" },
      timeout: "10s",
    });
    pollRequests.add(1);
    pollDuration.add(pollResponse.timings.duration);
    if (pollResponse.status === 200) {
      let pollBody;
      try {
        pollBody = pollResponse.json();
      } catch (_) {
        pollBody = null;
      }
      const status = pollBody && pollBody.data && pollBody.data.status;
      if (status === "completed") {
        lifecycleDuration.add(Date.now() - iterationStartedAt);
        completedRuns.add(1);
        terminalSuccess.add(true);
        check(pollResponse, {
          "run completed": () => true,
        });
        return;
      }
      if (status === "failed" || status === "cancelled" || status === "interrupted") {
        lifecycleDuration.add(Date.now() - iterationStartedAt);
        failedRuns.add(1);
        terminalSuccess.add(false);
        check(pollResponse, {
          "run did not fail": () => false,
        });
        return;
      }
    }
    sleep(pollIntervalSeconds);
  }

  lifecycleDuration.add(Date.now() - iterationStartedAt);
  timedOutRuns.add(1);
  terminalSuccess.add(false);
  check(null, {
    "run reached terminal state before timeout": () => false,
  });
}
