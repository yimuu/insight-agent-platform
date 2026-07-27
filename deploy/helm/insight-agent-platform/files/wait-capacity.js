import http from "k6/http";
import { check, sleep } from "k6";
import { Counter, Trend } from "k6/metrics";

const baseUrl = __ENV.BASE_URL || "http://insight-agent-platform:3000";
const profile = __ENV.PROFILE || "limited-wait";
const virtualUsers = Number.parseInt(__ENV.VUS || "50", 10);
const holdDurationSeconds = Number.parseFloat(__ENV.HOLD_DURATION_SECONDS || "1800");
const pollIntervalSeconds = Number.parseFloat(__ENV.POLL_INTERVAL_SECONDS || "0.1");

const accepted = new Counter("wait_run_accepted");
const capacityRejected = new Counter("wait_run_capacity_rejected");
const terminalSuccess = new Counter("wait_run_terminal_success");
const createFailures = new Counter("wait_run_create_failure");
const signalFailures = new Counter("wait_run_signal_failure");
const getDuration = new Trend("wait_run_get_duration", true);
const wakeToTerminalDuration = new Trend("wait_run_wake_to_terminal_duration", true);
const slotRecoveryDuration = new Trend("wait_run_slot_recovery_duration", true);

export const options = {
  scenarios: {
    wait_capacity: {
      executor: "per-vu-iterations",
      vus: virtualUsers,
      iterations: 1,
      maxDuration: `${Math.ceil(holdDurationSeconds + 180)}s`,
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
  const deadline = Date.now() + 60_000;
  let response;
  while (Date.now() < deadline) {
    response = http.get(`${baseUrl}/health/ready`, {
      tags: {endpoint: "health_ready"},
      timeout: "5s",
    });
    if (response.status === 200) {
      return;
    }
    sleep(1);
  }
  throw new Error(
    `runtime is not ready after bounded retry: HTTP ${response && response.status}`,
  );
}

function responseBody(response) {
  try {
    return response.json();
  } catch (_) {
    return null;
  }
}

function createWaitingRun(sequence, measureSlotRecovery = false) {
  const requestId = `k6-${profile}-wait-${__VU}-${sequence}-${Date.now()}`;
  const response = http.post(
    `${baseUrl}/v1/agents/benchmark_wait/runs`,
    "{}",
    {
      headers: {
        "content-type": "application/json",
        "x-request-id": requestId,
      },
      tags: { endpoint: "create_wait_run" },
      timeout: "10s",
    },
  );
  const body = responseBody(response);
  const runId =
    response.headers["X-Run-Id"] ||
    response.headers["x-run-id"] ||
    (body && body.data && body.data.run_id);
  if (response.status === 202 && body && body.code === "OK" && runId) {
    accepted.add(1);
    if (measureSlotRecovery) {
      slotRecoveryDuration.add(response.timings.duration);
      check(response, {
        "released slot accepts replacement within one second": () =>
          response.timings.duration <= 1000,
      });
    }
    return runId;
  }
  createFailures.add(1);
  check(response, {"waiting Run accepted": () => false});
  return null;
}

function signalAndAwaitTerminal(runId, sequence, phase) {
  const startedAt = Date.now();
  const response = http.post(
    `${baseUrl}/v1/runs/${runId}/signals/continue`,
    JSON.stringify({
      message_id: `k6-${profile}-signal-${__VU}-${sequence}-${startedAt}`,
      value: `released-${__VU}-${sequence}`,
    }),
    {
      headers: {"content-type": "application/json"},
      tags: {
        endpoint: `signal_wait_run_${phase}`,
        name: "POST /v1/runs/:run_id/signals/continue",
      },
      timeout: "10s",
    },
  );
  if (response.status !== 200) {
    signalFailures.add(1);
    check(response, {"wait signal accepted": () => false});
    return false;
  }
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    const poll = http.get(`${baseUrl}/v1/runs/${runId}`, {
      tags: {
        endpoint: `get_wait_run_${phase}`,
        name: "GET /v1/runs/:run_id",
      },
      timeout: "5s",
    });
    if (phase === "churn") {
      getDuration.add(poll.timings.duration);
    }
    const body = responseBody(poll);
    const status = body && body.data && body.data.status;
    if (status === "completed") {
      terminalSuccess.add(1);
      if (phase === "churn") {
        wakeToTerminalDuration.add(Date.now() - startedAt);
      }
      return true;
    }
    if (status === "failed" || status === "cancelled" || status === "interrupted") {
      signalFailures.add(1);
      return false;
    }
    sleep(pollIntervalSeconds);
  }
  signalFailures.add(1);
  return false;
}

export default function () {
  const testStartedAt = Date.now();
  const holdStartsAt = testStartedAt + 60_000;
  const testDeadline = holdStartsAt + holdDurationSeconds * 1000;
  let sequence = 0;
  // Gate A measures resident durable waits rather than a short-Run burst.
  // Spread admission over fifty seconds so initialization does not accidentally
  // turn this profile into the Gate B workload.
  sleep(__VU - 1);
  let runId = createWaitingRun(sequence);
  if (!runId) {
    return;
  }

  // Give all 50 VUs time to occupy their admission slot. Only one VU probes
  // the overflow contract so rejection traffic cannot become a hot loop.
  sleep(Math.max(0, (holdStartsAt - Date.now()) / 1000));
  if (__VU === 1) {
    const overflow = http.post(
      `${baseUrl}/v1/agents/benchmark_wait/runs`,
      "{}",
      {
        headers: {
          "content-type": "application/json",
          "x-request-id": `k6-${profile}-overflow-${Date.now()}`,
        },
        tags: {endpoint: "create_wait_overflow"},
        timeout: "10s",
      },
    );
    const body = responseBody(overflow);
    const rejected =
      overflow.status === 429 &&
      body &&
      body.code === "RUN_CAPACITY_EXCEEDED" &&
      (overflow.headers["Retry-After"] || overflow.headers["retry-after"]) === "1";
    if (rejected) {
      capacityRejected.add(1);
    }
    check(overflow, {"51st Run is retryable capacity rejection": () => rejected});
  }

  // Ten groups of five VUs churn once per minute. Each individual VU wakes
  // every ten minutes, closes its old Run, and immediately occupies the freed
  // slot with a new durable wait.
  const groupDelaySeconds =
    Math.floor((__VU - 1) / 5) * 60 + ((__VU - 1) % 5);
  sleep(
    Math.min(
      groupDelaySeconds,
      Math.max(0, (testDeadline - Date.now() - 15_000) / 1000),
    ),
  );
  while (Date.now() + 15_000 < testDeadline) {
    sequence += 1;
    if (!signalAndAwaitTerminal(runId, sequence, "churn")) {
      break;
    }
    runId = createWaitingRun(sequence, true);
    if (!runId) {
      break;
    }
    sleep(Math.min(600, Math.max(0, (testDeadline - Date.now()) / 1000)));
  }

  // Do not leave 50 active waits behind for the next qualification profile.
  if (runId) {
    // Teardown is outside the resident-wait measurement. Spreading it avoids
    // making cleanup dominate cgroup throttling evidence.
    sleep(__VU - 1);
    signalAndAwaitTerminal(runId, sequence + 1, "cleanup");
  }
}
