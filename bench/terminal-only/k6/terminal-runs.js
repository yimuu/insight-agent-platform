import http from "k6/http";
import exec from "k6/execution";
import { check, sleep } from "k6";
import { Counter, Rate, Trend } from "k6/metrics";

const baseUrl = __ENV.BASE_URL || "http://127.0.0.1:3000";
const agentId = __ENV.AGENT_ID || "action_demo";
const profile = __ENV.PROFILE || "terminal-only";
const duration = __ENV.DURATION || "2h";
const arrivalRate = Number.parseInt(__ENV.ARRIVAL_RATE || "10", 10);
const preAllocatedVUs = Number.parseInt(__ENV.PREALLOCATED_VUS || "20", 10);
const maxVUs = Number.parseInt(__ENV.MAX_VUS || "50", 10);
const pollInterval = Number.parseFloat(__ENV.POLL_INTERVAL_SECONDS || "0.02");
const runTimeout = Number.parseFloat(__ENV.RUN_TIMEOUT_SECONDS || "15");

function durationSeconds(value) {
  const match = /^([1-9][0-9]*)(s|m|h)$/.exec(value);
  if (match === null) {
    throw new Error(`unsupported exact-arrival duration: ${value}`);
  }
  const amount = Number.parseInt(match[1], 10);
  if (match[2] === "h") {
    return amount * 3600;
  }
  if (match[2] === "m") {
    return amount * 60;
  }
  return amount;
}

const configuredDurationSeconds = durationSeconds(duration);
const expectedArrivals = configuredDurationSeconds * arrivalRate;
const arrivalSlotMs = 1000 / arrivalRate;

const accepted = new Counter("terminal_run_accepted");
const terminalObserved = new Counter("terminal_run_terminal_observed");
const succeeded = new Counter("terminal_run_succeeded");
const failed = new Counter("terminal_run_failed");
const interrupted = new Counter("terminal_run_interrupted");
const rejected = new Counter("terminal_run_rejected");
const polls = new Counter("terminal_run_poll_requests");
const acceptedClosure = new Rate("terminal_run_accepted_closure");
const scheduledSuccess = new Rate("terminal_run_scheduled_success");
const lifecycle = new Trend("terminal_run_lifecycle_duration", true);
const createDuration = new Trend("terminal_run_create_duration", true);
const arrivalsScheduled = new Counter("terminal_run_arrivals_scheduled");
const arrivalLateness = new Trend("terminal_run_arrival_lateness", true);
const arrivalsLate = new Counter("terminal_run_arrivals_late");

export const options = {
  scenarios: {
    measured: {
      executor: "shared-iterations",
      vus: Math.min(maxVUs, expectedArrivals),
      iterations: expectedArrivals,
      maxDuration: `${configuredDurationSeconds + runTimeout + 30}s`,
    },
  },
  discardResponseBodies: false,
  summaryTrendStats: ["avg", "min", "med", "p(90)", "p(95)", "p(99)", "max"],
};

export function handleSummary(data) {
  const serialized = JSON.stringify(data);
  return {
    stdout: `K6_SUMMARY_JSON_BEGIN\n${serialized}\nK6_SUMMARY_JSON_END\n`,
    [__ENV.SUMMARY_PATH || "summary.json"]: serialized,
  };
}

export function setup() {
  const deadline = Date.now() + 60_000;
  while (Date.now() < deadline) {
    const response = http.get(`${baseUrl}/health/ready`, {
      tags: { endpoint: "health_ready" },
      timeout: "5s",
    });
    if (response.status === 200) {
      return;
    }
    sleep(1);
  }
  throw new Error("runtime did not become ready within 60 seconds");
}

function responseData(response) {
  try {
    const body = response.json();
    return body && body.data;
  } catch (_) {
    return null;
  }
}

function terminalKind(status) {
  if (status === "completed" || status === "succeeded") {
    return "succeeded";
  }
  if (status === "failed" || status === "cancelled" || status === "timed_out") {
    return "failed";
  }
  if (status === "interrupted") {
    return "interrupted";
  }
  return null;
}

function waitForExactArrival() {
  const ordinal = exec.scenario.iterationInTest;
  const targetMs =
    exec.scenario.startTime + ((ordinal + 1) * 1000) / arrivalRate;
  let delayMs = targetMs - Date.now();
  while (delayMs > 0) {
    sleep(delayMs / 1000);
    delayMs = targetMs - Date.now();
  }
  const latenessMs = Date.now() - targetMs;
  arrivalsScheduled.add(1);
  arrivalLateness.add(latenessMs);
  if (latenessMs >= arrivalSlotMs) {
    arrivalsLate.add(1);
  }
  return ordinal;
}

export default function () {
  const arrivalOrdinal = waitForExactArrival();
  const startedAt = Date.now();
  const requestId =
    `terminal-${profile}-${arrivalOrdinal}-${startedAt}`;
  const create = http.post(
    `${baseUrl}/v1/agents/${agentId}/runs`,
    JSON.stringify({
      text: `terminal-only qualification ${profile} ${__VU}/${__ITER}`,
    }),
    {
      headers: {
        "content-type": "application/json",
        "x-request-id": requestId,
      },
      tags: { endpoint: "create_terminal_run" },
      timeout: "10s",
    },
  );
  const created = responseData(create);
  const runId =
    create.headers["X-Run-Id"] ||
    create.headers["x-run-id"] ||
    (created && created.run_id);
  const admitted = create.status === 202 && typeof runId === "string";
  check(create, {"terminal run admitted": () => admitted});
  if (!admitted) {
    rejected.add(1);
    return;
  }
  accepted.add(1);
  createDuration.add(create.timings.duration);

  const deadline = Date.now() + runTimeout * 1000;
  while (Date.now() < deadline) {
    const response = http.get(`${baseUrl}/v1/runs/${runId}`, {
      tags: { endpoint: "get_terminal_run" },
      timeout: "10s",
    });
    polls.add(1);
    if (response.status === 200) {
      const data = responseData(response);
      const kind = terminalKind(data && data.status);
      if (kind !== null) {
        terminalObserved.add(1);
        acceptedClosure.add(true);
        lifecycle.add(Date.now() - startedAt);
        if (kind === "succeeded") {
          succeeded.add(1);
          scheduledSuccess.add(true);
        } else if (kind === "interrupted") {
          interrupted.add(1);
          scheduledSuccess.add(false);
        } else {
          failed.add(1);
          scheduledSuccess.add(false);
        }
        return;
      }
    }
    sleep(pollInterval);
  }

  acceptedClosure.add(false);
  scheduledSuccess.add(false);
  lifecycle.add(Date.now() - startedAt);
  check(null, {"accepted run reached a terminal view": () => false});
}
