import http from "k6/http";
import { check, sleep } from "k6";
import { Counter, Rate, Trend } from "k6/metrics";

const baseUrl = __ENV.BASE_URL;
const agentId = __ENV.AGENT_ID || "conversation_demo";
const batchId = __ENV.BATCH_ID;
const tenantId = __ENV.TENANT_ID || `gate-d-${batchId}`;
const conversationCount = Number.parseInt(__ENV.CONVERSATIONS || "100", 10);
const turns = Number.parseInt(__ENV.TURNS || "100", 10);
const pageSize = Number.parseInt(__ENV.PAGE_SIZE || "17", 10);
const replayEvery = Number.parseInt(__ENV.REPLAY_EVERY || "10", 10);
const contentRepeat = Number.parseInt(__ENV.CONTENT_REPEAT || "1", 10);
const pollInterval = Number.parseFloat(__ENV.POLL_INTERVAL_SECONDS || "0.02");
const runTimeout = Number.parseFloat(__ENV.RUN_TIMEOUT_SECONDS || "15");
const capacityRetryTimeout = Number.parseFloat(
  __ENV.CAPACITY_RETRY_TIMEOUT_SECONDS || "60",
);
const capacityRetryInterval = Number.parseFloat(
  __ENV.CAPACITY_RETRY_INTERVAL_SECONDS || "0.1",
);
const capacityRetryMaxAttempts = Number.parseInt(
  __ENV.CAPACITY_RETRY_MAX_ATTEMPTS || "64",
  10,
);

const conversationsCreated = new Counter("conversation_created");
const turnAttempts = new Counter("conversation_turn_attempts");
const turnCapacityRejected = new Counter(
  "conversation_turn_capacity_rejected",
);
const turnFreshAcceptance = new Counter(
  "conversation_turn_fresh_acceptance",
);
const turnsAccepted = new Counter("conversation_turn_accepted");
const turnsSucceeded = new Counter("conversation_turn_succeeded");
const replaysVerified = new Counter("conversation_replay_verified");
const pagesRead = new Counter("conversation_pages_read");
const paginationVerified = new Counter("conversation_pagination_verified");
const workloadSuccess = new Rate("conversation_workload_success");
const recentPageLatency = new Trend("conversation_recent_page_latency", true);

export const options = {
  scenarios: {
    conversations: {
      executor: "per-vu-iterations",
      vus: conversationCount,
      iterations: 1,
      maxDuration: __ENV.MAX_DURATION || "30m",
      gracefulStop: "30s",
    },
  },
  discardResponseBodies: false,
  summaryTrendStats: ["avg", "min", "med", "p(90)", "p(95)", "p(99)", "max"],
};

export function handleSummary(data) {
  return {
    [__ENV.SUMMARY_PATH || "gate-d-summary.json"]: JSON.stringify(data),
  };
}

export function setup() {
  const deadline = Date.now() + 60_000;
  while (Date.now() < deadline) {
    const ready = http.get(`${baseUrl}/health/ready`, {timeout: "5s"});
    if (ready.status === 200) {
      return;
    }
    sleep(1);
  }
  throw new Error("runtime did not become ready");
}

function data(response) {
  try {
    const body = response.json();
    return body && body.data;
  } catch (_) {
    return null;
  }
}

function errorCode(response) {
  try {
    const body = response.json();
    return body && body.code;
  } catch (_) {
    return null;
  }
}

function headers(userId, requestId) {
  return {
    "content-type": "application/json",
    "x-tenant-id": tenantId,
    "x-user-id": userId,
    "x-request-id": requestId,
  };
}

function postTurnWithCapacityRetry(
  conversationId,
  userId,
  requestId,
  payload,
) {
  const deadline = Date.now() + capacityRetryTimeout * 1000;
  let attempt = 0;
  let response = null;
  while (attempt < capacityRetryMaxAttempts) {
    if (attempt > 0 && Date.now() >= deadline) {
      return response;
    }
    response = http.post(
      `${baseUrl}/v1/conversations/${conversationId}/messages`,
      payload,
      {headers: headers(userId, requestId), timeout: "10s"},
    );
    attempt += 1;
    turnAttempts.add(1);
    if (
      response.status !== 429 ||
      errorCode(response) !== "RUN_CAPACITY_EXCEEDED"
    ) {
      return response;
    }
    const retryAfterRaw =
      response.headers["Retry-After"] ||
      response.headers["retry-after"] ||
      "";
    const retryAfterText =
      typeof retryAfterRaw === "string" ? retryAfterRaw.trim() : "";
    const runIdentityAbsent =
      !response.headers["X-Run-ID"] &&
      !response.headers["x-run-id"] &&
      !response.headers["X-Message-ID"] &&
      !response.headers["x-message-id"];
    const capacityContractValid =
      /^[1-9][0-9]*$/.test(retryAfterText) &&
      runIdentityAbsent;
    check(response, {
      "capacity rejection is explicit and non-admitting": () =>
        capacityContractValid,
    });
    if (!capacityContractValid) {
      return response;
    }
    turnCapacityRejected.add(1);
    const remainingSeconds = (deadline - Date.now()) / 1000;
    if (remainingSeconds <= 0) {
      return response;
    }
    const retryAfterSeconds = Number.parseInt(retryAfterText, 10);
    sleep(Math.min(
      Math.max(retryAfterSeconds, capacityRetryInterval),
      remainingSeconds,
    ));
  }
  return response;
}

function waitForSuccess(conversationId, userId, runId) {
  const deadline = Date.now() + runTimeout * 1000;
  while (Date.now() < deadline) {
    const response = http.get(
      `${baseUrl}/v1/conversations/${conversationId}/messages?limit=2`,
      {
      headers: {
        "x-tenant-id": tenantId,
        "x-user-id": userId,
      },
      tags: {endpoint: "gate_d_wait_assistant"},
      timeout: "10s",
    });
    const page = data(response);
    if (
      response.status === 200 &&
      page &&
      page.messages.some(
        (message) => message.role === "assistant" && message.run_id === runId,
      )
    ) {
      const run = http.get(`${baseUrl}/v1/runs/${runId}`, {
        headers: {
          "x-tenant-id": tenantId,
          "x-user-id": userId,
        },
        tags: {endpoint: "gate_d_wait_terminal"},
        timeout: "10s",
      });
      const runData = data(run);
      const status = runData && runData.status;
      return (
        run.status === 200 &&
        (status === "completed" || status === "succeeded")
      );
    }
    sleep(pollInterval);
  }
  return false;
}

export default function () {
  const userId = `gate-d-user-${__VU}`;
  const createRequest = `gate-d-create-${batchId}-${__VU}`;
  const created = http.post(
    `${baseUrl}/v1/conversations`,
    JSON.stringify({agent_id: agentId}),
    {headers: headers(userId, createRequest), timeout: "10s"},
  );
  const conversation = data(created);
  const conversationId = conversation && conversation.conversation_id;
  if (!check(created, {
    "conversation created": () =>
      created.status === 201 && typeof conversationId === "string",
  })) {
    workloadSuccess.add(false);
    return;
  }
  conversationsCreated.add(1);

  for (let turn = 0; turn < turns; turn += 1) {
    const requestId = `gate-d-turn-${batchId}-${__VU}-${turn}`;
    const text = (
      `Gate D conversation ${__VU} turn ${turn}. `
    ).repeat(contentRepeat);
    const payload = JSON.stringify({content: {text}});
    const response = postTurnWithCapacityRetry(
      conversationId,
      userId,
      requestId,
      payload,
    );
    const turnData = data(response);
    const runId = turnData && turnData.run && turnData.run.run_id;
    const messageId =
      turnData && turnData.user_message && turnData.user_message.message_id;
    const accepted =
      response.status === 202 &&
      typeof runId === "string" &&
      typeof messageId === "string" &&
      turnData.replayed === false;
    if (!check(response, {"conversation turn accepted": () => accepted})) {
      console.error(JSON.stringify({
        event: "conversation_turn_rejected",
        vu: __VU,
        turn,
        http_status: response.status,
        code: errorCode(response),
      }));
      workloadSuccess.add(false);
      return;
    }
    turnsAccepted.add(1);
    turnFreshAcceptance.add(1);
    if (!waitForSuccess(conversationId, userId, runId)) {
      check(null, {"conversation turn succeeded": () => false});
      workloadSuccess.add(false);
      return;
    }
    turnsSucceeded.add(1);

    if (replayEvery > 0 && turn % replayEvery === 0) {
      const replay = http.post(
        `${baseUrl}/v1/conversations/${conversationId}/messages`,
        payload,
        {headers: headers(userId, requestId), timeout: "10s"},
      );
      const replayData = data(replay);
      const replayOk =
        replay.status === 202 &&
        replayData &&
        replayData.replayed === true &&
        replayData.run.run_id === runId &&
        replayData.user_message.message_id === messageId;
      if (!check(replay, {"turn replay returns same message and run": () => replayOk})) {
        workloadSuccess.add(false);
        return;
      }
      replaysVerified.add(1);
    }
  }

  let cursor = null;
  const seen = new Set();
  const roles = [];
  let previousOrder = Number.MAX_SAFE_INTEGER;
  let firstPage = true;
  let pageCount = 0;
  for (;;) {
    const suffix =
      `?limit=${pageSize}` +
      (cursor === null ? "" : `&cursor=${encodeURIComponent(cursor)}`);
    const response = http.get(
      `${baseUrl}/v1/conversations/${conversationId}/messages${suffix}`,
      {
        headers: {
          "x-tenant-id": tenantId,
          "x-user-id": userId,
        },
        timeout: "10s",
        tags: {endpoint: "gate_d_message_page"},
      },
    );
    if (firstPage) {
      recentPageLatency.add(response.timings.duration);
      firstPage = false;
    }
    const page = data(response);
    if (response.status !== 200 || !page || !Array.isArray(page.messages)) {
      check(response, {"message page readable": () => false});
      workloadSuccess.add(false);
      return;
    }
    const pageShapeOk =
      page.messages.length > 0 &&
      page.messages.length <= pageSize &&
      (
        page.next_cursor === null ||
        page.next_cursor === undefined ||
        page.messages.length === pageSize
      );
    if (!check(response, {"message page respects cursor limit": () => pageShapeOk})) {
      workloadSuccess.add(false);
      return;
    }
    pageCount += 1;
    pagesRead.add(1);
    for (const message of page.messages) {
      if (
        seen.has(message.message_id) ||
        message.message_order >= previousOrder
      ) {
        check(response, {"cursor page has no duplicate or order reversal": () => false});
        workloadSuccess.add(false);
        return;
      }
      seen.add(message.message_id);
      roles.push(message.role);
      previousOrder = message.message_order;
    }
    cursor = page.next_cursor;
    if (cursor === null || cursor === undefined) {
      break;
    }
  }

  let rolesCorrect = roles.length === turns * 2;
  for (let index = 0; index < roles.length && rolesCorrect; index += 2) {
    rolesCorrect =
      roles[index] === "assistant" &&
      roles[index + 1] === "user";
  }
  const expectedPages = Math.ceil((turns * 2) / pageSize);
  const paginationOk =
    seen.size === turns * 2 &&
    rolesCorrect &&
    pageCount === expectedPages &&
    pageCount > 1;
  check(null, {
    "cursor pagination is complete, unique, and ordered": () => paginationOk,
  });
  if (paginationOk) {
    paginationVerified.add(1);
    workloadSuccess.add(true);
  } else {
    console.error(JSON.stringify({
      event: "conversation_pagination_rejected",
      vu: __VU,
      roles,
      seen_messages: seen.size,
      expected_messages: turns * 2,
      page_count: pageCount,
      expected_pages: expectedPages,
      page_size: pageSize,
    }));
    workloadSuccess.add(false);
  }
}
