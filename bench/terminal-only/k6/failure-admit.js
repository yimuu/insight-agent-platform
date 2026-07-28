import http from "k6/http";
import { check, sleep } from "k6";
import { Counter } from "k6/metrics";

const baseUrl = __ENV.BASE_URL;
const agentId = __ENV.AGENT_ID;
const batchId = __ENV.BATCH_ID;
const conversationId = __ENV.CONVERSATION_ID || "";
const tenantId = __ENV.TENANT_ID || "gate-c-tenant";
const userId = __ENV.USER_ID || "gate-c-user";
const runCount = Number.parseInt(__ENV.RUN_COUNT || "50", 10);
const holdSeconds = Number.parseFloat(__ENV.HOLD_SECONDS || "45");
const accepted = new Counter("gate_c_accepted");

export const options = {
  scenarios: {
    admit: {
      executor: "shared-iterations",
      vus: runCount,
      iterations: runCount,
      maxDuration: `${Math.ceil(holdSeconds + 30)}s`,
    },
  },
  discardResponseBodies: false,
};

export function handleSummary(data) {
  return {
    [__ENV.SUMMARY_PATH || "gate-c-k6-summary.json"]: JSON.stringify(data),
  };
}

export default function () {
  const requestId = `gate-c-${batchId}-${__VU}-${__ITER}`;
  const content = {
    text: `terminal Gate C ${batchId} ${__VU}/${__ITER}`,
    effect_id: `gate-c-effect-${batchId}-${__VU}-${__ITER}`,
    idempotency_key: requestId,
  };
  let url = `${baseUrl}/v1/agents/${agentId}/runs`;
  let body = content;
  const headers = {
    "content-type": "application/json",
    "x-request-id": requestId,
  };
  if (conversationId !== "") {
    url = `${baseUrl}/v1/conversations/${conversationId}/messages`;
    body = {content};
    headers["x-tenant-id"] = tenantId;
    headers["x-user-id"] = userId;
  }
  const response = http.post(url, JSON.stringify(body), {
    headers,
    timeout: "15s",
    tags: {endpoint: "gate_c_admit"},
  });
  const admitted = response.status === 202;
  check(response, {"Gate C admission accepted": () => admitted});
  if (admitted) {
    accepted.add(1);
    sleep(holdSeconds);
  }
}
