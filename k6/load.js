import http from 'k6/http';
import { check, fail } from 'k6';
import exec from 'k6/execution';
import { Counter, Rate, Trend } from 'k6/metrics';

const BASE_TIME_MS = Date.parse('2026-10-01T00:00:00Z');
// adhoc and trace filter fields that only "index everything" profiles index;
// adhoc_count runs the adhoc filter as a count without ORDER BY/LIMIT.
const ALL_READ_KINDS = ['timeline', 'attributes', 'tags', 'adhoc', 'trace', 'adhoc_count'];
// Must match the generator in internal/event.
const REGIONS = ['eu-west', 'us-east', 'ap-south', 'ap-east'];
const STATUSES = [200, 201, 202, 204, 400, 404, 429, 500, 503];
const TRACE_SAMPLES = 1000;

function integer(name, fallback, minimum, maximum) {
  const value = Number(__ENV[name] === undefined ? fallback : __ENV[name]);
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    throw new Error(`${name} must be an integer in [${minimum}, ${maximum}]`);
  }
  return value;
}

function duration(name, fallback) {
  const value = __ENV[name] || fallback;
  const units = { ms: 1, s: 1000, m: 60000, h: 3600000 };
  const pattern = /(\d+(?:\.\d+)?)(ms|s|m|h)/g;
  let consumed = '';
  let milliseconds = 0;
  let part;
  while ((part = pattern.exec(value)) !== null) {
    consumed += part[0];
    milliseconds += Number(part[1]) * units[part[2]];
  }
  if (consumed !== value || milliseconds <= 0 || !Number.isSafeInteger(milliseconds)) {
    throw new Error(`${name} must be a positive duration such as 30s or 2m`);
  }
  return { value, milliseconds };
}

const seedCount = integer('SEED_COUNT', 200000, 1, 1000000000);
const batchSize = integer('BATCH_SIZE', 10, 1, 10000);
const writePercent = integer('WRITE_PERCENT', 70, 0, 100);
const preAllocatedVUs = integer('PREALLOCATED_VUS', 100, 1, 100000);
const maxVUs = integer('MAX_VUS', 400, preAllocatedVUs, 100000);
const p95Limit = integer('P95_MS', 200, 1, 600000);
const readLimit = integer('READ_LIMIT', 50, 1, 1000);
const warmupRate = integer('WARMUP_RATE', 100, 1, 1000000);
const warmup = duration('WARMUP_DURATION', '20s');
const stepDuration = duration('STEP_DURATION', '30s');
const transition = duration('TRANSITION_DURATION', '10s');
const requestTimeout = duration('REQUEST_TIMEOUT', '5s');
const READ_KINDS = (__ENV.READ_KINDS || 'timeline,attributes,tags').split(',').map((kind) => kind.trim());
if (READ_KINDS.length === 0 || READ_KINDS.some((kind) => !ALL_READ_KINDS.includes(kind))) {
  throw new Error(`READ_KINDS must be a comma-separated subset of ${ALL_READ_KINDS.join(',')}`);
}
const baseURL = (__ENV.BASE_URL || 'http://bench:8080').replace(/\/+$/, '');
const runID = __ENV.RUN_ID || 'benchmark';
if (!/^[A-Za-z0-9][A-Za-z0-9_.-]*$/.test(runID)) {
  throw new Error('RUN_ID must contain only letters, digits, dots, dashes, and underscores');
}
const rates = (__ENV.RATE_STEPS || '100,300,600').split(',').map((item) => {
  const rate = Number(item.trim());
  if (!Number.isSafeInteger(rate) || rate < 1 || rate > 1000000) {
    throw new Error('RATE_STEPS must be a comma-separated list of positive integer rates');
  }
  return rate;
});

const stages = [];
const phases = [];
let elapsedMS = 0;
let previousRate = warmupRate;
function addPhase(name, kind, target, durationValue) {
  stages.push({ target, duration: durationValue.value });
  phases.push({
    name, kind, target, start_rate: previousRate,
    start_ms: elapsedMS, duration_ms: durationValue.milliseconds,
    end_ms: elapsedMS + durationValue.milliseconds,
  });
  elapsedMS += durationValue.milliseconds;
  previousRate = target;
}
addPhase('warmup', 'warmup', warmupRate, warmup);
rates.forEach((rate, index) => {
  if (previousRate !== rate) {
    addPhase(`ramp_${index + 1}_${rate}`, 'transition', rate, transition);
  }
  addPhase(`step_${index + 1}_${rate}`, 'measurement', rate, stepDuration);
});
const maximumIterations = Math.ceil(elapsedMS / 1000 * Math.max(warmupRate, ...rates)) + 1;
if (!Number.isSafeInteger(BASE_TIME_MS + seedCount + (maximumIterations + 1) * batchSize)) {
  throw new Error('Requested profile exceeds the safe integer range for event IDs/timestamps');
}

const dbMS = new Trend('db_ms', true);
const operations = new Counter('operations');
const writeDocuments = new Counter('write_documents');
const readEvents = new Counter('read_events');
const requestErrors = new Rate('request_errors');
const emptyReads = new Rate('empty_read_rate');
const activeOps = [];
if (writePercent > 0) activeOps.push('write');
if (writePercent < 100) activeOps.push(...READ_KINDS);
const thresholds = {
  http_req_failed: ['rate<0.01'],
  checks: ['rate>0.99'],
  request_errors: ['rate<0.01'],
  dropped_iterations: ['count==0'],
};
activeOps.forEach((op) => {
  thresholds[`http_req_duration{measured:true,op:${op}}`] = [`p(95)<${p95Limit}`];
  thresholds[`db_ms{measured:true,op:${op}}`] = ['p(95)>=0'];
});
phases.forEach((phase) => {
  const filter = `phase:${phase.name}`;
  thresholds[`operations{${filter}}`] = ['count>0'];
  thresholds[`request_errors{${filter}}`] = ['rate<0.01'];
  thresholds[`http_req_duration{${filter}}`] = ['p(95)>=0'];
  if (writePercent > 0) thresholds[`write_documents{${filter}}`] = ['count>=0'];
  if (writePercent < 100) thresholds[`empty_read_rate{${filter}}`] = ['rate>=0'];
  activeOps.forEach((op) => {
    const opFilter = `${filter},op:${op}`;
    thresholds[`operations{${opFilter}}`] = ['count>=0'];
    thresholds[`request_errors{${opFilter}}`] = ['rate>=0'];
    if (op !== 'write') thresholds[`empty_read_rate{${opFilter}}`] = ['rate>=0'];
    thresholds[`http_req_duration{${opFilter}}`] = phase.kind === 'measurement'
      ? [`p(95)<${p95Limit}`] : ['p(95)>=0'];
    thresholds[`db_ms{${opFilter}}`] = ['p(95)>=0'];
  });
});

http.setResponseCallback(http.expectedStatuses(200));

export const options = {
  scenarios: {
    mixed: {
      executor: 'ramping-arrival-rate',
      startRate: warmupRate,
      timeUnit: '1s',
      stages,
      preAllocatedVUs,
      maxVUs,
      gracefulStop: requestTimeout.value,
    },
  },
  thresholds,
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  systemTags: ['status', 'method', 'name', 'scenario', 'expected_response'],
  tags: { run_id: runID },
};

export function setup() {
  const response = http.get(`${baseURL}/health`, {
    timeout: requestTimeout.value,
    tags: { name: 'GET /health', op: 'health', phase: 'setup', measured: 'false' },
  });
  if (response.status !== 200) {
    fail(`Benchmark service is not ready: HTTP ${response.status}`);
  }
  if (!READ_KINDS.includes('trace')) return { traces: [] };
  const traces = http.get(`${baseURL}/admin/traces?seed_count=${seedCount}&count=${TRACE_SAMPLES}`, {
    timeout: requestTimeout.value,
    tags: { name: 'GET /admin/traces', op: 'setup', phase: 'setup', measured: 'false' },
  });
  if (traces.status !== 200) fail(`Cannot sample trace IDs: HTTP ${traces.status}`);
  return { traces: traces.json() };
}

function hash(value, salt) {
  let result = (value ^ salt) >>> 0;
  result = Math.imul(result ^ (result >>> 16), 0x7feb352d);
  result = Math.imul(result ^ (result >>> 15), 0x846ca68b);
  return (result ^ (result >>> 16)) >>> 0;
}

function twoDigits(value) {
  return value < 10 ? `0${value}` : String(value);
}

function phaseAt(milliseconds) {
  for (let index = 0; index < phases.length; index++) {
    if (milliseconds < phases[index].end_ms) return phases[index];
  }
  return phases[phases.length - 1];
}

function readRequest(op, iteration, data) {
  const tenant = 1 + hash(iteration, 0x1245ab37) % 100;
  // Fixed historical windows keep the read working set comparable across engines.
  const fromMS = BASE_TIME_MS + (hash(iteration, 0x163abf65) % 2 === 0 ? 1 : Math.floor(seedCount / 2));
  const toMS = BASE_TIME_MS + seedCount + 1;
  const window = `from_ms=${fromMS}&to_ms=${toMS}&limit=${readLimit}`;
  switch (op) {
    case 'attributes':
      return `/read?kind=attributes&tenant=${tenant}&${window}`
        + `&service=svc-${twoDigits(hash(iteration, 0x596b2df1) % 20)}&level=error`;
    case 'tags':
      return `/read?kind=tags&tenant=${tenant}&${window}&tag=tag-${twoDigits(hash(iteration, 0x273ec98b) % 20)}`;
    case 'adhoc':
    case 'adhoc_count': {
      const region = REGIONS[hash(iteration, 0x3c6ef372) % REGIONS.length];
      const status = STATUSES[hash(iteration, 0x78dde6e4) % STATUSES.length];
      const path = op === 'adhoc' ? '/read' : '/count';
      return `${path}?kind=adhoc&tenant=${tenant}&${window}&level=error&region=${region}&status=${status}`;
    }
    case 'trace': {
      // A known seeded event over the whole seeded range: exactly one match.
      const sample = data.traces[hash(iteration, 0x5be0cd19) % data.traces.length];
      return `/read?kind=trace&tenant=${sample.tenant}&from_ms=${BASE_TIME_MS + 1}&to_ms=${toMS}`
        + `&limit=${readLimit}&trace_id=${sample.trace_id}`;
    }
    default:
      return `/read?kind=timeline&tenant=${tenant}&${window}`;
  }
}

export default function (data) {
  const iteration = exec.scenario.iterationInTest;
  const slot = (iteration * 37) % 100;
  const isWrite = slot < writePercent;
  const readOrdinal = Math.floor(iteration / 100) * (100 - writePercent) + slot - writePercent;
  const op = isWrite ? 'write' : READ_KINDS[readOrdinal % READ_KINDS.length];
  const phase = phaseAt(Date.now() - exec.scenario.startTime);
  const tags = { phase: phase.name, measured: String(phase.kind === 'measurement'), op };
  const params = {
    timeout: requestTimeout.value,
    tags: Object.assign({ name: isWrite ? 'POST /write' : `GET ${op}` }, tags),
  };
  let response;
  if (isWrite) {
    params.headers = { 'Content-Type': 'application/json' };
    // One scenario owns the ID range; read iterations leave harmless gaps.
    response = http.post(`${baseURL}/write`, JSON.stringify({
      start_id: seedCount + 1 + iteration * batchSize,
      count: batchSize,
    }), params);
  } else {
    response = http.get(baseURL + readRequest(op, iteration, data), params);
  }
  let body = null;
  try { body = response.json(); } catch (_) { /* Validation records malformed responses. */ }
  const hasTiming = body !== null && typeof body.db_ms === 'number'
    && Number.isFinite(body.db_ms) && body.db_ms >= 0;
  const isCount = op === 'adhoc_count';
  let validCount = body !== null && Number.isInteger(body.count) && body.count >= 0;
  if (validCount && isWrite) validCount = body.count === batchSize;
  else if (validCount && !isCount) {
    validCount = Array.isArray(body.events) && body.count === body.events.length && body.count <= readLimit;
  }
  const valid = check(response, {
    'status and response contract valid': (result) => result.status === 200 && hasTiming && validCount,
  }, tags);
  operations.add(1, tags);
  requestErrors.add(!valid, tags);
  if (hasTiming) dbMS.add(body.db_ms, tags);
  if (valid && isWrite) writeDocuments.add(body.count, tags);
  if (valid && !isWrite) {
    readEvents.add(body.count, tags);
    emptyReads.add(body.count === 0, tags);
  }
}

function metricValue(data, name, key) {
  const metric = data.metrics[name];
  return metric && metric.values && typeof metric.values[key] === 'number' ? metric.values[key] : 0;
}

export function handleSummary(data) {
  const failedThresholds = [];
  Object.keys(data.metrics).forEach((name) => {
    const metricThresholds = data.metrics[name].thresholds || {};
    Object.keys(metricThresholds).forEach((expression) => {
      if (!metricThresholds[expression].ok) failedThresholds.push(`${name}: ${expression}`);
    });
  });
  const config = {
    run_id: runID, base_url: baseURL, seed_count: seedCount, base_time_ms: BASE_TIME_MS,
    write_percent: writePercent, batch_size: batchSize, read_limit: readLimit, read_kinds: READ_KINDS,
    rate_steps: rates, warmup_rate: warmupRate, warmup_duration: warmup.value,
    step_duration: stepDuration.value, transition_duration: transition.value,
    preallocated_vus: preAllocatedVUs, max_vus: maxVUs, p95_limit_ms: p95Limit,
    request_timeout: requestTimeout.value, phases,
    read_window: 'alternating whole seed range and latter half; excludes concurrent writes',
    id_allocation: 'SEED_COUNT + 1 + scenario.iterationInTest * BATCH_SIZE',
  };
  const report = Object.assign({}, data, {
    schema_version: 1, config, failed_thresholds: failedThresholds,
    generated_at: new Date().toISOString(),
  });
  const lines = [
    `\n${runID}: ${failedThresholds.length === 0 ? 'PASS' : 'THRESHOLDS FAILED'}`,
    `HTTP failed: ${(metricValue(data, 'http_req_failed', 'rate') * 100).toFixed(2)}%`
      + ` | invalid responses: ${(metricValue(data, 'request_errors', 'rate') * 100).toFixed(2)}%`
      + ` | dropped iterations: ${metricValue(data, 'dropped_iterations', 'count')}`,
    `Documents written: ${metricValue(data, 'write_documents', 'count')}`
      + ` | empty reads: ${(metricValue(data, 'empty_read_rate', 'rate') * 100).toFixed(2)}%`,
    'Phase / operation: requests, completed req/s, HTTP p95/p99 ms, DB p95 ms',
  ];
  phases.forEach((phase) => {
    const documents = metricValue(data, `write_documents{phase:${phase.name}}`, 'count');
    lines.push(`  ${phase.name}: ${documents} documents written, ${(documents * 1000 / phase.duration_ms).toFixed(1)} documents/s`);
    activeOps.forEach((op) => {
      const filter = `phase:${phase.name},op:${op}`;
      const count = metricValue(data, `operations{${filter}}`, 'count');
      const p95 = metricValue(data, `http_req_duration{${filter}}`, 'p(95)');
      const p99 = metricValue(data, `http_req_duration{${filter}}`, 'p(99)');
      const dbP95 = metricValue(data, `db_ms{${filter}}`, 'p(95)');
      lines.push(`  ${phase.name} / ${op}: ${count}, ${(count * 1000 / phase.duration_ms).toFixed(1)}`
        + `, ${p95.toFixed(2)}/${p99.toFixed(2)}, ${dbP95.toFixed(2)}`);
    });
  });
  if (failedThresholds.length > 0) lines.push(`Failed thresholds: ${failedThresholds.join('; ')}`);
  lines.push(`Summary: /results/${runID}.json\n`);
  return { stdout: `${lines.join('\n')}\n`, [`/results/${runID}.json`]: JSON.stringify(report, null, 2) };
}
