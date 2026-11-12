import http from 'k6/http';
import { check } from 'k6';

const baseUrl = __ENV.BASE_URL || 'http://localhost:8080';
const accessToken = __ENV.ACCESS_TOKEN;
const codes = (__ENV.CODES || '005930').split(',').map((code) => code.trim()).filter(Boolean);
const rate = Number(__ENV.RPS || 50);
const duration = __ENV.DURATION || '5m';
const preAllocatedVUs = Number(__ENV.PRE_ALLOCATED_VUS || Math.max(rate, 50));
const maxVUs = Number(__ENV.MAX_VUS || Math.max(preAllocatedVUs * 2, 100));

if (!accessToken) {
  throw new Error('ACCESS_TOKEN is required');
}

export const options = {
  discardResponseBodies: true,
  scenarios: {
    stream: {
      executor: 'constant-arrival-rate',
      rate,
      timeUnit: '1s',
      duration,
      preAllocatedVUs,
      maxVUs,
    },
  },
  thresholds: {
    'http_req_failed{endpoint:stream}': ['rate<0.01'],
    'http_req_duration{endpoint:stream}': ['p(95)<300'],
    'checks{endpoint:stream}': ['rate>0.99'],
  },
};

export default function () {
  const code = codes[(__VU + __ITER) % codes.length];
  const response = http.get(`${baseUrl}/api/v1/rooms/${code}/stream?limit=50`, {
    headers: { Authorization: `Bearer ${accessToken}` },
    tags: { endpoint: 'stream' },
  });
  check(response, { 'stream status is 200': (result) => result.status === 200 }, { endpoint: 'stream' });
}
