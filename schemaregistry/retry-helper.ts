const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function fullJitter(baseDelayMs: number, maxDelayMs: number, retriesAttempted: number): number {
  return Math.random() * Math.min(maxDelayMs, baseDelayMs * 2 ** retriesAttempted)
}

function isSuccess(statusCode: number): boolean {
  return statusCode >= 200 && statusCode <= 299
}

function isRetriable(statusCode: number): boolean {
  return statusCode == 408 || statusCode == 429
    || statusCode == 500 || statusCode == 502 || statusCode == 503 || statusCode == 504;
}

export { sleep, fullJitter, isSuccess, isRetriable };
