export const MAX_ATTEMPTS = 5;

// Delay in milliseconds before the next attempt, indexed by the attempt number
// that just failed. attempt 1 failed -> retry attempt 2 waits 30s, etc
const RETRY_DELAY_MS = [
    null,         // [0] unused
    30_000,       // [1] attempt 1 failed → retry in 30 seconds
    300_000,      // [2] attempt 2 failed → retry in 5 minutes
    1_800_000,    // [3] attempt 3 failed → retry in 30 minutes
    7_200_000,    // [4] attempt 4 failed → retry in 2 hours
    // [5] attempt 5 failed → dead letter, no further retries   
];

// Returns the in ms before the next retry, or null if there are no more entries
export function computeRetryDelay(failedAttemptNumber){
    return RETRY_DELAY_MS[failedAttemptNumber] ?? null;
}

export function isLastAttempt(attemptNumber){
    return attemptNumber >= MAX_ATTEMPTS;
}