import { describe, it, expect } from 'vitest';
import {
    computeRetryDelay,
    isLastAttempt,
    MAX_ATTEMPTS,
} from '../utils/retrySchedule.js';

describe('Retry schedule', () => {
    it('attempt 1 failed → 30 second delay', () => {
        expect(computeRetryDelay(1)).toBe(30_000);
    });

    it('attempt 2 failed → 5 minute delay', () => {
        expect(computeRetryDelay(2)).toBe(300_000);
    });

    it('attempt 3 failed → 30 minute delay', () => {
        expect(computeRetryDelay(3)).toBe(1_800_000);
    });

    it('attempt 4 failed → 2 hour delay', () => {
        expect(computeRetryDelay(4)).toBe(7_200_000);
    });

    it('attempt 5 failed → null (dead letter, no more retries)', () => {
        expect(computeRetryDelay(5)).toBeNull();
    });

    it('MAX_ATTEMPTS is 5', () => {
        expect(MAX_ATTEMPTS).toBe(5);
    });

    it('isLastAttempt is true at attempt 5', () => {
        expect(isLastAttempt(5)).toBe(true);
    });

    it('isLastAttempt is false at attempt 4', () => {
        expect(isLastAttempt(4)).toBe(false);
    });
});
