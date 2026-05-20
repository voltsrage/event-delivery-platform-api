import { describe, it, expect } from 'vitest';
import { computeHmac } from '../utils/computeHmac.js';
import crypto from 'node:crypto';

const SECRET    = 'whsec_abc123secret';
const TIMESTAMP = '1715644800';
const PAYLOAD   = { orderId: 'abc-123', amount: 5000 };

describe('computeHmac', () => {
    it('produces a deterministic 64-character hex string', () => {
        const sig = computeHmac(SECRET, TIMESTAMP, PAYLOAD);
        expect(sig).toHaveLength(64);
        expect(sig).toMatch(/^[0-9a-f]{64}$/);
        // Same inputs → same output every time.
        expect(computeHmac(SECRET, TIMESTAMP, PAYLOAD)).toBe(sig);
    });

    it('matches a reference HMAC computed independently', () => {
        const signedPayload = `${TIMESTAMP}.${JSON.stringify(PAYLOAD)}`;
        const expected      = crypto
        .createHmac('sha256', SECRET)
        .update(signedPayload)
        .digest('hex');
        expect(computeHmac(SECRET, TIMESTAMP, PAYLOAD)).toBe(expected);
    });

    it('different timestamp produces different signature (replay protection)', () => {
        const sig1 = computeHmac(SECRET, '1715644800', PAYLOAD);
        const sig2 = computeHmac(SECRET, '1715644860', PAYLOAD);
        expect(sig1).not.toBe(sig2);
    });

    it('different payload produces different signature', () => {
        const sig1 = computeHmac(SECRET, TIMESTAMP, { orderId: 'abc-123' });
        const sig2 = computeHmac(SECRET, TIMESTAMP, { orderId: 'xyz-999' });
        expect(sig1).not.toBe(sig2);
    });

    it('different secret produces different signature', () => {
        const sig1 = computeHmac('secret-a', TIMESTAMP, PAYLOAD);
        const sig2 = computeHmac('secret-b', TIMESTAMP, PAYLOAD);
        expect(sig1).not.toBe(sig2);
    });
});