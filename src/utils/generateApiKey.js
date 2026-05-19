import crypto from 'crypto';

export function generateApiKey() {
    // 32 random bytes = 64 hex chars of entropy - brute force is infeasible
    const rawKey = `sk_live_${crypto.randomBytes(32).toString('hex')}`;
    // SHA-256. not bcrypt: the key is already 256 bits of entropy so a salt is unnecessary
    // bcrypt adds a random salt to protect low-entropy inputs(passwords). A random key
    // has no low-entropy problem to solve - SHA-256 is sufficient and fast enough for
    // per-request key lookup without adding bcrypt's 200ms cost to every API call.
    const keyHash = crypto.createHash('sha256').update(rawKey).digest('hex');
    const keyPrefix = rawKey.slice(0,15); // "sk_live_a1b2c3d" — display only

    return {rawKey, keyHash, keyPrefix};
}