import crypto from 'node:crypto';

// SIgn timestamp.payload together - ties the signature to a specific moment
// A subscriber validate: (1) HMAC matches, (2) timestamp is within 5 minutes of now()
// An attacker who captures a valid webhook cannot replay it after the window expires
// because the timestamp check fails even though the HMAC is mathematically still valid

export function computeHmac(secretRaw, timestamp, payload){
    const signedPayload = `${timestamp}.${JSON.stringify(payload)}`;
    return crypto
        .createHmac('sha256', secretRaw)
        .update(signedPayload)
        .digest('hex');
}