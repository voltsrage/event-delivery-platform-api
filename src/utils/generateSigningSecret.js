import crypto from 'node:crypto';

export function generateSigningSecret() {
    const rawSecret = `whsec_${crypto.randomBytes(32).toString('hex')}`;
    const secretHash = crypto.createHash('sha256').update(rawSecret).digest('hex');

    const secretPrefix = rawSecret.slice(0, 13); // "whsec_" + 7 chars

    return {rawSecret, secretHash, secretPrefix};
}