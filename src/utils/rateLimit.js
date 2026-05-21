import { redis } from "../db/redis.js";
import { TooManyRequestsError } from "../errors/AppError";

// Limits keyed by action name. windowSecs is the counter TTL in second.
const LIMITS = {
    'events:publish': {max: 1_000, windowSecs: 60},
    'subscriptions:create': {max: 10, windowSecs: 3_600}
}

export async function checkRateLimit(tenantId, action){
    const {max, windowSecs} = LIMITS[action];
    const key = `rl:${action}:${tenantId}`;

    const count = await redis.incr(key);

    // Set TTL only when the key is first created. Resetting on every increment
    // would restart the window on each request - the counter would never expire
    if(count === 1)
    {
        await redis.expire(key, windowSecs);
    }

    if(count > max){
        // ttl() returns remaining seconds; fall back to full window if key has not TTL
        const ttl = await redis.ttl(key);
        throw new TooManyRequestsError(
            `Rate limit exceeded. Try again in ${ttl > 0 ? ttl : windowSecs} seconds.`,
            'RATE_LIMITED',
            ttl > 0 ? ttl : windowSecs,
        )
    }
}