import { computeHmac } from "../utils/computeHmac";

const DELIVERY_TIMEOUT_MS = 10_000;
const MAX_RESPONSE_BODY_LEN = 1024;

export async function deliverWebhook({subscription, event}){
    const timestamp = Math.floor(Date.now() / 1000).toString();
    const signature = computeHmac(subscription.secretRaw, timestamp, event.payload);

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), DELIVERY_TIMEOUT_MS);
    const startedAt = Date.now();

    try{
        const response = await fetch(subscription.endpoint, {
            method: 'POST',
            headers: {
                'X-Webhook-Event-Id': event.eventId,
                'X-Webhook-Topic': event.topicName,
                'X-Webhook-Timestamp': timestamp,
                // Prefix "sha256" lets subscribers distinguish algorithm versions in the future
                'X-Webhook-Signature': `sha256=${signature}`
            },
            body: JSON.stringify(event.payload)    
        });

        const text = await response.text();

        return {
            success: response.ok,
            httpStatus: response.status,
            responseBody: text.slice(0, MAX_RESPONSE_BODY_LEN),
            durationMs: Date.now() - startedAt
        };
    }
    catch(err){
        // AbortError = 10s timeout elapsed.Other errors = DNS failure, connection refused, etc
        return {
            success: false,
            httpStatus: null,
            responseBody: err.name === 'AbortError' ? 'timeout after 10s' : err.message,
            durationMs: Date.now() - startedAt
        }
    }
    finally
    {
        clearTimeout(timeoutId);
    }
}