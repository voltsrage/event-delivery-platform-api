// Builds the Elasticsearch document from the fields available in both
// the delivery worker and the retry worker.
export function buildDeliveryLogDocument({
    tenantId,
    eventId,
    subscriptionId,
    topicName,
    endpoint,
    status,
    httpStatus,
    attemptNumber,
    payload,
    responseBody,
    attemptedAt,
    nextRetryAt,
})
{
    return {
        tenant_id: tenantId,
        event_id: eventId,
        subscription_id: subscriptionId,
        topic_name: topicName,
        endpoint_url: endpoint,
        status,
        http_status: httpStatus,
        attempt_number: attemptNumber,
        // payload is stored as a JSON string so Elasticsearch can tokenise its conteent.
        // Storing as an object would require 'nested' or 'flattened' type and
        // prevent standard full-text search across arbitrary payload shapes.
        payload: JSON.stringify(payload),
        response_body: responseBody ?? null,
        attempted_at: attemptedAt instanceof Date
            ? attemptedAt.toISOString()
            : attemptedAt,
        next_retry_at: nextRetryAt instanceof Date
            ? nextRetryAt.toISOString()
            : nextRetryAt,    
    }
}