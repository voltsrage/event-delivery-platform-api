import esClient from "./esClient.js";

export const DELIVERY_LOGS_INDEX = process.env.ELASTICSEARCH_INDEX ?? 'delivery-logs';

// Field type rationale:
// keyword - exact-match filters (term queries) and aggregations; not tokenised
//          Used for IDS, enum-like fields (status) and endpoint URLS
// text - full-text search; analyzed (tokenised, lowercases, stemmed),
//       Used for payload and response_body so subscribers can search
//       for content inside the JSON payload
// integer - numeric range queries and sorting
// date - date range queries; expects ISO 8601 strings.
const MAPPING = {
    properties: {
        tenant_id: {type: 'keyword'},
        event_id: {type: 'keyword'},
        subscription_id: {type: 'keyword'},
        topic_name: {type: 'keyword'},
        endpoint_url: {type: 'keyword'},
        status: {type: 'keyword'}, // exact: 'success' | 'failed' | 'dead_lettered'
        http_status: {type: 'integer'},
        attempt_number: {type: 'integer'},
        payload: {type: 'text', analyzer: 'standard'}, // full-text searchable
        response_body: {type: 'text'},
        attempted_at: {type: 'date'},
        next_retry_at: {type: 'date'},        
    }
};

// Called once at worker startup. Safe to call multiple times - skips creation
// if the index already exists. Idempotent
export async function ensureDeliveryLogsIndex() {
    const exists = await esClient.indices.exists({index: DELIVERY_LOGS_INDEX});
    if (exists) return;

    await esClient.indices.create({
        index: DELIVERY_LOGS_INDEX,
        mappings: MAPPING,
        settings: {
            // Single shard - appropriate for a single-node local cluster
            // In production: use 3+ shards to distribute across nodes
            number_of_shards: 1,
            number_of_replicas: 0
        }
    });

    console.log(`[es] index "${DELIVERY_LOGS_INDEX}" created`);
}