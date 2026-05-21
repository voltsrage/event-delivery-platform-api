import esClient from "./esClient.js";
import { DELIVERY_LOGS_INDEX } from "./deliveryLogsIndex.js";
import { paginatedResponse } from "../utils/paginate.js";
import { NotFoundError } from "../errors/AppError.js";

function buildQuery({tenantId, status, topicName, from, to, q})
{
    // tenant_id is always in filter - it is a hard requirement, not a relevance signal.
    // filter clauses are cached by Elasticsearch; must clauses are scored
    const filters = [
        {term: {tenant_id: tenantId}},
    ];

    if(status) filters.push({term: {status}});
    if(topicName) filters.push({term: {topic_name: topicName}});

    if(from || to){
        const range = {};
        if(from) range.gte = from;
        if(to) range.lte = to;
        filters.push({range: {attempted_at: range}});
    }

    return {
        bool: {
            filter: filters,
            // must clause only when q is present - it affects scoring (relevance ordering).
            // Without q, there is no must clause and all matching documents score equally
            ...(q ? {must: [{match: {payload : q}}]} : {})
        }
    }
}

function hitToLog(hit){
    return {id: hit._id, ...hit._source};
}

export async function searchDeliveryLogs({
    tenantId, status, topicName, from, to, q, page, pageSize, skip
})
{
    const result = await esClient.search({
        index: DELIVERY_LOGS_INDEX,
        from: skip,
        size: pageSize,
        // track_total_hits: true ensures an accurate count beyond Elasticsearch's
        // default 10,000 document cap. Required for correct totalPages on large datasets
        track_total_hits: true,
        sort: [{attempted_at: {order: 'asc'}}],
        query: buildQuery({tenantId, status, topicName, from, to, q})
    });

    const total = result.hits.total.value;
    const items = result.hits.hits.map(hitToLog);

    return paginatedResponse(items, total, page, pageSize);
}

export async function getDeliveryLogById({tenantId, attemptId}) {
    let result;
    try{
        result = await esClient.get({index: DELIVERY_LOGS_INDEX, id: attemptId});
    }
    catch(err){
        // The @elastic/elasticsearch client throws when the document is not found.
        if (err.meta?.statusCode === 404) {
            throw new NotFoundError('Delivery log not found.', 'DELIVERY_LOG_NOT_FOUND');
        }
        throw err;
    }
    
    if(!result.found){
        throw new NotFoundError('Delivery log not found.', 'DELIVERY_LOG_NOT_FOUND');
    }

    // esClient.get does not accept a query filter - tenant isolation must be enforced
    // in application code after fetching. A mismatch returns 404, not 403, so the 
    // caller cannot infer whether a document with that ID exists for another tenant
    if(result._source.tenant_id !== tenantId){
        throw new NotFoundError('Delivery log not found.', 'DELIVERY_LOG_NOT_FOUND');
    }

    return hitToLog(result);
}