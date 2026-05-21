import { parsePagination } from "../utils/paginate.js";
import * as deliveryLogSearch from '../search/deliveryLogSearch.js';
import { ApiResponse } from "../utils/ApiResponse.js";

export async function searchDeliveryLogs(req, res){
    const {status, topicName, from, to, q} = req.query;
    const {page, pageSize, skip} = parsePagination(req.query);

    const result = await deliveryLogSearch.searchDeliveryLogs({
        tenantId: req.tenantId,
        status,
        topicName,
        from,
        to,
        q,
        page,
        pageSize,
        skip,
    });

    return res.send(ApiResponse.success(result));
}

export async function getDeliveryLogById(req, res){
    const log = await deliveryLogSearch.getDeliveryLogById({tenantId: req.tenantId, attemptId: req.params.attemptId});

    return res.send(ApiResponse.success(log));
}