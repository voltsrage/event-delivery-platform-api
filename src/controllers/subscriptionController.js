import { parsePagination } from "../utils/paginate.js";
import * as subscriptionService from '../services/subscriptionService.js';
import {ApiResponse} from '../utils/ApiResponse.js';
import * as replayService from '../services/replayService.js';
import { checkRateLimit } from "../utils/rateLimit.js";

export async function createSubscription(req, res){
    await checkRateLimit(req.tenantId, 'subscriptions:create');
    
    const {topicId} = req.params;
    const {endpoint} = req.body;

    const sub = await subscriptionService.createSubscription({tenantId: req.tenantId, topicId, endpoint});

    return res.status(201).send(ApiResponse.created(sub));
}

export async function listSubscriptions(req, res){
    const {topicId} = req.params;
    const {page, pageSize, skip} = parsePagination(req.query);

    const result = await subscriptionService.listSubscriptions({tenantId: req.tenantId, topicId,page, pageSize, skip});

    return res.send(ApiResponse.success(result));
}

export async function getSubscriptionById(req, res){
    const sub = await subscriptionService.getSubscriptionById({tenantId: req.tenantId, subscriptionId: req.params.id});

    return res.send(ApiResponse.success(sub));
}

export async function updateSubscription(req, res){
    const {endpoint, isEnabled} = req.body;

    const sub = await subscriptionService.updateSubscription(
        {
            tenantId: req.tenantId,
            subscriptionId: req.params.id,
            endpoint,
            isEnabled
        });

    return res.send(ApiResponse.success(sub));
}

export async function deleteSubscription(req, res)
{
    await subscriptionService.deleteSubscription({tenantId: req.tenantId, subscriptionId: req.params.id});

    return res.status(204).send(ApiResponse.success(null));
}

export async function rotateSecret(req, res){
    const sub = await subscriptionService.rotateSecret({tenantId: req.tenantId, subscriptionId: req.params.id});

    return res.send(ApiResponse.success(sub));
}

export async function replaySubscription(req, res)
{
    const {from} = req.body;
    const result = await replayService.replaySubscription({
        tenantId: req.tenantId,
        subscriptionId: req.params.id,
        from
    });

    return res.send(ApiResponse.success(result));
}