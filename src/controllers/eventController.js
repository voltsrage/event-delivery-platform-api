import * as eventService from '../services/eventService.js';
import { ApiResponse } from '../utils/ApiResponse.js';

export async function createEvent(req, res){
    const {topicId} = req.params;
    const {eventType, payload, idempotencyKey} = req.body;

    const event = await eventService.createEvent({
        tenantId: req.tenantId,
        topicId,
        eventType,
        payload,
        idempotencyKey
    });

    return res.status(202).send(ApiResponse.accepted(event));
}