import * as topicService from '../services/topicService.js';
import { parsePagination } from '../utils/paginate.js';
import {ApiResponse} from '..//utils/ApiResponse.js';

export async function createTopic(req, res)
{
    const {name, description} = req.body;
    const topic = await topicService.createTopic({tenantId: req.tenantId, name, description});

    return res.status(201).send(ApiResponse.created(topic));
}

export async function listTopics(req, res)
{
    const pagination = parsePagination(req.query);
    const result = await topicService.listTopics({tenantId: req.tenantId, ...pagination});

    return res.send(ApiResponse.success(result));
}

export async function getTopicById(req, res)
{
    const result = await topicService.getTopicById({tenantId: req.tenantId, topicId: req.params.id});

    return res.send(ApiResponse.success(result));
}

export async function deleteTopic(req, res){
    await topicService.deleteTopic({tenantId: req.tenantId, topicId: req.params.id});

    return res.send(ApiResponse.success(null));
}