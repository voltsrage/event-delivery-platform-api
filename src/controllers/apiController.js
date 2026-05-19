import {ApiResponse} from '../utils/ApiResponse.js';
import * as apiService from '../services/apiKeyService.js';
import { ValidationError } from '../errors/AppError.js';

export async function createApiKey(req, res){
    const {label} = req.body ?? {};

    const result = await apiService.createApiKey({tenantId: req.tenantId, label});

    return res.status(201).send(ApiResponse.created(result));
}

export async function listApiKeys(req, res){
    const keys = await apiService.listApiKeys({tenantId: req.tenantId});

    return res.send(ApiResponse.success({items: keys}));
}

export async function revokeApiKey(req, res){
    await apiService.revokeApiKey({tenantId: req.tenantId, keyId: req.params.id});

    return res.send(ApiResponse.success(null));
}
