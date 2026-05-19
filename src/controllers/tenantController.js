import {ApiResponse} from '../utils/ApiResponse.js';
import * as tenantService from '../services/tenantService.js';
import { ValidationError } from '../errors/AppError.js';

export async function createTenant(req, res){
    const {name, email} = req.body;

    if(name.length === 0 ||  email.length === 0)
        throw new ValidationError('name and email are required', 'VARIABLES_REQUIRED')

    const result = await tenantService.createTenant({name, email});

    return res.status(201).send(ApiResponse.created(result));
}
