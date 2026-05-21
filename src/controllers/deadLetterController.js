import { parsePagination } from "../utils/paginate.js";
import { ApiResponse } from "../utils/ApiResponse.js";
import * as deadLetterService from '../services/deadLetterService.js';

export async function listDeadLetters(req, res)
{
    const {page, pageSize, skip} = parsePagination(req.query);
    const result = await deadLetterService.listDeadLetters({
        tenantId: req.tenantId,
        page,
        pageSize,
        skip
    });

    return res.send(ApiResponse.success(result));
}

export async function getDeadLetterById(req, res)
{
    const result = await deadLetterService.getDeadLetterById({
        tenantId: req.tenantId,
        deadLetterId: req.params.id
    });

    return res.send(ApiResponse.success(result));
}

export async function retryDeadLetter(req, res){
    const result = await deadLetterService.retryDeadLetter({
        tenantId: req.tenantId,
        deadLetterId: req.params.id
    });

    return res.send(ApiResponse.success(result));
}