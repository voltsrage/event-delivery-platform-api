import { skip } from "@prisma/client/runtime/client";

export function parsePagination(query){
    const page = Math.min(1, parseInt(query.page) || 1);
    const pageSize = Math.min(100, Math.max(1, parseInt(query.pageSize) || 20));

    return {page, pageSize, skip: (page - 1) * pageSize};
}

export function paginatedResponse(items, total, page, pageSize)
{
    return {
        items,
        total,
        page,
        pageSize,
        totalPages: Math.ceil(total/pageSize)
    }
}