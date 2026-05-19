import {withTenant} from '../utils/withTenant.js';
import {NotFoundError, ConflictError} from '../errors/AppError.js';
import { paginatedResponse } from '../utils/paginate.js';
import { Prisma } from '../../generated/prisma/client.ts';

export async function createTopic({tenantId, name, description}){
    try{
        const topic = await withTenant(tenantId, async (tx) => {
            return tx.topic.create({
                data: {tenantId, name, description: description ?? null}
            });
        });

        return toPublicTopic(topic);
    }
    catch(err)
    {
        // P2002 = unique constraint violation
        // The constraint is (tenant_id, name) - the same name under a different tenant succeeds

        if(
            err instanceof Prisma.PrismaClientKnownRequestError &&
            err.code === 'P2002'
        )
        {
            throw new ConflictError(`A topic named '${name}' already exists`, 'TOPIC_NAME_TAKEN')
        }

        throw err;
    }
}

export async function listTopics({tenantId, page, pageSize, skip}){
    const [items, total] = await withTenant(tenantId, async (tx) => {
        return Promise.all([
            tx.topic.findMany({
                where: {deletedAt: null},
                orderBy: {createdAt: 'desc'},
                skip,
                take: pageSize,
                select: {
                    id: true,
                    name: true,
                    description: true,
                    createdAt: true
                }
            }),
            tx.topic.count({where : {deletedAt: null}})
        ])
    });

    return paginatedResponse(items, total, page, pageSize);
}

export async function getTopicById({tenantId, topicId}){
    const [topic, subscriptionCount] = await withTenant(tenantId, async (tx) => {
        const topic = await tx.topic.findFirst({
            where: {id: topicId, deletedAt: null}
        });

        if(!topic) return [null, 0];

        // subscriptions is also RLS-protected - this count is automatically scoped
        // to the authenticated tenant by SET LOCAL, no WHERE tenant_id needed
        const count = await tx.subscription.count({where: {topicId: topic.id}});

        return [topic, count];
    })

    if(!topic) {
        throw new NotFoundError('Topic not found.', 'TOPIC_NOT_FOUND');
    }

    return { ...toPublicTopic(topic), subscriptionCount};
}

export async function deleteTopic({tenantId, topicId}){
    await withTenant(tenantId, async (tx) => {
        const topic = await tx.topic.findFirst({
            where: {id: topicId, deletedAt: null}
        })

        if(!topic) throw new NotFoundError('Topic not found.', 'TOPIC_NOT_FOUND');

        // Subscriptions have no meaning without an active topic - hard delete them.
        // Events are the permanent audit trail - they retain their topic_id FK and are
        // untouched. The topic row itself is soft-deleted so the FK from events remains valid
        await tx.subscription.deleteMany({where: {topicId}});

        await tx.topic.update({
            where: {id: topicId},
            data: {deletedAt: new Date()}
        });
    });
}


function toPublicTopic(topic){
    return{
        id: topic.id,
        name: topic.name,
        description: topic.description,
        createdAt: topic.createdAt
    }
}