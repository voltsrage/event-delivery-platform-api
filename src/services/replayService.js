import { withTenant } from '../utils/withTenant.js';
import { retryQueue } from '../queues/retryQueue.js';
import { NotFoundError, ValidationError } from '../errors/AppError.js';

export async function replaySubscription({ tenantId, subscriptionId, from }) {
    const fromDate = new Date(from);
    if (isNaN(fromDate.getTime())) {
        throw new ValidationError('`from` must be a valid ISO 8601 date', 'INVALID_FROM_DATE');
    }

    const events = await withTenant(tenantId, async (tx) => {
        const sub = await tx.subscription.findUnique({ where: { id: subscriptionId } });
        if (!sub) throw new NotFoundError('Subscription not found.', 'SUBSCRIPTION_NOT_FOUND');

        return tx.event.findMany({
            where: { topicId: sub.topicId, createdAt: { gte: fromDate } },
            orderBy: { createdAt: 'asc' },
            select: { id: true },
        });
    });

    if (events.length > 0) {
        await retryQueue.addBulk(
            events.map((e) => ({
                name: 'replay',
                data: { eventId: e.id, subscriptionId, tenantId, nextAttemptNumber: 1 },
            }))
        );
    }

    return { enqueuedCount: events.length };
}
