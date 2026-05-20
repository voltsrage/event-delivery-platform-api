import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';

// Mock kafkajs before importing the module under test.
// vi.mock is hoisted to the top of the file by Vitest.
vi.mock('kafkajs', () => {
    const send    = vi.fn().mockResolvedValue(undefined);
    const connect = vi.fn().mockResolvedValue(undefined);
    const disconnect = vi.fn().mockResolvedValue(undefined);
    const producer   = vi.fn(function() { return { send, connect, disconnect }; });
    const Kafka      = vi.fn(function() { return { producer }; });
    return { Kafka };
});

describe('Kafka producer', () => {
    let connectProducer, disconnectProducer, publishToKafka;
    let mockSend;

    beforeEach(async () => {
        vi.resetModules();
        // Re-import after resetting modules so the mock is applied fresh.
        const kafkajs   = await import('kafkajs');
        const clientMod = await import('../kafka/client.js');
        mockSend        = kafkajs.Kafka.mock.results[0]?.value.producer().send;

        const producerMod = await import('../kafka/producer.js');
        connectProducer   = producerMod.connectProducer;
        disconnectProducer = producerMod.disconnectProducer;
        publishToKafka    = producerMod.publishToKafka;
    });

    afterEach(() => {
        vi.clearAllMocks();
    });

    it('sends to the correct topic', async () => {
        await connectProducer();
        await publishToKafka({
            eventId:   'evt-1',
            tenantId:  'tenant-a',
            topicId:   'topic-1',
            topicName: 'order.created',
            eventType: 'order.created',
            payload:   { orderId: 'abc' },
            publishedAt: '2026-05-14T00:00:00.000Z',
        });

        expect(mockSend).toHaveBeenCalledOnce();
        const call = mockSend.mock.calls[0][0];
        expect(call.topic).toBe('platform.events');
    });

    it('uses tenantId as the partition key', async () => {
        await connectProducer();
        await publishToKafka({
            eventId: 'evt-2', tenantId: 'tenant-xyz',
            topicId: 't1', topicName: 'foo', eventType: 'foo',
            payload: {}, publishedAt: '2026-05-14T00:00:00.000Z',
        });

        const message = mockSend.mock.calls[0][0].messages[0];
        expect(message.key).toBe('tenant-xyz');
    });

    it('serialises the full message envelope as JSON', async () => {
        await connectProducer();
        const envelope = {
            eventId: 'evt-3', tenantId: 'tenant-a', topicId: 't1',
            topicName: 'order.created', eventType: 'order.created',
            payload: { amount: 100 }, publishedAt: '2026-05-14T00:00:00.000Z',
        };
        await publishToKafka(envelope);

        const raw    = mockSend.mock.calls[0][0].messages[0].value;
        const parsed = JSON.parse(raw);
        expect(parsed.eventId).toBe('evt-3');
        expect(parsed.payload).toEqual({ amount: 100 });
        expect(parsed.publishedAt).toBe('2026-05-14T00:00:00.000Z');
    });

    it('rejects if send throws (Kafka unavailable)', async () => {
        await connectProducer();
        mockSend.mockRejectedValueOnce(new Error('broker not available'));
        await expect(publishToKafka({
            eventId: 'e', tenantId: 't', topicId: 'tp', topicName: 'x',
            eventType: 'x', payload: {}, publishedAt: '2026-05-14T00:00:00.000Z',
        })).rejects.toThrow('broker not available');
    });
});