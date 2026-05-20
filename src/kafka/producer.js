import kafka from "./client.js";

/*
`allowAutoTopicCreation: false` ensures that a typo in `KAFKA_TOPIC` fails loudly 
(Kafka throws `UNKNOWN_TOPIC_OR_PARTITION`) instead of silently creating a topic with default settings 
(1 partition, 7-day retention is not guaranteed by default).

*/
const producer = kafka.producer({
    allowAutoTopicCreation: false
    // acks: -1 - all in-sync replicas must acknowledge before send() resolved
    // With replication-factor 1 locally this is identical to acks: 1.
    // In a 3-broker production cluster (RF-3) this prevents data loss on leader failure
});

export async function connectProducer()
{
    await producer.connect();
}

export async function disconnectProducer()
{
    await producer.disconnect();
}


export async function publishToKafka(message){
    await producer.send({
        topic: process.env.KAFKA_TOPIC ?? 'platform.events',
        messages: [
            {
                // Partition key = tenantId.
                // All events for the same tenant land on the same partition,
                // guaranteeing ordering within a tenant.
                // Trade-off; a tenant with disproportionate volume creates a hot partition
                key: message.tenantId,
                value: JSON.stringify(message)
            }
        ]
    })
}