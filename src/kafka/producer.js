export async function publishToKafka(message){
    console.log('[outbox] kafka stub - would publish: ', JSON.stringify({
        eventId: message.eventId,
        tenantId: message.tenantId,
        eventType: message.eventType,
        topicName: message.topicName
    }));
}