import { Kafka } from "kafkajs";

/*
`brokers` is split on commas so a multi-broker `KAFKA_BROKERS=broker1:9092,broker2:9092,broker3:9092`
works without any code change. The KafkaJS client uses all listed brokers for leader discovery and failover.
*/
const kafka = new Kafka({
    clientId: process.env.KAFKA_CLIENT_ID ?? 'event-platform-outbox',
    brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',')
    // In production: add ssl: true and sasl credentials here
});

export default kafka;