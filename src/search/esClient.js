import { Client } from "@elastic/elasticsearch";

const esClient = new Client({
    node: process.env.ELASTIC_SEARCH_URL ?? 'http://localhost:9200'
    // In production: add auth, TLS, and certificate fingerprint here
});

export default esClient;