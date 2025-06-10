const { Kafka } = require('kafkajs');
const fetch = require('node-fetch');
let EventSource;
try {
  EventSource = require('eventsource').default;
} catch {
  EventSource = require('eventsource');
}

const KAFKA_BROKER = 'localhost:9092';
const STREAM_URL = 'https://api.chainweb.com/chainweb/0.0/mainnet01/header/updates';

const kafka = new Kafka({ clientId: 'kadena-ingestor', brokers: [KAFKA_BROKER] });
const { Partitioners } = require('kafkajs');

const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner,
});


async function publishToKafka(topic, data) {
  try {
    await producer.send({
      topic,
      messages: [{ value: JSON.stringify(data) }],
    });
    console.log(`✅ Sent to Kafka topic "${topic}":`, data);
  } catch (err) {
    console.error('❌ Kafka publish error:', err.message);
  }
}

async function fetchPayload(chainId, payloadHash) {
  const url = `https://api.chainweb.com/chainweb/0.0/mainnet01/chain/${chainId}/payload/${payloadHash}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch payload: ${res.status}`);
  return res.json();
}

function decodeTransaction(tx) {
  try {
    const txObj = JSON.parse(Buffer.from(tx, 'base64').toString('utf-8'));
    return txObj;
  } catch (e) {
    console.warn('⚠️ Skipping malformed transaction:', tx);
    return null;
  }
}

async function processBlockHeader(blockHeader) {
  const { chainId, height, payloadHash, txCount } = blockHeader;
  if (!payloadHash || txCount === 0) return;

  const blockMeta = { chainId, height, payloadHash };
  console.log('🔄 Block with txs:', blockMeta);

  try {
    await publishToKafka('kadena.blocks', blockMeta);

    const payload = await fetchPayload(chainId, payloadHash);
    const decodedTxs = payload.transactions
      .map(decodeTransaction)
      .filter((tx) => tx !== null);

    for (const tx of decodedTxs) {
      await publishToKafka('kadena.transactions', { chainId, height, tx });
    }
  } catch (e) {
    console.error('❌ Failed to process BlockHeader:', e.message);
  }
}

async function startIngestor() {
  console.log('🚀 Connecting to Kafka...');
  await producer.connect();
  console.log('✅ Kafka connected');
  console.log(`📡 Connecting to SSE: ${STREAM_URL}`);

  const es = new EventSource(STREAM_URL);

  es.onmessage = (msg) => {
    try {
      const blockHeader = JSON.parse(msg.data);
      const { txCount } = blockHeader;
      if (txCount && txCount > 0) {
        processBlockHeader(blockHeader);
      } else {
        console.log(`⏩ Skipping block at height ${blockHeader.height} with ${txCount} transactions`);
      }
    } catch (err) {
      console.error('❌ Failed to parse SSE message:', err.message);
    }
  };

  es.onerror = (err) => {
    console.error('❌ SSE connection error:', err);
  };
}

startIngestor().catch((err) => {
  console.error('❌ Ingestor failed to start:', err.message);
  process.exit(1);
});
