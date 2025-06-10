const { Kafka, Partitioners } = require('kafkajs');
const fetch = require('node-fetch');
const EventSource = require('eventsource'); // CJS-compatible import

const KAFKA_BROKER = 'localhost:9092';
const STREAM_URL = 'https://api.chainweb.com/chainweb/0.0/mainnet01/header/updates';

// Kafka setup
const kafka = new Kafka({
  clientId: 'kadena-ingestor',
  brokers: [KAFKA_BROKER],
});

const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner,
});

// Utility to send to Kafka
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

// Fetch payload by hash
async function fetchPayload(chainId, payloadHash) {
  const url = `https://api.chainweb.com/chainweb/0.0/mainnet01/chain/${chainId}/payload/${payloadHash}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch payload: ${res.status}`);
  return res.json();
}

// Decode a base64 Pact transaction
function decodeTransaction(tx) {
  try {
    return JSON.parse(Buffer.from(tx, 'base64').toString('utf-8'));
  } catch {
    console.warn('⚠️ Skipping malformed transaction');
    return null;
  }
}

// Handle blocks with txs
async function processBlockHeader(blockHeader) {
  const { chainId, height, payloadHash, txCount } = blockHeader;
  if (!payloadHash || !txCount || txCount === 0) return;

  const blockMeta = { chainId, height, payloadHash };
  console.log('🔄 Block with txs:', blockMeta);

  try {
    await publishToKafka('kadena.blocks', blockMeta);

    const payload = await fetchPayload(chainId, payloadHash);
    const decodedTxs = payload.transactions
      .map(decodeTransaction)
      .filter(tx => tx !== null);

    for (const tx of decodedTxs) {
      await publishToKafka('kadena.transactions', { chainId, height, tx });
    }
  } catch (err) {
    console.error('❌ Failed to process block:', err.message);
  }
}

// Main loop
async function startIngestor() {
  console.log('🚀 Connecting to Kafka...');
  await producer.connect();
  console.log('✅ Kafka connected');
  console.log(`📡 Connecting to SSE: ${STREAM_URL}`);

  const es = new EventSource(STREAM_URL);

  es.addEventListener('BlockHeader', (msg) => {
    try {
      const blockHeader = JSON.parse(msg.data);
      console.log('📨 BlockHeader received:', blockHeader);

      if (blockHeader.txCount && blockHeader.txCount > 0) {
        processBlockHeader(blockHeader);
      } else {
        console.log(`⏩ Skipping block at height ${blockHeader.height} with 0 transactions`);
      }
    } catch (err) {
      console.error('❌ Failed to parse BlockHeader:', err.message, msg.data);
    }
  });

  es.onerror = (err) => {
    console.error('❌ SSE error:', err.message || err);
  };
}

// Start
startIngestor().catch(err => {
  console.error('❌ Ingestor failed to start:', err.message);
  process.exit(1);
});