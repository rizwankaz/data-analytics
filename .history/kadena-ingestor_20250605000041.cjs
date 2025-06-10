const { Kafka, Partitioners } = require('kafkajs');
const EventSource = require('eventsource');

// Polyfill fetch using dynamic import
const fetch = (...args) => import('node-fetch').then(({ default: fetch }) => fetch(...args));

const KAFKA_BROKER = 'localhost:9092';
const STREAM_URL = 'https://api.chainweb.com/chainweb/0.0/mainnet01/header/updates';

const kafka = new Kafka({
  clientId: 'kadena-ingestor',
  brokers: [KAFKA_BROKER],
});

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
    return JSON.parse(Buffer.from(tx, 'base64').toString('utf-8'));
  } catch {
    console.warn('⚠️ Skipping malformed transaction');
    return null;
  }
}

function extractEventType(code) {
  const match = code.match(/\(\s*free\.(radio02|mesh03)\.([a-zA-Z0-9\-]+)/);
  return match ? `${match[1]}.${match[2]}` : 'unknown';
}

function extractTxSummary(tx) {
  try {
    const { cmd } = tx;
    const parsedCmd = JSON.parse(cmd);
    const {
      meta: { sender, creationTime, gasLimit, gasPrice },
      payload: { exec: { code } = {} } = {},
    } = parsedCmd;

    return {
      sender,
      creationTime: new Date(creationTime * 1000).toISOString(),
      eventType: extractEventType(code || ''),
      gas: gasLimit * gasPrice,
      tx,
    };
  } catch {
    console.warn('⚠️ Could not extract summary for tx:', tx.hash);
    return null;
  }
}

async function processBlockHeader(eventData) {
  const { header, payloadHash, txCount } = eventData;
  const { chainId, height } = header;

  if (txCount > 0 && !payloadHash) {
    console.warn(`⚠️ Block ${height} on chain ${chainId} has txs (${txCount}) but no payloadHash!`);
    return;
  }

  if (!payloadHash || txCount === 0) {
    console.log(`⏩ Skipping block ${height} on chain ${chainId} with ${txCount} txs`);
    return;
  }

  const blockMeta = { chainId, height, payloadHash };
  console.log('🔄 Block with txs:', blockMeta);

  try {
    await publishToKafka('kadena.blocks', blockMeta);

    const payload = await fetchPayload(chainId, payloadHash);
    const decodedTxs = payload.transactions
      .map(decodeTransaction)
      .filter(Boolean)
      .map(extractTxSummary)
      .filter(Boolean);

    for (const txSummary of decodedTxs) {
      await publishToKafka('kadena.transactions', {
        chainId,
        height,
        ...txSummary,
      });
    }
  } catch (e) {
    console.error('❌ Failed to process block:', e.message);
  }
}

async function startIngestor() {
  console.log('🚀 Connecting to Kafka...');
  await producer.connect();
  console.log('✅ Kafka connected');
  console.log(`📡 Connecting to SSE: ${STREAM_URL}`);

  const es = new EventSource(STREAM_URL);

  es.addEventListener('BlockHeader', (msg) => {
    try {
      const blockData = JSON.parse(msg.data);
      processBlockHeader(blockData);
    } catch (err) {
      console.error('❌ Failed to parse BlockHeader:', err.message, msg.data);
    }
  });

  es.onerror = (err) => {
    console.error('❌ SSE error:', err.message || err);
  };
}

startIngestor().catch(err => {
  console.error('❌ Ingestor failed to start:', err.message);
  process.exit(1);
});
