// kadena-ingestor.cjs

const { Kafka, Partitioners } = require('kafkajs');
const EventSource = require('eventsource');

// ✅ Dynamic import for node-fetch to fix "fetch is not a function"
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

function decodeTransaction(txBase64) {
  try {
    const jsonStr = Buffer.from(txBase64, 'base64').toString('utf-8');
    const tx = JSON.parse(jsonStr);

    const cmd = JSON.parse(tx.cmd);
    const { meta, payload } = cmd;

    const sender = meta?.sender || 'unknown';
    const creationTime = new Date(meta?.creationTime * 1000).toISOString();
    const gasLimit = meta?.gasLimit || 0;

    let eventType = 'unknown';
    if (payload?.exec?.code) {
      const match = payload.exec.code.match(/free\.(radio02|mesh03)\.(\w+)/);
      if (match) {
        eventType = `free.${match[1]}.${match[2]}`;
      }
    }

    return {
      ...tx,
      sender,
      creationTime,
      gasLimit,
      eventType,
    };
  } catch (e) {
    console.warn('⚠️ Skipping malformed transaction');
    return null;
  }
}

async function processBlockHeader(eventData) {
  const { header, payloadHash, txCount } = eventData;
  const { chainId, height } = header;

  if (!payloadHash || typeof txCount !== 'number' || txCount === 0) {
    console.log(`⏩ Skipping block ${height} on chain ${chainId} with ${txCount} txs`);
    return;
  }

  const blockMeta = { chainId, height, payloadHash };
  console.log('🔄 Block with txs:', blockMeta);
  await publishToKafka('kadena.blocks', blockMeta);

  try {
    const payload = await fetchPayload(chainId, payloadHash);
    const decodedTxs = payload.transactions
      .map(decodeTransaction)
      .filter((tx) => tx !== null);

    for (const tx of decodedTxs) {
      await publishToKafka('kadena.transactions', { chainId, height, tx });
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
      const { header, txCount } = blockData;

      if (typeof txCount !== 'number' || txCount === 0) {
        console.log(`⏩ Skipping block ${header.height} on chain ${header.chainId} with ${txCount} txs`);
        return;
      }

      processBlockHeader(blockData);
    } catch (err) {
      console.error('❌ Failed to parse BlockHeader:', err.message);
    }
  });

  es.onerror = (err) => {
    console.error('❌ SSE error:', err.message || err);
  };
}

startIngestor().catch((err) => {
  console.error('❌ Ingestor failed to start:', err.message);
  process.exit(1);
});
