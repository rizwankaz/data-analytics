const { Kafka, Partitioners } = require('kafkajs');
const EventSource = require('eventsource');
const fetch = (...args) => import('node-fetch').then(({default: fetch}) => fetch(...args));

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

async function fetchBlockHeader(chainId, height) {
  const url = `https://api.chainweb.com/chainweb/0.0/mainnet01/chain/${chainId}/header/${height}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch header: ${res.status}`);
  return res.json();
}

async function fetchPayloadHashWithRetry(chainId, height, retries = 5, delay = 3000) {
  for (let i = 0; i < retries; i++) {
    try {
      const header = await fetchBlockHeader(chainId, height);
      if (header.payloadHash) return header.payloadHash;
    } catch (e) {
      console.warn(`⚠️ Retry ${i+1}/${retries} for block ${height} on chain ${chainId}: ${e.message}`);
    }
    await new Promise(res => setTimeout(res, delay));
  }
  throw new Error(`PayloadHash not found after ${retries} retries`);
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

function extractInfo(tx) {
  try {
    const cmd = JSON.parse(tx.cmd);
    const { sender, creationTime, gasLimit, gasPrice } = cmd.meta;
    const gas = gasLimit * gasPrice;
    const code = cmd.payload?.exec?.code || '';
    const match = code.match(/\(free\.([^)\s]+)/);
    const eventType = match ? match[1] : 'unknown';
    return {
      sender,
      creationTime: new Date(creationTime * 1000).toISOString(),
      eventType,
      gas,
    };
  } catch (err) {
    console.warn('⚠️ Failed to extract transaction info:', err.message);
    return {};
  }
}

async function processBlock(chainId, height) {
  try {
    const payloadHash = await fetchPayloadHashWithRetry(chainId, height);
    const blockMeta = { chainId, height, payloadHash };
    console.log('🔄 Block with txs:', blockMeta);
    await publishToKafka('kadena.blocks', blockMeta);

    const payload = await fetchPayload(chainId, payloadHash);
    const decodedTxs = payload.transactions.map(decodeTransaction).filter(tx => tx);

    for (const tx of decodedTxs) {
      const info = extractInfo(tx);
      await publishToKafka('kadena.transactions', { chainId, height, ...info, tx });
    }
  } catch (e) {
    console.error(`❌ Failed to process block ${height} on chain ${chainId}:`, e.message);
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
      const { chainId, height, payloadHash } = header;

      if (txCount && txCount > 0) {
        processBlock(chainId, height);
      } else {
        console.log(`⏩ Skipping block ${height} on chain ${chainId} with ${txCount} txs`);
      }
    } catch (err) {
      console.error('❌ Failed to parse BlockHeader:', err.message);
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
