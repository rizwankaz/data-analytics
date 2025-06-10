const { Kafka, Partitioners } = require('kafkajs');
const fetch = require('node-fetch');
const EventSource = require('eventsource');

// Configuration
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
    const decoded = Buffer.from(tx, 'base64').toString('utf-8');
    return JSON.parse(decoded);
  } catch {
    console.warn('⚠️ Skipping malformed transaction');
    return null;
  }
}

function formatCreationTime(unixTime) {
  const millis = unixTime * 1000;
  return new Date(millis).toISOString();
}

async function processBlockHeader(eventData) {
  const { header, txCount } = eventData;
  const { chainId, height, payloadHash } = header;

  if (!payloadHash) {
    console.warn(`⚠️ Block ${height} on chain ${chainId} has txs (${txCount}) but no payloadHash!`);
    return;
  }


  if (txCount === 0) {
    console.log(`⏩ Skipping block ${height} on chain ${chainId} with 0 txs`);
    return;
  }

  const blockMeta = { chainId, height, payloadHash };
  console.log('🔄 Block with txs:', blockMeta);

  try {
    await publishToKafka('kadena.blocks', blockMeta);

    const payload = await fetchPayload(chainId, payloadHash);
    const decodedTxs = payload.transactions.map(decodeTransaction).filter(Boolean);

    for (const tx of decodedTxs) {
      const cmdText = tx.cmd;
      let parsedCmd = {};

      try {
        parsedCmd = JSON.parse(cmdText);
      } catch {
        console.warn('⚠️ Malformed cmd JSON, skipping tx');
        continue;
      }

      const meta = parsedCmd.meta || {};
      const sender = meta.sender || 'unknown';
      const creationTime = meta.creationTime ? formatCreationTime(meta.creationTime) : null;
      const gasLimit = meta.gasLimit || 0;
      const gasPrice = meta.gasPrice || 0;
      const gas = gasLimit * gasPrice;

      const code = parsedCmd.payload?.exec?.code || '';
      const match = code.match(/\(\s*free\.([a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+)/);
      const eventType = match ? match[1] : 'unknown';

      const enrichedTx = {
        chainId,
        height,
        sender,
        creationTime,
        eventType,
        gas,
        tx,
      };

      await publishToKafka('kadena.transactions', enrichedTx);
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
