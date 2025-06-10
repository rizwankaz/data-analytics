const { Kafka, Partitioners } = require('kafkajs');
const fetch = require('node-fetch');
const EventSource = require('eventsource');

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
    const json = Buffer.from(tx, 'base64').toString('utf-8');
    return JSON.parse(json);
  } catch (e) {
    console.warn('⚠️ Skipping malformed transaction');
    return null;
  }
}

function extractTxInfo(tx) {
  const meta = tx?.cmd?.meta || {};
  const sender = meta.sender || 'unknown';
  const creationTime = meta.creationTime ? new Date(meta.creationTime * 1000).toISOString() : 'unknown';
  const gasLimit = meta.gasLimit || 0;

  const code = tx?.cmd?.payload?.exec?.code || '';
  const knownEvents = ['free.radio02.add-received-with-chain', 'free.radio02.direct-to-send', 'free.mesh03'];
  const matchedEvents = knownEvents.filter(event => code.includes(event));

  return { sender, creationTime, gasLimit, matchedEvents };
}

async function processBlockHeader(eventData) {
  const { header, payloadHash, txCount, transactions } = eventData;
  const { chainId, height } = header;

  const blockMeta = { chainId, height, payloadHash };
  console.log('🔄 Block with txs:', blockMeta);

  try {
    await publishToKafka('kadena.blocks', blockMeta);

    const payload = await fetchPayload(chainId, payloadHash);
    const decodedTxs = payload.transactions
      .map(decodeTransaction)
      .filter(tx => tx !== null);

    for (const tx of decodedTxs) {
      const info = extractTxInfo(tx);
      console.log(`📤 Tx by ${info.sender} at ${info.creationTime} [gasLimit: ${info.gasLimit}]`);
      if (info.matchedEvents.length > 0) {
        console.log(`🔔 Matched event(s): ${info.matchedEvents.join(', ')}`);
      }

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
      const { header, txCount, payloadHash } = blockData;
      const { height, chainId } = header;

      if (typeof txCount === 'number' && txCount > 0 && payloadHash) {
        processBlockHeader(blockData);
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
