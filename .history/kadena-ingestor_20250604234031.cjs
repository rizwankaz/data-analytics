const { Kafka, Partitioners } = require('kafkajs');
const fetch = require('node-fetch');
const EventSource = require('eventsource');

const KAFKA_BROKER = 'localhost:9092';
const STREAM_URL = 'https://api.chainweb.com/chainweb/0.0/mainnet01/header/updates';
const EVENT_PREFIXES = ['free.radio02.add-receieved-with-chain', 'free.radio02.direct-to-send', 'free.mesh03'];

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

function extractTxMetadata(txObj) {
  const { cmd } = txObj;
  const cmdObj = JSON.parse(cmd);

  const sender = cmdObj.meta?.sender || 'unknown';
  const creationTime = cmdObj.meta?.creationTime
    ? new Date(cmdObj.meta.creationTime * 1000).toISOString()
    : 'unknown';
  const gasLimit = cmdObj.meta?.gasLimit || 0;

  let code = cmdObj.payload?.exec?.code || '';
  const eventTypes = EVENT_PREFIXES.filter((event) => code.includes(event));

  return { sender, creationTime, gasLimit, eventTypes };
}

async function processBlockHeader(eventData) {
  const { header, payloadHash, txCount } = eventData;
  const { chainId, height } = header;

  if (!payloadHash || !txCount || txCount === 0) {
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
      .filter(tx => tx !== null);

    for (const tx of decodedTxs) {
      const enriched = {
        chainId,
        height,
        ...extractTxMetadata(tx),
        tx,
      };
      await publishToKafka('kadena.transactions', enriched);
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

startIngestor().catch(err => {
  console.error('❌ Ingestor failed to start:', err.message);
  process.exit(1);
});
