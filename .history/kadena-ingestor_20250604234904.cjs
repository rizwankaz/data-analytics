const { Kafka, Partitioners } = require('kafkajs');
const fetch = (...args) => import('node-fetch').then(({ default: f }) => f(...args));
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
    return JSON.parse(Buffer.from(tx, 'base64').toString('utf-8'));
  } catch {
    console.warn('⚠️ Skipping malformed transaction');
    return null;
  }
}

function extractTxMetadata(txObj) {
  try {
    const cmdObj = JSON.parse(txObj.cmd);
    const sender = cmdObj.meta?.sender || 'unknown';
    const creationTime = new Date((cmdObj.meta?.creationTime || 0) * 1000).toISOString();
    const gasLimit = cmdObj.meta?.gasLimit || 0;

    let eventType = 'unknown';
    const code = cmdObj.payload?.exec?.code || '';
    if (code.includes('free.radio02.add-receieved-with-chain')) eventType = 'radio02.add';
    else if (code.includes('free.radio02.direct-to-send')) eventType = 'radio02.send';
    else if (code.includes('free.mesh03')) eventType = 'mesh03';

    return { sender, creationTime, eventType, gasLimit };
  } catch {
    return { sender: 'unknown', creationTime: 'invalid', eventType: 'unknown', gasLimit: 0 };
  }
}

async function processBlockHeader({ header, txCount, payloadHash }) {
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
      const metadata = extractTxMetadata(tx);
      await publishToKafka('kadena.transactions', {
        chainId,
        height,
        sender: metadata.sender,
        creationTime: metadata.creationTime,
        eventType: metadata.eventType,
        gasLimit: metadata.gasLimit,
        tx,
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
      const header = blockData.header;
      const txCount = blockData.txCount ?? header?.txCount;
      const payloadHash = blockData.payloadHash ?? header?.payloadHash;
      const { height, chainId } = header;

      if (typeof txCount === 'number' && txCount > 0 && payloadHash) {
        processBlockHeader({ header, txCount, payloadHash });
      } else {
        console.log(`⏩ Skipping block ${height} on chain ${chainId} with ${txCount} txs`);
      }
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
