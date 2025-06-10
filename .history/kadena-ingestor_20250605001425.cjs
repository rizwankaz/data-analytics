// kadena-ingestor.cjs

const fetch = require('node-fetch');
const { Kafka } = require('kafkajs');
const EventSource = require('eventsource');

// Kafka config
const kafka = new Kafka({
  clientId: 'kadena-ingestor',
  brokers: ['localhost:9092'],
});

const producer = kafka.producer();

async function getPayloadHash(chainId, height, retries = 5, delay = 1000) {
  const url = `https://api.chainweb.com/chainweb/0.0/mainnet01/chain/${chainId}/header/${height}`;
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const res = await fetch(url);
      if (!res.ok) throw new Error(`Failed to fetch header: ${res.status}`);
      const data = await res.json();
      if (data.payloadHash) return data.payloadHash;
      throw new Error('No payloadHash in header');
    } catch (err) {
      console.warn(`⚠️ Retry ${attempt}/${retries} for header fetch on chain ${chainId}, height ${height}: ${err.message}`);
      if (attempt < retries) await new Promise((res) => setTimeout(res, delay * attempt));
    }
  }
  throw new Error(`❌ Failed to retrieve payloadHash after ${retries} retries`);
}

async function start() {
  console.log('🚀 Connecting to Kafka...');
  await producer.connect();
  console.log('✅ Kafka connected');

  const sseUrl = 'https://api.chainweb.com/chainweb/0.0/mainnet01/header/updates';
  console.log(`📡 Connecting to SSE: ${sseUrl}`);
  const sse = new EventSource(sseUrl);

  sse.onopen = () => {
    console.log('🔌 SSE connection established');
  };

  sse.onerror = (err) => {
    console.error('❌ SSE error:', err);
  };

  sse.onmessage = async (msg) => {
    console.log('📥 New SSE message received');
    try {
      const data = JSON.parse(msg.data);
      const { header, txCount } = data;
      const { chainId, height } = header;
      let { payloadHash } = header;

      if (txCount === 0) {
        console.log(`⏩ Skipping block ${height} on chain ${chainId} with 0 txs`);
        return;
      }

      if (!payloadHash) {
        console.warn(`⚠️ Block ${height} on chain ${chainId} has txs (${txCount}) but no payloadHash! Attempting recovery...`);
        try {
          payloadHash = await getPayloadHash(chainId, height);
        } catch (err) {
          console.error(`❌ Giving up on block ${height} chain ${chainId}: ${err.message}`);
          return;
        }
      }

      console.log(`🔄 Processing block ${height} on chain ${chainId} with payloadHash ${payloadHash}`);

      await producer.send({
        topic: 'kadena.blocks',
        messages: [{ value: JSON.stringify({ chainId, height, payloadHash }) }],
      });

      const payloadUrl = `https://api.chainweb.com/chainweb/0.0/mainnet01/chain/${chainId}/payload/${payloadHash}`;
      const res = await fetch(payloadUrl);
      if (!res.ok) throw new Error(`Failed to fetch payload: ${res.status}`);
      const payload = await res.json();

      for (const tx of payload.transactions) {
        let decoded;
        try {
          decoded = JSON.parse(tx.cmd);
        } catch {
          decoded = { meta: {}, payload: {} };
        }

        const meta = decoded.meta || {};
        const creationTime = meta.creationTime
          ? new Date(meta.creationTime * 1000).toISOString()
          : 'unknown';
        const sender = meta.sender || 'unknown';
        const gasLimit = meta.gasLimit || 0;
        const gasPrice = meta.gasPrice || 0;
        const gas = gasLimit * gasPrice;

        const code = decoded.payload?.exec?.code || '';
        const match = code.match(/\(free\.([a-zA-Z0-9_.-]+)/);
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

        await producer.send({
          topic: 'kadena.transactions',
          messages: [{ value: JSON.stringify(enrichedTx) }],
        });

        console.log(`✅ Sent transaction to Kafka:`, enrichedTx);
      }
    } catch (err) {
      console.error('❌ Processing error:', err.message);
    }
  };
}

start();
