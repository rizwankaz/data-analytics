// kadena-ingestor.cjs

const fetch = require('node-fetch');
const { Kafka } = require('kafkajs');
const EventSource = require('eventsource');

const kafka = new Kafka({
  clientId: 'kadena-ingestor',
  brokers: ['localhost:9092']
});

const producer = kafka.producer();

async function start() {
  console.log('🚀 Connecting to Kafka...');
  await producer.connect();
  console.log('✅ Kafka connected');

  const sseUrl = 'https://api.chainweb.com/chainweb/0.0/mainnet01/header/updates';
  console.log(`📡 Connecting to SSE: ${sseUrl}`);
  const sse = new EventSource(sseUrl);

  sse.onmessage = async (msg) => {
    try {
      const event = JSON.parse(msg.data);
      const header = event.header;
      const txCount = event.txCount;
      const { chainId, height, payloadHash } = header;

      if (txCount === 0) {
        console.log(`⏩ Skipping block ${height} on chain ${chainId} with 0 txs`);
        return;
      }

      if (!payloadHash) {
        console.warn(`⚠️ Block ${height} on chain ${chainId} has txs (${txCount}) but no payloadHash!`);
        return;
      }

      console.log('🔄 Block with txs:', { chainId, height, payloadHash });

      await producer.send({
        topic: 'kadena.blocks',
        messages: [{ value: JSON.stringify({ chainId, height, payloadHash }) }]
      });

      const payloadUrl = `https://api.chainweb.com/chainweb/0.0/mainnet01/chain/${chainId}/payload/${payloadHash}`;
      const res = await fetch(payloadUrl);
      if (!res.ok) throw new Error(`Payload fetch failed with status ${res.status}`);
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
        const match = code.match(/\(free\.([a-zA-Z0-9.-]+)/);
        const eventType = match ? match[1] : 'unknown';

        const enrichedTx = {
          chainId,
          height,
          sender,
          creationTime,
          eventType,
          gas,
          tx
        };

        await producer.send({
          topic: 'kadena.transactions',
          messages: [{ value: JSON.stringify(enrichedTx) }]
        });

        console.log(`✅ Sent to Kafka topic "kadena.transactions":`, enrichedTx);
      }
    } catch (err) {
      console.error(`❌ Failed to process block: ${err.message}`);
    }
  };

  sse.onerror = (err) => {
    console.error('❌ SSE connection error:', err);
  };
}

start();
