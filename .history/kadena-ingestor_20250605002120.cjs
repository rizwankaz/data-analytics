const axios = require('axios');
const { Kafka, Partitioners } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'kadena-ingestor',
  brokers: ['localhost:9092'],
});

const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner,
});

async function start() {
  console.log('🚀 Connecting to Kafka...');
  await producer.connect();
  console.log('✅ Kafka connected');

  const sseUrl = 'https://api.chainweb.com/chainweb/0.0/mainnet01/header/updates';
  console.log(`📡 Connecting to SSE manually via axios: ${sseUrl}`);

  const response = await axios.get(sseUrl, {
    responseType: 'stream',
    headers: { Accept: 'text/event-stream' },
  });

  console.log('🔌 SSE stream opened');

  let currentEvent = '';
  response.data.on('data', async (chunk) => {
    const lines = chunk.toString().split('\n');

    for (const line of lines) {
      if (line.startsWith('data:')) {
        currentEvent += line.slice(5).trim();
      } else if (line === '') {
        if (currentEvent) {
          await handleEvent(currentEvent);
          currentEvent = '';
        }
      }
    }
  });

  response.data.on('error', (err) => {
    console.error('❌ SSE stream error:', err);
  });
}

async function handleEvent(jsonStr) {
  try {
    const event = JSON.parse(jsonStr);
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
      messages: [{ value: JSON.stringify({ chainId, height, payloadHash }) }],
    });

    const payloadUrl = `https://api.chainweb.com/chainweb/0.0/mainnet01/chain/${chainId}/payload/${payloadHash}`;
    const res = await axios.get(payloadUrl);
    const payload = res.data;

    for (const encoded of payload.transactions) {
      let raw;
      try {
        const decodedStr = Buffer.from(encoded, 'base64').toString('utf8');
        raw = JSON.parse(decodedStr);
      } catch (err) {
        console.warn('⚠️ Could not decode or parse transaction:', err.message);
        continue;
      }

      const meta = raw.meta || {};
      const creationTime = meta.creationTime
        ? new Date(meta.creationTime * 1000).toISOString()
        : 'unknown';
      const sender = meta.sender || 'unknown';
      const gasLimit = meta.gasLimit || 0;
      const gasPrice = meta.gasPrice || 0;
      const gas = gasLimit * gasPrice;

      const code = raw.payload?.exec?.code || '';
      const match = code.match(/\(free\.([a-zA-Z0-9_.-]+)/);
      const eventType = match ? match[1] : 'unknown';

      const enrichedTx = {
        chainId,
        height,
        sender,
        creationTime,
        eventType,
        gas,
        tx: raw, // decoded JSON tx
      };

      await producer.send({
        topic: 'kadena.transactions',
        messages: [{ value: JSON.stringify(enrichedTx) }],
      });

      console.log(`✅ Sent to Kafka topic "kadena.transactions":`, enrichedTx);
    }
  } catch (err) {
    console.error(`❌ Failed to process block: ${err.message}`);
  }
}

start();
