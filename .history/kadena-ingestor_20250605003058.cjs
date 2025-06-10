// kadena-ingestor.cjs

const axios = require('axios');
const { Kafka } = require('kafkajs');
const { TextDecoder } = require('util');

// Kafka setup
const kafka = new Kafka({
  clientId: 'kadena-ingestor',
  brokers: ['localhost:9092']
});

const producer = kafka.producer();

function extractFields(cmdString) {
  try {
    const cmd = JSON.parse(cmdString);
    const meta = cmd.meta || {};
    const code = cmd.payload?.exec?.code || '';

    const sender = meta.sender || 'unknown';
    const creationTime = meta.creationTime
      ? new Date(meta.creationTime * 1000).toISOString()
      : 'unknown';
    const eventTypeMatch = code.match(/\(free\.([^) ]+)/);
    const eventType = eventTypeMatch ? eventTypeMatch[1] : 'unknown';
    const gas = meta.gasLimit && meta.gasPrice
      ? meta.gasLimit * meta.gasPrice
      : 0;

    return { sender, creationTime, eventType, gas };
  } catch (e) {
    console.error('❌ Failed to extract fields:', e.message);
    return { sender: 'unknown', creationTime: 'unknown', eventType: 'unknown', gas: 0 };
  }
}

async function start() {
  console.log('🚀 Connecting to Kafka...');
  await producer.connect();
  console.log('✅ Kafka connected');

  const sseUrl = 'https://api.chainweb.com/chainweb/0.0/mainnet01/header/updates';
  console.log(`📡 Connecting to SSE manually via axios: ${sseUrl}`);

  const res = await axios({
    method: 'get',
    url: sseUrl,
    responseType: 'stream',
    headers: { Accept: 'text/event-stream' }
  });

  console.log('🔌 SSE stream opened');

  const decoder = new TextDecoder('utf-8');
  let buffer = '';

  res.data.on('data', async (chunk) => {
    buffer += decoder.decode(chunk, { stream: true });

    while (buffer.includes('\n\n')) {
      const [rawEvent, rest] = buffer.split('\n\n', 2);
      buffer = rest;

      const lines = rawEvent.split('\n');
      const dataLines = lines
        .filter((line) => line.startsWith('data: '))
        .map((line) => line.slice(6));

      const json = dataLines.join('');
      if (!json) continue;

      try {
        const event = JSON.parse(json);
        const header = event.header;
        const txCount = event.txCount;
        const { chainId, height, payloadHash } = header;

        if (txCount === 0) {
          console.log(`⏩ Skipping block ${height} on chain ${chainId} with 0 txs`);
          continue;
        }

        if (!payloadHash) {
          console.warn(`⚠️ Block ${height} on chain ${chainId} has txs (${txCount}) but no payloadHash!`);
          continue;
        }

        console.log('🔄 Block with txs:', { chainId, height, payloadHash });

        await producer.send({
          topic: 'kadena.blocks',
          messages: [{ value: JSON.stringify({ chainId, height, payloadHash }) }]
        });

        const payloadUrl = `https://api.chainweb.com/chainweb/0.0/mainnet01/chain/${chainId}/payload/${payloadHash}`;
        const txsRes = await axios.get(payloadUrl);

        for (const tx of txsRes.data.transactions) {
          const fields = extractFields(tx.cmd);
          const enrichedTx = {
            chainId,
            height,
            ...fields,
            tx
          };

          await producer.send({
            topic: 'kadena.transactions',
            messages: [{ value: JSON.stringify(enrichedTx) }]
          });

          console.log('✅ Sent to Kafka topic "kadena.transactions":', enrichedTx);
        }
      } catch (err) {
        console.error('❌ Failed to process block:', err.message);
      }
    }
  });

  res.data.on('end', () => {
    console.warn('⚠️ SSE stream ended');
  });

  res.data.on('error', (err) => {
    console.error('❌ Stream error:', err.message);
  });
}

start().catch((err) => {
  console.error('❌ Fatal error in stream:', err);
});
