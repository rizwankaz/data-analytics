// kadena-ingestor.cjs

const axios = require('axios');
const { Kafka } = require('kafkajs');
const readline = require('readline');

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

  const response = await axios({
    method: 'get',
    url: sseUrl,
    responseType: 'stream',
    headers: { Accept: 'text/event-stream' }
  });

  console.log('🔌 SSE stream opened');

  const rl = readline.createInterface({
    input: response.data,
    crlfDelay: Infinity
  });

  let buffer = '';

  rl.on('line', async (line) => {
    if (line.startsWith('data: ')) {
      buffer += line.slice(6);
    } else if (line.trim() === '') {
      if (!buffer) return;

      try {
        const event = JSON.parse(buffer);
        buffer = '';

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
        const res = await axios.get(payloadUrl);

        for (const tx of res.data.transactions) {
          const decodedFields = extractFields(tx.cmd);
          const enrichedTx = {
            chainId,
            height,
            ...decodedFields,
            tx
          };

          await producer.send({
            topic: 'kadena.transactions',
            messages: [{ value: JSON.stringify(enrichedTx) }]
          });

          console.log('✅ Sent to Kafka topic "kadena.transactions":', enrichedTx);
        }
      } catch (err) {
        console.error(`❌ Failed to process block: ${err.message}`);
        buffer = '';
      }
    }
  });

  rl.on('close', () => {
    console.warn('⚠️ SSE stream closed');
  });
}

start().catch((err) => {
  console.error('❌ Fatal error in stream:', err);
});
