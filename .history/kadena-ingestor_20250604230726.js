const { Kafka, Partitioners, logLevel } = require('kafkajs');
const { request } = require('undici');
const { createParser } = require('eventsource-parser');
const fetch = require('node-fetch'); // Needed for payload fetch

// Kadena SSE stream
const STREAM_URL = 'https://api.chainweb.com/chainweb/0.0/mainnet01/header/updates';

// Kadena payload API
const PAYLOAD_URL = (chainId, hash) =>
  `https://api.chainweb.com/chainweb/0.0/mainnet01/chain/${chainId}/payload/${hash}`;

// Kafka setup
const kafka = new Kafka({
  clientId: 'kadena-ingestor',
  brokers: ['localhost:9092'],
  createPartitioner: Partitioners.LegacyPartitioner,
  logLevel: logLevel.ERROR,
});

const producer = kafka.producer();

// Send block metadata to Kafka
async function sendBlockToKafka(block) {
  try {
    await producer.send({
      topic: 'kadena.blocks',
      messages: [{ key: String(block.chainId), value: JSON.stringify(block) }],
    });
    console.log('✅ Sent to Kafka topic "kadena.blocks":', block);
  } catch (err) {
    console.error('❌ Kafka block send error:', err.message);
  }
}

// Fetch and decode payload, then send transactions to Kafka
async function fetchAndDecodePayload(chainId, payloadHash) {
  try {
    const res = await fetch(PAYLOAD_URL(chainId, payloadHash));
    const json = await res.json();

    const txs = json.transactions || [];
    for (const tx of txs) {
      if (typeof tx !== 'string') {
        console.warn('⚠️ Skipping malformed transaction:', tx);
        continue;
      }

      try {
        const decoded = Buffer.from(tx, 'base64').toString();
        const parsed = JSON.parse(decoded); // Optional if you want raw
        await producer.send({
          topic: 'kadena.transactions',
          messages: [{ key: String(chainId), value: JSON.stringify(parsed) }],
        });
        console.log('🔹 Decoded tx sent to Kafka:', parsed);
      } catch (err) {
        console.error(`❌ Failed to decode tx:`, err.message);
      }
    }
  } catch (err) {
    console.error(`❌ Payload fetch/decode error:`, err.message);
  }
}

async function startIngestor() {
  console.log('🚀 Connecting to Kafka...');
  await producer.connect();
  console.log('✅ Kafka connected');
  console.log(`📡 Connecting to SSE: ${STREAM_URL}`);

  const response = await request(STREAM_URL, {
    method: 'GET',
    headers: { accept: 'text/event-stream' },
  });

  const parser = createParser(async (event) => {
    if (event.event === 'BlockHeader') {
      try {
        const parsed = JSON.parse(event.data);

        const { header, payloadHash, txCount } = parsed;

        if (!header || typeof txCount !== 'number' || !payloadHash) {
          console.warn('⚠️ Incomplete or malformed BlockHeader:', parsed);
          return;
        }

        if (txCount > 0) {
          const block = {
            chainId: header.chainId,
            height: header.height,
            payloadHash,
          };

          console.log('🔄 Block with txs:', block);
          await sendBlockToKafka(block);
          await fetchAndDecodePayload(block.chainId, block.payloadHash);
        } else {
          console.log(`⏩ Skipping block at height ${header.height} with 0 transactions`);
        }
      } catch (err) {
        console.error('❌ BlockHeader parse error:', err.message);
      }
    }
  });

  for await (const chunk of response.body) {
    parser.feed(chunk.toString());
  }
}

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('\n👋 Shutting down gracefully...');
  await producer.disconnect();
  process.exit(0);
});

// Start the process
startIngestor();
