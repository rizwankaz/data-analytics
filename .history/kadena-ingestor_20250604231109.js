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

  const parser = createParser({
    onEvent: async (event) => {
      if (event.event === 'BlockHeader') {
        try {
          const parsed = JSON.parse(event.data);
  
          if (!parsed || !parsed.header || parsed.txCount === undefined) {
            console.warn('⚠️ Incomplete or malformed BlockHeader:', parsed);
            return;
          }
  
          if (parsed.txCount === 0) {
            // Skip blocks with no transactions
            return;
          }
  
          const { chainId, height, payloadHash } = parsed.header;
          if (!payloadHash) {
            console.warn(`⚠️ Missing payloadHash for block ${height} on chain ${chainId}`);
            return;
          }
  
          const blockMeta = { chainId, height, payloadHash };
          console.log('🔄 Block with txs:', blockMeta);
          await publishToKafka(blockMeta);
  
          // Fetch payload and decode transactions
          const payloadUrl = `https://api.chainweb.com/chainweb/0.0/mainnet01/chain/${chainId}/payload/${payloadHash}`;
          const payloadRes = await fetch(payloadUrl);
          const payloadData = await payloadRes.json();
  
          if (payloadData.transactions) {
            for (const tx of payloadData.transactions) {
              try {
                const txData = JSON.parse(Buffer.from(tx, 'base64').toString('utf-8'));
                console.log('✅ Decoded tx:', txData);
                // Optionally publish to another Kafka topic here
              } catch (err) {
                console.warn('⚠️ Skipping malformed transaction:', tx);
                continue;
              }
            }
          } else {
            console.warn(`⚠️ No transactions found in payload ${payloadHash}`);
          }
        } catch (err) {
          console.error('❌ Failed to process BlockHeader:', err.message);
        }
      }
    },
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
