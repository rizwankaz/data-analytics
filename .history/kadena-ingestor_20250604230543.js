const { Kafka, Partitioners, logLevel } = require('kafkajs');
const { request } = require('undici');
const { createParser } = require('eventsource-parser');

// Kadena SSE stream
const STREAM_URL = 'https://api.chainweb.com/chainweb/0.0/mainnet01/header/updates';
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

// Publish to Kafka topic
async function publishToKafka(topic, msg) {
  try {
    await producer.send({
      topic,
      messages: [{ key: String(msg.chainId), value: JSON.stringify(msg) }],
    });
    console.log(`✅ Sent to Kafka topic "${topic}":`, msg);
  } catch (error) {
    console.error('❌ Kafka send error:', error);
  }
}

// Fetch payload from Kadena API
async function fetchPayload(chainId, payloadHash) {
  const url = PAYLOAD_URL(chainId, payloadHash);
  try {
    const res = await request(url);
    const body = await res.body.json();
    return body.transactions || [];
  } catch (err) {
    console.error(`❌ Failed to fetch payload for chain ${chainId}, hash ${payloadHash}:`, err.message);
    return [];
  }
}

// Start listening and processing
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
          if (parsed.txCount > 0) {
            const { chainId, height, payloadHash } = parsed.header;
            const output = { chainId, height, payloadHash };

            console.log('🔄 Block with txs:', output);
            await publishToKafka('kadena.blocks', output);

            const transactions = await fetchPayload(chainId, payloadHash);

            for (const tx of transactions) {
              if (!tx || !tx.cmd || !tx.hash) {
                console.warn('⚠️ Skipping malformed transaction:', tx);
                continue;
              }

              try {
                const decoded = Buffer.from(tx.cmd, 'base64').toString('utf8');
                const parsedTx = JSON.parse(decoded);

                const txOut = {
                  chainId,
                  height,
                  txHash: tx.hash,
                  command: parsedTx,
                };

                await publishToKafka('kadena.transactions', txOut);
              } catch (err) {
                console.error(`❌ Failed to decode tx ${tx.hash}:`, err.message);
              }
            }
          } else {
            console.log(`⏩ Skipping block at height ${parsed.header.height} with 0 transactions`);
          }
        } catch (err) {
          console.error('❌ Failed to parse BlockHeader:', err.message);
        }
      }
    },
  });

  for await (const chunk of response.body) {
    parser.feed(chunk.toString());
  }
}

// Start the pipeline
startIngestor();

process.on('SIGINT', async () => {
  console.log('\n👋 Shutting down gracefully...');
  await producer.disconnect();
  process.exit(0);
});
