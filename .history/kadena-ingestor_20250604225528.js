const { Kafka, Partitioners, logLevel } = require('kafkajs');
const { request } = require('undici');
const { createParser } = require('eventsource-parser');

// Kadena SSE stream
const STREAM_URL = 'https://api.chainweb.com/chainweb/0.0/mainnet01/header/updates';

// Kafka setup
const kafka = new Kafka({
  clientId: 'kadena-ingestor',
  brokers: ['localhost:9092'],
  createPartitioner: Partitioners.LegacyPartitioner,
  logLevel: logLevel.ERROR,
});
const producer = kafka.producer();

async function publishToKafka(msg) {
  try {
    await producer.send({
      topic: 'kadena.blocks',
      messages: [{ key: String(msg.chainId), value: JSON.stringify(msg) }],
    });
    console.log('✅ Sent to Kafka:', msg);
  } catch (error) {
    console.error('❌ Kafka send error:', error);
  }
}

async function startIngestor() {
  console.log('🚀 Connecting to Kafka...');
  await producer.connect();
  console.log('✅ Kafka connected');
  console.log(`📡 Connecting to SSE: ${STREAM_URL}`);

  const response = await request(STREAM_URL, {
    method: 'GET',
    headers: {
      accept: 'text/event-stream',
    },
  });

  const parser = createParser({
    onEvent(event) {
      if (event.event === 'BlockHeader') {
        try {
          const parsed = JSON.parse(event.data);
          if (parsed.txCount > 0) {
            const output = {
              chainId: parsed.header.chainId,
              height: parsed.header.height,
              payloadHash: parsed.payloadHash,
            };
            console.log('🔄 Block with txs:', output);
            publishToKafka(output);
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

startIngestor();

process.on('SIGINT', async () => {
  console.log('\n👋 Shutting down gracefully...');
  await producer.disconnect();
  process.exit(0);
});
