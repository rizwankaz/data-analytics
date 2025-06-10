const { Kafka, Partitioners, logLevel } = require('kafkajs');
const eventsource = require('eventsource');

// Kadena SSE stream for block headers
const STREAM_URL = 'https://api.chainweb.com/chainweb/0.0/mainnet01/header/updates';

// Kafka setup
const kafka = new Kafka({
  clientId: 'kadena-ingestor',
  brokers: ['localhost:9092'], // change if your broker is remote
  createPartitioner: Partitioners.LegacyPartitioner,
  logLevel: logLevel.ERROR, // suppress info logs like the partitioner warning
});

const producer = kafka.producer();

// Publish message to Kafka
async function publishToKafka(msg) {
  try {
    await producer.send({
      topic: 'kadena.blocks',
      messages: [
        {
          key: String(msg.chainId),
          value: JSON.stringify(msg),
        },
      ],
    });
    console.log('✅ Sent to Kafka:', msg);
  } catch (error) {
    console.error('❌ Kafka send error:', error);
  }
}

// Main ingestor logic
async function startIngestor() {
  try {
    console.log('🚀 Connecting to Kafka...');
    await producer.connect();
    console.log('✅ Kafka connected');
  } catch (err) {
    console.error('❌ Kafka connection failed:', err);
    process.exit(1);
  }

  console.log(`📡 Connecting to SSE: ${STREAM_URL}`);
  const es = new eventsource(STREAM_URL);

  // Use onmessage because eventsource npm does NOT support named events
  es.onmessage = async (msg) => {
    if (!msg.data) return;

    try {
      const parsed = JSON.parse(msg.data);

      // Defensive structure check
      if (!parsed.header || !('txCount' in parsed) || !parsed.payloadHash) {
        console.warn('⚠️ Skipping malformed or incomplete message:', parsed);
        return;
      }

      if (parsed.txCount > 0) {
        const output = {
          chainId: parsed.header.chainId,
          height: parsed.header.height,
          payloadHash: parsed.payloadHash, // ✅ correct
        };

        console.log('🔄 Block with txs:', output);
        await publishToKafka(output);
      } else {
        console.log(`⏩ Skipping block at height ${parsed.header.height} with 0 transactions`);
      }
    } catch (err) {
      console.error('❌ Parse error:', err.message);
    }
  };

  es.onerror = (err) => {
    console.error('❌ SSE stream error:', err);
  };
}

// Run it
startIngestor();

// Optional: graceful shutdown
process.on('SIGINT', async () => {
  console.log('\n👋 Shutting down gracefully...');
  await producer.disconnect();
  process.exit(0);
});
