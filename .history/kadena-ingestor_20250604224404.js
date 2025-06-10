const { Kafka, Partitioners } = require('kafkajs');

const { EventSource } = require('eventsource');


// SSE stream from Kadena's mainnet
const STREAM_URL = 'https://api.chainweb.com/chainweb/0.0/mainnet01/header/updates';

// Kafka setup
const kafka = new Kafka({
    clientId: 'kadena-ingestor',
    brokers: ['localhost:9092'],
    createPartitioner: Partitioners.LegacyPartitioner
  });

const producer = kafka.producer();

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
  const es = new EventSource(STREAM_URL);

  es.addEventListener('BlockHeader', async (msg) => {
    if (!msg.data) return;
  
    try {
      const parsed = JSON.parse(msg.data);
      const { header, payloadHash, txCount } = parsed;
  
      if (txCount > 0) {
        const output = {
          chainId: header.chainId,
          height: header.height,
          payloadHash,
        };
  
        console.log('🔄 Block with txs:', output);
        await publishToKafka(output);
      } else {
        console.log(`⏩ Skipping block at height ${header.height} with 0 transactions`);
      }
    } catch (err) {
      console.error('❌ Parse error:', err.message);
    }
  });
  

  es.onerror = (err) => {
    console.error('❌ SSE stream error:', err);
  };
}

// Run the ingestor
startIngestor();
