const { Kafka } = require('kafkajs');
const EventSource = require('eventsource').EventSource;

// Kadena's header update stream
const STREAM_URL = 'https://api.chainweb.com/chainweb/0.0/mainnet01/header/updates';

const kafka = new Kafka({
  clientId: 'kadena-ingestor',
  brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
  retry: {
    initialRetryTime: 100,
    retries: 8
  },
  connectionTimeout: 3000,
});

const producer = kafka.producer();

async function startHeaderStream() {
  const es = new EventSource(STREAM_URL);

  es.onmessage = async (msg) => {
    if (!msg.data) return;

    try {
      const parsed = JSON.parse(msg.data);
      const { header, payloadHash, txCount } = parsed;

      if (txCount > 0) {
        const output = {
          chainId: header.chainId,
          height: header.height,
          payloadHash: payloadHash,
        };

        await publish(output);
      }
    } catch (e) {
      console.error('Failed to parse block header:', e.message);
    }
  };

  es.onerror = (err) => {
    console.error('SSE stream error:', err);
  };
}

(async () => {
  try {
    console.log('Connecting to Kafka...');
    await producer.connect();
    console.log('Successfully connected to Kafka');
    
    publish = async (msg) => {
      try {
        await producer.send({
          topic: 'kadena.blocks',
          messages: [{ key: String(msg.chainId), value: JSON.stringify(msg) }],
        });
        console.log('Message sent successfully:', msg.chainId);
      } catch (error) {
        console.error('Error sending message:', error);
      }
    };

    console.log('🚀 Kafka ready → starting SSE');
    await startHeaderStream();
  } catch (error) {
    console.error('Failed to connect to Kafka:', error);
    process.exit(1);
  }
})();