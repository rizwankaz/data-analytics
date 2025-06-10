const { Kafka } = require('kafkajs');
const WebSocket = require('ws');

// Kadena's WebSocket endpoint
const WS_URL = 'wss://api.chainweb.com/chainweb/0.0/mainnet01/ws/header/updates';

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
  console.log('Starting WebSocket connection to:', WS_URL);
  const ws = new WebSocket(WS_URL);

  ws.on('open', () => {
    console.log('WebSocket connection established');
  });

  ws.on('message', async (data) => {
    console.log('Received message:', data.toString());
    
    try {
      const parsed = JSON.parse(data);
      console.log('Parsed message:', JSON.stringify(parsed, null, 2));
      
      const { header, payloadHash, txCount } = parsed;
      console.log('Transaction count:', txCount);

      if (txCount > 0) {
        const output = {
          chainId: header.chainId,
          height: header.height,
          payloadHash: payloadHash,
        };

        console.log('Sending to Kafka:', output);
        await publish(output);
      } else {
        console.log('Skipping block with no transactions');
      }
    } catch (e) {
      console.error('Failed to parse block header:', e.message);
      console.error('Raw message:', data.toString());
    }
  });

  ws.on('error', (err) => {
    console.error('WebSocket error:', err);
  });

  ws.on('close', () => {
    console.log('WebSocket connection closed');
  });
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

    console.log('🚀 Kafka ready → starting WebSocket');
    await startHeaderStream();
  } catch (error) {
    console.error('Failed to connect to Kafka:', error);
    process.exit(1);
  }
})();