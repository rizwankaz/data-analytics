const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'kadena-ingestor',
  brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
});

const producer = kafka.producer();

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
    startHeaderStream();
  } catch (error) {
    console.error('Failed to connect to Kafka:', error);
    process.exit(1);
  }
})();