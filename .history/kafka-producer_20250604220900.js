const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'kadena-ingestor',
  brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
});

const producer = kafka.producer();

(async () => {
  await producer.connect();
  publish = (msg) =>
    producer.send({
      topic: 'kadena.blocks',
      messages: [{ key: String(msg.chainId), value: JSON.stringify(msg) }],
    });

  console.log('🚀 Kafka ready → starting SSE');
  startHeaderStream();
})();