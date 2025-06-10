// npm i kafkajs eventsource
const { Kafka } = require('kafkajs');
const EventSource = require('eventsource');      //  ← correct import

// -------------- config ------------------
const STREAM_URL =
  'https://api.chainweb.com/chainweb/0.0/mainnet01/header/updates';

const kafka = new Kafka({
  clientId: 'kadena-ingestor',
  brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
  // tune below if your broker isn’t local
  connectionTimeout: 3000,
  retry: { initialRetryTime: 100, retries: 8 },
});

const TOPIC = 'kadena.blocks';
// ----------------------------------------

const producer = kafka.producer();
let isDraining = false;                 // tiny local buffer for grace‑shutdown
const buffer = [];

// -------------- SSE setup ---------------
function startHeaderStream() {
  console.log('📡 Connecting to SSE:', STREAM_URL);
  const es = new EventSource(STREAM_URL);

  es.addEventListener('BlockHeader', (evt) => {
    try {
      const { header, payloadHash, txCount } = JSON.parse(evt.data);
      if (txCount === 0) return;        // skip empty blocks

      buffer.push({
        key: String(header.chainId),
        value: JSON.stringify({
          chainId: header.chainId,
          height: header.height,
          payloadHash,
        }),
      });

      // flush immediately; batching isn’t critical at 1 msg/sec
      flush();
    } catch (e) {
      console.error('❌ Bad SSE chunk', e);
    }
  });

  es.onerror = (err) => console.error('SSE error:', err);
}
// ----------------------------------------

async function flush() {
  if (isDraining || buffer.length === 0) return;
  isDraining = true;
  const batch = buffer.splice(0);       // grab all queued messages

  try {
    await producer.send({ topic: TOPIC, messages: batch });
    // eslint-disable-next-line no-console
    console.log(`✅ sent ${batch.length} block(s) to Kafka`);
  } catch (err) {
    console.error('Kafka send failed, re‑queueing', err);
    buffer.unshift(...batch);           // put them back
  } finally {
    isDraining = false;
  }
}

// -------------- bootstrap ---------------
(async () => {
  console.log('🚀 Connecting to Kafka…');
  await producer.connect();
  console.log('⚡ Kafka ready; launching SSE listener');
  startHeaderStream();
})();

// graceful exit
['SIGINT', 'SIGTERM'].forEach((sig) =>
  process.on(sig, async () => {
    console.log('\n⏳ draining buffer…');
    await flush();
    await producer.disconnect();
    process.exit(0);
  }),
);
