const EventSource = require('eventsource');

// Kadena's header update stream
const STREAM_URL = 'https://api.chainweb.com/chainweb/0.0/mainnet01/header/updates';

const es = new EventSource(STREAM_URL);

es.onmessage = (msg) => {
  // Ensure we’re only handling `BlockHeader` events
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

      // Output to console or forward to queue/buffer
      console.log('New block with transactions:', output);

      // TODO: Emit to Redis, Kafka, or in-memory pipeline
    }
  } catch (e) {
    console.error('Failed to parse block header:', e.message);
  }
};

es.onerror = (err) => {
  console.error('SSE stream error:', err);
};