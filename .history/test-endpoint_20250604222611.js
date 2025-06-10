const EventSource = require('eventsource').EventSource;

const STREAM_URL = 'https://api.chainweb.com/chainweb/0.0/mainnet01/header/updates';

console.log('Testing connection to:', STREAM_URL);

const es = new EventSource(STREAM_URL);

es.onopen = () => {
  console.log('Connection established');
};

es.onmessage = (msg) => {
  console.log('Raw message received:', msg.data);
  try {
    const parsed = JSON.parse(msg.data);
    console.log('Parsed message:', JSON.stringify(parsed, null, 2));
  } catch (e) {
    console.error('Failed to parse message:', e);
  }
};

es.onerror = (err) => {
  console.error('Connection error:', err);
}; 