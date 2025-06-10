const EventSource = require('eventsource');
const axios = require('axios');

const SSE_URL = 'https://api.chainweb.com/chainweb/0.0/mainnet01/header/updates';
const BASE_URL = 'https://api.chainweb.com/chainweb/0.0/mainnet01';

async function fetchPayload(chainId, payloadHash) {
  const url = `${BASE_URL}/chain/${chainId}/payload/${payloadHash}`;
  const res = await axios.get(url);
  return res.data.transactions || [];
}

function extractSender(cmdString) {
  try {
    const cmd = JSON.parse(cmdString);
    return cmd.meta?.sender || 'unknown';
  } catch {
    return 'unknown';
  }
}

function start() {
  console.log(`📡 Listening to all chains via: ${SSE_URL}`);
  const es = new EventSource(SSE_URL);

  es.onmessage = async (event) => {
    try {
      const { header, txCount } = JSON.parse(event.data);
      const { chainId, height, payloadHash } = header;

      if (!payloadHash || txCount === 0) return;

      const transactions = await fetchPayload(chainId, payloadHash);
      const uniqueSenders = new Set();

      for (const tx of transactions) {
        const sender = extractSender(tx.cmd);
        uniqueSenders.add(sender);
      }

      console.log(`${txCount} transactions by ${uniqueSenders.size} accounts in block ${height} on chain ${chainId}`);
    } catch (err) {
      console.error('❌ Failed to process block:', err.message);
    }
  };

  es.onerror = (err) => {
    console.error('❌ SSE connection error:', err.message);
  };
}

start();
