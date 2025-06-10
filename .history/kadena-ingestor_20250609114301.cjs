const EventSource = require('eventsource');
const axios = require('axios');

const BASE_URL = 'https://api.chainweb.com/chainweb/0.0/mainnet01';

// Fetch payload from a specific chain
async function fetchPayload(chainId, payloadHash) {
  const url = `${BASE_URL}/chain/${chainId}/payload/${payloadHash}`;
  const res = await axios.get(url);
  return res.data.transactions || [];
}

// Extract sender from transaction
function extractSender(cmdString) {
  try {
    const cmd = JSON.parse(cmdString);
    return cmd.meta?.sender || 'unknown';
  } catch {
    return 'unknown';
  }
}

// Listen to a specific chain
function listenToChain(chainId) {
  const sseUrl = `${BASE_URL}/chain/${chainId}/header/updates`;
  const es = new EventSource(sseUrl);

  es.onmessage = async (event) => {
    try {
      const { header, txCount } = JSON.parse(event.data);
      const { height, payloadHash } = header;

      if (!payloadHash || txCount === 0) return;

      const transactions = await fetchPayload(chainId, payloadHash);
      const uniqueSenders = new Set();

      for (const tx of transactions) {
        const sender = extractSender(tx.cmd);
        uniqueSenders.add(sender);
      }

      console.log(`${txCount} transactions by ${uniqueSenders.size} accounts in block ${height} on chain ${chainId}`);
    } catch (err) {
      console.error(`❌ Error on chain ${chainId}:`, err.message);
    }
  };

  es.onerror = (err) => {
    console.error(`❌ SSE error on chain ${chainId}:`, err.message);
  };

  console.log(`📡 Listening to chain ${chainId}`);
}

// Start listeners on all 20 chains
function start() {
  for (let chainId = 0; chainId < 20; chainId++) {
    listenToChain(chainId);
  }
}

start();
