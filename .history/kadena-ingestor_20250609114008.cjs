// kadena-ingestor.cjs

const EventSource = require('eventsource');
const axios = require('axios');

const sseUrl = 'https://api.chainweb.com/chainweb/0.0/mainnet01/header/updates';

function extractSender(cmdString) {
  try {
    const cmd = JSON.parse(cmdString);
    return cmd.meta?.sender || 'unknown';
  } catch (err) {
    return 'unknown';
  }
}

async function fetchPayload(chainId, payloadHash) {
  const url = `https://api.chainweb.com/chainweb/0.0/mainnet01/chain/${chainId}/payload/${payloadHash}`;
  const res = await axios.get(url);
  return res.data.transactions || [];
}

function start() {
  console.log(`📡 Connecting to SSE: ${sseUrl}`);
  const es = new EventSource(sseUrl);

  es.onmessage = async (event) => {
    try {
      const data = JSON.parse(event.data);
      const { header, txCount } = data;
      const { chainId, height, payloadHash } = header;

      if (txCount === 0) {
        console.log(`⏩ Block ${height} on chain ${chainId}: 0 txs`);
        return;
      }

      if (!payloadHash) {
        console.warn(`⚠️ Block ${height} has txs (${txCount}) but no payloadHash`);
        return;
      }

      const transactions = await fetchPayload(chainId, payloadHash);
      const uniqueSenders = new Set();

      for (const tx of transactions) {
        const sender = extractSender(tx.cmd);
        uniqueSenders.add(sender);
      }

      console.log(`🔢 Block ${height} | Chain ${chainId} | txCount: ${txCount} | uniqueSenders: ${uniqueSenders.size}`);
    } catch (err) {
      console.error('❌ Error processing message:', err.message);
    }
  };

  es.onerror = (err) => {
    console.error('❌ SSE error:', err);
  };
}

start();
