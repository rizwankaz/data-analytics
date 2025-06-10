// kadena-ingestor.cjs

const EventSource = require('eventsource');
const axios = require('axios');

const SSE_URL = 'https://api.chainweb.com/chainweb/0.0/mainnet01/header/updates';
const BASE_URL = 'https://api.chainweb.com/chainweb/0.0/mainnet01';

// Fetch payload with retry
async function fetchPayloadWithRetry(chainId, payloadHash, maxRetries = 6, baseDelay = 1000) {
  const url = `${BASE_URL}/chain/${chainId}/payload/${payloadHash}`;
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      const res = await axios.get(url);
      return res.data.transactions || [];
    } catch (err) {
      const isLast = attempt === maxRetries - 1;
      if (err.response?.status === 404 && !isLast) {
        const delay = baseDelay * Math.pow(2, attempt);
        console.warn(`⚠️ Payload not ready for chain ${chainId}, retry ${attempt + 1}/${maxRetries} in ${delay}ms`);
        await new Promise((r) => setTimeout(r, delay));
        continue;
      }
      throw err;
    }
  }
  throw new Error(`Max retries exceeded for payload ${payloadHash} on chain ${chainId}`);
}

// Parse the sender from the transaction command
function extractSender(cmdString) {
  try {
    if (typeof cmdString !== 'string') {
      console.warn('   ⚠️ cmd is not a string:', cmdString);
      return 'unknown';
    }

    const cmd = JSON.parse(cmdString);

    if (cmd?.meta?.sender) {
      return cmd.meta.sender;
    }

    console.warn('   ⚠️ No sender in cmd:', cmd);
    return 'unknown';
  } catch (err) {
    console.warn(`   ⚠️ JSON parse error: ${err.message}`);
    return 'unknown';
  }
}

// Start streaming from Chainweb SSE
function start() {
  console.log(`📡 Listening to all chains via: ${SSE_URL}`);
  const es = new EventSource(SSE_URL);

  es.addEventListener('BlockHeader', async (event) => {
    try {
      const { header, txCount } = JSON.parse(event.data);
      const { chainId, height, payloadHash } = header;

      if (!payloadHash || txCount === 0) return;

      const transactions = await fetchPayloadWithRetry(chainId, payloadHash);
      const uniqueSenders = new Set();

      console.log(`📦 Block ${height} on chain ${chainId} with ${transactions.length} transactions:`);

      for (const tx of transactions) {
        console.log('   🧾 Raw cmd:', tx.cmd);
        const sender = extractSender(tx.cmd);
        console.log(`   ↪ Sender: ${sender}`);
        uniqueSenders.add(sender);
      }

      const txWord = txCount === 1 ? 'transaction' : 'transactions';
      const acctWord = uniqueSenders.size === 1 ? 'account' : 'accounts';

      console.log(`${txCount} ${txWord} by ${uniqueSenders.size} ${acctWord} in block ${height} on chain ${chainId}`);
      console.log(`   👥 Unique senders:`, [...uniqueSenders]);
    } catch (err) {
      if (err.message.startsWith('Max retries exceeded')) {
        console.warn(`⚠️ Gave up on payload ${payloadHash} on chain ${header?.chainId}`);
      } else {
        console.error('❌ Failed to process block:', err.message);
      }
    }
  });

  es.onerror = (err) => {
    console.error('❌ SSE connection error:', err.message);
  };
}

start();
