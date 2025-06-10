// kadena-ingestor.cjs

const EventSource = require('eventsource');
const axios = require('axios');

const SSE_URL = 'https://api.chainweb.com/chainweb/0.0/mainnet01/header/updates';
const BASE_URL = 'https://api.chainweb.com/chainweb/0.0/mainnet01';

async function fetchPayloadWithRetry(chainId, payloadHash, maxRetries = 6, baseDelay = 1000) {
  const url = `${BASE_URL}/chain/${chainId}/payload/${payloadHash}`;

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      const res = await axios.get(url);
      return res.data.transactions || [];
    } catch (err) {
      const isLastAttempt = attempt === maxRetries - 1;
      const status = err.response?.status;

      if (status === 404 && !isLastAttempt) {
        const delay = baseDelay * Math.pow(2, attempt); // 1s, 2s, 4s, etc.
        console.warn(`⚠️ Payload not ready for chain ${chainId}, retry ${attempt + 1}/${maxRetries} after ${delay}ms...`);
        await new Promise((resolve) => setTimeout(resolve, delay));
      } else {
        throw err;
      }
    }
  }

  throw new Error(`Max retries exceeded for payload ${payloadHash} on chain ${chainId}`);
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

  es.addEventListener('BlockHeader', async (event) => {
    try {
      const { header, txCount } = JSON.parse(event.data);
      const { chainId, height, payloadHash } = header;

      if (!payloadHash || txCount === 0) return;

      const transactions = await fetchPayloadWithRetry(chainId, payloadHash);
      const uniqueSenders = new Set();

      for (const tx of transactions) {
        const sender = extractSender(tx.cmd);
        uniqueSenders.add(sender);
      }

      const txWord = txCount === 1 ? 'transaction' : 'transactions';
      const acctWord = uniqueSenders.size === 1 ? 'account' : 'accounts';

      console.log(`${txCount} ${txWord} by ${uniqueSenders.size} ${acctWord} in block ${height} on chain ${chainId}`);
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
