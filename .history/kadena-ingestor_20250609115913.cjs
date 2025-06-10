// kadena-ingestor.cjs

const EventSource = require('eventsource');
const axios = require('axios');
const { Buffer } = require('buffer');

const SSE_URL = 'https://api.chainweb.com/chainweb/0.0/mainnet01/header/updates';
const BASE_URL = 'https://api.chainweb.com/chainweb/0.0/mainnet01';

async function fetchPayloadWithRetry(chainId, payloadHash, maxRetries = 6, baseDelay = 1000) {
  const url = `${BASE_URL}/chain/${chainId}/payload/${payloadHash}`;

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      const res = await axios.get(url);
      return res.data.transactions || [];
    } catch (err) {
      const status = err.response?.status;
      const isLast = attempt === maxRetries - 1;

      if (status === 404 && !isLast) {
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

function extractSender(cmdString) {
  try {
    const cmd = JSON.parse(cmdString);
    const sender = cmd?.meta?.sender;
    return typeof sender === 'string' && sender.startsWith('k:') ? sender : 'unknown';
  } catch {
    return 'unknown';
  }
}

function extractModule(codeString) {
  try {
    const match = codeString.match(/\(?([a-z0-9_.-]+)\./i);
    return match ? match[1] : 'unknown';
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
      let totalGas = 0;

      console.log(`📦 Block ${height} on chain ${chainId} with ${transactions.length} transactions:`);

      for (const txBase64 of transactions) {
        try {
          const decoded = Buffer.from(txBase64, 'base64').toString('utf8');
          const txObj = JSON.parse(decoded);
          const cmdRaw = txObj?.cmd;
          const sender = cmdRaw ? extractSender(cmdRaw) : 'unknown';

          let module = 'unknown';
          let gas = 0;

          if (cmdRaw) {
            const cmd = JSON.parse(cmdRaw);
            const code = cmd?.payload?.exec?.code || '';
            module = extractModule(code);

            const gasLimit = cmd?.meta?.gasLimit || 0;
            const gasPrice = cmd?.meta?.gasPrice || 0;
            gas = gasLimit * gasPrice;
          }

          console.log(`   ↪ Sender: ${sender}`);
          console.log(`     📦 Module: ${module}`);
          console.log(`     ⛽ Gas: ${gas}`);
          uniqueSenders.add(sender);
          totalGas += gas;
        } catch (err) {
          console.warn(`   ⚠️ Failed to decode or parse tx: ${err.message}`);
          uniqueSenders.add('unknown');
        }
      }

      const txWord = txCount === 1 ? 'transaction' : 'transactions';
      const acctWord = uniqueSenders.size === 1 ? 'account' : 'accounts';

      console.log(`${txCount} ${txWord} by ${uniqueSenders.size} ${acctWord} in block ${height} on chain ${chainId}`);
      console.log(`   ⛽ Total estimated gas: ${totalGas}`);
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
