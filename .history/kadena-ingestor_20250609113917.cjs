// kadena-ingestor.cjs

const axios = require('axios');
const { TextDecoder } = require('util');

const sseUrl = 'https://api.chainweb.com/chainweb/0.0/mainnet01/header/updates';

async function fetchPayload(chainId, payloadHash) {
  const url = `https://api.chainweb.com/chainweb/0.0/mainnet01/chain/${chainId}/payload/${payloadHash}`;
  const res = await axios.get(url);
  return res.data.transactions || [];
}

function extractSender(cmdString) {
  try {
    const cmd = JSON.parse(cmdString);
    return cmd.meta?.sender || 'unknown';
  } catch (err) {
    return 'unknown';
  }
}

async function start() {
  console.log(`📡 Connecting to SSE: ${sseUrl}`);

  const res = await axios({
    method: 'get',
    url: sseUrl,
    responseType: 'stream',
    headers: { Accept: 'text/event-stream' }
  });

  const decoder = new TextDecoder('utf-8');
  let buffer = '';

  res.data.on('data', async (chunk) => {
    buffer += decoder.decode(chunk, { stream: true });

    while (buffer.includes('\n\n')) {
      const [rawEvent, rest] = buffer.split('\n\n', 2);
      buffer = rest;

      const lines = rawEvent.split('\n');
      const dataLines = lines
        .filter((line) => line.startsWith('data: '))
        .map((line) => line.slice(6));

      const json = dataLines.join('');
      if (!json) continue;

      try {
        const event = JSON.parse(json);
        const { header, txCount } = event;
        const { chainId, height, payloadHash } = header;

        if (txCount === 0) {
          console.log(`⏩ Block ${height} on chain ${chainId}: 0 txs`);
          continue;
        }

        if (!payloadHash) {
          console.warn(`⚠️ Block ${height} has txs (${txCount}) but no payloadHash`);
          continue;
        }

        const transactions = await fetchPayload(chainId, payloadHash);

        const uniqueSenders = new Set();
        for (const tx of transactions) {
          const sender = extractSender(tx.cmd);
          uniqueSenders.add(sender);
        }

        console.log(`🔢 Block ${height} | Chain ${chainId} | txCount: ${txCount} | uniqueSenders: ${uniqueSenders.size}`);
        // Optionally log: console.log([...uniqueSenders]);

      } catch (err) {
        console.error('❌ Error processing block:', err.message);
      }
    }
  });

  res.data.on('end', () => {
    console.warn('⚠️ SSE stream ended');
  });

  res.data.on('error', (err) => {
    console.error('❌ SSE stream error:', err.message);
  });
}

start().catch((err) => {
  console.error('❌ Fatal error:', err.message);
});
