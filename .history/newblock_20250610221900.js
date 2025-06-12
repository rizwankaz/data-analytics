const { SubscriptionClient } = require('subscriptions-transport-ws');
const WebSocket = require('ws');

// === CONFIG ===
const API_KEY = 'your-api-key-here';  // <-- Insert your Kadindexer API key here
const ENDPOINT = 'wss://api.mainnet.kadindexer.io/v0/graphql';

// === Initialize subscription client ===
const client = new SubscriptionClient(
  ENDPOINT,
  {
    reconnect: true,
    connectionParams: {
      Authorization: `Bearer ${API_KEY}`
    }
  },
  WebSocket
);

// === Define subscription query ===
const query = `
  subscription {
    newBlocks {
      hash
      height
      creationTime
      chainId
      minerAccount {
        accountName
        balance
      }
      transactions(first: 5) {
        edges {
          node {
            hash
            result {
              gas
            }
          }
        }
      }
    }
  }
`;

// === Subscribe ===
const observable = client.request({ query });

observable.subscribe({
  next: (data) => {
    const block = data?.data?.newBlocks;
    if (block) {
      console.log('📦 New Block Received:');
      console.log(`Chain ID: ${block.chainId}`);
      console.log(`Height: ${block.height}`);
      console.log(`Hash: ${block.hash}`);
      console.log(`Time: ${block.creationTime}`);
      console.log(`Miner: ${block.minerAccount?.accountName}`);
      console.log('Transactions:');
      block.transactions?.edges?.forEach(({ node }, idx) => {
        console.log(`  Tx ${idx + 1}: ${node.hash} (Gas: ${node.result?.gas})`);
      });
      console.log('-------------------------------');
    } else {
      console.warn('⚠️ Received empty block data:', data);
    }
  },
  error: (err) => {
    console.error('❌ Subscription error:', err);
  },
  complete: () => {
    console.log('✅ Subscription complete');
  }
});

// === Clean shutdown ===
process.on('SIGINT', () => {
  console.log('🔌 Closing subscription...');
  client.close();
  process.exit();
});
