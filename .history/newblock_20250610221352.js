const { createClient } = require('graphql-ws');
const WebSocket = require('ws');

const API_KEY = 'your-api-key-here';  // <-- put your actual key here

const client = createClient({
  url: 'wss://api.mainnet.kadindexer.io/v0/graphql',
  webSocketImpl: WebSocket,
  connectionParams: {
    headers: {
      Authorization: `Bearer ${API_KEY}`
    }
  }
});

const query = `
  subscription {
    newBlocks(chainIds: ["0", "1", "2"]) {
      hash
      height
      creationTime
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

(async () => {
  await new Promise((resolve, reject) => {
    client.subscribe(
      { query },
      {
        next: (data) => {
          console.log('📦 New Block Received:');
          console.dir(data, { depth: null });
        },
        error: (err) => {
          console.error('❌ Subscription error:', err);
          reject(err);
        },
        complete: () => {
          console.log('✅ Subscription complete');
          resolve();
        }
      }
    );
  });
})();
