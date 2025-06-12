const { createClient } = require('graphql-ws');
const WebSocket = require('ws');

// Setup WebSocket client
const client = createClient({
  url: 'wss://api.mainnet.kadindexer.io/v0/graphql',
  webSocketImpl: WebSocket
});

// Subscription query
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

// Subscribe to the stream
(async () => {
  await new Promise((resolve, reject) => {
    client.subscribe(
      {
        query
      },
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
