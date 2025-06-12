const { SubscriptionClient } = require('subscriptions-transport-ws');
const WebSocket = require('ws');

const API_KEY = 'your-api-key-here';

const client = new SubscriptionClient(
  'wss://api.mainnet.kadindexer.io/v0/graphql',
  {
    reconnect: true,
    connectionParams: {
      Authorization: `Bearer ${API_KEY}`
    }
  },
  WebSocket
);

const query = `
  subscription {
    newBlocks {
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

const observable = client.request({ query });

observable.subscribe({
  next: (data) => {
    console.log('📦 New Block Received:');
    console.dir(data, { depth: null });
  },
  error: (err) => {
    console.error('❌ Subscription error:', err);
  },
  complete: () => {
    console.log('✅ Subscription complete');
  }
});
