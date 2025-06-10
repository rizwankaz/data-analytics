const https = require('https');

const url = 'https://api.chainweb.com/chainweb/0.0/mainnet01/chain/0/header';

console.log('Testing REST endpoint:', url);

https.get(url, (res) => {
  let data = '';

  res.on('data', (chunk) => {
    data += chunk;
  });

  res.on('end', () => {
    try {
      const parsed = JSON.parse(data);
      console.log('Latest block data:', JSON.stringify(parsed, null, 2));
    } catch (e) {
      console.error('Failed to parse response:', e);
      console.log('Raw response:', data);
    }
  });
}).on('error', (err) => {
  console.error('Request failed:', err);
}); 