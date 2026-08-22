const http = require('http');
const WebSocket = require('ws');

const PORT = process.env.PORT || 10000;

// Render tarvitsee HTTP-palvelimen, joka kuuntelee PORT-arvoa.
const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok' }));
    return;
  }

  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('HuntCam signaling server is running');
});

const wss = new WebSocket.Server({ server });

wss.on('connection', (ws) => {
  console.log('HuntCam client connected');

  ws.isAlive = true;

  ws.on('pong', () => {
    ws.isAlive = true;
  });

  ws.on('message', (message) => {
    // Välitetään WebRTC offer / answer / ICE candidate
    // kaikille muille yhdistetyille HuntCam-asiakkaille.
    wss.clients.forEach((client) => {
      if (
        client !== ws &&
        client.readyState === WebSocket.OPEN
      ) {
        client.send(message.toString());
      }
    });
  });

  ws.on('close', () => {
    console.log('HuntCam client disconnected');
  });

  ws.on('error', (error) => {
    console.error('WebSocket error:', error);
  });
});

// Tarkistetaan yhteydet säännöllisesti.
// Tämä auttaa erityisesti 4G-yhteyksissä.
const heartbeat = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      return ws.terminate();
    }

    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

wss.on('close', () => {
  clearInterval(heartbeat);
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`HuntCam signaling server running on port ${PORT}`);
});
