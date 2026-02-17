const express = require('express');
const { WebSocketServer } = require('ws');
const http = require('http');

const app = express();
const PORT = process.env.PORT || 8080;

// Create HTTP server
const server = http.createServer(app);

// Create WebSocket server
const wss = new WebSocketServer({ server });

// Store connected clients
const senders = new Map();    // Tablets sending audio: Map<ws, {id, name, connectedAt}>
const receivers = new Map();  // Phone receiving audio: Map<ws, {selectedSender}>

// Statistics
let stats = {
  totalConnections: 0,
  activeSenders: 0,
  activeReceivers: 0,
  bytesRelayed: 0,
  startTime: Date.now()
};

// Simple web dashboard
app.get('/', (req, res) => {
  const uptime = Math.floor((Date.now() - stats.startTime) / 1000);
  const hours = Math.floor(uptime / 3600);
  const minutes = Math.floor((uptime % 3600) / 60);
  const seconds = uptime % 60;
  
  res.send(`
    <!DOCTYPE html>
    <html>
    <head>
      <title>Audio Relay Server</title>
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <style>
        body {
          font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
          max-width: 800px;
          margin: 50px auto;
          padding: 20px;
          background: #0f0f0f;
          color: #e0e0e0;
        }
        h1 { color: #4CAF50; text-align: center; }
        .status {
          background: #1a1a1a;
          padding: 20px;
          border-radius: 10px;
          margin: 20px 0;
          border-left: 4px solid #4CAF50;
        }
        .stat {
          display: flex;
          justify-content: space-between;
          padding: 10px 0;
          border-bottom: 1px solid #333;
        }
        .stat:last-child { border-bottom: none; }
        .label { color: #888; }
        .value { 
          color: #4CAF50; 
          font-weight: bold;
          font-size: 1.2em;
        }
        .online { color: #4CAF50; }
        .offline { color: #f44336; }
        .footer {
          text-align: center;
          margin-top: 30px;
          color: #666;
          font-size: 0.9em;
        }
      </style>
      <script>
        setInterval(() => location.reload(), 5000);
      </script>
    </head>
    <body>
      <h1>🎙️ Audio Relay Server</h1>
      
      <div class="status">
        <div class="stat">
          <span class="label">Server Status:</span>
          <span class="value online">● ONLINE</span>
        </div>
        <div class="stat">
          <span class="label">Uptime:</span>
          <span class="value">${hours}h ${minutes}m ${seconds}s</span>
        </div>
        <div class="stat">
          <span class="label">Active Senders (Tablets):</span>
          <span class="value">${stats.activeSenders}</span>
        </div>
        <div class="stat">
          <span class="label">Active Receivers (Phone):</span>
          <span class="value">${stats.activeReceivers}</span>
        </div>
        <div class="stat">
          <span class="label">Total Connections:</span>
          <span class="value">${stats.totalConnections}</span>
        </div>
        <div class="stat">
          <span class="label">Data Relayed:</span>
          <span class="value">${(stats.bytesRelayed / (1024 * 1024)).toFixed(2)} MB</span>
        </div>
      </div>

      <div class="footer">
        Auto-refreshes every 5 seconds
      </div>
    </body>
    </html>
  `);
});

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ 
    status: 'ok',
    senders: stats.activeSenders,
    receivers: stats.activeReceivers,
    uptime: Date.now() - stats.startTime
  });
});

// WebSocket connection handler
wss.on('connection', (ws, req) => {
  stats.totalConnections++;
  
  console.log(`[${new Date().toISOString()}] New connection from ${req.socket.remoteAddress}`);
  
  let clientType = null; // 'sender' or 'receiver'
  let clientId = `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;

  // Send welcome message
  ws.send(JSON.stringify({
    type: 'welcome',
    message: 'Connected to relay server',
    clientId: clientId
  }));

  ws.on('message', (data) => {
    try {
      // Check if this is a text message (control message)
      if (data.length < 1000) {
        try {
          const message = JSON.parse(data.toString());
          
          // Client identification
          if (message.type === 'identify') {
            clientType = message.role; // 'sender' or 'receiver'
            
            if (clientType === 'sender') {
              const deviceName = message.deviceName || `Tablet-${clientId.substr(0, 6)}`;
              senders.set(ws, {
                id: clientId,
                name: deviceName,
                connectedAt: Date.now()
              });
              stats.activeSenders = senders.size;
              console.log(`[${new Date().toISOString()}] SENDER connected: ${deviceName} (ID: ${clientId})`);
              
              ws.send(JSON.stringify({ 
                type: 'identified', 
                role: 'sender',
                senderId: clientId,
                deviceName: deviceName
              }));
              
              // Notify all receivers about new sender
              broadcastSenderList();
              
            } else if (clientType === 'receiver') {
              receivers.set(ws, {
                selectedSender: null, // null = listen to all (first sender by default)
                connectedAt: Date.now()
              });
              stats.activeReceivers = receivers.size;
              console.log(`[${new Date().toISOString()}] RECEIVER connected (ID: ${clientId})`);
              
              ws.send(JSON.stringify({ 
                type: 'identified', 
                role: 'receiver'
              }));
              
              // Send current sender list to this receiver
              sendSenderList(ws);
            }
            
            return;
          }
          
          // Receiver selects which sender to listen to
          if (message.type === 'selectSender' && clientType === 'receiver') {
            const receiverInfo = receivers.get(ws);
            if (receiverInfo) {
              receiverInfo.selectedSender = message.senderId; // null means "listen to all"
              console.log(`[${new Date().toISOString()}] RECEIVER switched to sender: ${message.senderId || 'ALL'}`);
              ws.send(JSON.stringify({ 
                type: 'senderSelected', 
                senderId: message.senderId 
              }));
            }
            return;
          }
          
          // Keep-alive ping
          if (message.type === 'ping') {
            ws.send(JSON.stringify({ type: 'pong' }));
            return;
          }
          
        } catch (e) {
          // Not JSON, treat as audio data
        }
      }

      // If this is a sender, relay audio to receivers listening to this sender
      if (clientType === 'sender') {
        stats.bytesRelayed += data.length;
        const senderInfo = senders.get(ws);
        
        receivers.forEach((receiverInfo, receiverWs) => {
          if (receiverWs.readyState === 1) { // WebSocket.OPEN
            // Check if receiver wants this sender's audio
            const shouldRelay = receiverInfo.selectedSender === null || 
                               receiverInfo.selectedSender === senderInfo.id;
            
            if (shouldRelay) {
              try {
                // Prepend sender ID to audio data so receiver knows which tablet it's from
                const header = Buffer.from(JSON.stringify({ 
                  senderId: senderInfo.id,
                  senderName: senderInfo.name 
                }) + '\n');
                receiverWs.send(Buffer.concat([header, data]));
              } catch (err) {
                console.error(`Error sending to receiver: ${err.message}`);
              }
            }
          }
        });
      }
    } catch (error) {
      console.error(`Error processing message: ${error.message}`);
    }
  });

  ws.on('close', () => {
    if (clientType === 'sender') {
      const senderInfo = senders.get(ws);
      senders.delete(ws);
      stats.activeSenders = senders.size;
      console.log(`[${new Date().toISOString()}] SENDER disconnected: ${senderInfo?.name || clientId}`);
      
      // Notify receivers that a sender disconnected
      broadcastSenderList();
      
    } else if (clientType === 'receiver') {
      receivers.delete(ws);
      stats.activeReceivers = receivers.size;
      console.log(`[${new Date().toISOString()}] RECEIVER ${clientId} disconnected`);
    }
  });

  ws.on('error', (error) => {
    console.error(`WebSocket error for ${clientId}: ${error.message}`);
  });
});

// Helper function to send sender list to a specific receiver
function sendSenderList(receiverWs) {
  const senderList = Array.from(senders.values()).map(s => ({
    id: s.id,
    name: s.name,
    connectedAt: s.connectedAt
  }));
  
  try {
    receiverWs.send(JSON.stringify({
      type: 'senderList',
      senders: senderList
    }));
  } catch (err) {
    console.error('Error sending sender list:', err.message);
  }
}

// Helper function to broadcast sender list to all receivers
function broadcastSenderList() {
  const senderList = Array.from(senders.values()).map(s => ({
    id: s.id,
    name: s.name,
    connectedAt: s.connectedAt
  }));
  
  receivers.forEach((receiverInfo, receiverWs) => {
    if (receiverWs.readyState === 1) {
      try {
        receiverWs.send(JSON.stringify({
          type: 'senderList',
          senders: senderList
        }));
      } catch (err) {
        console.error('Error broadcasting sender list:', err.message);
      }
    }
  });
}

// Start server
server.listen(PORT, () => {
  console.log('========================================');
  console.log('🎙️  Audio Relay Server Started');
  console.log('========================================');
  console.log(`Port: ${PORT}`);
  console.log(`Dashboard: http://localhost:${PORT}`);
  console.log(`WebSocket: ws://localhost:${PORT}`);
  console.log('========================================');
});

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('SIGTERM received, shutting down gracefully...');
  server.close(() => {
    console.log('Server closed');
    process.exit(0);
  });
});
