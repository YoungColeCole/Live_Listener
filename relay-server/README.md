# Audio Relay Server

WebSocket relay server that streams audio from multiple Android tablets to a phone in real-time.

## Features

- ✅ Supports multiple sender devices (tablets)
- ✅ Switch between tablets on the receiver app
- ✅ High-quality audio streaming
- ✅ Real-time relay (minimal latency)
- ✅ Web dashboard for monitoring
- ✅ Auto-reconnection support
- ✅ Free to host on Render

## How It Works

```
Tablet 1 (Sender) ───┐
                     │
Tablet 2 (Sender) ───┼──> Relay Server ──> Phone (Receiver)
                     │
Tablet N (Sender) ───┘
```

## Local Testing

1. Install dependencies:
```bash
npm install
```

2. Start server:
```bash
npm start
```

3. Open browser to `http://localhost:8080` to see dashboard

4. Server is ready at `ws://localhost:8080`

## Deploy to Render (Free)

### Method 1: Direct Upload (No GitHub)

1. Go to https://render.com and sign up (free)
2. Click "New +" → "Web Service"
3. Choose "Build and deploy from a Git repository" or "Deploy from Docker"
4. Upload this folder or connect repo
5. Settings:
   - **Name**: audio-relay-server (or your choice)
   - **Environment**: Node
   - **Build Command**: `npm install`
   - **Start Command**: `npm start`
   - **Plan**: Free
6. Click "Create Web Service"
7. Wait 2-3 minutes for deployment
8. Your URL will be: `https://audio-relay-server.onrender.com` (or similar)

### Method 2: With GitHub

1. Create GitHub account (if you don't have one)
2. Create new repository
3. Upload this folder
4. Go to Render → New Web Service → Connect GitHub
5. Select your repo
6. Same settings as Method 1

## Important Notes

- **Free tier limitation**: Server may sleep after 15 minutes of inactivity. It will wake up when a client connects (takes ~30 seconds).
- **Keep awake**: Have the receiver app ping the server periodically to prevent sleep.
- **WebSocket URL**: After deployment, use `wss://` (not `ws://`) for secure connection.

## Testing Connection

Once deployed, visit your Render URL in a browser to see the dashboard. It will show:
- Number of connected senders (tablets)
- Number of connected receivers (phone)
- Total data relayed
- Uptime

## Troubleshooting

**Server not responding?**
- Check Render dashboard for logs
- Ensure WebSocket URL uses `wss://` (secure)

**Audio not flowing?**
- Verify senders are connecting (check dashboard)
- Verify receiver is connecting (check dashboard)
- Check app logs on devices

**Server sleeping?**
- This is normal on free tier
- First connection wakes it up (~30s)
- Consider upgrading to paid tier for 24/7 uptime

## Server Stats

Visit the root URL (e.g., `https://yourapp.onrender.com`) to see:
- Active connections
- Data throughput
- Server uptime
