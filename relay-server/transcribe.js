'use strict';

const { GetObjectCommand } = require('@aws-sdk/client-s3');

const FIRESTORE_PROJECT = 'mspy-clone';
const FIRESTORE_BASE =
  `https://firestore.googleapis.com/v1/projects/${FIRESTORE_PROJECT}/databases/(default)/documents`;
const FIRESTORE_KEY =
  process.env.FIREBASE_WEB_API_KEY || 'AIzaSyB4ttc61v7UpsvIK8Uypfo6n-QIfUzzerY';
const MODEL = 'gpt-transcribe';
const PROVIDER = 'openai';
const MAX_BYTES = 25 * 1024 * 1024;
const STUCK_MS = 15 * 60 * 1000;
const inFlight = new Set();

function openaiKey() {
  return (process.env.OPENAI_API_KEY || '').trim();
}

function decodeValue(v) {
  if (!v) return null;
  if ('stringValue' in v) return v.stringValue;
  if ('integerValue' in v) return Number(v.integerValue);
  if ('doubleValue' in v) return v.doubleValue;
  if ('booleanValue' in v) return v.booleanValue;
  if ('timestampValue' in v) return v.timestampValue;
  if (v.mapValue) {
    const o = {};
    const fields = v.mapValue.fields || {};
    for (const [k, val] of Object.entries(fields)) o[k] = decodeValue(val);
    return o;
  }
  if (v.arrayValue) return (v.arrayValue.values || []).map(decodeValue);
  return null;
}

function decodeDoc(json) {
  const o = {};
  const fields = json.fields || {};
  for (const [k, val] of Object.entries(fields)) o[k] = decodeValue(val);
  return o;
}

function encodeValue(v) {
  if (v === null || v === undefined) return { nullValue: null };
  if (typeof v === 'string') return { stringValue: v };
  if (typeof v === 'boolean') return { booleanValue: v };
  if (typeof v === 'number') {
    return Number.isInteger(v) ? { integerValue: String(v) } : { doubleValue: v };
  }
  if (Array.isArray(v)) {
    return { arrayValue: { values: v.map(encodeValue) } };
  }
  const fields = {};
  for (const [k, val] of Object.entries(v)) {
    if (val === undefined) continue;
    fields[k] = encodeValue(val);
  }
  return { mapValue: { fields } };
}

async function firestoreGet(clipId) {
  const url = `${FIRESTORE_BASE}/callrecordings/${encodeURIComponent(clipId)}?key=${FIRESTORE_KEY}`;
  const res = await fetch(url);
  if (res.status === 404) return null;
  if (!res.ok) throw new Error(`firestore get ${res.status}: ${await res.text()}`);
  return decodeDoc(await res.json());
}

async function firestoreSetTranscription(clipId, transcription) {
  const url =
    `${FIRESTORE_BASE}/callrecordings/${encodeURIComponent(clipId)}` +
    `?key=${FIRESTORE_KEY}&updateMask.fieldPaths=transcription`;
  const res = await fetch(url, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      fields: { transcription: encodeValue(transcription) },
    }),
  });
  if (!res.ok) throw new Error(`firestore patch ${res.status}: ${await res.text()}`);
}

function transcriptionOf(doc) {
  return (doc && doc.transcription && typeof doc.transcription === 'object')
    ? doc.transcription
    : {};
}

function alreadyComplete(t) {
  return t.status === 'complete' && typeof t.fullText === 'string' && t.fullText.length > 0;
}

function inProgress(t) {
  if (t.status !== 'processing') return false;
  const started = Number(t.startedAt) || 0;
  return started > 0 && Date.now() - started < STUCK_MS;
}

async function transcribeBuffer(buffer, filename, contentType) {
  const key = openaiKey();
  if (!key) throw new Error('OPENAI_API_KEY is not set on the relay');
  if (buffer.length > MAX_BYTES) {
    throw new Error(`audio is ${buffer.length} bytes; OpenAI limit is ${MAX_BYTES}`);
  }
  const form = new FormData();
  form.append('file', new Blob([buffer], { type: contentType || 'audio/wav' }), filename || 'clip.wav');
  form.append('model', MODEL);
  const res = await fetch('https://api.openai.com/v1/audio/transcriptions', {
    method: 'POST',
    headers: { Authorization: `Bearer ${key}` },
    body: form,
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`openai ${res.status}: ${text.slice(0, 400)}`);
  const json = JSON.parse(text);
  const fullText = (json.text || '').trim();
  if (!fullText) throw new Error('openai returned empty transcript');
  return { fullText, language: json.language || '' };
}

function registerTranscribeRoutes(app, { r2, bucket }) {
  app.post('/api/transcribe', async (req, res) => {
    const clipId = String(req.body?.clipId || req.body?.id || '').trim();
    const force = req.body?.force === true;
    if (!clipId) {
      return res.status(400).json({ success: false, error: 'clipId required' });
    }
    if (!openaiKey()) {
      return res.status(503).json({ success: false, error: 'OPENAI_API_KEY not configured' });
    }
    if (inFlight.has(clipId) && !force) {
      return res.json({ success: true, skipped: true, reason: 'in_progress' });
    }

    let doc;
    try {
      doc = await firestoreGet(clipId);
    } catch (e) {
      return res.status(500).json({ success: false, error: e.message });
    }
    if (!doc) {
      return res.status(404).json({ success: false, error: 'recording not found' });
    }
    if (doc.type !== 'room') {
      return res.status(400).json({ success: false, error: 'only room clips are transcribed' });
    }
    if (doc.status === 'skipped') {
      return res.status(400).json({ success: false, error: 'skipped clips are not transcribed' });
    }
    const r2Key = String(req.body?.r2Key || doc.r2Key || '').trim();
    if (!r2Key || r2Key.includes('..') || !r2Key.startsWith('calls/')) {
      return res.status(400).json({ success: false, error: 'invalid r2Key' });
    }

    const existing = transcriptionOf(doc);
    if (!force && alreadyComplete(existing)) {
      return res.json({ success: true, skipped: true, reason: 'already_complete' });
    }
    if (!force && inProgress(existing)) {
      return res.json({ success: true, skipped: true, reason: 'in_progress' });
    }

    inFlight.add(clipId);
    res.status(202).json({ success: true, accepted: true, clipId });

    setImmediate(async () => {
      try {
        await firestoreSetTranscription(clipId, {
          status: 'processing',
          provider: PROVIDER,
          model: MODEL,
          startedAt: Date.now(),
        });
        const obj = await r2.send(new GetObjectCommand({ Bucket: bucket, Key: r2Key }));
        const bytes = await obj.Body.transformToByteArray();
        const buffer = Buffer.from(bytes);
        const filename = r2Key.split('/').pop() || 'clip.wav';
        const result = await transcribeBuffer(buffer, filename, obj.ContentType);
        await firestoreSetTranscription(clipId, {
          status: 'complete',
          provider: PROVIDER,
          model: MODEL,
          fullText: result.fullText,
          language: result.language,
          transcribedAt: Date.now(),
        });
        console.log(`[TRANSCRIBE] complete ${clipId} (${result.fullText.length} chars)`);
      } catch (err) {
        console.error(`[TRANSCRIBE] failed ${clipId}: ${err.message}`);
        try {
          await firestoreSetTranscription(clipId, {
            status: 'failed',
            provider: PROVIDER,
            model: MODEL,
            error: String(err.message || err).slice(0, 500),
            transcribedAt: Date.now(),
          });
        } catch (e2) {
          console.error(`[TRANSCRIBE] failed to write error ${clipId}: ${e2.message}`);
        }
      } finally {
        inFlight.delete(clipId);
      }
    });
  });
}

module.exports = {
  registerTranscribeRoutes,
  openaiConfigured: () => Boolean(openaiKey()),
};
