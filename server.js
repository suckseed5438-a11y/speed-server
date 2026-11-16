// เซิร์ฟเวอร์สำหรับจัดห้อง + ส่งสัญญาณ (signaling) ให้ WebRTC
// เสียงจริงๆ ไม่ผ่านเซิร์ฟเวอร์นี้ ส่งตรงระหว่างเพื่อนกับเพื่อน
// เข้ากันได้กับแอปเวอร์ชันเก่า (ฟิลด์ใหม่ทั้งหมดเป็นตัวเลือก)
'use strict';
const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const ROOMS = ['ห้องรวม', 'ห้องเกม', 'ห้องพักผ่อน']; // แก้ชื่อ/จำนวนห้องได้ตรงนี้
// รหัสผ่านเซิร์ฟเวอร์ (ไม่บังคับ): ตั้งใน Render > Environment > SERVER_PASSWORD  ถ้าไม่ตั้ง = ไม่ต้องใช้รหัส
const PASSWORD = String(process.env.SERVER_PASSWORD || '');

// ---------- ขีดจำกัด (กันสแปม/กันล่ม) ----------
const LIMIT = {
  maxPayload: 64 * 1024,   // ข้อความใหญ่สุดต่อครั้ง
  perIp: 8,                // การเชื่อมต่อพร้อมกันต่อ IP (คนบ้านเดียวกันใช้ IP เดียวกันได้)
  total: 300,              // การเชื่อมต่อทั้งหมด
  helloMs: 20000,          // ต้องส่ง hello ภายในเวลานี้
  resumeMs: 25000,         // หลุดแล้วกลับมาภายในเวลานี้ = ได้ที่เดิม (เสียงไม่ขาด)
  nameLen: 24, chatLen: 500, reasonLen: 100,
};
// ถังโทเคนต่อการเชื่อมต่อ: [ความจุ, เติมต่อวินาที]
const BUCKETS = { all: [400, 80], chat: [6, 1], status: [20, 5], poke: [6, 0.5], join: [15, 2], signal: [350, 70] };
const KICK_AFTER_DROPS = 600; // โดนตัดข้อความเกินนี้ภายใน 10 วิ = ตัดการเชื่อมต่อ

const log = (...a) => console.log(new Date().toISOString(), ...a);
process.on('uncaughtException', e => log('[uncaughtException]', e && e.stack || e));
process.on('unhandledRejection', e => log('[unhandledRejection]', e && e.stack || e));

// ---------- ตรวจ/ทำความสะอาดข้อมูล ----------
// ตัดอักขระควบคุม, ตัวกลับทิศ (bidi), ตัวอักษรล่องหน ออก แล้วยุบช่องว่าง
const CTRL = /[\u0000-\u001F\u007F-\u009F­؜ᅟᅠ឴឵᠋-᠏​-‏‪-‮⁠-⁯ㅤ︀-️﻿ﾠ￰-￻]/g;
function cleanText(v, max, keepNewlines = false) {
  if (typeof v !== 'string') return '';
  let s = v.normalize('NFC');
  s = keepNewlines ? s.replace(/\r\n?/g, '\n').replace(/[^\S\n]+/g, ' ') : s.replace(/\s+/g, ' ');
  s = s.replace(CTRL, m => (keepNewlines && m === '\n' ? m : ''));
  // ตัดตามจำนวนตัวอักษรจริง (ไม่ตัดกลาง emoji)
  return [...s.trim()].slice(0, max).join('').trim();
}
const isId = v => Number.isSafeInteger(v) && v > 0;
const isObj = v => v !== null && typeof v === 'object' && !Array.isArray(v);

function getIp(req) {
  // Render อยู่หลัง proxy -> IP จริงอยู่ใน x-forwarded-for ตัวแรก
  const xf = String(req.headers['x-forwarded-for'] || '').split(',')[0].trim();
  return (xf || req.socket.remoteAddress || '?').slice(0, 64);
}
function safeEqual(a, b) {
  const x = crypto.createHash('sha256').update(String(a)).digest();
  const y = crypto.createHash('sha256').update(String(b)).digest();
  return crypto.timingSafeEqual(x, y);
}

// ---------- เซิร์ฟเวอร์ ----------
const httpServer = http.createServer((req, res) => {
  // HTTP ธรรมดาไว้ให้บริการโฮสติ้งตรวจว่าเซิร์ฟเวอร์ยังทำงานอยู่ (health check)
  res.writeHead(200, { 'content-type': 'text/plain', 'x-content-type-options': 'nosniff' });
  res.end('ok');
});
httpServer.on('error', e => log('[http error]', e.message));
httpServer.on('clientError', (_e, sock) => { try { sock.destroy(); } catch {} });

const wss = new WebSocketServer({ server: httpServer, maxPayload: LIMIT.maxPayload, perMessageDeflate: false, clientTracking: false });
wss.on('error', e => log('[wss error]', e.message));
httpServer.listen(PORT);

const peers = new Map();   // id -> peer
const ipCount = new Map(); // ip -> จำนวนการเชื่อมต่อ
const authFails = new Map(); // ip -> [เวลาที่ใส่รหัสผิด]
let nextId = 1, conns = 0;

const send = (ws, obj) => { if (ws && ws.readyState === 1) { try { ws.send(JSON.stringify(obj)); } catch (e) { log('[send]', e.message); } } };

function roomMembers(room) {
  const out = [];
  for (const p of peers.values()) if (p.room === room && p.welcomed) out.push(p);
  return out;
}

// รวมการส่ง state หลายครั้งที่เกิดติดกันให้เหลือครั้งเดียว (ลดภาระเมื่อมีคนกดรัวๆ)
let stateTimer = null;
function broadcastState() {
  if (stateTimer) return;
  stateTimer = setTimeout(() => {
    stateTimer = null;
    const rooms = ROOMS.map(name => ({
      name,
      users: roomMembers(name).map(p => ({ id: p.id, name: p.name, mic: p.mic, spk: p.spk, sharing: !!p.sharing })),
    }));
    const msg = JSON.stringify({ type: 'state', rooms });
    for (const p of peers.values()) if (p.welcomed && p.ws && p.ws.readyState === 1) { try { p.ws.send(msg); } catch {} }
  }, 30);
}

function leaveRoom(p) {
  if (!p.room) return;
  const old = p.room;
  p.room = null;
  p.sharing = false; // ออกจากห้อง = หยุดแชร์จอ
  for (const m of roomMembers(old)) send(m.ws, { type: 'peer-left', id: p.id });
}

function removePeer(p) {
  clearTimeout(p.graceTimer);
  if (!peers.has(p.id)) return;
  leaveRoom(p);
  peers.delete(p.id);
  broadcastState();
}

// ชื่อซ้ำ -> เติม (2), (3) ... ให้แยกออก
function uniqueName(base, self) {
  const taken = new Set();
  for (const p of peers.values()) if (p !== self && p.welcomed) taken.add(p.name.toLocaleLowerCase());
  if (!taken.has(base.toLocaleLowerCase())) return base;
  for (let i = 2; i < 1000; i++) {
    const suf = ` (${i})`;
    const n = [...base].slice(0, LIMIT.nameLen - suf.length).join('') + suf;
    if (!taken.has(n.toLocaleLowerCase())) return n;
  }
  return base + ' #' + self.id;
}

function take(p, kind) {
  const [cap, rate] = BUCKETS[kind];
  const b = p.buckets[kind] || (p.buckets[kind] = { t: cap, at: Date.now() });
  const now = Date.now();
  b.t = Math.min(cap, b.t + (now - b.at) / 1000 * rate);
  b.at = now;
  if (b.t < 1) return false;
  b.t -= 1;
  return true;
}

function bindSocket(p, ws) {
  p.ws = ws;
  ws.peer = p;
}

wss.on('connection', (ws, req) => {
  const ip = getIp(req);
  if (conns >= LIMIT.total || (ipCount.get(ip) || 0) >= LIMIT.perIp) {
    try { ws.close(1013, 'too many connections'); } catch {}
    return;
  }
  conns++;
  ipCount.set(ip, (ipCount.get(ip) || 0) + 1);

  let p = {
    id: nextId++, name: 'ไม่ระบุชื่อ', room: null, ws, alive: true, mic: false, spk: false, dnd: false, sharing: false,
    welcomed: false, token: crypto.randomBytes(16).toString('hex'), buckets: {}, drops: 0, dropsAt: Date.now(), pokes: [], ip,
  };
  ws.peer = p;
  ws.isAlive = true;
  const helloTimer = setTimeout(() => { if (!ws.peer.welcomed) try { ws.close(1008, 'no hello'); } catch {} }, LIMIT.helloMs);

  ws.on('error', e => log('[ws error]', ip, e.message)); // สำคัญ: ไม่มีตัวนี้ เฟรมเสีย/ข้อความใหญ่เกินจะทำเซิร์ฟเวอร์ล่ม
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw, isBinary) => {
    const p = ws.peer;
    if (!p || p.ws !== ws) return;
    if (isBinary) return;
    if (!take(p, 'all')) return dropped(p, ws);
    let m;
    try { m = JSON.parse(raw.toString('utf8')); } catch { return; }
    if (!isObj(m) || typeof m.type !== 'string') return;
    try { handle(p, ws, m); } catch (e) { log('[handle]', m.type, e && e.stack || e); }
  });

  ws.on('close', () => {
    clearTimeout(helloTimer);
    conns--;
    const n = (ipCount.get(ip) || 1) - 1;
    n > 0 ? ipCount.set(ip, n) : ipCount.delete(ip);
    const p = ws.peer;
    if (!p || p.ws !== ws) return; // ถูกแทนที่ด้วยการเชื่อมต่อใหม่แล้ว (resume)
    p.ws = null;
    if (!p.welcomed) { peers.delete(p.id); return; }
    // เก็บที่ไว้ให้สักพัก เผื่อเน็ตกระตุกแล้วกลับมา (เสียง P2P ยังคุยกันต่อได้ระหว่างนี้)
    if (p.canResume) {
      p.graceTimer = setTimeout(() => removePeer(p), LIMIT.resumeMs);
    } else removePeer(p);
  });
});

function dropped(p, ws) {
  const now = Date.now();
  if (now - p.dropsAt > 10000) { p.drops = 0; p.dropsAt = now; }
  if (++p.drops > KICK_AFTER_DROPS && !ws.kicked) { ws.kicked = true; log('[flood] kick', p.ip, p.id); try { ws.close(1008, 'flood'); } catch {} }
}

function handle(p, ws, m) {
  if (m.type === 'ping') return send(ws, { type: 'pong', t: Number(m.t) || 0 });

  if (m.type === 'hello') {
    if (p.welcomed) return;
    if (PASSWORD) {
      const fails = (authFails.get(p.ip) || []).filter(t => Date.now() - t < 60000);
      if (fails.length >= 6) { send(ws, { type: 'error', code: 'auth-wait', msg: 'ใส่รหัสผิดหลายครั้ง รอ 1 นาทีแล้วลองใหม่' }); return ws.close(4003, 'auth'); }
      if (typeof m.pass !== 'string' || !safeEqual(m.pass, PASSWORD)) {
        fails.push(Date.now()); authFails.set(p.ip, fails);
        send(ws, { type: 'error', code: 'auth', msg: 'รหัสผ่านเซิร์ฟเวอร์ไม่ถูกต้อง' });
        return ws.close(4001, 'auth');
      }
    }
    // กลับมาหลังหลุด -> ได้ id/ห้องเดิม
    if (typeof m.resume === 'string' && m.resume.length === 32) {
      for (const old of peers.values()) {
        if (old.welcomed && old.token && old.token.length === m.resume.length && safeEqual(old.token, m.resume)) {
          clearTimeout(old.graceTimer);
          const prevWs = old.ws;
          peers.delete(p.id);
          bindSocket(old, ws);
          old.buckets = p.buckets;
          old.ip = p.ip;
          if (prevWs && prevWs !== ws) { prevWs.peer = null; try { prevWs.terminate(); } catch {} }
          send(ws, { type: 'welcome', id: old.id, name: old.name, token: old.token, resumed: true, room: old.room, features: FEATURES });
          broadcastState();
          return;
        }
      }
    }
    const base = cleanText(m.name, LIMIT.nameLen) || 'ไม่ระบุชื่อ';
    p.name = uniqueName(base, p);
    p.welcomed = true;
    p.canResume = m.resumable === true;
    peers.set(p.id, p);
    send(ws, { type: 'welcome', id: p.id, name: p.name, token: p.canResume ? p.token : undefined, features: FEATURES });
    broadcastState();
    return;
  }

  if (!p.welcomed) return; // ต้อง hello ก่อน

  switch (m.type) {
    case 'bye': // ออกเอง (กดออก/ปิดแอป) -> ไม่ต้องเก็บที่ไว้รอ
      p.canResume = false;
      removePeer(p);
      p.ws = null;
      ws.peer = null;
      try { ws.close(1000, 'bye'); } catch {}
      break;

    case 'status': { // สถานะปิดไมค์ / ปิดลำโพง ไว้แสดงไอคอนให้คนอื่นเห็น
      if (!take(p, 'status')) return;
      const before = `${p.mic}|${p.spk}|${p.sharing}`;
      p.mic = m.mic === true;
      p.spk = m.spk === true;
      // client เก่าไม่ส่งฟิลด์นี้มา ก็ไม่แตะค่าเดิม (เข้ากันได้ย้อนหลัง)
      if (typeof m.dnd === 'boolean') p.dnd = m.dnd; // ห้ามรบกวน: ไม่ส่ง Poke ให้คนนี้
      if (typeof m.sharing === 'boolean') p.sharing = m.sharing && !!p.room;
      if (before !== `${p.mic}|${p.spk}|${p.sharing}`) broadcastState();
      break;
    }

    case 'join': {
      if (typeof m.room !== 'string' || !ROOMS.includes(m.room)) return;
      if (!take(p, 'join')) return send(ws, { type: 'notice', code: 'join-rate', msg: 'ย้ายห้องเร็วเกินไป รอสักครู่แล้วลองใหม่' });
      leaveRoom(p);
      const existing = roomMembers(m.room).map(x => x.id);
      p.room = m.room;
      // คนใหม่เป็นฝ่ายส่ง offer ไปหาคนที่อยู่ในห้องแล้ว
      send(ws, { type: 'joined', room: m.room, peers: existing });
      broadcastState();
      break;
    }

    case 'leave':
      if (!take(p, 'join')) return;
      leaveRoom(p);
      broadcastState();
      break;

    case 'signal': { // ส่งต่อข้อความ (sdp / candidate / watch / live ...) ให้คนในห้องเดียวกันเท่านั้น
      if (!take(p, 'signal')) return dropped(p, ws);
      const to = Number(m.to);
      if (!isId(to) || to === p.id || !isObj(m.data)) return;
      const target = peers.get(to);
      if (target && p.room && target.room === p.room) send(target.ws, { type: 'signal', from: p.id, data: m.data });
      break;
    }

    case 'poke': { // สะกิดคนอื่น (ข้ามห้องได้ ขอแค่ออนไลน์ในเซิร์ฟเวอร์เดียวกัน)
      if (!take(p, 'poke')) return send(ws, { type: 'poke-result', to: Number(m.to) || 0, ok: false, why: 'spam' });
      const to = Number(m.to);
      const target = isId(to) ? peers.get(to) : null;
      if (!target || target === p || !target.welcomed) return;
      const now = Date.now();
      p.pokes = p.pokes.filter(x => now - x.t < 10000);
      const lastToTarget = p.pokes.filter(x => x.to === target.id).pop();
      if (p.pokes.length >= 5 || (lastToTarget && now - lastToTarget.t < 3000)) {
        return send(ws, { type: 'poke-result', to: target.id, ok: false, why: 'spam' }); // ถี่เกินไป
      }
      if (target.dnd) return send(ws, { type: 'poke-result', to: target.id, ok: false, why: 'dnd', name: target.name });
      p.pokes.push({ t: now, to: target.id });
      send(target.ws, { type: 'poke', from: p.id, fromName: p.name, reason: cleanText(m.reason, LIMIT.reasonLen) });
      send(ws, { type: 'poke-result', to: target.id, ok: true, name: target.name });
      break;
    }

    case 'chat': {
      if (!p.room) return;
      if (!take(p, 'chat')) return send(ws, { type: 'notice', code: 'chat-rate', msg: 'พิมพ์เร็วเกินไป รอสักครู่' });
      const text = cleanText(m.text, LIMIT.chatLen);
      if (!text) return;
      for (const x of roomMembers(p.room)) send(x.ws, { type: 'chat', from: p.name, fromId: p.id, text });
      break;
    }
  }
}
const FEATURES = ['ping', 'resume', 'notice', 'auth'];

// ตัดคนที่หลุดไปแล้วโดยไม่ได้ปิดการเชื่อมต่อ (ping ทุก 15 วิ ไม่ตอบ = ตัด)
setInterval(() => {
  for (const p of peers.values()) {
    const ws = p.ws;
    if (!ws) continue;
    if (!ws.isAlive) { try { ws.terminate(); } catch {} continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch {}
  }
  const now = Date.now();
  for (const [ip, arr] of authFails) if (!arr.some(t => now - t < 60000)) authFails.delete(ip);
}, 15000).unref();

log(`เซิร์ฟเวอร์พร้อมที่พอร์ต ${PORT} | ห้อง: ${ROOMS.join(', ')} | รหัสผ่าน: ${PASSWORD ? 'เปิดใช้' : 'ไม่ใช้'}`);
