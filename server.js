// เซิร์ฟเวอร์สำหรับจัดห้อง + ส่งสัญญาณ (signaling) ให้ WebRTC
// เสียงจริงๆ ไม่ผ่านเซิร์ฟเวอร์นี้ ส่งตรงระหว่างเพื่อนกับเพื่อน
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const ROOMS = ['ห้องรวม', 'ห้องเกม', 'ห้องพักผ่อน']; // แก้ชื่อ/จำนวนห้องได้ตรงนี้

const http = require('http');
// HTTP ธรรมดาไว้ให้บริการโฮสติ้งตรวจว่าเซิร์ฟเวอร์ยังทำงานอยู่ (health check)
const httpServer = http.createServer((_req, res) => { res.writeHead(200); res.end('ok'); });
const wss = new WebSocketServer({ server: httpServer });
httpServer.listen(PORT);
const peers = new Map(); // id -> { id, name, room, ws, alive }
let nextId = 1;

const send = (ws, obj) => ws.readyState === 1 && ws.send(JSON.stringify(obj));

function roomMembers(room) {
  return [...peers.values()].filter(p => p.room === room);
}

function broadcastState() {
  const rooms = ROOMS.map(name => ({
    name,
    users: roomMembers(name).map(p => ({ id: p.id, name: p.name, mic: p.mic, spk: p.spk, sharing: !!p.sharing })),
  }));
  for (const p of peers.values()) send(p.ws, { type: 'state', rooms });
}

function leaveRoom(p) {
  if (!p.room) return;
  const old = p.room;
  p.room = null;
  p.sharing = false; // ออกจากห้อง = หยุดแชร์จอ
  for (const m of roomMembers(old)) send(m.ws, { type: 'peer-left', id: p.id });
}

wss.on('connection', ws => {
  const p = { id: nextId++, name: 'ไม่ระบุชื่อ', room: null, ws, alive: true, mic: false, spk: false };
  peers.set(p.id, p);
  ws.on('pong', () => (p.alive = true));

  ws.on('message', raw => {
    let m;
    try { m = JSON.parse(raw); } catch { return; }

    switch (m.type) {
      case 'hello':
        p.name = String(m.name || 'ไม่ระบุชื่อ').slice(0, 24);
        send(ws, { type: 'welcome', id: p.id });
        broadcastState();
        break;

      case 'status': // สถานะปิดไมค์ / ปิดลำโพง ไว้แสดงไอคอนให้คนอื่นเห็น
        p.mic = !!m.mic;
        p.spk = !!m.spk;
        // แชร์จอ: client เก่าไม่ส่งฟิลด์นี้มา ก็ไม่แตะค่าเดิม (เข้ากันได้ย้อนหลัง)
        if (typeof m.sharing === 'boolean') p.sharing = m.sharing && !!p.room;
        broadcastState();
        break;

      case 'join': {
        if (!ROOMS.includes(m.room)) return;
        leaveRoom(p);
        const existing = roomMembers(m.room).map(x => x.id);
        p.room = m.room;
        // คนใหม่เป็นฝ่ายส่ง offer ไปหาคนที่อยู่ในห้องแล้ว
        send(ws, { type: 'joined', room: m.room, peers: existing });
        broadcastState();
        break;
      }

      case 'leave':
        leaveRoom(p);
        broadcastState();
        break;

      case 'signal': { // ส่งต่อข้อความอะไรก็ได้ (sdp / candidate / watch / unwatch) ให้คนในห้องเดียวกัน
        const target = peers.get(m.to);
        if (target && p.room && target.room === p.room) {
          send(target.ws, { type: 'signal', from: p.id, data: m.data });
        }
        break;
      }

      case 'chat':
        if (!p.room) return;
        const text = String(m.text || '').slice(0, 500);
        for (const x of roomMembers(p.room)) send(x.ws, { type: 'chat', from: p.name, text });
        break;
    }
  });

  ws.on('close', () => {
    leaveRoom(p);
    peers.delete(p.id);
    broadcastState();
  });
});

// ตัดคนที่หลุดไปแล้วโดยไม่ได้ปิดการเชื่อมต่อ
setInterval(() => {
  for (const p of peers.values()) {
    if (!p.alive) { p.ws.terminate(); continue; }
    p.alive = false;
    p.ws.ping();
  }
}, 15000);

console.log(`เซิร์ฟเวอร์พร้อมที่พอร์ต ${PORT} | ห้อง: ${ROOMS.join(', ')}`);
