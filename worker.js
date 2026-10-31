// Knockback PVP backend
// Secret: ADMIN_KEY  (npx wrangler secret put ADMIN_KEY)

const clean = s => String(s || '').replace(/[^\w .\-]/g, '').trim().slice(0, 16) || 'Player';
const same = (a, b) => {
  a = String(a || ''); b = String(b || '');
  let d = a.length ^ b.length;
  for (let i = 0; i < Math.max(a.length, b.length); i++) d |= (a.charCodeAt(i) || 0) ^ (b.charCodeAt(i) || 0);
  return d === 0;
};

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === '/ws') {
      if (req.headers.get('Upgrade') !== 'websocket') return new Response('Expected websocket', { status: 426 });
      return env.HUB.get(env.HUB.idFromName('main')).fetch(req);
    }
    return new Response('Knockback PVP server is running.');
  }
};

export class Hub {
  constructor(state, env) {
    this.state = state; this.env = env;
    this.players = new Map(); this.rooms = new Map(); this.queue = [];
    this.nextId = 1; this.bans = null;
  }

  async fetch(req) {
    if (!this.bans) this.bans = (await this.state.storage.get('bans')) || {};
    const [client, server] = Object.values(new WebSocketPair());
    server.accept();
    const p = { id: String(this.nextId++), ws: server, ip: req.headers.get('CF-Connecting-IP') || 'unknown', device: '', name: 'Player', admin: false, ready: false, room: null };
    server.addEventListener('message', e => this.onMsg(p, e.data).catch(() => {}));
    server.addEventListener('close', () => this.drop(p));
    server.addEventListener('error', () => this.drop(p));
    return new Response(null, { status: 101, webSocket: client });
  }

  send(p, o) { try { p.ws.send(JSON.stringify(o)); } catch (e) {} }
  opp(p) { const r = p.room; return r ? (r.a === p ? r.b : r.a) : null; }

  banOf(p) {
    for (const k of ['d:' + p.device, 'i:' + p.ip]) {
      const b = this.bans[k];
      if (!b) continue;
      if (b.until && b.until < Date.now()) { delete this.bans[k]; continue; }
      return b;
    }
    return null;
  }

  async onMsg(p, raw) {
    if (typeof raw !== 'string' || raw.length > 4000) return;
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (m.t === 'relay') { const o = this.opp(p); if (o) this.send(o, { t: 'relay', d: m.d }); return; }

    if (m.t === 'hello') {
      p.device = String(m.device || '').slice(0, 64); p.name = clean(m.name);
      const ban = this.banOf(p);
      if (ban) { this.send(p, { t: 'banned', until: ban.until, reason: ban.reason }); p.ws.close(1008, 'banned'); return; }
      p.admin = !!this.env.ADMIN_KEY && same(m.key, this.env.ADMIN_KEY);
      p.ready = true; this.players.set(p.id, p);
      return this.send(p, { t: 'ready', admin: p.admin });
    }
    if (!p.ready) return;

    switch (m.t) {
      case 'name': p.name = clean(m.name); break;
      case 'auth': p.admin = !!this.env.ADMIN_KEY && same(m.key, this.env.ADMIN_KEY); this.send(p, { t: 'authed', admin: p.admin }); break;
      case 'host': {
        this.leave(p);
        let code; do { code = Math.random().toString(36).slice(2, 6).toUpperCase(); } while (this.rooms.has(code) || code.length < 4);
        const room = { code, map: [0, 1, 2].includes(m.map) ? m.map : 0, a: p, b: null };
        this.rooms.set(code, room); p.room = room; this.send(p, { t: 'hosted', code }); break;
      }
      case 'join': {
        const room = this.rooms.get(String(m.code || '').toUpperCase());
        if (!room || room.b || room.a === p) return this.send(p, { t: 'error', msg: 'Room not found or already full.' });
        this.leave(p); room.b = p; p.room = room; this.start(room); break;
      }
      case 'queue': {
        this.leave(p);
        const other = this.queue.shift();
        if (other) {
          const code = Math.random().toString(36).slice(2, 6).toUpperCase();
          const room = { code, map: Math.floor(Math.random() * 3), a: other, b: p };
          this.rooms.set(code, room); other.room = room; p.room = room; this.start(room);
        } else { this.queue.push(p); this.send(p, { t: 'queued' }); }
        break;
      }
      case 'cancel': case 'leave': this.leave(p); break;
    }

    if (!p.admin) return;
    switch (m.t) {
      case 'players':
        this.send(p, { t: 'players', list: [...this.players.values()].map(q => ({ id: q.id, name: q.name, admin: q.admin, room: q.room ? q.room.code : null, me: q === p })) }); break;
      case 'kick': { const q = this.players.get(String(m.id)); if (q && !q.admin) { this.send(q, { t: 'kicked' }); q.ws.close(1000, 'kicked'); } break; }
      case 'ban': {
        const q = this.players.get(String(m.id)); if (!q || q.admin) break;
        const mins = Number(m.mins) || 0, entry = { until: mins > 0 ? Date.now() + mins * 60000 : 0, reason: String(m.reason || '').slice(0, 80) };
        if (q.device) this.bans['d:' + q.device] = entry;
        if (q.ip !== 'unknown') this.bans['i:' + q.ip] = entry;
        await this.state.storage.put('bans', this.bans);
        this.send(q, { t: 'banned', until: entry.until, reason: entry.reason }); q.ws.close(1008, 'banned'); break;
      }
      case 'unban': this.bans = {}; await this.state.storage.put('bans', this.bans); break;
      case 'fling': for (const q of this.players.values()) if (!q.admin) this.send(q, { t: 'fling' }); break;
    }
  }

  start(room) {
    const info = q => ({ name: q.name, admin: q.admin });
    this.send(room.a, { t: 'matched', code: room.code, map: room.map, role: 'a', opp: info(room.b) });
    this.send(room.b, { t: 'matched', code: room.code, map: room.map, role: 'b', opp: info(room.a) });
  }

  leave(p) {
    this.queue = this.queue.filter(q => q !== p);
    const room = p.room; if (!room) return;
    const o = room.a === p ? room.b : room.a;
    if (o) { o.room = null; this.send(o, { t: 'opp_left' }); }
    this.rooms.delete(room.code); p.room = null;
  }

  drop(p) { this.leave(p); this.players.delete(p.id); }
}
