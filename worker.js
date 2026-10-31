// Knockback PVP backend — Hub (WebSockets) + Accounts (REST)
// Secrets needed:
//   npx wrangler secret put ADMIN_KEY
//   npx wrangler secret put EMAIL_FROM   (e.g. noreply@yourdomain.com)
// wrangler.toml durable_objects bindings needed:
//   HUB  → Hub
//   ACCT → AccountStore

const clean = s => String(s || '').replace(/[^\w .\-]/g, '').trim().slice(0, 16) || 'Player';
const same = (a, b) => {
  a = String(a || ''); b = String(b || '');
  let d = a.length ^ b.length;
  for (let i = 0; i < Math.max(a.length, b.length); i++) d |= (a.charCodeAt(i) || 0) ^ (b.charCodeAt(i) || 0);
  return d === 0;
};
const CORS = { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST, OPTIONS', 'Access-Control-Allow-Headers': 'Content-Type' };
const json = (o, s = 200) => new Response(JSON.stringify(o), { status: s, headers: { 'Content-Type': 'application/json', ...CORS } });

// ── Main entry ──────────────────────────────────────────────────────────────
export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (req.method === 'OPTIONS') return new Response(null, { headers: CORS });

    if (url.pathname === '/ws') {
      if (req.headers.get('Upgrade') !== 'websocket') return new Response('Expected websocket', { status: 426 });
      return env.HUB.get(env.HUB.idFromName('main')).fetch(req);
    }

    if (url.pathname.startsWith('/api/')) {
      return env.ACCT.get(env.ACCT.idFromName('main')).fetch(req);
    }

    return new Response('Knockback PVP server is running.');
  }
};

// ── Hub Durable Object (unchanged logic, kept exactly as yours) ─────────────
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

// ── AccountStore Durable Object ─────────────────────────────────────────────
export class AccountStore {
  constructor(state, env) {
    this.state = state;
    this.env = env;
  }

  async fetch(req) {
    const url = new URL(req.url);
    const path = url.pathname.replace('/api/', '');
    if (req.method !== 'POST') return json({ error: 'POST only' }, 405);
    let body;
    try { body = await req.json(); } catch { return json({ error: 'Invalid JSON' }, 400); }
    switch (path) {
      case 'signup': return this.signup(body);
      case 'login':  return this.login(body);
      case 'verify': return this.verify(body);
      case 'resend': return this.resend(body);
      case 'logout': return json({ ok: true });
      default:       return json({ error: 'Not found' }, 404);
    }
  }

  async signup({ email, username, password }) {
    if (!email || !username || !password) return json({ error: 'All fields required.' });
    email = email.toLowerCase().trim();
    username = clean(username);
    if (username.length < 3) return json({ error: 'Username must be 3–16 characters.' });
    if (password.length < 8) return json({ error: 'Password must be 8+ characters.' });
    if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(email)) return json({ error: 'Invalid email.' });

    if (await this.state.storage.get('email:' + email)) return json({ error: 'Email already registered.' });
    if (await this.state.storage.get('user:' + username.toLowerCase())) return json({ error: 'Username taken.' });

    const code = randCode(), hash = await hashPw(password);
    await this.state.storage.put('pending:' + email, JSON.stringify({ email, username, hash, code, expires: Date.now() + 600000 }));
    await this.sendCode(email, username, code);
    return json({ ok: true, needCode: true });
  }

  async login({ login, password }) {
    if (!login || !password) return json({ error: 'Email and password required.' });
    login = login.trim();

    // Try email, then username
    let email = login.toLowerCase();
    let raw = await this.state.storage.get('email:' + email);
    if (!raw) {
      const emailForUser = await this.state.storage.get('user:' + login.toLowerCase());
      if (emailForUser) { email = emailForUser; raw = await this.state.storage.get('email:' + email); }
    }
    if (!raw) return json({ error: 'Account not found.' });
    const a = JSON.parse(raw);
    if (!await checkPw(password, a.hash)) return json({ error: 'Wrong password.' });

    if (!a.verified) {
      const code = randCode();
      a.verifyCode = code; a.verifyExpires = Date.now() + 600000;
      await this.state.storage.put('email:' + email, JSON.stringify(a));
      await this.sendCode(email, a.username, code);
      return json({ needCode: true, email });
    }

    const token = randToken();
    a.tokens = (a.tokens || []).filter(t => t.expires > Date.now());
    a.tokens.push({ token, expires: Date.now() + 30 * 86400000 });
    await this.state.storage.put('email:' + email, JSON.stringify(a));
    return json({ ok: true, token, username: a.username });
  }

  async verify({ email, code }) {
    if (!email || !code) return json({ error: 'Email and code required.' });
    email = email.toLowerCase().trim();
    code = String(code).trim();

    // Pending signup
    const pendingRaw = await this.state.storage.get('pending:' + email);
    if (pendingRaw) {
      const p = JSON.parse(pendingRaw);
      if (Date.now() > p.expires) { await this.state.storage.delete('pending:' + email); return json({ error: 'Code expired. Sign up again.' }); }
      if (p.code !== code) return json({ error: 'Wrong code.' });
      const token = randToken();
      const acct = { email, username: p.username, hash: p.hash, verified: true, tokens: [{ token, expires: Date.now() + 30 * 86400000 }] };
      await this.state.storage.put('email:' + email, JSON.stringify(acct));
      await this.state.storage.put('user:' + p.username.toLowerCase(), email);
      await this.state.storage.delete('pending:' + email);
      return json({ ok: true, token, username: p.username });
    }

    // Existing account (login verify)
    const raw = await this.state.storage.get('email:' + email);
    if (!raw) return json({ error: 'Account not found.' });
    const a = JSON.parse(raw);
    if (!a.verifyCode || a.verifyCode !== code) return json({ error: 'Wrong code.' });
    if (Date.now() > (a.verifyExpires || 0)) return json({ error: 'Code expired.' });
    a.verified = true; delete a.verifyCode; delete a.verifyExpires;
    const token = randToken();
    a.tokens = (a.tokens || []).filter(t => t.expires > Date.now());
    a.tokens.push({ token, expires: Date.now() + 30 * 86400000 });
    await this.state.storage.put('email:' + email, JSON.stringify(a));
    return json({ ok: true, token, username: a.username });
  }

  async resend({ email }) {
    if (!email) return json({ error: 'Email required.' });
    email = email.toLowerCase().trim();

    const pendingRaw = await this.state.storage.get('pending:' + email);
    if (pendingRaw) {
      const p = JSON.parse(pendingRaw);
      p.code = randCode(); p.expires = Date.now() + 600000;
      await this.state.storage.put('pending:' + email, JSON.stringify(p));
      await this.sendCode(email, p.username, p.code);
      return json({ ok: true });
    }
    const raw = await this.state.storage.get('email:' + email);
    if (!raw) return json({ error: 'Account not found.' });
    const a = JSON.parse(raw);
    a.verifyCode = randCode(); a.verifyExpires = Date.now() + 600000;
    await this.state.storage.put('email:' + email, JSON.stringify(a));
    await this.sendCode(email, a.username, a.verifyCode);
    return json({ ok: true });
  }

  async sendCode(email, username, code) {
    // MailChannels — free on Cloudflare Workers, no API key needed
    // Requires SPF record: v=spf1 include:relay.mailchannels.net ~all
    try {
      await fetch('https://api.mailchannels.net/tx/v1/send', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          personalizations: [{ to: [{ email, name: username }] }],
          from: {
            email: this.env.EMAIL_FROM || 'noreply@knockbackpvp.zertztt.workers.dev',
            name: 'Knockback PVP'
          },
          subject: 'Your code: ' + code,
          content: [{
            type: 'text/html',
            value: `<div style="font-family:system-ui,sans-serif;max-width:480px;margin:auto;background:#0f172a;color:#f1f5f9;padding:32px;border-radius:16px">
              <h2 style="color:#22d3ee;margin:0 0 12px">Knockback PVP</h2>
              <p style="margin:0 0 20px">Hey <strong>${username}</strong>, here's your verification code:</p>
              <div style="font-size:2.4rem;font-weight:900;letter-spacing:.3em;color:#fbbf24;background:#1e293b;border-radius:12px;padding:20px;text-align:center;margin-bottom:20px">${code}</div>
              <p style="color:#94a3b8;font-size:.85rem;margin:0">Expires in 10 minutes. Didn't request this? Ignore it.</p>
            </div>`
          }]
        })
      });
    } catch (_) {}
  }
}

// ── Helpers ─────────────────────────────────────────────────────────────────
function randCode() {
  return String(Math.floor(100000 + Math.random() * 900000));
}

function randToken() {
  const arr = new Uint8Array(32);
  crypto.getRandomValues(arr);
  return [...arr].map(b => b.toString(16).padStart(2, '0')).join('');
}

async function hashPw(pw) {
  const enc = new TextEncoder();
  const salt = crypto.getRandomValues(new Uint8Array(16));
  const key = await crypto.subtle.importKey('raw', enc.encode(pw), 'PBKDF2', false, ['deriveBits']);
  const bits = await crypto.subtle.deriveBits({ name: 'PBKDF2', salt, iterations: 100000, hash: 'SHA-256' }, key, 256);
  return btoa(String.fromCharCode(...salt)) + '.' + btoa(String.fromCharCode(...new Uint8Array(bits)));
}

async function checkPw(pw, stored) {
  try {
    const [saltB64, hashB64] = stored.split('.');
    const salt = Uint8Array.from(atob(saltB64), c => c.charCodeAt(0));
    const key = await crypto.subtle.importKey('raw', new TextEncoder().encode(pw), 'PBKDF2', false, ['deriveBits']);
    const bits = await crypto.subtle.deriveBits({ name: 'PBKDF2', salt, iterations: 100000, hash: 'SHA-256' }, key, 256);
    return btoa(String.fromCharCode(...new Uint8Array(bits))) === hashB64;
  } catch { return false; }
}
