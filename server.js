// ChestCounterCoop - serveur relais (Node >= 18, dependance : ws)
// Lancer : npm install && npm start   (PORT=8080 par defaut)
const http = require('http');
const fs = require('fs');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT || 8080);
const DATA_FILE = process.env.DATA_FILE || './rooms.json';
const MAX_PLAYERS = 16;
const MAX_CHESTS_PER_MATCH = 500;
const LIVE_TTL_MS = 15 * 60 * 1000; // un match en direct sans nouvelle depuis 15 min est ignore
const ROOM_TTL_MS = 30 * 24 * 3600 * 1000; // salon supprime apres 30 jours sans activite
const ROOM_RE = /^[A-Za-z0-9-]{4,32}$/;
const NAME_RE = /^[\p{L}\p{N}_ .-]{1,24}$/u;

// rooms: code -> { players: { name: { total, treasures, last, matches: [ids] } }, updated }
let rooms = {};
try { rooms = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')); } catch { rooms = {}; }

let saveTimer = null;
function scheduleSave() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    fs.writeFile(DATA_FILE, JSON.stringify(rooms), () => {});
  }, 2000);
}

function snapshot(code) {
  const r = rooms[code];
  const now = Date.now();
  const players = Object.entries(r.players)
    .map(([name, p]) => {
      // match en cours : compte tant qu'il n'est pas termine et qu'il a ete mis a jour recemment
      const live = p.live && !p.matches.includes(p.live.matchId) && now - p.live.at < LIVE_TTL_MS ? p.live : null;
      return {
        name,
        total: p.total + (live ? live.chests : 0),
        treasures: p.treasures + (live ? live.treasures : 0),
        last: p.last,
        matches: p.matches.length,
        live: live ? live.chests : 0,
        best: p.best || 0,
        avg: p.matches.length ? Math.round((p.total / p.matches.length) * 10) / 10 : 0,
      };
    })
    .sort((a, b) => b.total - a.total);
  const top = players.reduce((m, p) => (p.best > (m ? m.best : 0) ? p : m), null);
  return {
    type: 'state',
    room: code,
    total: players.reduce((s, p) => s + p.total, 0),
    treasures: players.reduce((s, p) => s + p.treasures, 0),
    record: top ? { player: top.name, chests: top.best } : null,
    players,
  };
}

const sockets = new Map(); // code -> Set<ws>
function broadcast(code) {
  const msg = JSON.stringify(snapshot(code));
  for (const ws of sockets.get(code) || []) if (ws.readyState === 1) ws.send(msg);
}

function broadcastEvent(code, event) {
  const msg = JSON.stringify({ type: 'event', ...event });
  for (const ws of sockets.get(code) || []) if (ws.readyState === 1) ws.send(msg);
}

function addMatch(code, player, matchId, chests, treasures) {
  const r = rooms[code];
  let p = r.players[player];
  if (!p) {
    if (Object.keys(r.players).length >= MAX_PLAYERS) return false;
    p = r.players[player] = { total: 0, treasures: 0, last: 0, matches: [] };
  }
  if (p.matches.includes(matchId)) return false; // deja compte : deduplication
  p.matches.push(matchId);
  if (p.live && p.live.matchId === matchId) p.live = null; // le resultat final remplace l'estimation en direct
  p.total += chests;
  p.treasures += treasures;
  p.last = chests;
  if (chests > (p.best || 0)) p.best = chests;
  r.updated = Date.now();
  return true;
}

function validMatch(m) {
  return typeof m.matchId === 'string' && m.matchId.length > 0 && m.matchId.length <= 80
    && Number.isInteger(m.chests) && m.chests >= 0 && m.chests <= MAX_CHESTS_PER_MATCH
    && (m.treasures === undefined || (Number.isInteger(m.treasures) && m.treasures >= 0 && m.treasures <= MAX_CHESTS_PER_MATCH));
}

const server = http.createServer((req, res) => {
  if (req.url === '/health') { res.end('ok'); return; }
  if (req.url === '/new') { // genere un code de salon aleatoire
    const code = crypto.randomBytes(5).toString('hex').toUpperCase();
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify({ room: code }));
    return;
  }
  res.statusCode = 404; res.end();
});

const wss = new WebSocketServer({ server, maxPayload: 64 * 1024 });

wss.on('connection', (ws) => {
  let room = null, player = null, role = null;
  let msgCount = 0;
  const rateTimer = setInterval(() => { msgCount = 0; }, 10000);
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    if (++msgCount > 200) { ws.close(1008, 'rate limit'); return; }
    let m;
    try { m = JSON.parse(raw.toString()); } catch { return; }
    if (!m || typeof m.type !== 'string') return;

    if (m.type === 'join') {
      if (!ROOM_RE.test(m.room || '')) return ws.send(JSON.stringify({ type: 'error', error: 'bad_room' }));
      role = m.role === 'viewer' ? 'viewer' : 'player';
      if (role === 'player' && !NAME_RE.test(m.player || '')) return ws.send(JSON.stringify({ type: 'error', error: 'bad_name' }));
      room = m.room.toUpperCase();
      player = role === 'player' ? m.player.trim() : null;
      if (!rooms[room]) rooms[room] = { players: {}, updated: Date.now() };
      if (!sockets.has(room)) sockets.set(room, new Set());
      sockets.get(room).add(ws);
      ws.send(JSON.stringify(snapshot(room)));
      return;
    }

    if (!room) return;
    if (role !== 'player') return;

    if (m.type === 'match' && validMatch(m)) {
      const prev = rooms[room].players[player];
      const seen = prev && prev.live && prev.live.matchId === m.matchId ? prev.live.treasures : 0; // deja annonces en direct
      const fresh = (m.treasures || 0) - seen;
      if (addMatch(room, player, m.matchId, m.chests, m.treasures || 0)) {
        scheduleSave();
        if (fresh > 0) broadcastEvent(room, { kind: 'treasure', player, count: fresh, at: Date.now() });
      }
      broadcast(room); // renvoie aussi l'etat a l'expediteur (accuse de reception)
    } else if (m.type === 'live' && validMatch(m)) {
      const r = rooms[room];
      let p = r.players[player];
      if (!p && Object.keys(r.players).length < MAX_PLAYERS) p = r.players[player] = { total: 0, treasures: 0, last: 0, matches: [] };
      if (p && !p.matches.includes(m.matchId)) {
        const before = p.live && p.live.matchId === m.matchId ? p.live.treasures : 0;
        if ((m.treasures || 0) > before) broadcastEvent(room, { kind: 'treasure', player, count: (m.treasures || 0) - before, at: Date.now() });
        p.live = { matchId: m.matchId, chests: m.chests, treasures: m.treasures || 0, at: Date.now() };
        r.updated = Date.now();
        broadcast(room); // pas de sauvegarde disque : le direct est ephemere
      }
    } else if (m.type === 'sync' && Array.isArray(m.matches) && m.matches.length <= 2000) {
      let changed = false;
      for (const x of m.matches) if (validMatch(x) && addMatch(room, player, x.matchId, x.chests, x.treasures || 0)) changed = true;
      if (changed) scheduleSave();
      broadcast(room);
    }
  });

  ws.on('close', () => {
    clearInterval(rateTimer);
    if (room && sockets.has(room)) sockets.get(room).delete(ws);
  });
});

// heartbeat + nettoyage des salons inactifs
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false; ws.ping();
  }
  const now = Date.now();
  for (const code of Object.keys(rooms)) {
    if (now - rooms[code].updated > ROOM_TTL_MS && !(sockets.get(code) && sockets.get(code).size)) { delete rooms[code]; scheduleSave(); }
  }
}, 30000);

server.listen(PORT, () => console.log(`ChestCounterCoop relais sur :${PORT}`));
