'use strict';
/**
 * Chicken Gun — Master Server (список лобби + WebRTC signaling).
 *
 * Одна маленькая Node.js-программа, без базы данных: все лобби живут в памяти.
 * Разворачивается бесплатно на Render.com / Railway.app (см. README.md).
 *
 *  HTTP (для HTTPRequest в Godot):
 *    GET /             — состояние сервера
 *    GET /health       — для пингов «не спи» (UptimeRobot / cron-job.org)
 *    GET /lobbies      — JSON-список всех живых лобби (?map=…&mode=…&q=…)
 *    GET /lobbies/:id  — одно лобби
 *
 *  WebSocket /ws (для WebSocketPeer в Godot) — JSON-сообщения:
 *    → {type:"host", name, map, mode, max_players, host_name, version}
 *    ← {type:"hosted", lobby_id, peer_id:1, lobby}
 *    → {type:"update", players, name?, map?, mode?}            (хост обновляет лобби)
 *    → {type:"join", lobby_id, player_name, version}
 *    ← {type:"joined", lobby_id, peer_id, lobby}                (игроку)
 *    ← {type:"peer_joined", peer_id, player_name}                (хосту)
 *    → {type:"offer"|"answer", to, sdp}       ←→ ретранслируется как {…, from}
 *    → {type:"candidate", to, media, index, name} ←→ ретранслируется как {…, from}
 *    → {type:"ping"}  ← {type:"pong"}                            (heartbeat)
 *    → {type:"leave"}
 *    ← {type:"peer_left", peer_id}                               (хосту)
 *    ← {type:"lobby_closed"}                                     (всем игрокам лобби)
 *    ← {type:"error", code, message}
 *
 *  Лобби удаляется автоматически, когда сокет хоста закрылся или хост
 *  перестал отвечать на ping дольше LOBBY_TIMEOUT_MS.
 */

const http = require('http');
const { URL } = require('url');
const crypto = require('crypto');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number(process.env.PORT) || 8080;
const LOBBY_TIMEOUT_MS = 45_000;   // хост молчит дольше — лобби удаляется
const SWEEP_INTERVAL_MS = 10_000;  // как часто проверяем «живость»
const MAX_LOBBIES = 1000;
const MAX_STR = 40;                // максимальная длина имён/строк
const MAX_MSG_BYTES = 64 * 1024;   // SDP бывает большим, но не настолько
const MAX_PLAYERS_LIMIT = 32;

/** @type {Map<string, Lobby>} */
const lobbies = new Map();

/**
 * @typedef {Object} Lobby
 * @property {string} id
 * @property {string} name
 * @property {string} map
 * @property {string} mode
 * @property {number} max_players
 * @property {number} players
 * @property {string} host_name
 * @property {string} version
 * @property {number} created_at
 * @property {number} last_seen
 * @property {WebSocket} host
 * @property {Map<number, WebSocket>} peers
 */

// ----------------------------------------------------------------- утилиты

const ID_ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // без 0/O и 1/I
function newLobbyId() {
  for (;;) {
    let id = '';
    const bytes = crypto.randomBytes(6);
    for (let i = 0; i < 6; i++) id += ID_ALPHABET[bytes[i] % ID_ALPHABET.length];
    if (!lobbies.has(id)) return id;
  }
}

/** Уникальный id пира Godot: положительный int32 >= 2 (1 — всегда хост). */
function newPeerId(lobby) {
  for (;;) {
    const id = (crypto.randomBytes(4).readUInt32BE(0) % 0x7ffffffd) + 2;
    if (!lobby.peers.has(id)) return id;
  }
}

function clampStr(v, fallback = '') {
  if (typeof v !== 'string') return fallback;
  const s = v.replace(/[\u0000-\u001f]/g, '').trim();
  return (s.length ? s : fallback).slice(0, MAX_STR);
}

function clampInt(v, min, max, fallback) {
  const n = Number(v);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(max, Math.max(min, Math.floor(n)));
}

function publicLobby(l) {
  return {
    id: l.id,
    name: l.name,
    map: l.map,
    mode: l.mode,
    players: l.players,
    max_players: l.max_players,
    host_name: l.host_name,
    version: l.version,
    age_sec: Math.floor((Date.now() - l.created_at) / 1000),
  };
}

function send(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    try { ws.send(JSON.stringify(obj)); } catch (_) { /* ignore */ }
  }
}

function sendError(ws, code, message) {
  send(ws, { type: 'error', code, message });
}

function log(...args) {
  console.log(new Date().toISOString(), ...args);
}

// ----------------------------------------------------------------- HTTP

function json(res, status, body) {
  const data = JSON.stringify(body);
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Length': Buffer.byteLength(data),
    'Cache-Control': 'no-store',
    'Access-Control-Allow-Origin': '*',           // для Web-экспорта Godot
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
  });
  res.end(data);
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://localhost');
  const path = url.pathname.replace(/\/+$/, '') || '/';

  if (req.method === 'OPTIONS') return json(res, 204, {});
  if (req.method !== 'GET') return json(res, 405, { error: 'method_not_allowed' });

  if (path === '/' || path === '/health') {
    return json(res, 200, {
      ok: true,
      service: 'chicken-gun-master',
      lobbies: lobbies.size,
      clients: wss.clients.size,
      uptime_sec: Math.floor(process.uptime()),
      time: Date.now(),
    });
  }

  if (path === '/lobbies') {
    const map = url.searchParams.get('map');
    const mode = url.searchParams.get('mode');
    const q = (url.searchParams.get('q') || '').toLowerCase();
    const list = [];
    for (const l of lobbies.values()) {
      if (map && l.map !== map) continue;
      if (mode && l.mode !== mode) continue;
      if (q && !l.name.toLowerCase().includes(q) && !l.id.toLowerCase().includes(q)) continue;
      list.push(publicLobby(l));
    }
    list.sort((a, b) => b.players - a.players || a.age_sec - b.age_sec);
    return json(res, 200, { lobbies: list, count: list.length, time: Date.now() });
  }

  const m = path.match(/^\/lobbies\/([A-Za-z0-9]{4,12})$/);
  if (m) {
    const l = lobbies.get(m[1].toUpperCase());
    if (!l) return json(res, 404, { error: 'lobby_not_found' });
    return json(res, 200, { lobby: publicLobby(l) });
  }

  return json(res, 404, { error: 'not_found' });
});

// ----------------------------------------------------------------- WebSocket

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: MAX_MSG_BYTES });

wss.on('connection', (ws, req) => {
  ws.isAlive = true;
  ws.role = null;       // 'host' | 'peer'
  ws.lobbyId = null;
  ws.peerId = 0;
  ws.ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').toString().split(',')[0].trim();

  ws.on('pong', () => { ws.isAlive = true; touch(ws); });

  ws.on('message', (data, isBinary) => {
    if (isBinary) return;
    let msg;
    try { msg = JSON.parse(data.toString()); } catch (_) { return sendError(ws, 'bad_json', 'Invalid JSON'); }
    if (!msg || typeof msg.type !== 'string') return sendError(ws, 'bad_message', 'Missing type');
    touch(ws);
    try {
      handleMessage(ws, msg);
    } catch (e) {
      log('handler error', e);
      sendError(ws, 'internal', 'Internal error');
    }
  });

  ws.on('close', () => handleClose(ws));
  ws.on('error', () => { /* close последует */ });
});

function touch(ws) {
  if (ws.role === 'host') {
    const l = lobbies.get(ws.lobbyId);
    if (l) l.last_seen = Date.now();
  }
}

function handleMessage(ws, msg) {
  switch (msg.type) {
    case 'ping':
      return send(ws, { type: 'pong', t: Date.now() });

    case 'host':
      return handleHost(ws, msg);

    case 'update': {
      if (ws.role !== 'host') return sendError(ws, 'not_host', 'Only the host can update a lobby');
      const l = lobbies.get(ws.lobbyId);
      if (!l) return sendError(ws, 'lobby_not_found', 'Lobby not found');
      if (msg.players !== undefined) l.players = clampInt(msg.players, 0, MAX_PLAYERS_LIMIT, l.players);
      if (msg.name !== undefined) l.name = clampStr(msg.name, l.name);
      if (msg.map !== undefined) l.map = clampStr(msg.map, l.map);
      if (msg.mode !== undefined) l.mode = clampStr(msg.mode, l.mode);
      if (msg.max_players !== undefined) l.max_players = clampInt(msg.max_players, 2, MAX_PLAYERS_LIMIT, l.max_players);
      return send(ws, { type: 'updated', lobby: publicLobby(l) });
    }

    case 'join':
      return handleJoin(ws, msg);

    case 'offer':
    case 'answer':
    case 'candidate':
      return relay(ws, msg);

    case 'leave':
      try { ws.close(1000, 'leave'); } catch (_) { /* ignore */ }
      return;

    default:
      return sendError(ws, 'unknown_type', `Unknown message type: ${msg.type}`);
  }
}

function handleHost(ws, msg) {
  if (ws.role) return sendError(ws, 'already_in_lobby', 'This connection is already in a lobby');
  if (lobbies.size >= MAX_LOBBIES) return sendError(ws, 'server_full', 'Too many lobbies');

  const lobby = {
    id: newLobbyId(),
    name: clampStr(msg.name, 'Chicken Gun'),
    map: clampStr(msg.map, 'chalohouse'),
    mode: clampStr(msg.mode, 'Deathmatch'),
    max_players: clampInt(msg.max_players, 2, MAX_PLAYERS_LIMIT, 10),
    players: 1,
    host_name: clampStr(msg.host_name, 'Host'),
    version: clampStr(msg.version, ''),
    created_at: Date.now(),
    last_seen: Date.now(),
    host: ws,
    peers: new Map(),
  };
  lobbies.set(lobby.id, lobby);
  ws.role = 'host';
  ws.lobbyId = lobby.id;
  ws.peerId = 1;
  log(`lobby ${lobby.id} created by "${lobby.host_name}" (${ws.ip}) "${lobby.name}" map=${lobby.map}`);
  send(ws, { type: 'hosted', lobby_id: lobby.id, peer_id: 1, lobby: publicLobby(lobby) });
}

function handleJoin(ws, msg) {
  if (ws.role) return sendError(ws, 'already_in_lobby', 'This connection is already in a lobby');
  const id = clampStr(msg.lobby_id).toUpperCase();
  const lobby = lobbies.get(id);
  if (!lobby) return sendError(ws, 'lobby_not_found', 'Lobby not found (maybe the host left)');
  if (lobby.host.readyState !== WebSocket.OPEN) return sendError(ws, 'host_offline', 'Host is offline');
  if (lobby.players >= lobby.max_players) return sendError(ws, 'lobby_full', 'Lobby is full');

  const peerId = newPeerId(lobby);
  const playerName = clampStr(msg.player_name, 'Player');
  lobby.peers.set(peerId, ws);
  ws.role = 'peer';
  ws.lobbyId = lobby.id;
  ws.peerId = peerId;
  ws.playerName = playerName;
  log(`peer ${peerId} "${playerName}" (${ws.ip}) joins lobby ${lobby.id}`);

  // Сначала хосту (он должен успеть создать WebRTCPeerConnection и add_peer ДО оффера),
  // потом игроку — он создаёт оффер только после этого ответа.
  send(lobby.host, { type: 'peer_joined', peer_id: peerId, player_name: playerName });
  send(ws, { type: 'joined', lobby_id: lobby.id, peer_id: peerId, lobby: publicLobby(lobby) });
}

function relay(ws, msg) {
  if (!ws.role) return sendError(ws, 'not_in_lobby', 'Join or host a lobby first');
  const lobby = lobbies.get(ws.lobbyId);
  if (!lobby) return sendError(ws, 'lobby_not_found', 'Lobby not found');
  const to = clampInt(msg.to, 1, 0x7fffffff, 0);
  let target = null;
  if (ws.role === 'host') target = lobby.peers.get(to) || null;      // хост → игрок
  else if (to === 1) target = lobby.host;                             // игрок → хост
  if (!target || target.readyState !== WebSocket.OPEN) return sendError(ws, 'peer_not_found', `Peer ${to} not found`);

  const out = { type: msg.type, from: ws.peerId };
  if (msg.type === 'candidate') {
    out.media = clampStr(msg.media, '0');
    out.index = clampInt(msg.index, 0, 64, 0);
    out.name = typeof msg.name === 'string' ? msg.name.slice(0, 1024) : '';
  } else {
    out.sdp = typeof msg.sdp === 'string' ? msg.sdp.slice(0, MAX_MSG_BYTES) : '';
  }
  send(target, out);
}

function handleClose(ws) {
  if (!ws.role || !ws.lobbyId) return;
  const lobby = lobbies.get(ws.lobbyId);
  if (!lobby) return;
  if (ws.role === 'host' && lobby.host === ws) {
    closeLobby(lobby, 'host_left');
  } else if (ws.role === 'peer') {
    lobby.peers.delete(ws.peerId);
    send(lobby.host, { type: 'peer_left', peer_id: ws.peerId });
    log(`peer ${ws.peerId} left lobby ${lobby.id}`);
  }
  ws.role = null;
  ws.lobbyId = null;
}

function closeLobby(lobby, reason) {
  lobbies.delete(lobby.id);
  for (const p of lobby.peers.values()) {
    send(p, { type: 'lobby_closed', reason });
    p.role = null;
    p.lobbyId = null;
  }
  lobby.peers.clear();
  log(`lobby ${lobby.id} closed (${reason})`);
}

// ----------------------------------------------------------------- уборка мёртвых лобби

setInterval(() => {
  const now = Date.now();
  // 1) WebSocket-пинг: кто не ответил pong с прошлого раза — рвём соединение
  for (const ws of wss.clients) {
    if (ws.isAlive === false) { ws.terminate(); continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch (_) { /* ignore */ }
  }
  // 2) лобби, чей хост давно молчит или уже отвалился
  for (const l of lobbies.values()) {
    if (l.host.readyState !== WebSocket.OPEN || now - l.last_seen > LOBBY_TIMEOUT_MS) {
      closeLobby(l, 'timeout');
      try { l.host.terminate(); } catch (_) { /* ignore */ }
    }
  }
}, SWEEP_INTERVAL_MS);

server.listen(PORT, '0.0.0.0', () => log(`Chicken Gun master server listening on :${PORT}`));

process.on('SIGTERM', () => { log('SIGTERM, shutting down'); server.close(() => process.exit(0)); });
