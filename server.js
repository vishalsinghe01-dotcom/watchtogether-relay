// WatchTogether relay: rooms (max 3), host = content chooser, shared playback state, chat, emoji, WebRTC signaling.
// Never touches video/audio data.
const http = require("http");
const { WebSocketServer } = require("ws");
const crypto = require("crypto");

const MAX_PER_ROOM = 3;
const PORT = process.env.PORT || 8080;
const rooms = new Map(); // code -> { members: Map(id -> {ws,name}), host, open }

const httpServer = http.createServer((req, res) => { res.writeHead(200); res.end("WatchTogether relay OK"); });
const wss = new WebSocketServer({ server: httpServer });
const send = (ws, o) => ws.readyState === 1 && ws.send(JSON.stringify(o));
const clean = (s, n) => String(s || "").slice(0, n);
const bcast = (r, o, except) => { for (const [pid, p] of r.members) if (pid !== except) send(p.ws, o); };

wss.on("connection", (ws) => {
  let code = null, id = null;

  ws.on("message", (raw) => {
    if (raw.length > 8192) return;
    let m; try { m = JSON.parse(raw); } catch { return; }

    if (m.t === "join" && !code) {
      const c = clean(m.room, 32).toUpperCase();
      if (!c) return send(ws, { t: "error", msg: "bad_room" });
      let r = rooms.get(c);
      if (m.create) {
        if (r) return send(ws, { t: "error", msg: "room_exists" });
        r = { members: new Map(), host: null, open: null };
        rooms.set(c, r);
      } else if (!r) return send(ws, { t: "error", msg: "no_such_room" });
      if (r.members.size >= MAX_PER_ROOM) return send(ws, { t: "error", msg: "room_full" });

      id = crypto.randomBytes(4).toString("hex");
      const name = clean(m.name, 20) || "Guest";
      const peers = [...r.members].map(([pid, p]) => ({ id: pid, name: p.name }));
      r.members.set(id, { ws, name });
      if (!r.host) r.host = id;
      code = c;
      send(ws, { t: "joined", id, host: r.host, peers, max: MAX_PER_ROOM, open: r.open });
      bcast(r, { t: "peer_joined", id, name }, id);
      return;
    }

    const r = code && rooms.get(code);
    if (!r) return;
    const me = r.members.get(id);

    if (m.t === "open") {                       // only the host decides what plays
      if (id !== r.host) return;
      r.open = {
        apiName: clean(m.apiName, 100), url: clean(m.url, 2000), name: clean(m.name, 200),
        episodeId: Number.isInteger(m.episodeId) ? m.episodeId : null,
        season: Number.isInteger(m.season) ? m.season : null,
        episode: Number.isInteger(m.episode) ? m.episode : null,
      };
      bcast(r, { t: "open", ...r.open }, id);
    } else if (["state", "chat", "emoji"].includes(m.t)) { // everyone can control playback
      const out = { t: m.t, from: id, name: me.name };
      if (m.t === "state") Object.assign(out, { playing: !!m.playing, pos: +m.pos || 0 });
      if (m.t === "chat") out.text = clean(m.text, 300);
      if (m.t === "emoji") out.e = clean(m.e, 8);
      bcast(r, out, id);
    } else if (m.t === "rtc") {
      const target = r.members.get(m.to);
      if (target) send(target.ws, { t: "rtc", from: id, kind: m.kind, data: m.data });
    }
  });

  ws.on("close", () => {
    const r = code && rooms.get(code);
    if (!r) return;
    r.members.delete(id);
    if (r.members.size === 0) return void rooms.delete(code);
    if (r.host === id) r.host = r.members.keys().next().value; // promote next person
    bcast(r, { t: "peer_left", id, host: r.host });
  });
});
httpServer.listen(PORT, () => console.log("WatchTogether relay on :" + PORT));
