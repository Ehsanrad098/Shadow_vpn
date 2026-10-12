import http from "node:http";
import net from "node:net";
import { WebSocketServer } from "ws";

const PORT = parseInt(process.env.PORT, 10) || 8080;
const UUID = (process.env.UUID || "24d9c72d-1f63-4f24-9b8b-e85df649f801").trim().toLowerCase();
const PROXY_PATH = "/shadow-ws";

// سرور HTTP برای سابسکریپشن و استتار
const server = http.createServer((req, res) => {
  const host = req.headers["x-forwarded-host"] || req.headers.host || "localhost";
  const url = new URL(req.url, `http://${host}`);

  if (url.pathname === "/sub") {
    const rawLink = `vless://${UUID}@${host}:443?encryption=none&security=tls&sni=${host}&fp=chrome&alpn=http/1.1&type=ws&host=${host}&path=${encodeURIComponent(PROXY_PATH)}#Shadow-Node`;
    const b64 = Buffer.from(rawLink).toString("base64");
    
    res.writeHead(200, {
      "Content-Type": "text/plain; charset=utf-8",
      "Cache-Control": "no-store, no-cache, must-revalidate",
      "Access-Control-Allow-Origin": "*",
      "Profile-Update-Interval": "24",
      "Subscription-Userinfo": "upload=0; download=0; total=107374182400; expire=0"
    });
    res.end(b64);
    return;
  }

  // صفحه استتار عمومی
  res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
  res.end(`<!DOCTYPE html><html><head><title>Edge Gateway</title></head><body style="font-family:sans-serif;text-align:center;padding:50px;"><h2>Edge Routing Active</h2><p>Node operational.</p></body></html>`);
});

const wss = new WebSocketServer({ server, path: PROXY_PATH });

wss.on("connection", (ws) => {
  let remoteSocket = null;
  let isHandshaked = false;
  let isConnected = false;
  let headerBuffer = Buffer.alloc(0);
  let pendingWriteQueue = [];
  let isClosed = false;

  const teardown = () => {
    if (isClosed) return;
    isClosed = true;

    pendingWriteQueue = [];

    if (remoteSocket) {
      remoteSocket.removeAllListeners();
      remoteSocket.destroy();
      remoteSocket = null;
    }

    if (ws.readyState === ws.OPEN || ws.readyState === ws.CONNECTING) {
      try {
        ws.close(1000);
      } catch (_) {}
    }
  };

  ws.on("message", (msg) => {
    if (isClosed) return;
    const chunk = Buffer.isBuffer(msg) ? msg : Buffer.from(msg);

    if (!isHandshaked) {
      headerBuffer = Buffer.concat([headerBuffer, chunk]);

      if (headerBuffer.length < 24) return;

      const version = headerBuffer[0];
      const uuidBytes = headerBuffer.subarray(1, 17);
      
      let clientUuid = "";
      for (let i = 0; i < 16; i++) {
        clientUuid += (uuidBytes[i] < 16 ? "0" : "") + uuidBytes[i].toString(16);
        if ([3, 5, 7, 9].includes(i)) clientUuid += "-";
      }

      if (clientUuid.toLowerCase() !== UUID) {
        teardown();
        return;
      }

      const optLen = headerBuffer[17];
      let cursor = 18 + optLen;

      if (headerBuffer.length < cursor + 4) return;

      const command = headerBuffer[cursor++];
      if (command !== 1) { // فقط ترافیک استریم TCP
        teardown();
        return;
      }

      const port = headerBuffer.readUInt16BE(cursor);
      cursor += 2;
      const addrType = headerBuffer[cursor++];
      let host = "";

      if (addrType === 1) { // IPv4
        if (headerBuffer.length < cursor + 4) return;
        host = headerBuffer.subarray(cursor, cursor + 4).join(".");
        cursor += 4;
      } else if (addrType === 2) { // Domain
        if (headerBuffer.length < cursor + 1) return;
        const len = headerBuffer[cursor++];
        if (headerBuffer.length < cursor + len) return;
        host = headerBuffer.subarray(cursor, cursor + len).toString("utf-8");
        cursor += len;
      } else if (addrType === 3) { // IPv6
        if (headerBuffer.length < cursor + 16) return;
        const parts = [];
        for (let i = 0; i < 16; i += 2) {
          parts.push(headerBuffer.readUInt16BE(cursor + i).toString(16));
        }
        host = parts.join(":");
        cursor += 16;
      } else {
        teardown();
        return;
      }

      isHandshaked = true;

      // پاسخ استاندارد VLESS
      try {
        ws.send(Buffer.from([version, 0]));
      } catch (_) {
        teardown();
        return;
      }

      const remainingData = headerBuffer.subarray(cursor);
      if (remainingData.length > 0) {
        pendingWriteQueue.push(remainingData);
      }
      headerBuffer = Buffer.alloc(0);

      // اتصال مستقیم TCP به مقصد
      remoteSocket = net.createConnection({ host, port });
      remoteSocket.setNoDelay(true);

      remoteSocket.on("connect", () => {
        if (isClosed) {
          remoteSocket.destroy();
          return;
        }
        isConnected = true;

        while (pendingWriteQueue.length > 0) {
          const bufferedChunk = pendingWriteQueue.shift();
          const canContinue = remoteSocket.write(bufferedChunk);
          if (!canContinue) break;
        }
      });

      remoteSocket.on("drain", () => {
        if (isClosed || !isConnected) return;
        while (pendingWriteQueue.length > 0) {
          const bufferedChunk = pendingWriteQueue.shift();
          const canContinue = remoteSocket.write(bufferedChunk);
          if (!canContinue) return;
        }
        if (ws.readyState === ws.OPEN) {
          try { ws.resume(); } catch (_) {}
        }
      });

      remoteSocket.on("data", (data) => {
        if (isClosed) return;
        if (ws.readyState === ws.OPEN) {
          if (ws.bufferedAmount > 2 * 1024 * 1024) {
            remoteSocket.pause();
            ws.send(data, () => {
              if (remoteSocket && !remoteSocket.destroyed) {
                remoteSocket.resume();
              }
            });
          } else {
            ws.send(data);
          }
        }
      });

      remoteSocket.on("error", teardown);
      remoteSocket.on("close", teardown);
      remoteSocket.on("end", teardown);
    } else {
      if (!isConnected) {
        pendingWriteQueue.push(chunk);
        if (pendingWriteQueue.length > 200) {
          teardown();
        }
      } else if (remoteSocket && !remoteSocket.destroyed) {
        const canContinue = remoteSocket.write(chunk);
        if (!canContinue && ws.readyState === ws.OPEN) {
          try { ws.pause(); } catch (_) {}
        }
      }
    }
  });

  ws.on("close", teardown);
  ws.on("error", teardown);
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Shadow Gateway operational on 0.0.0.0:${PORT}`);
});
