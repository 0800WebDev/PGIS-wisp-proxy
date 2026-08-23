const http = require("http");
const WebSocket = require("ws");

const PRIMARY = [
  "wss://pgis-wisp.onrender.com/",
  "wss://pgis-wisp-2.onrender.com/",
  "wss://pgis-wisp-3.onrender.com/",
  "wss://pgis-wisp-4.onrender.com/"
];

const FALLBACK = [
  "wss://wisp.classroom.lat/",
  "wss://homework--spmspy0800.replit.app/wisp/",
  "wss://bare-server.fly.dev/wisp/",
  "wss://wisp.mercurywork.shop/"
];

const CHECK_INTERVAL = 30000;
const CONNECTION_TIMEOUT = 5000;

let bestServer = null;
let lastCheck = 0;
let checking = false;

function checkServer(url) {
  return new Promise(resolve => {
    const start = Date.now();
    let finished = false;

    const finish = result => {
      if (finished) return;
      finished = true;
      resolve(result);
    };

    let ws;

    try {
      ws = new WebSocket(url);
    } catch {
      finish(null);
      return;
    }

    const timeout = setTimeout(() => {
      try {
        ws.terminate();
      } catch {}

      finish(null);
    }, CONNECTION_TIMEOUT);

    ws.on("open", () => {
      clearTimeout(timeout);

      const latency = Date.now() - start;

      try {
        ws.close();
      } catch {}

      finish({
        url,
        latency
      });
    });

    ws.on("error", () => {
      clearTimeout(timeout);
      finish(null);
    });

    ws.on("close", () => {
      clearTimeout(timeout);
    });
  });
}

async function findBestServer(servers) {
  const results = await Promise.all(
    servers.map(server => checkServer(server))
  );

  const healthy = results
    .filter(Boolean)
    .sort((a, b) => a.latency - b.latency);

  return healthy[0] || null;
}

async function selectServer() {
  if (checking) return bestServer;

  checking = true;

  try {
    console.log("Checking primary Wisp servers...");

    let result = await findBestServer(PRIMARY);

    if (result) {
      console.log(
        `Selected primary: ${result.url} (${result.latency}ms)`
      );

      bestServer = result;
      return result;
    }

    console.log("All primary servers are down. Checking fallback servers...");

    result = await findBestServer(FALLBACK);

    if (result) {
      console.log(
        `Selected fallback: ${result.url} (${result.latency}ms)`
      );

      bestServer = result;
      return result;
    }

    console.log("All Wisp servers are down.");

    bestServer = null;
    return null;
  } finally {
    checking = false;
    lastCheck = Date.now();
  }
}

async function getBestServer() {
  if (!bestServer || Date.now() - lastCheck >= CHECK_INTERVAL) {
    await selectServer();
  }

  return bestServer;
}

const fs = require("fs");
const path = require("path");

const server = http.createServer((req, res) => {
  if (req.url === "/" || req.url === "/index.html") {
    const file = fs.readFileSync(
      path.join(__dirname, "index.html")
    );

    res.writeHead(200, {
      "Content-Type": "text/html; charset=utf-8"
    });

    res.end(file);
    return;
  }

  res.writeHead(404, {
    "Content-Type": "text/plain"
  });

  res.end("Not found");
});

const wss = new WebSocket.Server({
  noServer: true
});

server.on("upgrade", async (req, socket, head) => {
  const selected = await getBestServer();

  if (!selected) {
    socket.write(
      "HTTP/1.1 503 Service Unavailable\r\n" +
      "Connection: close\r\n" +
      "\r\n"
    );

    socket.destroy();
    return;
  }

  wss.handleUpgrade(req, socket, head, client => {
    proxyConnection(client, selected.url);
  });
});

function proxyConnection(client, target) {
  console.log(`Client connected -> ${target}`);

  const upstream = new WebSocket(target);

  let clientOpen = false;
  let upstreamOpen = false;
  let closed = false;

  const closeBoth = () => {
    if (closed) return;

    closed = true;

    try {
      if (client.readyState === WebSocket.OPEN) {
        client.close();
      }
    } catch {}

    try {
      if (
        upstream.readyState === WebSocket.OPEN ||
        upstream.readyState === WebSocket.CONNECTING
      ) {
        upstream.close();
      }
    } catch {}
  };

  const timeout = setTimeout(() => {
    if (!upstreamOpen) {
      console.log(`Failed to connect to ${target}`);

      try {
        upstream.terminate();
      } catch {}

      closeBoth();
    }
  }, CONNECTION_TIMEOUT);

  client.binaryType = "arraybuffer";
  upstream.binaryType = "arraybuffer";

  client.on("open", () => {
    clientOpen = true;
  });

  upstream.on("open", () => {
    clearTimeout(timeout);

    upstreamOpen = true;

    console.log(`Connected upstream -> ${target}`);
  });

  client.on("message", (data, isBinary) => {
    if (
      upstream.readyState === WebSocket.OPEN
    ) {
      upstream.send(data, {
        binary: isBinary
      });
    }
  });

  upstream.on("message", (data, isBinary) => {
    if (
      client.readyState === WebSocket.OPEN
    ) {
      client.send(data, {
        binary: isBinary
      });
    }
  });

  client.on("close", () => {
    closeBoth();
  });

  client.on("error", () => {
    closeBoth();
  });

  upstream.on("close", () => {
    closeBoth();
  });

  upstream.on("error", () => {
    closeBoth();
  });
}

server.listen(process.env.PORT || 3000, () => {
  console.log(
    `Wisp rerouter listening on port ${process.env.PORT || 3000}`
  );

  selectServer();
});

setInterval(() => {
  selectServer();
}, CHECK_INTERVAL);
