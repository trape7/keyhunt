import express from "express";
import { createServer as createViteServer } from "vite";
import fs from "fs";
import path from "path";
import { Worker } from "worker_threads";
import Database from "better-sqlite3";

const app = express();
const PORT = 3000;

// Database Setup
const db = new Database("satoshi_ghost.db");
db.exec(`
  CREATE TABLE IF NOT EXISTS found_keys (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    address TEXT,
    private_key TEXT,
    wif TEXT,
    wif_u TEXT,
    network TEXT,
    wallet_type TEXT,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
  );
  CREATE TABLE IF NOT EXISTS scan_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    total_scanned INTEGER,
    keys_per_second INTEGER,
    network TEXT
  );
`);

app.use(express.json({ limit: '50mb' }));

// Scan state
let scanWorkers: Worker[] = [];
let scanStatus = {
  active: false,
  currentKey: "0000000000000000000000000000000000000000000000000000000000000001",
  sampleAddresses: [] as string[],
  keysPerSecond: 0,
  totalScanned: 0,
  startTime: 0,
  found: [] as any[],
  logs: [] as string[],
  progress: 0,
  targetsCount: 0,
  acceleration: "CPU" as "CPU" | "GPU"
};

// SSE Clients
let clients: any[] = [];

function broadcastStatus() {
  const data = JSON.stringify(scanStatus);
  clients.forEach(client => client.res.write(`data: ${data}\n\n`));
}

// Update loop for SSE
setInterval(() => {
  if (scanStatus.active) {
    broadcastStatus();
  }
}, 1000);

// API Routes
app.get("/api/puzzles", (req, res) => {
  try {
    const content = fs.readFileSync(path.resolve(process.cwd(), "puzzles.txt"), "utf-8");
    const puzzles = content.trim().split("\n").filter(l => l && !l.startsWith("#")).map(line => {
      const [name, address, start, end] = line.split(",");
      return { name, address, start, end };
    });
    res.json(puzzles);
  } catch (error) {
    res.status(500).json({ error: "Failed to load puzzles" });
  }
});

app.get("/api/history", (req, res) => {
  const found = db.prepare("SELECT * FROM found_keys ORDER BY timestamp DESC").all();
  res.json(found);
});

app.post("/api/start_scan", (req, res) => {
  if (scanStatus.active) {
    return res.status(400).json({ error: "Scan already running" });
  }

  const { type, targets, startHex, endHex, random, network = 'BTC', threads = 1, acceleration = "CPU" } = req.body;

  // Reset status
  scanStatus = {
    active: true,
    currentKey: startHex,
    sampleAddresses: [],
    keysPerSecond: 0,
    totalScanned: 0,
    startTime: Date.now(),
    found: [],
    logs: [`Starting ${network} ${random ? 'random' : 'sequential'} scan with ${threads} threads using ${acceleration}...`],
    progress: 0,
    targetsCount: targets.length,
    acceleration
  };

  const workerPath = path.resolve(process.cwd(), "scanner-worker.ts");
  
  const workerStats = new Map<number, { kps: number, total: number, progress: number }>();

  for (let i = 0; i < threads; i++) {
    const worker = new Worker(workerPath, {
      workerData: { 
        targets, 
        startHex, 
        endHex, 
        random, 
        network,
        threadIndex: i,
        totalThreads: threads,
        acceleration
      },
      execArgv: ["--import", "tsx"]
    });

    worker.on("message", (msg) => {
      if (msg.type === "update") {
        workerStats.set(i, { kps: msg.kps, total: msg.total, progress: msg.progress });
        
        let totalKps = 0;
        let totalScanned = 0;
        let avgProgress = 0;
        
        workerStats.forEach(stat => {
          totalKps += stat.kps;
          totalScanned += stat.total;
          avgProgress += stat.progress;
        });

        scanStatus.keysPerSecond = totalKps;
        scanStatus.totalScanned = totalScanned;
        scanStatus.progress = avgProgress / threads;
        
        if (!scanStatus.active) return;
        if (i === 0 || scanStatus.logs.length === 0) {
           scanStatus.currentKey = msg.currentKey;
           scanStatus.sampleAddresses = msg.sampleAddresses;
        }
      } else if (msg.type === "found") {
        // Save to DB
        const stmt = db.prepare(`
          INSERT INTO found_keys (address, private_key, wif, wif_u, network, wallet_type)
          VALUES (?, ?, ?, ?, ?, ?)
        `);
        stmt.run(msg.data.address, msg.data.privateKey, msg.data.wif, msg.data.wifU, msg.data.network, msg.data.walletType);

        scanStatus.found.push(msg.data);
        scanStatus.logs.push(`MATCH FOUND: ${msg.data.address}`);
        broadcastStatus();
      } else if (msg.type === "done") {
        scanStatus.logs.push(`Thread ${i} completed.`);
      }
    });

    worker.on("error", (err) => {
      console.error(`Worker ${i} error:`, err);
      scanStatus.logs.push(`Thread ${i} Error: ${err.message}`);
      broadcastStatus();
    });

    scanWorkers.push(worker);
  }

  res.json({ status: "started" });
});

app.post("/api/stop_scan", (req, res) => {
  scanWorkers.forEach(w => w.terminate());
  scanWorkers = [];
  scanStatus.active = false;
  scanStatus.logs.push("Scan stopped by user.");
  broadcastStatus();
  res.json({ status: "stopped" });
});

app.get("/api/stream", (req, res) => {
  res.writeHead(200, {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
  });

  const clientId = Date.now();
  clients.push({ id: clientId, res });

  req.on("close", () => {
    clients = clients.filter(c => c.id !== clientId);
  });
});

async function startServer() {
  // Vite middleware for development
  if (process.env.NODE_ENV !== "production") {
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: "spa",
    });
    app.use(vite.middlewares);
  } else {
    const distPath = path.join(process.cwd(), 'dist');
    app.use(express.static(distPath));
    app.get('*', (req, res) => {
      res.sendFile(path.join(distPath, 'index.html'));
    });
  }

  app.listen(PORT, "0.0.0.0", () => {
    console.log(`Server running on http://localhost:${PORT}`);
  });
}

startServer();
