import express from "express";
import xrpl from "xrpl";

const app = express();
app.use(express.json());

// === Required environment variables ===
const {
  SUPABASE_URL,                    // e.g. https://oxnrijmtgoonjqqkdpec.supabase.co
  SUPABASE_SERVICE_ROLE_KEY,       // from Supabase Settings → API → service_role
  SUPABASE_WEBHOOK_URL,            // e.g. https://.../functions/v1/xrp-payout-webhook
  XRP_HOT_WALLET_SEED,             // family seed starting with "s..."
  XRPL_SERVERS,                    // optional: comma-separated list of WS urls
  XRPL_SERVER,                     // optional: single WS url
  POLLING_INTERVAL_MS = "5000",    // optional: default 5s
  MAX_CONCURRENT = "2",            // optional: default 2
  CONTROL_BEARER_TOKEN             // optional: if you want to protect /process
} = process.env;

// XRPL endpoints (fallback list)
const WS_DEFAULTS =
  (XRPL_SERVERS?.split(",").map(s => s.trim()).filter(Boolean)) ||
  ([XRPL_SERVER].filter(Boolean)) ||
  ["wss://xrplcluster.com", "wss://s2.ripple.com", "wss://s1.ripple.com"];

let client;
let wallet;
let inFlight = 0;

function ensureEnv() {
  if (!SUPABASE_URL) throw new Error("SUPABASE_URL missing");
  if (!SUPABASE_SERVICE_ROLE_KEY) throw new Error("SUPABASE_SERVICE_ROLE_KEY missing");
  if (!SUPABASE_WEBHOOK_URL) throw new Error("SUPABASE_WEBHOOK_URL missing");
  if (!XRP_HOT_WALLET_SEED) throw new Error("XRP_HOT_WALLET_SEED missing");
}

function supabaseHeaders() {
  return {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
    "Content-Type": "application/json"
  };
}

async function connectClient() {
  for (const url of WS_DEFAULTS) {
    try {
      const c = new xrpl.Client(url);
      await c.connect();
      client = c;
      console.log("[XRPL] Connected:", url);
      return;
    } catch (e) {
      console.error("[XRPL] Failed:", url, e.message);
    }
  }
  throw new Error("No XRPL server reachable");
}

// Pull queued payouts from Supabase REST
async function fetchQueued(limit = 5) {
  // If your timestamp column is named differently, change requested_at to created_at
  const url = `${SUPABASE_URL}/rest/v1/payout_requests?status=eq.queued&chain=eq.XRP&order=requested_at.asc&limit=${limit}`;
  const res = await fetch(url, { headers: supabaseHeaders() });
  if (!res.ok) {
    const t = await res.text();
    throw new Error(`Fetch queued failed: ${res.status} ${t}`);
  }
  return res.json();
}

async function postWebhook(body) {
  const res = await fetch(SUPABASE_WEBHOOK_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body)
  });
  if (!res.ok) {
    const t = await res.text();
    throw new Error(`Webhook failed: ${res.status} ${t}`);
  }
  try { return await res.json(); } catch { return { ok: true }; }
}

async function processOne(row) {
  inFlight++;
  try {
    const { id, wallet_address, amount, destination_tag, webhook_secret } = row;

    // Mark "processing" (your webhook allows this transition)
    await postWebhook({ request_id: id, status: "processing", webhook_secret });

    // Build + sign + submit XRPL Payment
    const tx = {
      TransactionType: "Payment",
      Account: wallet.classicAddress,
      Amount: xrpl.xrpToDrops(String(amount)),
      Destination: wallet_address,
      ...(destination_tag != null ? { DestinationTag: Number(destination_tag) } : {})
    };

    const prepared = await client.autofill(tx);
    const signed = wallet.sign(prepared);
    const result = await client.submitAndWait(signed.tx_blob);

    const engine = result?.result?.engine_result || result?.engine_result;
    const txHash = result?.result?.tx_json?.hash || result?.tx_json?.hash;

    if (engine !== "tesSUCCESS") {
      throw new Error(`Engine result: ${engine || "unknown"}`);
    }

    // Report success
    await postWebhook({
      request_id: id,
      status: "completed",
      tx_id: txHash,
      webhook_secret
    });

    console.log("[PAYOUT] Success", id, txHash);

  } catch (err) {
    console.error("[PAYOUT] Failed", row?.id, err.message);
    // Report failure (best effort)
    try {
      await postWebhook({
        request_id: row.id,
        status: "failed",
        error_message: String(err.message || err).slice(0, 500),
        webhook_secret: row.webhook_secret
      });
    } catch (e) {
      console.error("[WEBHOOK] Error reporting failure:", e.message);
    }
  } finally {
    inFlight--;
  }
}

async function pollLoop() {
  if (!client || !client.isConnected()) {
    try { await connectClient(); } catch (e) {
      console.error("[XRPL] reconnect failed:", e.message);
      return;
    }
  }
  if (inFlight >= Number(MAX_CONCURRENT)) return;

  try {
    const rows = await fetchQueued(Number(MAX_CONCURRENT));
    for (const row of rows) {
      if (inFlight >= Number(MAX_CONCURRENT)) break;
      processOne(row);
    }
  } catch (e) {
    console.error("[POLL] error:", e.message);
  }
}

// Health endpoint
app.get("/health", async (_req, res) => {
  try {
    ensureEnv();
    if (!wallet) wallet = xrpl.Wallet.fromSeed(XRP_HOT_WALLET_SEED);
    if (!client || !client.isConnected()) {
      try { await connectClient(); } catch {}
    }
    res.json({
      ok: true,
      xrpl_connected: !!(client && client.isConnected()),
      hot_wallet: wallet?.classicAddress || null,
      inflight: inFlight
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Optional: manual trigger
app.post("/process", (req, res) => {
  if (CONTROL_BEARER_TOKEN) {
    const auth = req.headers.authorization || "";
    if (auth !== `Bearer ${CONTROL_BEARER_TOKEN}`) {
      return res.status(401).json({ ok: false, error: "Unauthorized" });
    }
  }
  setTimeout(pollLoop, 50);
  res.json({ ok: true, started: true });
});

// Boot
const port = process.env.PORT || 3000;
app.listen(port, async () => {
  try {
    ensureEnv();
    wallet = xrpl.Wallet.fromSeed(XRP_HOT_WALLET_SEED);
    console.log("[BOOT] Hot wallet:", wallet.classicAddress);
  } catch (e) {
    console.error("[BOOT] env error:", e.message);
  }
  setInterval(pollLoop, Number(POLLING_INTERVAL_MS || 5000));
  console.log("[SERVER] Listening on", port);
});
