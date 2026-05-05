"""
BSC Token Balance Monitor — Web App
=====================================
Chạy trên Render free tier
- Setup token + ví qua giao diện web
- Quét số dư mỗi N giây dùng multi-thread
- Báo Telegram THỐNG KÊ TỔNG QUAN khi có ví tăng (gom nhóm theo mức tăng)
- So sánh với snapshot BAN ĐẦU (mốc cố định)
- Self-ping mỗi 5 phút để không bị Render sleep
"""

import os, json, time, threading, logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from flask import Flask, render_template, request, jsonify, redirect, url_for
from web3 import Web3
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

CONFIG_FILE   = "config.json"
SNAPSHOT_FILE = "snapshot.json"

state = {
    "running": False, "config": None,
    "snapshot": {}, "wallet_index": {}, "logs": [],
    "stats": {"total_wallets": 0, "scanned": 0, "alerts_total": 0,
               "last_scan": None, "last_scan_time": None, "cycle": 0}
}
state_lock = threading.Lock()

BSC_RPCS = [
    "https://bsc-dataseed1.binance.org/",
    "https://bsc-dataseed2.binance.org/",
    "https://bsc-dataseed3.binance.org/",
    "https://bsc-dataseed4.binance.org/",
    "https://bsc-dataseed1.defibit.io/",
    "https://bsc-dataseed2.defibit.io/",
]

BALANCE_ABI = [
    {"constant": True, "inputs": [{"name": "_owner", "type": "address"}],
     "name": "balanceOf", "outputs": [{"name": "balance", "type": "uint256"}], "type": "function"},
    {"constant": True, "inputs": [], "name": "decimals",
     "outputs": [{"name": "", "type": "uint8"}], "type": "function"},
    {"constant": True, "inputs": [], "name": "symbol",
     "outputs": [{"name": "", "type": "string"}], "type": "function"},
]

_local = threading.local()

def get_w3():
    if not hasattr(_local, "w3"):
        for rpc in BSC_RPCS:
            try:
                w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 5}))
                if w3.is_connected():
                    _local.w3 = w3
                    return w3
            except Exception:
                continue
        raise ConnectionError("Không kết nối được RPC BSC!")
    return _local.w3

def add_log(msg, level="info"):
    ts = datetime.now().strftime("%H:%M:%S")
    with state_lock:
        state["logs"].append({"time": ts, "msg": msg, "level": level})
        if len(state["logs"]) > 200:
            state["logs"] = state["logs"][-200:]

def shorten(addr): return f"{addr[:6]}...{addr[-4:]}"

def load_config():
    return json.load(open(CONFIG_FILE)) if os.path.exists(CONFIG_FILE) else None

def save_config(cfg):
    json.dump(cfg, open(CONFIG_FILE, "w"), indent=2)

def load_snapshot():
    return json.load(open(SNAPSHOT_FILE)) if os.path.exists(SNAPSHOT_FILE) else {}

def save_snapshot(snap):
    json.dump(snap, open(SNAPSHOT_FILE, "w"), indent=2)

def get_token_info(contract_addr):
    try:
        w3 = get_w3()
        c  = w3.eth.contract(address=Web3.to_checksum_address(contract_addr), abi=BALANCE_ABI)
        return c.functions.symbol().call(), c.functions.decimals().call()
    except Exception as e:
        log.error(f"get_token_info: {e}")
        return "TOKEN", 18

def get_balance(wallet_addr, contract_addr, decimals, retries=10):
    for attempt in range(1, retries + 1):
        try:
            w3 = get_w3()
            c  = w3.eth.contract(address=Web3.to_checksum_address(contract_addr), abi=BALANCE_ABI)
            raw = c.functions.balanceOf(Web3.to_checksum_address(wallet_addr)).call()
            return (wallet_addr, raw / (10 ** decimals))
        except Exception as e:
            if attempt < retries:
                # Reset w3 để lần retry dùng RPC khác
                if hasattr(_local, "w3"):
                    del _local.w3
                time.sleep(0.5)
            else:
                return (wallet_addr, None)

def scan_all(wallets, contract_addr, decimals, num_threads=10):
    results = {}
    with ThreadPoolExecutor(max_workers=num_threads) as ex:
        futures = {ex.submit(get_balance, w, contract_addr, decimals): w for w in wallets}
        for f in as_completed(futures):
            addr, bal = f.result()
            if bal is not None:
                results[addr] = bal
    return results

def send_telegram(token, chat_id, message):
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"},
            timeout=10
        )
        return r.status_code == 200
    except Exception as e:
        add_log(f"Telegram lỗi: {e}", "error")
        return False

def run_monitor_cycle():
    with state_lock:
        if not state["running"] or not state["config"]:
            return
        cfg      = state["config"]
        snapshot = dict(state["snapshot"])
        idx      = state["wallet_index"]

    wallets     = cfg["wallets"]
    contract    = cfg["contract"]
    decimals    = cfg["decimals"]
    symbol      = cfg["symbol"]
    tg_token    = cfg["tg_token"]
    tg_chat     = cfg["tg_chat"]
    num_threads = cfg.get("num_threads", 10)

    with state_lock:
        state["stats"]["cycle"] += 1
        cycle = state["stats"]["cycle"]

    add_log(f"Vòng #{cycle} — quét {len(wallets)} ví...", "info")
    t0 = time.time()
    current = scan_all(wallets, contract, decimals, num_threads)
    elapsed = time.time() - t0

    # Báo lỗi RPC nếu có ví không quét được
    failed_wallets = [addr for addr in wallets if addr not in current]
    if failed_wallets:
        err_msg = (
            f"⚠️ <b>RPC LỖI — Vòng #{cycle}</b>\n"
            f"Không quét được <b>{len(failed_wallets)} ví</b> sau 10 lần retry.\n"
            f"⚠️ Kết quả vòng này có thể thiếu sót!"
        )
        send_telegram(tg_token, tg_chat, err_msg)
        add_log(f"⚠️ {len(failed_wallets)} ví lỗi RPC", "warning")

    # So sánh với snapshot BAN ĐẦU (không update khi tăng — giữ mốc gốc)
    increased    = []
    new_snapshot = dict(snapshot)

    for addr, new_bal in current.items():
        old_bal = snapshot.get(addr, 0.0)
        diff    = new_bal - old_bal

        if diff > 0.0001:
            # KHÔNG update snapshot — giữ mốc gốc cố định
            increased.append(diff)
        elif new_bal < old_bal:
            # Số dư giảm → cập nhật snapshot xuống mức mới
            new_snapshot[addr] = new_bal

    save_snapshot(new_snapshot)
    with state_lock:
        state["snapshot"]                  = new_snapshot
        state["stats"]["scanned"]          = len(current)
        state["stats"]["last_scan"]        = datetime.now().strftime("%d/%m %H:%M:%S")
        state["stats"]["last_scan_time"]   = round(elapsed, 1)
        if increased:
            state["stats"]["alerts_total"] += len(increased)

    if increased:
        total_amount = sum(increased)
        avg_amount   = total_amount / len(increased)

        # ─── Gom nhóm theo mức tăng (số nguyên) ─────────────
        # 1u  : 0-<1 USDT
        # 2u  : 1-<2 USDT
        # 3u  : 2-<3 USDT
        # 4u  : 3-<4 USDT
        # 5u  : 4-<5 USDT
        # ≥6u : ≥5 USDT
        buckets = [[], [], [], [], [], []]
        for diff in increased:
            if diff < 1:
                buckets[0].append(diff)
            elif diff < 2:
                buckets[1].append(diff)
            elif diff < 3:
                buckets[2].append(diff)
            elif diff < 4:
                buckets[3].append(diff)
            elif diff < 5:
                buckets[4].append(diff)
            else:
                buckets[5].append(diff)

        labels = ["1u", "2u", "3u", "4u", "5u", "≥6u"]

        # Build các dòng phân bố — chỉ hiện nhóm có ví, ẩn nhóm rỗng
        bucket_lines = []
        for label, bucket in zip(labels, buckets):
            if bucket:
                count = len(bucket)
                bsum  = sum(bucket)
                bucket_lines.append(
                    f"   • <b>{label:<4}</b>: {count:>3} ví  "
                    f"(+{bsum:,.2f} {symbol})"
                )

        msg = (
            f"💰 <b>{symbol} TĂNG — Vòng #{cycle}</b>\n"
            f"🕐 {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n\n"
            f"📊 <b>Tổng quan:</b>\n"
            f"   • Số ví tăng: <b>{len(increased)}/{len(wallets)} ví</b>\n"
            f"   • Tổng tăng: <b>+{total_amount:,.2f} {symbol}</b>\n"
            f"   • Trung bình: <b>+{avg_amount:,.2f} {symbol}/ví</b>\n"
            f"─────────────────\n"
            f"📈 <b>Phân bố mức tăng:</b>\n"
            + "\n".join(bucket_lines)
        )

        send_telegram(tg_token, tg_chat, msg)
        add_log(f"✅ {len(increased)} ví tăng — tổng +{total_amount:,.2f} {symbol}", "success")
    else:
        add_log(f"Vòng #{cycle} xong ({elapsed:.1f}s) — không có ví nào tăng", "info")

# ─── SCHEDULER ───────────────────────────────────────────────────────────────

scheduler = BackgroundScheduler(daemon=True)

def start_scheduler(interval=60):
    scheduler.remove_all_jobs()
    scheduler.add_job(run_monitor_cycle, "interval", seconds=interval, id="monitor")
    if not scheduler.running:
        scheduler.start()

def stop_scheduler():
    scheduler.remove_all_jobs()

# ─── SELF-PING ────────────────────────────────────────────────────────────────

def self_ping():
    url = os.environ.get("RENDER_EXTERNAL_URL", "")
    if url:
        try: requests.get(f"{url}/ping", timeout=10)
        except Exception: pass

ping_sc = BackgroundScheduler(daemon=True)
ping_sc.add_job(self_ping, "interval", minutes=5, id="ping")
ping_sc.start()

# ─── ROUTES ──────────────────────────────────────────────────────────────────

@app.route("/ping")
def ping(): return "pong", 200

@app.route("/")
def index():
    cfg = load_config()
    with state_lock: running = state["running"]
    if running: return redirect(url_for("dashboard"))
    return render_template("setup.html", config=cfg)

@app.route("/setup", methods=["POST"])
def setup():
    data = request.get_json()
    wallets_raw = data.get("wallets", "")
    contract    = data.get("contract", "").strip()
    tg_token    = data.get("tg_token", "").strip()
    tg_chat     = data.get("tg_chat", "").strip()
    interval    = int(data.get("interval", 60))
    num_threads = int(data.get("num_threads", 10))

    wallets = []
    seen    = set()
    for line in wallets_raw.splitlines():
        addr = line.strip()
        if not addr or addr.startswith("#"): continue
        if addr.startswith("0x") and len(addr) == 42:
            try:
                cs = Web3.to_checksum_address(addr)
                if cs not in seen:
                    wallets.append(cs); seen.add(cs)
            except Exception: pass

    if not wallets:
        return jsonify({"ok": False, "error": "Không có địa chỉ ví hợp lệ"}), 400
    if not contract or not contract.startswith("0x"):
        return jsonify({"ok": False, "error": "Contract address không hợp lệ"}), 400

    try:
        symbol, decimals = get_token_info(contract)
    except Exception as e:
        return jsonify({"ok": False, "error": f"Không lấy được token info: {e}"}), 400

    cfg = {"wallets": wallets, "contract": contract, "symbol": symbol,
           "decimals": decimals, "tg_token": tg_token, "tg_chat": tg_chat,
           "interval": interval, "num_threads": num_threads}
    save_config(cfg)

    add_log(f"Đang snapshot {len(wallets)} ví...", "info")
    snapshot = scan_all(wallets, contract, decimals, num_threads)
    save_snapshot(snapshot)
    add_log(f"Snapshot xong — {len(snapshot)} ví", "success")

    idx = {addr: i + 1 for i, addr in enumerate(wallets)}
    with state_lock:
        state["running"]      = True
        state["config"]       = cfg
        state["snapshot"]     = snapshot
        state["wallet_index"] = idx
        state["stats"]["total_wallets"] = len(wallets)
        state["stats"]["cycle"]         = 0
        state["stats"]["alerts_total"]  = 0

    start_scheduler(interval)
    send_telegram(tg_token, tg_chat,
        f"🟢 <b>BSC {symbol} Monitor đã bật</b>\n"
        f"📋 Theo dõi <b>{len(wallets)} ví</b>\n"
        f"⏱ Quét mỗi <b>{interval}s</b> | 🧵 <b>{num_threads} luồng</b>")

    return jsonify({"ok": True, "symbol": symbol, "decimals": decimals, "total": len(wallets)})

@app.route("/dashboard")
def dashboard():
    with state_lock:
        running = state["running"]
        cfg     = state["config"]
        stats   = dict(state["stats"])
    if not running: return redirect(url_for("index"))
    return render_template("dashboard.html", config=cfg, stats=stats)

@app.route("/api/logs")
def api_logs():
    with state_lock:
        logs  = list(state["logs"][-50:])
        stats = dict(state["stats"])
    return jsonify({"logs": logs, "stats": stats})

@app.route("/stop", methods=["POST"])
def stop():
    with state_lock:
        cfg = state["config"]
        state["running"] = False
    stop_scheduler()
    if cfg: send_telegram(cfg["tg_token"], cfg["tg_chat"], "🔴 <b>Monitor đã tắt</b>")
    add_log("Monitor đã dừng", "warning")
    return jsonify({"ok": True})

@app.route("/reset", methods=["POST"])
def reset():
    with state_lock:
        state.update({"running": False, "config": None, "snapshot": {},
                       "wallet_index": {}, "logs": [],
                       "stats": {"total_wallets": 0, "scanned": 0, "alerts_total": 0,
                                  "last_scan": None, "last_scan_time": None, "cycle": 0}})
    stop_scheduler()
    for f in [CONFIG_FILE, SNAPSHOT_FILE]:
        if os.path.exists(f): os.remove(f)
    return jsonify({"ok": True})

def init_from_disk():
    cfg = load_config()
    if not cfg: return
    snap = load_snapshot()
    idx  = {addr: i + 1 for i, addr in enumerate(cfg["wallets"])}
    with state_lock:
        state["running"]      = True
        state["config"]       = cfg
        state["snapshot"]     = snap
        state["wallet_index"] = idx
        state["stats"]["total_wallets"] = len(cfg["wallets"])
    start_scheduler(cfg.get("interval", 60))
    add_log("Đã khôi phục config sau khi restart", "info")

init_from_disk()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
