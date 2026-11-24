"""بوت سكالبينج مجاني لـ Binance: بيانات لحظية + إشعارات ntfy. ملف واحد مستقل."""
import asyncio, json, os, sys, time
import pandas as pd
import requests
import websockets

# ===== الإعدادات =====
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "DOGEUSDT", "PAXGUSDT"]  # PAXG = ذهب
NAMES = {"PAXGUSDT": "الذهب (PAXG)"}
TF = "5m"                 # 1m / 3m / 5m / 15m
SL_ATR, TP_ATR = 1.0, 1.5
MIN_ATR_PCT = 0.03
VOL_SPIKE = 1.8
COOLDOWN_MIN = 30
RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "21000"))   # 5 ساعات و50 دقيقة
NTFY_TOPIC = os.environ.get("NTFY_TOPIC", "")
# ======================

REST_URLS = ["https://data-api.binance.vision/api/v3/klines",
             "https://api.binance.com/api/v3/klines",
             "https://api-gcp.binance.com/api/v3/klines"]
WS_URLS = ["wss://data-stream.binance.vision/stream",
           "wss://stream.binance.com:9443/stream"]
COLS = ["Open", "High", "Low", "Close", "Volume"]
cooldown = {}

def rsi(c, n):
    d = c.diff()
    up = d.clip(lower=0).ewm(alpha=1/n, adjust=False).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1/n, adjust=False).mean()
    return 100 - 100 / (1 + up / dn)

def prep(df):
    c = df["Close"]
    df["ema9"], df["ema21"], df["ema50"] = (c.ewm(span=n, adjust=False).mean() for n in (9, 21, 50))
    df["rsi7"] = rsi(c, 7)
    pc = c.shift()
    tr = pd.concat([df.High - df.Low, (df.High - pc).abs(), (df.Low - pc).abs()], axis=1).max(axis=1)
    df["atr"] = tr.ewm(alpha=1/14, adjust=False).mean()
    m, s = c.rolling(20).mean(), c.rolling(20).std()
    df["bbu"], df["bbl"] = m + 2 * s, m - 2 * s
    df["hh"], df["ll"] = df.High.rolling(20).max().shift(), df.Low.rolling(20).min().shift()
    df["vavg"] = df.Volume.rolling(20).mean()
    tp = (df.High + df.Low + c) / 3
    day = df.index.date
    pv = (tp * df.Volume).groupby(day).cumsum()
    v = df.Volume.groupby(day).cumsum()
    df["vwap"] = pv / v.where(v > 0)
    return df

def signals(df):
    t, p = df.iloc[-2], df.iloc[-3]
    out = []
    up, dn = t.ema21 > t.ema50, t.ema21 < t.ema50
    if p.ema9 <= p.ema21 and t.ema9 > t.ema21 and t.Close > t.ema50 and 40 < t.rsi7 < 75:
        out.append(("شراء", "تقاطع EMA9/21 مع الاتجاه"))
    if p.ema9 >= p.ema21 and t.ema9 < t.ema21 and t.Close < t.ema50 and 25 < t.rsi7 < 60:
        out.append(("بيع", "تقاطع EMA9/21 مع الاتجاه"))
    if t.Low < t.bbl and t.Close > t.bbl and t.rsi7 < 30:
        out.append(("شراء", "ارتداد من الحد السفلي لبولينجر"))
    if t.High > t.bbu and t.Close < t.bbu and t.rsi7 > 70:
        out.append(("بيع", "رفض من الحد العلوي لبولينجر"))
    if t.Volume > 0 and t.Volume > VOL_SPIKE * t.vavg:
        if t.Close > t.hh and (pd.isna(t.vwap) or t.Close > t.vwap):
            out.append(("شراء", "اختراق قمة 20 شمعة بحجم مرتفع"))
        if t.Close < t.ll and (pd.isna(t.vwap) or t.Close < t.vwap):
            out.append(("بيع", "كسر قاع 20 شمعة بحجم مرتفع"))
    if not pd.isna(t.vwap):
        if up and t.Low <= t.vwap * 1.0005 and t.Close > t.vwap and t.Close > t.Open:
            out.append(("شراء", "ارتداد من VWAP في اتجاه صاعد"))
        if dn and t.High >= t.vwap * 0.9995 and t.Close < t.vwap and t.Close < t.Open:
            out.append(("بيع", "ارتداد من VWAP في اتجاه هابط"))
    return t, out

def push(title, body, priority="urgent", tags="zap"):
    if not NTFY_TOPIC:
        return
    requests.post(f"https://ntfy.sh/{NTFY_TOPIC}", data=body.encode("utf-8"), timeout=15,
                  headers={"Title": title.encode("utf-8"), "Priority": priority, "Tags": tags})

def notify(sym, side, title, t):
    name, a, px = NAMES.get(sym, sym), float(t.atr), float(t.Close)
    sl = px - SL_ATR * a if side == "شراء" else px + SL_ATR * a
    tp = px + TP_ATR * a if side == "شراء" else px - TP_ATR * a
    body = f"{title}\nدخول: {px:.4f}\nوقف: {sl:.4f}\nهدف: {tp:.4f}\nRSI7: {t.rsi7:.0f} | إطار {TF}"
    print(f"[{time.strftime('%H:%M:%S')}] {side} {name} - {title}", flush=True)
    push(f"{side} {name} ({TF})", body)

def check(sym, df):
    tmp = prep(pd.concat([df, df.iloc[[-1]]]))
    t, sigs = signals(tmp)
    if float(t.atr) / float(t.Close) * 100 < MIN_ATR_PCT:
        return
    for side, title in sigs:
        key = f"{sym}|{side}"
        if time.time() - cooldown.get(key, 0) < COOLDOWN_MIN * 60:
            continue
        notify(sym, side, title, t)
        cooldown[key] = time.time()

def seed(sym):
    err = None
    for url in REST_URLS:
        try:
            r = requests.get(url, params={"symbol": sym, "interval": TF, "limit": 300}, timeout=15)
            r.raise_for_status()
            now_ms = time.time() * 1000
            rows = [k for k in r.json() if k[6] < now_ms]
            return pd.DataFrame([[float(x) for x in k[1:6]] for k in rows], columns=COLS,
                                index=pd.to_datetime([k[0] for k in rows], unit="ms", utc=True))
        except Exception as e:
            err = e
    raise RuntimeError(f"تعذر جلب بيانات {sym}: {err}")

async def run():
    frames = {s: seed(s) for s in SYMBOLS}
    streams = "/".join(f"{s.lower()}@kline_{TF}" for s in SYMBOLS)
    i = 0
    while True:
        url = f"{WS_URLS[i % len(WS_URLS)]}?streams={streams}"
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                print(f"متصل | {len(SYMBOLS)} أصل | إطار {TF} | topic={'OK' if NTFY_TOPIC else 'غير محدد'}", flush=True)
                async for raw in ws:
                    d = json.loads(raw)["data"]
                    k = d["k"]
                    if not k["x"]:
                        continue
                    sym, ts = d["s"], pd.to_datetime(k["t"], unit="ms", utc=True)
                    df = frames[sym]
                    if ts in df.index:
                        continue
                    df.loc[ts] = [float(k[x]) for x in ("o", "h", "l", "c", "v")]
                    frames[sym] = df.iloc[-400:]
                    await asyncio.to_thread(check, sym, frames[sym])
        except Exception as e:
            print("انقطع الاتصال:", e, file=sys.stderr, flush=True)
            i += 1
            await asyncio.sleep(5)
            try:
                frames = {s: seed(s) for s in SYMBOLS}
            except Exception as e2:
                print(e2, file=sys.stderr, flush=True)

if __name__ == "__main__":
    if os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch":
        push("تم تشغيل المراقب", f"المراقب يعمل الآن على {len(SYMBOLS)} أصل (إطار {TF})", "default", "white_check_mark")
    try:
        asyncio.run(asyncio.wait_for(run(), timeout=RUN_SECONDS))
    except (asyncio.TimeoutError, TimeoutError):
        print("انتهت مدة التشغيل، سيبدأ تشغيل جديد تلقائيًا")
