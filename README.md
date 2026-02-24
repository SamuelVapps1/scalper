# Bybit Signal Bot (DRY RUN, Alerts Only)

This project watches Bybit market candles and sends **signal alerts only**.

It does **not** place orders, does **not** connect to trading endpoints, and is safe for paper monitoring.

Environment values are read from `.env` in the project root (same folder as `bot.py`).

---

## 1) What this bot does

- Fetches public candle data from Bybit (`/v5/market/kline`)
- Calculates indicators (EMA, RSI, MACD, ATR)
- Detects 2 predefined signal setups
- Logs every signal to `signals_log.csv`
- Sends Telegram alerts (if configured)
- Applies anti-spam cooldown: 30 minutes per `symbol + setup`

If Telegram is not configured, it still runs and logs signals locally.

---

## 2) What are Telegram `bot token` and `chat_id`?

You need 2 values for Telegram alerts:

- `TELEGRAM_BOT_TOKEN`: password-like token for your bot
- `TELEGRAM_CHAT_ID`: the chat where messages will be sent

### Step A: Create a bot with BotFather

1. Open Telegram and search for **BotFather**
2. Start chat with BotFather
3. Send `/newbot`
4. Follow prompts (name + username ending in `bot`)
5. BotFather gives you a token like:
   `123456789:AA...`
6. Save this token for `.env` as `TELEGRAM_BOT_TOKEN`

### Step B: Get your `chat_id` using `getUpdates`

1. Start a chat with your new bot and send any message (for example: `hi`)
2. In browser, open:

   `https://api.telegram.org/bot<YOUR_BOT_TOKEN>/getUpdates`

3. Find `"chat":{"id": ... }` in the JSON response
4. Copy that number into `.env` as `TELEGRAM_CHAT_ID`

Tip: If `getUpdates` is empty, send another message to your bot and refresh.

---

## 3) Create `.env` from `.env.example`

In PowerShell, inside the project folder:

```powershell
Copy-Item .env.example .env
```

Then open `.env` and set values, especially:

- `WATCHLIST` (example: `BTCUSDT,ETHUSDT`)
- `INTERVAL` (example: `15`)
- `LOOKBACK` (example: `300`)
- `SCAN_SECONDS` (example: `60`)
- `TELEGRAM_BOT_TOKEN` (optional but needed for Telegram alerts)
- `TELEGRAM_CHAT_ID` (optional but needed for Telegram alerts)
- `TELEGRAM_FORMAT=compact` (`compact|verbose`, message verbosity for Telegram alerts)
- `TELEGRAM_MAX_CHARS_COMPACT=900` (max chars for compact messages; longer text is truncated)
- `TELEGRAM_MAX_CHARS_VERBOSE=2500` (max chars for verbose messages; longer text is truncated)
- `TELEGRAM_SEND_BLOCKED=0` (send blocked intent telegrams; default off to reduce spam)
- `TELEGRAM_SEND_DASHBOARD=0` (recommended default; keeps Telegram concise and avoids dashboard spam)
- `EARLY_ENABLED=1` (enable 5m EARLY pre-alert bridge)
- `EARLY_TF=5` (timeframe used for EARLY alerts)
- `EARLY_LOOKBACK_5M=180` (lookback candles for 5m EARLY evaluation)
- `EARLY_MIN_CONF=0.35` (minimum confidence for EARLY alert)
- `EARLY_REQUIRE_15M_CONTEXT=1` (fetch/use 5m only if 15m produced RB/FB candidates)
- `EARLY_MAX_ALERTS_PER_SYMBOL_PER_15M=1` (dedupe cap per symbol per active 15m candle)
- `THRESHOLD_PROFILE=A` (strategy threshold preset: `A|B|C`; active profile drives live DRY RUN signals)
- `DB_PATH=./data/scalper.db` (SQLite path; default uses WAL mode in ./data)

For dynamic Top-N watchlist mode (`WATCHLIST_MODE=topn`), quality filters are available:

- `WATCHLIST_MIN_TURNOVER_24H=100000000` (default 100M)
- `WATCHLIST_EXCLUDE_SYMBOLS=PEPEUSDT,FLOKIUSDT,BONKUSDT` (exact symbol excludes)
- `WATCHLIST_EXCLUDE_REGEX=` (optional Python regex, example: `^(1000|10000)|.*(PEPE|FLOKI|BONK).*`)
- `WATCHLIST_MAX_SPREAD_BPS=` (optional max spread in bps; empty/0 disables)

---

## 4) Run on Windows (venv, step-by-step)

From PowerShell in this project folder:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python bot.py
```

You should see scan logs in terminal and signals in `signals_log.csv`.

To stop the bot: press `Ctrl + C`.

---

## Quick Test

Run these from PowerShell in the project root:

```powershell
python bot.py --help
python bot.py --test-telegram
python bot.py --test-telegram-formats
python bot.py --once
```

Expected behavior:

- `--help`: prints usage and exits (does not start scanning)
- `--test-telegram`: sends `✅ Telegram OK (test)` and exits
- `--test-telegram-formats`: prints sample ALLOW/BLOCK/CLOSE/EARLY in both compact and verbose modes (no send)
- `--once`: runs exactly one scan pass and exits

Telegram compact examples:

- compact allow:
  `ALLOW[15m] BTCUSDT LONG conf=0.74`
  `entry=65780.0000 sl=65466.3300 tp=66250.4900`
  `sl%=0.48 tp%=0.72`
  `setup=Range breakout -> retest -> go`
  `risk open=1/3 trades=4 cooldown=OFF`
- compact blocked:
  `BLOCK[15m] ETHUSDT SHORT conf=0.62`
  `setup=EMA200 fade after sweep (trap)`
  `risk_reason=MAX_OPEN_POSITIONS_REACHED (3)`
- compact close:
  `CLOSE[15m] XRPUSDT SHORT`
  `pnl=0.4503 reason=TP bars=3`
  `after daily_pnl=1.3432 consec_losses=0 cooldown=OFF`

Code note:

- Telegram integration uses `send_telegram(token, chat_id, text)` in `telegram_notify.py`

---

## 5) Common errors and fixes

### A) Missing env vars / no signals / no Telegram alerts

Symptoms:
- Bot warns watchlist is empty, or does nothing useful
- Telegram messages are skipped

Fix:
1. Check `.env` exists (not `.env.example` only)
2. Ensure `WATCHLIST` is set (example: `BTCUSDT,ETHUSDT`)
3. If using Telegram, set both `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`
4. Restart the bot after editing `.env`

### B) `requests` error (network/API problem)

Symptoms:
- Connection timeout
- DNS/SSL error
- HTTP error while fetching Bybit data

Fix:
1. Check internet connection
2. Wait and retry (temporary API/network issues happen)
3. Confirm `BYBIT_BASE_URL=https://api.bybit.com` in `.env`
4. Ensure firewall/proxy is not blocking Python

### C) Telegram error (message not sent)

Symptoms:
- Log says Telegram send failed

Fix:
1. Verify bot token is correct (from BotFather)
2. Verify chat_id is correct (from `getUpdates`)
3. Send at least one direct message to the bot, then retry
4. Make sure there are no extra spaces/quotes in `.env`

---

## Project files

- `bot.py` - main loop and cooldown logic
- `bybit.py` - public Bybit kline fetch
- `signals.py` - indicators + setup rules
- `telegram_notify.py` - Telegram `sendMessage`
- `storage.py` - storage facade over SQLite (WAL)
- `sqlite_store.py` - sqlite3 backend (signals/intents/risk/positions/fills/kv)
- `config.py` - loads `.env`
- `requirements.txt` - Python dependencies
