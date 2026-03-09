# Scalper Scanner (Bybit)

Canonical runtime entrypoint:

```bash
python -m scalper.scanner
```

Main run commands:

```bash
python -m scalper.scanner --once --paper
python -m scalper.scanner --loop --paper
python -m scalper.scanner --test-telegram-formats
python scripts/format_one_signal.py
```

`bot.py` is only a thin wrapper to `scalper.scanner.main`.

Environment values are loaded from the repository root `.env`. You can override path with `ENV_PATH`.

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

---

## 4) Run (Windows/Linux)

From PowerShell in this project folder:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m scalper.scanner --once --paper
```

You should see scan logs in terminal and signals in `signals_log.csv`.

To stop the bot: press `Ctrl + C`.

---

## Quick Test

Run these from PowerShell in the project root:

```powershell
python -m scalper.scanner --help
python -m scalper.scanner --test-telegram
python -m scalper.scanner --test-telegram-formats
python -m scalper.scanner --once --paper
```

Expected behavior:

- `--help`: prints usage and exits
- `--test-telegram`: sends `Telegram OK (test)` and exits
- `--test-telegram-formats`: prints enriched message formats to stdout (no send)
- `--once --paper`: one paper scan cycle

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

## Server Runbook

```bash
git pull
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python -m scalper.scanner --once --paper
python -m scalper.scanner --loop --paper
```

Optional systemd/tmux:
- tmux: `tmux new -s scalper 'source venv/bin/activate && python -m scalper.scanner --loop --paper'`
- systemd: run same command in service `ExecStart`.

## Project files

- `bot.py` - wrapper to `scalper.scanner.main`
- `scalper/scanner.py` - canonical runtime pipeline
- `bybit.py` - Bybit public HTTP client (retry/backoff/rate-limit pacing)
- `watchlist.py` - market/static watchlist provider and rotation
- `signals.py` - intent evaluation + early signals
- `telegram_notify.py` - Telegram `sendMessage`
- `storage.py` - CSV signal logging
- `config.py` - loads `.env`
- `requirements.txt` - Python dependencies
