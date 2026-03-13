Set-Location "C:\Users\Admin\Desktop\scalper"

# UTF-8 pre Windows console
chcp 65001 | Out-Null
$env:PYTHONUTF8="1"
$env:PYTHONIOENCODING="utf-8"
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new()

# spusti loop (bez --once)
& ".\.venv\Scripts\python.exe" -u "bot.py" --loop --paper --log-level INFO