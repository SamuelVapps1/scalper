Set-Location "C:\Users\Admin\Desktop\scalper"

chcp 65001 | Out-Null
$env:PYTHONUTF8="1"
$env:PYTHONIOENCODING="utf-8"
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new()

& ".\.venv\Scripts\python.exe" "bot.py" --loop