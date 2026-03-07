@echo off
REM One-shot run (same as Task Scheduler: tick every 5 min).
REM Task Scheduler: Program .venv\Scripts\python.exe, Args: bot.py --once --paper --log-level INFO, Start in: this folder.
cd /d C:\Users\Admin\Desktop\scalper
call .\.venv\Scripts\activate.bat
python bot.py --once --paper --log-level INFO
