"""Tests for robust Telegram sending (timeout, retries, no crash)."""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_send_telegram_handles_timeout_gracefully():
    """send_telegram returns False on ReadTimeout, never raises."""
    import requests

    from telegram_notify import send_telegram

    with patch("telegram_notify.requests.post") as mock_post:
        mock_post.side_effect = requests.exceptions.ReadTimeout("Connection timed out")
        result = send_telegram(token="fake", chat_id="123", text="test")
        assert result is False
        assert mock_post.call_count == 3


def test_send_telegram_handles_connection_error_gracefully():
    """send_telegram returns False on ConnectionError, never raises."""
    import requests

    from telegram_notify import send_telegram

    with patch("telegram_notify.requests.post") as mock_post:
        mock_post.side_effect = requests.exceptions.ConnectionError("Network unreachable")
        result = send_telegram(token="fake", chat_id="123", text="test")
        assert result is False


def test_send_telegram_succeeds_on_retry():
    """send_telegram retries and succeeds on second attempt."""
    import requests
    from unittest.mock import MagicMock

    from telegram_notify import send_telegram

    with patch("telegram_notify.requests.post") as mock_post:
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_post.side_effect = [
            requests.exceptions.ReadTimeout("timeout"),
            mock_resp,
        ]
        result = send_telegram(token="fake", chat_id="123", text="test")
        assert result is True
        assert mock_post.call_count == 2


def test_send_telegram_returns_true_on_success():
    """send_telegram returns True when API responds OK."""
    from telegram_notify import send_telegram

    with patch("telegram_notify.requests.post") as mock_post:
        mock_post.return_value.raise_for_status = lambda: None
        result = send_telegram(token="fake", chat_id="123", text="test")
        assert result is True
        assert mock_post.call_count == 1
