from __future__ import annotations

from typing import Any
import json
import urllib.error
import urllib.parse
import urllib.request


class TelegramAPIError(RuntimeError):
    pass


class TelegramAPI:
    def __init__(self, token: str) -> None:
        self._token = token
        self._base_url = f"https://api.telegram.org/bot{token}"
        self._file_url = f"https://api.telegram.org/file/bot{token}"

    def _call(self, method: str, payload: dict[str, Any]) -> dict[str, Any]:
        endpoint = f"{self._base_url}/{method}"
        data = json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            endpoint,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=70) as response:
                body = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            details = exc.read().decode("utf-8", errors="replace")
            raise TelegramAPIError(
                f"HTTP {exc.code} in {method}: {details}"
            ) from exc
        except urllib.error.URLError as exc:
            raise TelegramAPIError(f"Network error in {method}: {exc}") from exc

        parsed = json.loads(body)
        if not parsed.get("ok"):
            raise TelegramAPIError(f"Telegram error in {method}: {parsed}")
        return parsed["result"]

    def get_updates(self, offset: int, timeout_sec: int) -> list[dict[str, Any]]:
        return self._call(
            "getUpdates",
            {
                "offset": offset,
                "timeout": timeout_sec,
                "allowed_updates": ["message"],
            },
        )

    def send_message(
        self,
        chat_id: int,
        text: str,
        reply_markup: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": True,
        }
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        return self._call("sendMessage", payload)

    def send_chat_action(self, chat_id: int, action: str = "typing") -> None:
        self._call("sendChatAction", {"chat_id": chat_id, "action": action})

    def get_file(self, file_id: str) -> dict[str, Any]:
        return self._call("getFile", {"file_id": file_id})

    def download_file(self, file_path: str) -> bytes:
        url = f"{self._file_url}/{file_path}"
        request = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(request, timeout=70) as response:
            return response.read()

