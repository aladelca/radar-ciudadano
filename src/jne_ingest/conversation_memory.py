from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import hmac
import re
import secrets
from threading import Lock
from typing import Deque, Dict, List, Optional


@dataclass
class ConversationTurn:
    query: str
    summary: str
    mode: str
    timestamp_utc: str


class ConversationMemoryStore:
    _SESSION_PATTERN = re.compile(r"^s1_([0-9a-f]{16})_([0-9a-f]{16})$")

    def __init__(
        self,
        *,
        max_sessions: int = 500,
        max_turns_per_session: int = 20,
        signing_key: Optional[str] = None,
    ) -> None:
        self._max_sessions = max(10, max_sessions)
        self._max_turns = max(3, max_turns_per_session)
        self._lock = Lock()
        self._sessions: Dict[str, Deque[ConversationTurn]] = {}
        self._session_order: Deque[str] = deque()
        normalized_key = str(signing_key or "").strip()
        self._signing_key = normalized_key.encode("utf-8") if normalized_key else secrets.token_bytes(32)

    def resolve_session_id(self, session_id: Optional[str]) -> str:
        normalized = str(session_id or "").strip()
        if normalized and self._is_valid_session_id(normalized):
            return normalized
        return self._issue_session_id()

    def get_recent_turns(self, session_id: str, *, limit: int = 3) -> List[Dict[str, str]]:
        session_key = self.resolve_session_id(session_id)
        with self._lock:
            turns = list(self._sessions.get(session_key, deque()))
        return [
            {
                "query": t.query,
                "summary": t.summary,
                "mode": t.mode,
                "timestamp_utc": t.timestamp_utc,
            }
            for t in turns[-max(1, limit) :]
        ]

    def append_turn(self, *, session_id: str, query: str, summary: str, mode: str) -> None:
        session_key = self.resolve_session_id(session_id)
        turn = ConversationTurn(
            query=query.strip(),
            summary=summary.strip(),
            mode=mode.strip() or "unknown",
            timestamp_utc=datetime.now(timezone.utc).isoformat(),
        )
        with self._lock:
            if session_key not in self._sessions:
                self._sessions[session_key] = deque(maxlen=self._max_turns)
            self._touch_session(session_key)
            self._sessions[session_key].append(turn)
            self._prune_sessions_if_needed()

    def _prune_sessions_if_needed(self) -> None:
        while len(self._sessions) > self._max_sessions and self._session_order:
            oldest = self._session_order.popleft()
            self._sessions.pop(oldest, None)

    def _touch_session(self, session_id: str) -> None:
        try:
            self._session_order.remove(session_id)
        except ValueError:
            pass
        self._session_order.append(session_id)

    def _issue_session_id(self) -> str:
        nonce = secrets.token_hex(8)
        sig = self._build_signature(nonce)
        return f"s1_{nonce}_{sig}"

    def _build_signature(self, nonce: str) -> str:
        digest = hmac.new(
            self._signing_key,
            f"s1:{nonce}".encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return digest[:16]

    def _is_valid_session_id(self, session_id: str) -> bool:
        match = self._SESSION_PATTERN.match(session_id)
        if not match:
            return False
        nonce, signature = match.groups()
        expected = self._build_signature(nonce)
        return hmac.compare_digest(signature, expected)
