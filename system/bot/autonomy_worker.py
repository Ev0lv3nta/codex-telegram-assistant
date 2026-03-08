from __future__ import annotations

import asyncio
import hashlib
import logging
import re
from contextlib import suppress
from datetime import datetime, timedelta, timezone

from aiogram import Bot

from .autonomy_journal import (
    AutonomyJournalEntry,
    append_autonomy_journal_entry,
    read_recent_autonomy_journal_entries,
)
from .autonomy_planner import extract_autonomy_continuation, extract_self_review, parse_wakeup_decision
from .autonomy_requests import read_active_autonomy_request_summaries
from .autonomy_store import AutonomyStore, AutonomyTask
from .codex_runner import CodexRunner
from .config import Settings
from .delivery import deliver_agent_response, parse_agent_response
from .prompts import build_autonomy_wakeup_prompt
from .queue_store import QueueStore


class AutonomyWorker:
    _LOW_VALUE_STATUS_RE = re.compile(
        r"(mainpid|activeentertimestamp|systemctl|сервис\b.*\bactive\b|service\b.*\bactive\b|pid=|heartbeat\b.*\b(жив|ok|active))",
        re.IGNORECASE,
    )
    _NOTIFY_OWNER_BLOCK_RE = re.compile(
        r"\n?\[\[notify-owner\]\].*?\[\[/notify-owner\]\]\n?",
        re.IGNORECASE | re.DOTALL,
    )

    def __init__(
        self,
        settings: Settings,
        queue_store: QueueStore,
        autonomy_store: AutonomyStore,
        bot: Bot,
        runner: CodexRunner,
        stop_event: asyncio.Event,
        wake_event: asyncio.Event | None = None,
    ) -> None:
        self._settings = settings
        self._queue_store = queue_store
        self._autonomy_store = autonomy_store
        self._bot = bot
        self._runner = runner
        self._stop_event = stop_event
        self._wake_event = wake_event or asyncio.Event()
        self._logger = logging.getLogger("assistant.autonomy")

    @staticmethod
    def _compact_text(value: str, *, limit: int = 160) -> str:
        return " ".join((value or "").split()).strip()[:limit]

    @classmethod
    def _notification_fingerprint(cls, text: str, file_paths: list[str]) -> str:
        payload = f"{cls._compact_text(text, limit=500)}|{'|'.join(sorted(file_paths))}"
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    @classmethod
    def _needs_user_response_pause(cls, text: str) -> bool:
        clean = cls._compact_text(text, limit=500).lower()
        if not clean:
            return False
        if clean.endswith("?"):
            return True
        markers = (
            "подтверди",
            "подтвердите",
            "нужен рестарт",
            "перезапуст",
            "требуется подтверждение",
            "жду ответа",
            "нужен ответ",
            "нужен твой ответ",
            "нужно подтверждение",
            "wait for user",
            "need your confirmation",
            "need your reply",
        )
        return any(marker in clean for marker in markers)

    def _journal(self, status: str, title: str, summary: str, task_id: int | None = None) -> None:
        append_autonomy_journal_entry(
            self._settings.assistant_root,
            AutonomyJournalEntry(
                status=status,
                title=title,
                summary=summary,
                task_id=task_id,
            ),
        )

    def _recent_task_lines(self, chat_id: int, *, limit: int = 4) -> list[str]:
        tasks = self._autonomy_store.list_tasks(chat_id=chat_id, limit=limit, order_by="recent")
        lines: list[str] = []
        for task in tasks:
            title = task.title.strip() or "(без названия)"
            if task.status in {"pending", "running"}:
                details = (task.details or task.result_text or task.error_text or "").strip()
            else:
                details = (task.result_text or task.error_text or task.details or "").strip()
            summary = f"#{task.id} [{task.status}] {title}"
            if details:
                compact = " ".join(details.split())
                summary = f"{summary} — {compact[:140]}".rstrip()
            lines.append(summary)
        return lines

    def _recent_journal_lines(self, *, limit: int = 3) -> list[str]:
        return read_recent_autonomy_journal_entries(
            self._settings.assistant_root,
            limit=limit,
        )

    def _recent_user_lines(self, chat_id: int, *, limit: int = 5) -> list[str]:
        tasks = self._queue_store.list_tasks(
            chat_id=chat_id,
            statuses={"done", "failed"},
            limit=limit,
            order_by="recent",
        )
        lines: list[str] = []
        for task in tasks:
            text = " ".join((task.text or "").split()).strip()
            if not text:
                continue
            lines.append(text[:220])
        return lines

    def _active_request_lines(self, *, limit: int = 5) -> list[str]:
        return read_active_autonomy_request_summaries(
            self._settings.assistant_root,
            limit=limit,
        )

    async def _wait_for_stop_or_wakeup(self, timeout_sec: float) -> None:
        if timeout_sec <= 0:
            return
        stop_wait = asyncio.create_task(self._stop_event.wait())
        wake_wait = asyncio.create_task(self._wake_event.wait())
        try:
            done, pending = await asyncio.wait(
                {stop_wait, wake_wait},
                timeout=max(0, timeout_sec),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if wake_wait in done:
                self._wake_event.clear()
            for task in pending:
                task.cancel()
            for task in pending:
                with suppress(asyncio.CancelledError):
                    await task
        finally:
            if not stop_wait.done():
                stop_wait.cancel()
            if not wake_wait.done():
                wake_wait.cancel()

    def _has_duplicate_waiting_blocker(self, chat_id: int, text: str) -> bool:
        candidate = self._compact_text(text, limit=500)
        if not candidate:
            return False
        waiting_tasks = self._autonomy_store.list_tasks(
            chat_id=chat_id,
            statuses={"waiting_user"},
            limit=10,
            order_by="recent",
        )
        for task in waiting_tasks:
            if self._compact_text(task.result_text, limit=500) == candidate:
                return True
        return False

    @classmethod
    def _token_set(cls, text: str) -> set[str]:
        return {
            token
            for token in re.findall(r"[a-zA-Zа-яА-ЯёЁ0-9]+", (text or "").lower())
            if len(token) >= 4
        }

    @classmethod
    def _looks_like_confirmation_reply(cls, text: str) -> bool:
        clean = cls._compact_text(text, limit=160).lower()
        if not clean:
            return False
        confirmation_markers = (
            "да",
            "ага",
            "ок",
            "окей",
            "хорошо",
            "подтверж",
            "перезапускай",
            "запускай",
            "продолж",
            "делай",
            "можно",
            "согласен",
        )
        return any(marker in clean for marker in confirmation_markers)

    @classmethod
    def _should_resume_waiting_task(cls, task: AutonomyTask, latest_user_text: str) -> bool:
        clean = cls._compact_text(latest_user_text, limit=220)
        if not clean or cls._looks_like_negative_idle_reply(clean):
            return False
        if cls._looks_like_confirmation_reply(clean):
            return True
        user_tokens = cls._token_set(clean)
        if not user_tokens:
            return False
        task_tokens = cls._token_set(" ".join([task.title, task.details, task.result_text]))
        return len(user_tokens & task_tokens) >= 2

    def _resume_relevant_waiting_task(self, chat_id: int, *, user_signal: int) -> int:
        latest_user_lines = self._recent_user_lines(chat_id, limit=1)
        latest_user_text = latest_user_lines[0] if latest_user_lines else ""
        if not latest_user_text:
            return 0
        waiting_tasks = self._autonomy_store.list_tasks(
            chat_id=chat_id,
            statuses={"waiting_user"},
            limit=10,
            order_by="recent",
        )
        for task in waiting_tasks:
            if task.blocked_user_signal is None or task.blocked_user_signal >= user_signal:
                continue
            if not self._should_resume_waiting_task(task, latest_user_text):
                continue
            self._autonomy_store.requeue_task(task.id)
            return 1
        return 0

    @classmethod
    def _looks_like_negative_idle_reply(cls, text: str) -> bool:
        clean = cls._compact_text(text, limit=160).lower()
        if not clean:
            return False
        markers = (
            "нет",
            "не надо",
            "не нужно",
            "ничего не нужно",
            "сейчас ничего не нужно",
            "пока не нужно",
            "ничего",
        )
        return any(marker in clean for marker in markers)

    @classmethod
    def _looks_like_positive_need_signal(cls, text: str) -> bool:
        clean = cls._compact_text(text, limit=220).lower()
        if not clean:
            return False
        if cls._looks_like_negative_idle_reply(clean):
            return False
        markers = (
            "нужно",
            "надо",
            "интересно",
            "изуч",
            "поресерч",
            "ресерч",
            "исслед",
            "собери",
            "методич",
            "сводк",
            "найди",
            "посмотри",
        )
        return any(marker in clean for marker in markers)

    @classmethod
    def _is_low_value_notification(cls, task: AutonomyTask, text: str) -> bool:
        clean = cls._compact_text(text, limit=500)
        if not clean:
            return True
        if task.kind == "review" and cls._LOW_VALUE_STATUS_RE.search(clean):
            return True
        if clean.lower().startswith("сервис бота сейчас живой"):
            return True
        return False

    @classmethod
    def _owner_notification_text(cls, task: AutonomyTask, text: str) -> str:
        clean_text, _ = extract_autonomy_continuation(text or "")
        clean_text, _ = extract_self_review(clean_text)
        clean_text, _ = cls._extract_notify_owner(clean_text)
        lines: list[str] = []
        for raw_line in clean_text.splitlines():
            line = raw_line.rstrip()
            if not line.strip():
                lines.append("")
                continue
            upper = line.strip().upper()
            if upper in {"ACTION: ENQUEUE", "ACTION: STEP", "ACTION: COMPLETE", "DETAILS:", "RESULT:"}:
                continue
            if upper.startswith(("TITLE:", "KIND:", "PRIORITY:", "DELAY_SEC:")):
                continue
            lines.append(line)
        return "\n".join(lines).strip()

    @classmethod
    def _extract_notify_owner(cls, text: str) -> tuple[str, bool]:
        raw = text or ""
        has_marker = "[[notify-owner]]" in raw.lower()
        clean = cls._NOTIFY_OWNER_BLOCK_RE.sub("\n", raw)
        clean = re.sub(r"\n{3,}", "\n\n", clean).strip()
        return clean, has_marker

    @staticmethod
    def _is_internal_complete_closure(raw_message: str, *, file_paths: list[str]) -> bool:
        if file_paths:
            return False
        return (raw_message or "").strip().upper().startswith("ACTION: COMPLETE")

    @staticmethod
    def _scheduled_after(delay_sec: int) -> str:
        return (datetime.now(timezone.utc) + timedelta(seconds=delay_sec)).isoformat()

    def _schedule_mode(self, chat_id: int, mode: str, *, delay_sec: int | None = None, at: str | None = None) -> None:
        self._autonomy_store.set_mode(chat_id, mode)
        if at:
            self._autonomy_store.set_next_wakeup(chat_id, at)
            return
        if delay_sec is not None:
            self._autonomy_store.schedule_next_wakeup_in(chat_id, delay_sec)

    @staticmethod
    def _compose_stored_result(
        clean_message: str,
        *,
        suffix: str,
        followup_id: int | None = None,
        followup_delay_sec: int = 0,
        self_reviews: list[str] | None = None,
    ) -> str:
        chunks: list[str] = []
        text = clean_message.strip()
        if text:
            chunks.append(text)
        if followup_id is not None:
            chunks.append(
                f"[autonomy-next-task: #{followup_id} scheduled_after={followup_delay_sec}s]"
            )
        if suffix.strip():
            chunks.append(suffix.strip())
        if self_reviews:
            chunks.extend(item.strip() for item in self_reviews if item.strip())
        return "\n\n".join(chunks).strip()

    @staticmethod
    def _format_self_review_block(change: str, why: str, risk: str, check: str) -> str:
        lines = ["[[self-review]]"]
        if change.strip():
            lines.append(f"CHANGE: {change.strip()}")
        if why.strip():
            lines.append(f"WHY: {why.strip()}")
        if risk.strip():
            lines.append(f"RISK: {risk.strip()}")
        if check.strip():
            lines.append(f"CHECK: {check.strip()}")
        lines.append("[[/self-review]]")
        return "\n".join(lines)

    async def run(self) -> None:
        if not self._settings.autonomy_enabled:
            return

        while not self._stop_event.is_set():
            timeout_sec = self._settings.autonomy_loop_poll_sec
            chat_id = self._queue_store.get_last_active_chat_id()
            if chat_id is not None:
                # When autonomy is explicitly stopped, `next_wakeup` is cleared.
                # Treat that state as "sleep until wake_event / periodic poll",
                # not as "wake immediately", otherwise the loop spins hot.
                if not self._autonomy_store.autonomy_paused(chat_id):
                    until_due = self._autonomy_store.seconds_until_next_wakeup(chat_id)
                    if until_due is None or until_due <= 0:
                        timeout_sec = 0
                    else:
                        timeout_sec = min(timeout_sec, until_due)
            await self._wait_for_stop_or_wakeup(timeout_sec)
            if self._stop_event.is_set():
                return
            await self._run_once()

    async def _run_once(self) -> None:
        chat_id = self._queue_store.get_last_active_chat_id()
        if chat_id is None:
            self._logger.debug("Autonomy heartbeat skipped: no active chat")
            return
        if self._autonomy_store.autonomy_paused(chat_id):
            self._autonomy_store.set_mode(chat_id, "stopped")
            return
        current_signal = self._queue_store.get_user_signal(chat_id)
        force_wakeup = current_signal > self._autonomy_store.get_last_seen_user_signal(chat_id)
        if not self._autonomy_store.wakeup_due(chat_id) and not force_wakeup:
            return
        self._autonomy_store.mark_heartbeat("loop")

        if self._queue_store.pending_user_tasks(chat_id) > 0:
            self._schedule_mode(
                chat_id,
                "user_busy",
                delay_sec=self._settings.autonomy_busy_retry_sec,
            )
            self._autonomy_store.mark_heartbeat("skipped_user_pending")
            return

        resumed_waiting = self._resume_relevant_waiting_task(
            chat_id,
            user_signal=current_signal,
        )
        if resumed_waiting:
            self._logger.info(
                "Resumed %s waiting autonomy task(s) for chat=%s after user signal=%s",
                resumed_waiting,
                chat_id,
                current_signal,
            )

        session_owner = self._queue_store.get_session_owner(chat_id)
        if session_owner and session_owner != "autonomy":
            self._schedule_mode(
                chat_id,
                "session_busy",
                delay_sec=self._settings.autonomy_busy_retry_sec,
            )
            self._autonomy_store.mark_heartbeat("skipped_session_busy")
            return

        baseline_signal = current_signal
        if not self._queue_store.try_acquire_session_lease(
            chat_id=chat_id,
            owner="autonomy",
            ttl_sec=self._settings.session_lease_sec,
        ):
            self._schedule_mode(
                chat_id,
                "session_busy",
                delay_sec=self._settings.autonomy_busy_retry_sec,
            )
            self._autonomy_store.mark_heartbeat("skipped_session_busy")
            return

        try:
            self._autonomy_store.set_last_seen_user_signal(chat_id, current_signal)
            active_request_lines = self._active_request_lines()
            task = self._autonomy_store.claim_next_ready_task(chat_id=chat_id)
            if task is None:
                handled_idle = await self._maybe_handle_idle_state(
                    chat_id=chat_id,
                    baseline_signal=baseline_signal,
                    active_request_lines=active_request_lines,
                )
                if handled_idle:
                    return
                next_pending = self._autonomy_store.get_next_pending_task(chat_id)
                next_pending_at = next_pending.scheduled_for if next_pending is not None else ""
                if next_pending_at:
                    self._autonomy_store.set_active_mission(
                        chat_id,
                        task_id=next_pending.id,
                        title=next_pending.title,
                        details=next_pending.details,
                        kind=next_pending.kind,
                        source=next_pending.source,
                        phase="scheduled",
                        scheduled_for=next_pending_at,
                    )
                    self._schedule_mode(chat_id, "sleeping_scheduled", at=next_pending_at)
                    self._autonomy_store.mark_heartbeat("sleeping_scheduled")
                    return
            await self._run_wakeup(chat_id, task, baseline_signal, active_request_lines)
        finally:
            self._queue_store.release_session_lease(chat_id, "autonomy")

    async def _maybe_handle_idle_state(
        self,
        *,
        chat_id: int,
        baseline_signal: int,
        active_request_lines: list[str],
    ) -> bool:
        if active_request_lines:
            self._autonomy_store.clear_idle_snooze(chat_id)
            self._autonomy_store.clear_idle_interest_prompt(chat_id)
            return False

        if self._autonomy_store.active_task_count(chat_id) > 0:
            return False

        waiting_user_count = self._autonomy_store.counts_for_chat(chat_id).get("waiting_user", 0)
        if waiting_user_count > 0:
            return False

        mission = self._autonomy_store.get_active_mission(chat_id)

        latest_user_lines = self._recent_user_lines(chat_id, limit=1)
        latest_user_line = latest_user_lines[0] if latest_user_lines else ""
        if self._looks_like_negative_idle_reply(latest_user_line):
            self._autonomy_store.mark_idle_snooze_until(
                chat_id,
                self._scheduled_after(self._settings.autonomy_idle_sleep_sec),
            )
            self._schedule_mode(
                chat_id,
                "cooldown",
                delay_sec=self._settings.autonomy_idle_sleep_sec,
            )
            self._autonomy_store.mark_heartbeat("sleeping_user_declined")
            return True
        if self._looks_like_positive_need_signal(latest_user_line):
            self._autonomy_store.clear_idle_snooze(chat_id)
            return False

        prompted_at = self._autonomy_store.get_idle_interest_prompt_at(chat_id)
        prompted_signal = self._autonomy_store.get_idle_interest_prompt_signal(chat_id)
        if prompted_at and baseline_signal > prompted_signal:
            self._autonomy_store.clear_idle_interest_prompt(chat_id)
            if self._looks_like_negative_idle_reply(latest_user_line):
                self._autonomy_store.mark_idle_snooze_until(
                    chat_id,
                    self._scheduled_after(self._settings.autonomy_idle_sleep_sec),
                )
                self._schedule_mode(
                    chat_id,
                    "cooldown",
                    delay_sec=self._settings.autonomy_idle_sleep_sec,
                )
                self._autonomy_store.mark_heartbeat("sleeping_user_declined")
                return True

        if self._autonomy_store.idle_snoozed(chat_id):
            self._schedule_mode(
                chat_id,
                "cooldown",
                at=self._autonomy_store.get_idle_snooze_until(chat_id),
            )
            self._autonomy_store.mark_heartbeat("sleeping_idle")
            return True

        if mission is None and not self._settings.autonomy_idle_ask_enabled:
            self._autonomy_store.clear_active_mission(chat_id)
            self._schedule_mode(
                chat_id,
                "idle",
                delay_sec=self._settings.autonomy_default_sleep_sec,
            )
            self._autonomy_store.mark_heartbeat("noop")
            return True

        self._schedule_mode(
            chat_id,
            "idle",
            delay_sec=self._settings.autonomy_default_sleep_sec,
        )
        self._autonomy_store.mark_heartbeat("sleeping_idle")
        return True

    async def _run_wakeup(
        self,
        chat_id: int,
        task: AutonomyTask | None,
        baseline_signal: int,
        active_request_lines: list[str],
    ) -> None:
        self._schedule_mode(chat_id, "active_mission", delay_sec=self._settings.autonomy_busy_retry_sec)
        session_id = self._queue_store.get_chat_session_id(chat_id)
        current_task = task
        persisted_task_id = task.id if task is not None else None
        persisted_task_source = task.source if task is not None else "heartbeat"
        persisted_parent_id = task.parent_task_id if task is not None else None
        current_title = task.title if task is not None else ""
        current_details = task.details if task is not None else ""
        current_kind = task.kind if task is not None else "general"
        current_priority = task.priority if task is not None else 100
        step_results: list[str] = []
        step_self_reviews: list[str] = []
        parsed_texts: list[str] = []

        self._autonomy_store.set_active_mission(
            chat_id,
            task_id=persisted_task_id,
            title=current_title or "Автономный сеанс",
            details=current_details,
            kind=current_kind,
            source=persisted_task_source,
            phase="running",
            scheduled_for=task.scheduled_for if task is not None else "",
        )

        for step_index in range(self._settings.autonomy_session_step_limit):
            prompt = build_autonomy_wakeup_prompt(
                current_task_id=persisted_task_id,
                current_task_title=current_title,
                current_task_details=current_details,
                current_task_kind=current_kind,
                current_task_continuation_count=current_task.continuation_count if current_task is not None else 0,
                active_request_lines=active_request_lines,
                recent_task_lines=self._recent_task_lines(chat_id),
                recent_journal_lines=self._recent_journal_lines(),
                recent_user_lines=self._recent_user_lines(chat_id),
                include_bootstrap=not bool(session_id),
            )
            result = await asyncio.to_thread(self._runner.run, prompt, session_id)

            if result.session_id and result.session_id != session_id:
                session_id = result.session_id
                self._queue_store.set_chat_session_id(chat_id, result.session_id)

            user_signal_changed = self._queue_store.get_user_signal(chat_id) != baseline_signal
            suffix = ""
            if user_signal_changed:
                suffix = "\n\n[autonomy-paused: user activity detected]"

            if not result.success:
                failure_text = (result.message or "").strip() + suffix
                if persisted_task_id is not None:
                    self._autonomy_store.fail_task(persisted_task_id, failure_text)
                    self._autonomy_store.clear_active_mission(chat_id)
                    self._schedule_mode(
                        chat_id,
                        "cooldown",
                        delay_sec=self._settings.autonomy_default_sleep_sec,
                    )
                    self._journal(
                        "failed",
                        current_title or "Автономный шаг",
                        (result.message or "Автономная задача завершилась ошибкой.").strip(),
                        task_id=persisted_task_id,
                    )
                self._autonomy_store.mark_heartbeat(
                    "failed_after_user_interrupt" if user_signal_changed else "failed"
                )
                return

            clean_message, continuation = extract_autonomy_continuation(result.message or "")
            clean_message, self_review = extract_self_review(clean_message)
            decision = parse_wakeup_decision(clean_message)
            if (
                decision.action != "STEP"
                and clean_message.strip()
                and clean_message.strip().upper() != "ACTION: NOOP"
            ):
                fallback_title = current_title or "Автономный шаг"
                fallback_kind = current_kind or "general"
                fallback_details = current_details or ""
                decision = parse_wakeup_decision(
                    "\n".join(
                        [
                            "ACTION: STEP",
                            f"TITLE: {fallback_title}",
                            f"KIND: {fallback_kind}",
                            "PRIORITY: 100",
                            "DETAILS:",
                            fallback_details,
                            "RESULT:",
                            clean_message.strip(),
                        ]
                    ).strip()
                )

            if decision.action == "COMPLETE":
                if persisted_task_id is None:
                    self._autonomy_store.clear_active_mission(chat_id)
                    self._schedule_mode(
                        chat_id,
                        "idle",
                        delay_sec=self._settings.autonomy_default_sleep_sec,
                    )
                    self._autonomy_store.mark_heartbeat(
                        "noop_after_user_interrupt" if user_signal_changed else "noop"
                    )
                    return

                result_text = decision.result_text.strip() or "Текущая автономная задача закрыта без дополнительного шага."
                final_text = "\n\n".join([*step_results, result_text]).strip()
                stored = self._compose_stored_result(
                    final_text,
                    suffix=suffix,
                    self_reviews=step_self_reviews,
                )
                self._autonomy_store.complete_task(persisted_task_id, stored)
                self._autonomy_store.clear_active_mission(chat_id)
                self._schedule_mode(
                    chat_id,
                    "idle",
                    delay_sec=self._settings.autonomy_default_sleep_sec,
                )
                parsed = parse_agent_response(final_text)
                await self._maybe_notify_completion(
                    chat_id=chat_id,
                    task=AutonomyTask(
                        id=persisted_task_id,
                        chat_id=chat_id,
                        kind=current_kind or "general",
                        title=current_title or "Автономный шаг",
                        details=current_details,
                        priority=current_priority,
                        status="done",
                        created_at=task.created_at if task is not None else "",
                        scheduled_for=task.scheduled_for if task is not None else "",
                        parent_task_id=persisted_parent_id,
                        source=persisted_task_source,
                        started_at=task.started_at if task is not None else None,
                        finished_at=None,
                        blocked_user_signal=None,
                        result_text=final_text,
                        error_text="",
                    ),
                    text=parsed.text,
                    file_paths=parsed.file_paths,
                    user_signal_changed=user_signal_changed,
                    raw_message=final_text,
                )
                self._autonomy_store.mark_heartbeat(
                    "closed_after_user_interrupt" if user_signal_changed else "closed"
                )
                summary = parsed.text or final_text
                self._journal("completed", current_title or "Автономный шаг", summary, task_id=persisted_task_id)
                return

            if decision.action != "STEP":
                if persisted_task_id is not None:
                    self._autonomy_store.requeue_task(persisted_task_id)
                    self._autonomy_store.set_active_mission(
                        chat_id,
                        task_id=persisted_task_id,
                        title=current_title or "Автономный сеанс",
                        details=current_details,
                        kind=current_kind,
                        source=persisted_task_source,
                        phase="scheduled" if current_task is not None and current_task.scheduled_for else "running",
                        scheduled_for=current_task.scheduled_for if current_task is not None else "",
                    )
                self._schedule_mode(
                    chat_id,
                    "idle",
                    delay_sec=self._settings.autonomy_default_sleep_sec,
                )
                self._autonomy_store.mark_heartbeat(
                    "noop_after_user_interrupt" if user_signal_changed else "noop"
                )
                return

            effective_title = decision.title.strip() or (current_title or "Автономный шаг")
            effective_kind = decision.kind.strip() or (current_kind or "general")
            effective_details = decision.details.strip() or current_details
            effective_priority = decision.priority
            result_text = decision.result_text.strip() or clean_message.strip()
            if self_review is not None:
                step_self_reviews.append(
                    self._format_self_review_block(
                        self_review.change,
                        self_review.why,
                        self_review.risk,
                        self_review.check,
                    )
                )
            blocks_on_user = self._needs_user_response_pause(result_text)

            if (
                persisted_task_id is None
                and blocks_on_user
                and self._has_duplicate_waiting_blocker(chat_id, result_text)
            ):
                self._schedule_mode(
                    chat_id,
                    "waiting_user",
                    delay_sec=self._settings.autonomy_idle_sleep_sec,
                )
                self._autonomy_store.mark_heartbeat("noop")
                return

            if persisted_task_id is None:
                persisted_task_id = self._autonomy_store.enqueue_task(
                    chat_id=chat_id,
                    title=effective_title,
                    details=effective_details,
                    kind=effective_kind,
                    priority=effective_priority,
                    source="heartbeat",
                )
                persisted_task_source = "heartbeat"
            self._autonomy_store.set_active_mission(
                chat_id,
                task_id=persisted_task_id,
                title=effective_title,
                details=effective_details,
                kind=effective_kind,
                source=persisted_task_source,
                phase="running",
                scheduled_for=current_task.scheduled_for if current_task is not None else "",
            )

            current_title = effective_title
            current_kind = effective_kind
            current_details = effective_details
            current_priority = effective_priority
            step_results.append(result_text)
            parsed = parse_agent_response(result_text)
            if parsed.text:
                parsed_texts.append(parsed.text)

            if blocks_on_user:
                final_text = "\n\n".join(step_results).strip()
                self._autonomy_store.wait_for_user(
                    persisted_task_id,
                    self._compose_stored_result(
                        final_text,
                        suffix=suffix,
                        self_reviews=step_self_reviews,
                    ),
                    user_signal=baseline_signal,
                )
                self._schedule_mode(
                    chat_id,
                    "waiting_user",
                    delay_sec=self._settings.autonomy_idle_sleep_sec,
                )
                self._autonomy_store.set_active_mission(
                    chat_id,
                    task_id=persisted_task_id,
                    title=current_title,
                    details=current_details,
                    kind=current_kind,
                    source=persisted_task_source,
                    phase="waiting_user",
                    scheduled_for="",
                )
                await self._maybe_notify_completion(
                    chat_id=chat_id,
                    task=AutonomyTask(
                        id=persisted_task_id,
                        chat_id=chat_id,
                        kind=current_kind,
                        title=current_title,
                        details=current_details,
                        priority=current_priority,
                        status="waiting_user",
                        created_at=task.created_at if task is not None else "",
                        scheduled_for="",
                        parent_task_id=persisted_parent_id,
                        source=persisted_task_source,
                        started_at=task.started_at if task is not None else None,
                        finished_at=None,
                        blocked_user_signal=baseline_signal,
                        result_text=final_text,
                        error_text="",
                    ),
                    text=parsed.text,
                    file_paths=parsed.file_paths,
                    user_signal_changed=user_signal_changed,
                    raw_message=final_text,
                )
                self._autonomy_store.mark_heartbeat(
                    "waiting_after_user_interrupt" if user_signal_changed else "waiting_user"
                )
                summary = parsed.text or final_text
                if user_signal_changed:
                    summary += " Во время шага пришло сообщение владельца."
                self._journal("waiting_user", current_title, summary, task_id=persisted_task_id)
                return

            inline_continue = (
                continuation is not None
                and not user_signal_changed
                and continuation.delay_sec <= 0
                and step_index + 1 < self._settings.autonomy_session_step_limit
            )
            if inline_continue:
                current_title = continuation.title.strip() or effective_title
                current_kind = continuation.kind.strip() or effective_kind
                current_details = continuation.details.strip() or effective_details
                current_priority = continuation.priority
                continue

            final_text = "\n\n".join(step_results).strip()
            continued_task = False
            notification_task_status = "done"
            notification_task_scheduled_for = ""
            journal_status = "completed"
            continuation_limit_reached = (
                current_task is not None
                and current_task.continuation_count >= self._settings.autonomy_max_task_continuations
            )
            if continuation is not None and not user_signal_changed and continuation_limit_reached:
                final_text = (
                    f"{final_text}\n\n[autonomy-followup-suppressed: continuation limit reached]"
                ).strip()

            if (
                continuation is not None
                and not user_signal_changed
                and not continuation_limit_reached
            ):
                followup_delay_sec = max(1, continuation.delay_sec)
                next_title = continuation.title.strip() or current_title
                next_details = continuation.details.strip() or current_details
                next_kind = continuation.kind.strip() or current_kind
                next_priority = continuation.priority
                next_scheduled_for = self._scheduled_after(followup_delay_sec)
                self._autonomy_store.continue_task(
                    persisted_task_id,
                    title=next_title,
                    details=next_details,
                    kind=next_kind,
                    priority=next_priority,
                    scheduled_for=next_scheduled_for,
                    progress_text=self._compose_stored_result(
                        final_text,
                        suffix=suffix,
                        self_reviews=step_self_reviews,
                    ),
                )
                self._autonomy_store.set_active_mission(
                    chat_id,
                    task_id=persisted_task_id,
                    title=next_title,
                    details=next_details,
                    kind=next_kind,
                    source=persisted_task_source,
                    phase="scheduled",
                    scheduled_for=next_scheduled_for,
                )
                self._schedule_mode(chat_id, "sleeping_scheduled", delay_sec=followup_delay_sec)
                continued_task = True
                current_title = next_title
                current_details = next_details
                current_kind = next_kind
                current_priority = next_priority
                notification_task_status = "pending"
                notification_task_scheduled_for = next_scheduled_for
                journal_status = "continued"
            else:
                self._schedule_mode(
                    chat_id,
                    "cooldown" if active_request_lines else "idle",
                    delay_sec=self._settings.autonomy_default_sleep_sec,
                )
                self._autonomy_store.complete_task(
                    persisted_task_id,
                    self._compose_stored_result(
                        final_text,
                        suffix=suffix,
                        self_reviews=step_self_reviews,
                    ),
                )
                self._autonomy_store.clear_active_mission(chat_id)
            await self._maybe_notify_completion(
                chat_id=chat_id,
                task=AutonomyTask(
                    id=persisted_task_id,
                    chat_id=chat_id,
                    kind=current_kind,
                    title=current_title,
                    details=current_details,
                    priority=current_priority,
                    status=notification_task_status,
                    created_at=task.created_at if task is not None else "",
                    scheduled_for=notification_task_scheduled_for,
                    parent_task_id=persisted_parent_id,
                    source=persisted_task_source,
                    started_at=task.started_at if task is not None else None,
                    finished_at=None,
                    blocked_user_signal=None,
                    result_text=final_text,
                    error_text="",
                ),
                text="\n\n".join(parsed_texts).strip() or final_text,
                file_paths=parsed.file_paths,
                user_signal_changed=user_signal_changed,
                raw_message=final_text,
            )
            self._autonomy_store.mark_heartbeat(
                "completed_after_user_interrupt"
                if user_signal_changed
                else ("completed_continued" if continued_task else "completed")
            )
            summary = "\n\n".join(parsed_texts).strip() or final_text or "Автономный шаг завершился без текстового результата."
            if continued_task:
                summary += " Дальше продолжение этой же задачи уже запланировано."
            if user_signal_changed:
                summary += " Во время шага пришло сообщение владельца."
            self._journal(journal_status, current_title, summary, task_id=persisted_task_id)
            return

    async def _maybe_notify_completion(
        self,
        *,
        chat_id: int,
        task: AutonomyTask,
        text: str,
        file_paths: list[str],
        user_signal_changed: bool,
        raw_message: str,
    ) -> None:
        if not self._settings.autonomy_notify_enabled:
            return
        if user_signal_changed:
            return
        if not self._autonomy_store.notify_due(
            chat_id,
            cooldown_sec=self._settings.autonomy_notify_cooldown_sec,
        ):
            return

        clean_text = text.strip()
        min_chars = max(1, self._settings.autonomy_notify_min_chars)
        looks_like_question = clean_text.endswith("?") or clean_text.endswith("؟")
        _, notify_owner = self._extract_notify_owner(raw_message)
        should_notify = (
            bool(file_paths)
            or len(clean_text) >= min_chars
            or (looks_like_question and bool(clean_text))
        )
        if not should_notify:
            return
        if self._is_internal_complete_closure(raw_message, file_paths=file_paths):
            return
        if (
            task.kind in {"project", "maintenance", "review"}
            and not file_paths
            and not looks_like_question
            and not notify_owner
        ):
            return
        if self._is_low_value_notification(task, clean_text):
            return
        fingerprint = self._notification_fingerprint(clean_text, file_paths)
        if fingerprint == self._autonomy_store.get_notify_last_fingerprint(chat_id):
            return

        owner_text = self._owner_notification_text(task, clean_text)
        owner_message = owner_text
        if file_paths:
            directives = "\n".join(f"[[send-file:{path}]]" for path in file_paths)
            owner_message = f"{owner_text}\n{directives}".strip() if owner_text else directives

        delivery = await deliver_agent_response(
            bot=self._bot,
            chat_id=chat_id,
            settings=self._settings,
            raw_message=owner_message,
            logger=self._logger,
            text_prefix="Автономно: ",
            files_only_fallback="Автономно: файл(ы) готовы.",
        )
        if delivery.final_text or delivery.sent_files:
            self._autonomy_store.mark_notify_sent(chat_id)
            self._autonomy_store.mark_notify_fingerprint(chat_id, fingerprint)
