from __future__ import annotations

AGENTS_MD_PATH = "/root/personal-assistant/AGENTS.md"
AUTONOMY_PLAN_PATH = "/root/personal-assistant/topics/autonomy-companion-plan.md"
ASSISTANT_CONSTITUTION_PATH = "/root/personal-assistant/topics/assistant-constitution.md"
AUTONOMY_JOURNAL_GLOB = "/root/personal-assistant/system/tasks/autonomy_journal/YYYY-MM-DD.md"
AUTONOMY_REQUESTS_PATH = "/root/personal-assistant/system/tasks/autonomy_requests.md"
MEMORY_FILES = [
    "/root/personal-assistant/memory/about_user.md",
    "/root/personal-assistant/memory/about_self.md",
    "/root/personal-assistant/memory/open_loops.md",
    "/root/personal-assistant/memory/current_world_model.md",
    "/root/personal-assistant/memory/initiative_backlog.md",
    "/root/personal-assistant/memory/change_log.md",
]

from .memory_store import build_memory_prompt_note


def _attachments_block(attachments: list[str]) -> str:
    if not attachments:
        return ""
    lines = [f"- `{item}`" for item in attachments]
    return "\n".join(lines)


def _bootstrap_prefix(include_bootstrap: bool) -> str:
    if not include_bootstrap:
        return ""
    return (
        "Перед выполнением запроса открой и прочитай файл "
        f"`{AGENTS_MD_PATH}`. Следуй ему как основным инструкциям этой сессии.\n\n"
    )


def _send_files_protocol_note() -> str:
    return (
        "Если нужно отправить пользователю один или несколько файлов в Telegram, "
        "добавь в КОНЕЦ ответа отдельные строки формата:\n"
        "[[send-file:daily/2026-02-22.md]]\n"
        "[[send-file:topics/note.md]]\n"
        "Каждый путь указывай отдельно, только путь на сервере. "
        "Не оборачивай эти строки в код-блок."
    )


def _risky_action_confirmation_note() -> str:
    return (
        "Перед любым рискованным действием (удаление/перезапуск сервисов/массовые правки) "
        "сначала запроси у пользователя явное подтверждение, затем выполняй. "
        "Если пользователь просит перезапустить `personal-assistant-bot.service` из этого же бота, "
        "не используй первым шагом голый `systemctl restart`; предпочитай "
        "`python3 -m system.bot.self_restart request`, чтобы рестарт был зафиксирован в состоянии до обрыва процесса."
    )


def _wakeup_context_block(
    active_request_lines: list[str] | None = None,
    recent_task_lines: list[str] | None = None,
    recent_journal_lines: list[str] | None = None,
    recent_user_lines: list[str] | None = None,
) -> str:
    requests = [line.strip() for line in (active_request_lines or []) if line.strip()]
    tasks = [line.strip() for line in (recent_task_lines or []) if line.strip()]
    journal = [line.strip() for line in (recent_journal_lines or []) if line.strip()]
    recent_user = [line.strip() for line in (recent_user_lines or []) if line.strip()]
    if not requests and not tasks and not journal and not recent_user:
        return ""

    parts = ["Недавний контекст пробуждения:"]
    if requests:
        parts.append("Активные поручения владельца:")
        parts.extend(f"- {line}" for line in requests)
    if recent_user:
        parts.append("Последние пользовательские сигналы:")
        parts.extend(f"- {line}" for line in recent_user)
    if tasks:
        parts.append("Последние задачи:")
        parts.extend(f"- {line}" for line in tasks)
    if journal:
        parts.append("Краткие записи журнала:")
        parts.extend(f"- {line}" for line in journal)
    return "\n".join(parts)


def _workspace_memory_note() -> str:
    memory_files = "\n".join(f"- `{path}`" for path in MEMORY_FILES)
    return (
        "Перед осмыслением шага не тащи память в ответ по памяти и не пересказывай её механически.\n"
        "При необходимости сам открой нужные файлы workspace и опирайся на них как на источник истины:\n"
        f"{memory_files}\n"
        f"- `{ASSISTANT_CONSTITUTION_PATH}`\n"
        f"- `{AUTONOMY_PLAN_PATH}`\n"
        f"- `{AUTONOMY_JOURNAL_GLOB}`\n"
        f"- `{AUTONOMY_REQUESTS_PATH}`\n"
        "Открывай только то, что действительно нужно для текущего шага."
    )


def build_prompt(
    user_text: str,
    attachments: list[str],
    include_bootstrap: bool = False,
) -> str:
    text = (user_text or "").strip()
    attachment_block = _attachments_block(attachments)
    prefix = _bootstrap_prefix(include_bootstrap)
    send_files_note = _send_files_protocol_note()
    risky_note = _risky_action_confirmation_note()
    memory_note = build_memory_prompt_note()

    if text and not attachments:
        return f"{prefix}{text}\n\n{memory_note}\n\n{send_files_note}\n\n{risky_note}"

    if text and attachments:
        return (
            f"{prefix}{text}\n\n"
            "Вложения пользователя (пути на сервере):\n"
            f"{attachment_block}\n\n"
            f"{memory_note}\n\n"
            f"{send_files_note}\n\n"
            f"{risky_note}"
        )

    return (
        f"{prefix}"
        "Пользователь отправил вложения без текста.\n"
        "Вложения пользователя (пути на сервере):\n"
        f"{attachment_block}\n\n"
        f"{memory_note}\n\n"
        f"{send_files_note}\n\n"
        f"{risky_note}"
    )


def build_autonomy_wakeup_prompt(
    *,
    current_task_id: int | None = None,
    current_task_title: str = "",
    current_task_details: str = "",
    current_task_kind: str = "general",
    current_task_continuation_count: int = 0,
    active_request_lines: list[str] | None = None,
    recent_task_lines: list[str] | None = None,
    recent_journal_lines: list[str] | None = None,
    recent_user_lines: list[str] | None = None,
    include_bootstrap: bool = False,
) -> str:
    prefix = _bootstrap_prefix(include_bootstrap)
    send_files_note = _send_files_protocol_note()
    risky_note = _risky_action_confirmation_note()
    memory_note = build_memory_prompt_note()
    wakeup_context = _wakeup_context_block(
        active_request_lines,
        recent_task_lines,
        recent_journal_lines,
        recent_user_lines,
    )
    workspace_memory_note = _workspace_memory_note()

    current_task_block = ""
    if current_task_id is not None:
        details_block = current_task_details.strip() or "(без дополнительных деталей)"
        continuation_block = ""
        if current_task_continuation_count > 0:
            continuation_block = (
                f"- continuation_count: {current_task_continuation_count}\n"
                "Эта линия уже продолжалась раньше. Не дроби её на новый микрошаг, "
                "если текущий объём можно честно закрыть прямо сейчас.\n"
            )
            if current_task_continuation_count >= 2:
                continuation_block += (
                    "Лимит мелких follow-up'ов по этой линии уже практически исчерпан: "
                    "новый `[[autonomy-next]]` допустим только если без него задача реально "
                    "технически не помещается в этот проход или есть внешний блокер.\n"
                )
        self_review_block = ""
        if current_task_kind in {"project", "maintenance", "review"}:
            self_review_block = (
                "Если этот шаг меняет самого ассистента или его автономный контур "
                "(код, prompt, owner-facing поведение, память контура), то перед правкой "
                "сначала коротко зафиксируй для себя 4 вещи: что именно меняешь, зачем, "
                "главный риск и как проверишь результат. Для этого в конце ответа добавь "
                "внутренний блок такого вида:\n"
                "[[self-review]]\n"
                "CHANGE: ...\n"
                "WHY: ...\n"
                "RISK: ...\n"
                "CHECK: ...\n"
                "[[/self-review]]\n"
                "Этот блок нужен для внутреннего следа, не для владельца; не раздувай его "
                "в полотно и не отправляй владельцу как часть обычного результата.\n"
                "Если по такому шагу реально нужен owner-facing апдейт в чат, не делай это по умолчанию. "
                "Добавь в конец ответа отдельный служебный блок:\n"
                "[[notify-owner]]\n"
                "REASON: коротко, почему этот шаг действительно стоит показать владельцу\n"
                "[[/notify-owner]]\n"
                "Без этого блока внутренний project/maintenance/review шаг лучше считать тихим, "
                "если там нет прямого вопроса к владельцу или файла для отправки.\n"
            )
        current_task_block = (
            "Есть текущая автономная задача, её нужно считать главным кандидатом на этот heartbeat.\n"
            "Если можешь, сделай по ней один реальный шаг, а не придумывай другую тему.\n"
            "Текущая автономная задача:\n"
            f"- id: {current_task_id}\n"
            f"- kind: {current_task_kind}\n"
            f"- title: {current_task_title}\n"
            f"- details: {details_block}\n\n"
            f"{continuation_block}"
            f"{self_review_block}"
        )
    return (
        f"{prefix}"
        "Это автономный heartbeat-сеанс ассистента внутри той же общей сессии с владельцем.\n\n"
        "Сделай один осмысленный и безопасный автономный сеанс.\n"
        "Не уходи в бесконечную миссию, не разгоняй self-loop и не делай рискованных действий.\n"
        "Если задача требует продолжения, остановись на хорошем промежуточном результате и либо назначь следующий шаг позже, либо продолжи только если это всё ещё короткий безопасный сеанс.\n"
        "Не дроби задачу на микрошаги, если её можно честно закрыть в текущем проходе.\n"
        "Многошаговость допустима только там, где за один проход реально нельзя сделать весь оставшийся кусок без самообмана или где есть внешний блокер.\n"
        "Если уже делаешь одну и ту же линию не первый раз, предпочти более крупный законченный кусок вместо ещё одного мелкого follow-up.\n"
        "Перед началом коротко осмотрись: учти память, недавние пользовательские сигналы и автономные шаги.\n"
        f"{workspace_memory_note}\n"
        "Если можешь безопасно сделать небольшой ресерч, заметку, сводку или другой конкретный полезный результат, предпочитай это мета-размышлениям.\n"
        "Не выбирай шаги, чей единственный результат — сообщить, что сервис живой, heartbeat сработал, процесс `active`, PID изменился или другой операционный статус без явной пользы владельцу.\n"
        "Если активных поручений нет и полезной темы сейчас не видно, лучше честно сделать `ACTION: NOOP`, чем выдумывать техническую активность ради активности.\n"
        "Если сейчас разумнее спать и вернуться позже, это нормальный исход: не нужно любой ценой генерировать новый шаг.\n"
        "Если в `system/tasks/autonomy_requests.md` есть активные поручения владельца, считай их приоритетнее свободной инициативы.\n"
        "`system/tasks/autonomy_requests.md` в этом проекте считается single-chat inbox'ом владельца для одного основного рабочего чата, а не multi-chat механизмом.\n"
        "Когда поручение владельца полностью завершено, в рамках этого же шага убери его из активного списка: "
        "полностью удали его из раздела `## Активные` или иначе выведи из активного списка. Завершённое поручение не должно "
        "оставаться среди активных и не должно тянуть новые heartbeat-циклы.\n"
        "Если по ходу heartbeat тебе нужно написать владельцу, ты можешь сделать это прямо сейчас: "
        "не жди отдельного окна и не копи вопрос до следующего дня.\n"
        "Пиши, когда есть содержательный результат, блокер, важное наблюдение или короткий уточняющий вопрос. "
        "Не дёргай владельца по каждой мелочи и не отправляй пустые статус-апдейты без пользы.\n"
        "Если пишешь владельцу, по умолчанию делай это очень кратко: 1-3 короткие строки без полотна технических подробностей.\n"
        "Если от владельца не требуется действие, не перечисляй тестовые команды, служебные шаги, self-check, внутренние планы, file-by-file изменения и следующий шаг, который ты сам можешь сделать без него.\n"
        "Если ты уже упёрся во внешний блокер, который требует ответа или подтверждения владельца, "
        "сообщи об этом один раз и не ставь повторный follow-up на ту же самую причину ожидания.\n"
        "Если действительно делать нечего, ответь ровно `ACTION: NOOP` и больше ничего не добавляй.\n"
        "Если у тебя уже есть текущая автономная задача, но после проверки активных поручений и контекста видно, "
        "что она уже полностью закрыта или больше не нужна, ответь строго в формате:\n"
        "ACTION: COMPLETE\n"
        "RESULT:\n"
        "кратко объясни, почему задачу можно закрыть без нового шага\n"
        "Если хороший шаг есть, ответь строго в таком формате:\n"
        "ACTION: STEP\n"
        "TITLE: короткий заголовок сделанного шага\n"
        "KIND: research | note | project | review | maintenance | general\n"
        "PRIORITY: число от 1 до 500\n"
        "DETAILS:\n"
        "кратко, что именно за шаг был выбран\n"
        "RESULT:\n"
        "основной полезный результат этого шага для владельца\n"
        "После RESULT можно добавить обычный связный текст результата на нескольких строках.\n"
        "В конце сделай внутренний self-check: был ли шаг реально полезен, не ушёл ли ты в сторону и есть ли конкретный следующий шаг лучше, чем остановиться сейчас.\n"
        "Этот self-check нужен для внутреннего качества шага и не должен превращаться в длинный отчёт владельцу сам по себе.\n"
        "Если после этого шага уместно поставить ровно одну следующую автономную задачу, "
        "добавь В КОНЕЦ ответа служебный блок:\n"
        "[[autonomy-next]]\n"
        "ACTION: ENQUEUE\n"
        "TITLE: короткий следующий шаг\n"
        "KIND: research | note | project | review | maintenance | general\n"
        "PRIORITY: число от 1 до 500\n"
        "DELAY_SEC: число секунд до следующего запуска (`0`, если следующий маленький шаг лучше сделать прямо в этом же сеансе; положительное число, если лучше вернуться позже)\n"
        "DETAILS:\n"
        "краткое описание следующего шага\n"
        "[[/autonomy-next]]\n"
        "Если продолжение не нужно, не добавляй этот блок.\n"
        "Не ставь follow-up автоматически только ради того, чтобы не остановиться; он нужен только если реально есть следующий осмысленный шаг.\n"
        "Если владелец напишет, у него приоритет над автономностью.\n\n"
        f"{current_task_block}"
        f"{wakeup_context}\n\n"
        f"{memory_note}\n\n"
        f"{send_files_note}\n\n"
        f"{risky_note}"
    )
