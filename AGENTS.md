# Personal Assistant Vault (Codex)

This repository is the assistant's long-term memory. The chat history is NOT a reliable memory.
When in doubt, store information in the vault so it can be found later.

## What This Repo Is For

- Capture incoming messages/files (e.g. from Telegram) into a searchable archive.
- Keep a simple daily journal ("what happened today") with optional summaries.
- Maintain a few long-lived references (people, read-later list, projects) without creating chaos.

## Folder Map (Do Not Invent New Top-Level Folders)

- `00_inbox/`: raw incoming items (one message/file = one item). Never delete. Rarely rename.
- `01_capture/daily/`: daily notes `YYYY-MM-DD.md` (main journal + short records of everything).
- `01_capture/transcripts/`: long transcripts (voice-to-text, meeting dumps) when too big for daily.
- `01_capture/notes/`: occasional longform notes (conference notes, long writeups) when needed.
- `01_capture/read_later.md`: a single list of "read/watch later" links.
- `02_distill/`: optional distilled summaries (only if explicitly requested).
- `04_projects/`: project folders (only when something is truly ongoing).
- `88_files/`: documents (pdf/doc/zip/etc). Keep original filenames when possible.
- `89_images/`: images/screenshots.
- `90_memory/people/`: one file per person (contacts + context).
- `90_memory/people/_index.md`: a quick index of people for fast lookup.
- `90_memory/recipes.md`: ALL recipes live here (do not create per-recipe files by default).
- `99_process/`: process docs, templates, assistant change log.
- `system/`: future runtime code/config (do not touch unless user explicitly asks).
- `.agents/skills/`: Codex skills for this repo (do not touch unless user explicitly asks).

## Default Workflow (Intake)

For each new incoming item:

1. Always append an entry to today's daily file: `01_capture/daily/YYYY-MM-DD.md`.
2. Keep the entry short: what it is, why it matters, where the raw is (`00_inbox/...`) and any link/file path.
3. If it matches a long-lived category, ALSO update the corresponding place:
   - A "read later" link -> `01_capture/read_later.md`
   - A person's contact/info -> `90_memory/people/<person>.md` and `90_memory/people/_index.md`
   - A recipe -> `90_memory/recipes.md`
   - An ongoing work item -> `04_projects/<project>/...` (only if it is truly ongoing)

## File Creation Rules (Anti-Sprawl)

The main risk is "100500 folders/files". Avoid it with these rules:

- Prefer adding to existing files over creating new files.
- Do not create new directories unless they already exist above.
- Keep directory depth <= 2 levels from repo root (exception: inside a project folder in `04_projects/`).
- New Markdown files are allowed only for:
  - `90_memory/people/<person>.md` (one per person)
  - a new project folder under `04_projects/` (when needed)
  - long transcripts/notes under `01_capture/transcripts/` or `01_capture/notes/`
  - process/templates under `99_process/` (rare)
- If you are unsure where something belongs, put it ONLY into the daily note and ask 1 clarifying question.

## Daily Notes Format

Daily file: `01_capture/daily/YYYY-MM-DD.md`

For each entry, include:

- Short title (1 line)
- Context (source: Telegram / web / etc)
- Links/paths (to inbox item or attachment)
- Optional: "Next step" (if the user asked for action)

## Long Voice Transcripts / Dumps

If the incoming text is long (roughly > 2000-3000 words):

- Save the full text to `01_capture/transcripts/YYYY-MM-DD-<short-slug>.md`.
- In the daily file, store:
  - a 5-10 line summary
  - a link to the transcript file
  - any extracted long-lived facts (people, read-later links, etc)

## Search & Retrieval (Answering Questions)

- Prefer simple search first: filenames, headings, `rg` keyword search.
- Check these first for retrieval tasks:
  - `90_memory/people/_index.md`
  - `01_capture/read_later.md`
  - `04_projects/_index.md` (if present)
  - recent daily files
- When returning answers, include the relevant file paths so the user can open the source.

## Maintenance Mode (Self-Modification)

Do NOT edit `system/`, `.agents/skills/`, or `AGENTS.md` unless the user explicitly asks to.
When you do make changes to those areas, append a short entry to `99_process/assistant_changelog.md`.

