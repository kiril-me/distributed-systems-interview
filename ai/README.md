# AI conversation

[conversation.md](conversation.md) is the full working session with Claude Code that produced the design docs, the implementation,
the chaos testing and the GraphQL API in this repository. It is in order, from
the first review of my assumptions to this export.

## What is in it

- **Every message I sent**, word for word.
- **Every reply Claude showed me**, word for word, including the short
  "next-step" summaries the app displays at the end of a turn.
- **Every shell command Claude ran**, with its one-line description and a
  shortened result: the first 12 lines (at most about 1,500 characters), then
  a count of the lines left out. Commands longer than 25 lines are cut the
  same way.
- **The two subagents**: the brief each one was given, and the report each one
  sent back (writing `scripts/check_record.sh`, and an independent review for
  ways the record can lie).
- **File writes, edits and reads** are listed by path only. The resulting
  files are in the repository and its git history.

## What is left out

- **Claude's private reasoning** ("thinking"). Only what it showed me is here.
- **Full command output** beyond the shortened result.
- **File contents** written or edited, because the files themselves are in
  the repository.
- **The subagents' own step-by-step tool calls.** Only their briefs and final
  reports are kept.
- **System and app plumbing**: system prompts and reminders, the task-list
  updates the app pins under each message, tool-loading calls, and
  notifications that a background task had finished.
- **My email address and company domain.** They appeared only in the app's
  system context, not in any message. The export script still replaces them
  with `[redacted-email]` and `[redacted-domain]` wherever they would occur.
  My local username remains in file paths (`/Users/kiril/...`).

## How it was made

From the app's session export (`transcript.jsonl`), by a small script that
keeps the parts above and drops the rest. The script is not part of the
repository; the export it read is the source of truth.
