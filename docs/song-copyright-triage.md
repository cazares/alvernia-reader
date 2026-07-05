# Song Copyright Triage — Alvernia parish cancionero

> **Status: WORK IN PROGRESS · uncertified snapshot · rescued to git 2026-07-05**

## What this is

Copyright triage of the parish cancionero (song numbers 2–370; **~314 real
songs** after removing rubrics/section headers). Goal: determine which songs, if
any, are safe to include in a **public App Store listing** of the SignoVivo
songbook.

Governing rule (conservative): **when in doubt, do NOT call it public domain.**
An unknown/anonymous author does *not* make a song PD — it makes it an *orphan*
work that still cannot be cleared.

## Bottom line

The cancionero is **~92% copyrighted or orphan**. Only **~8% (~24 songs)** look
clearable for a public listing — so a fully-public listing is **not viable**
as-is. (See project memory `project_songbook_copyright_triage.md`.)

## ⚠️ Provenance & durability note

The triage was produced by an agent pass in a separate working session on
2026-07-05. Until this commit it lived **only** in an ephemeral temp-dir
scratchpad (`/private/tmp/…`), uncommitted — one reboot from total loss. This
commit rescues the working set into version control. **It is a snapshot of
in-progress, not-line-certified work** — treat statuses as agent assessments,
not legal advice.

## Files (in [`docs/song-copyright-triage-data/`](song-copyright-triage-data/))

| File | What |
|------|------|
| `master.tsv` | All 314 songs: `NUM⇥TITLE⇥INCIPIT` |
| `verified-rows.txt` | Classified rows (schema below). Covers batches 01,02,03,05,06 → 197 songs |
| `batch-01.tsv` … `batch-08.tsv` | The agent work-splits (~40 songs each) |
| `batch-08-followup-2026-07-05.txt` | 5 batch-08 UNKNOWNs resolved in a follow-up pass (this session) |
| `batch-08-unknowns-resolved.md` | Full evidence + source URLs for those 5 |

### Classification schema

`NUM | STATUS | HOLDER | REGIME | CONF | BASIS`

- **STATUS** — `PD` · `COPYRIGHT` · `UNKNOWN` (couldn't ID the song) · `PARISH-LOCAL` · `NOTASONG` (rubric/header)
- **REGIME** — `Traditional/None` · `OCP` · `GIA` · `CanZion/Indie` · `CCLI` · `Secular` · `Parish-local` · `Unknown`
- **CONF** — `High` · `Med` · `Low`

## Coverage / what's left

- ✅ Verified: batches **01, 02, 03, 05, 06** → 197 songs
- ✅ + **5 follow-ups** from batch 08 (this session)
- ❌ **Not yet verified: batches 04, 07, and the rest of 08 — ~112 songs**

## Batch-08 follow-up (2026-07-05) — the 5 stubborn UNKNOWNs

Targeted research resolved the 5 songs batch-08 left UNKNOWN/Low. **All 5 →
EXCLUDE** from a public listing (none was ever clearable; now firmly classified).

| # | Title | Verdict | Who / why |
|---|-------|---------|-----------|
| 33 | Gloria MSP | 🔴 COPYRIGHT | "MSP" = **Misioneros Servidores de la Palabra** (order, est. 2008), their *Misa Pascual* — **not** "Misa Pascual" as first guessed |
| 41 | Con estas ofrendas (RE) | 🟠 COPYRIGHT (orphan) | Canto de ofertorio, no traceable author; "(RE)" = key of D, not the title |
| 73 | Cada día que nace | 🟠 COPYRIGHT (orphan) | Lyrics confirmed, no author, absent from OCP/FyC/Hymnary |
| 224 | Había olvidado como era mi alma | 🔴 COPYRIGHT | Osiris Preciado (*Cantantes Católicos Gdl.*, Guadalajara) |
| 328 | El Señor Resusito | ⚪ UNKNOWN | Title shared by 1 PD hymn + 3+ copyrighted; no incipit to disambiguate |

### Open action (#328)

Only #328 has a path that could flip to *clearable*. Capture the **first printed
line** at #328 in the book, then:

- *"Muerte y tumba ya venció, ¡Aleluya!"* → Cabrera/Wesley PD hymn → **possibly clearable** (verify the printed edition isn't a copyrighted modern arrangement)
- *"…y de la muerte nos libró. Alegría y paz, hermanos…"* → copyrighted
- *"Aleluya, aleluya…"* matching Brotes de Olivo → copyrighted
- Text credited to **OCP / Flor y Canto** → copyrighted arrangement

---
*Triage produced by the `infallible-gates` session; rescued + extended by the
`nice-kepler` session, 2026-07-05.*
