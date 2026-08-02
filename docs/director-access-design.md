# SignoVivo — How the director becomes the director

**Status: DECIDED 2026-08-01 — «DIRIGIR», a device-gated one-tap button. Build alongside the
one budgeted native round (see `choir-pdf-distribution-plan.md`).**

The problem, in Miguel's words: *"the director NEVER remembers any method to become director,
AND the method has to be secure enough (but still easy) so that anyone looking over the
director's shoulder CANNOT learn how to become director"* — later sharpened to *"really
intuitive and efficient."*

Why it has felt unsolvable: for a **typed secret**, those two constraints are opposites —
memorable implies guessable, used-in-public implies observed. No better code fixes that. The
primitive is wrong; the fix is to stop *authenticating* knowledge and start *recognizing* the
device that is, in fact, always the director's.

This doc was first drafted by a 13-agent design pass whose recommendation (**ATRIL**, §6)
assumed the role might rotate. Miguel's answers then collapsed the problem; the settled design
below supersedes ATRIL. The grounded evidence (§1–§2) survives unchanged.

---

## 1. How it works today, and why it fails both constraints

The ♪ control opens the **same "IR A CANTO" numpad** used to jump to a song. `goToDraftSong` routes
on **digit length alone**: 2–4 digits is a song, **≥5 digits is a director code** posted to native
(`web/src/app.js:1172-1191`, gate at `:1178`, post at `:1184`). Native matches it against
`STANDARD_DIRECTOR_CODES`, baked at archive time from the gitignored `director-codes.private.json`
(`PdfReaderApp.tsx:53-57`, `:567`), raises a confirm Alert (`:576-604`), and only its `onPress` calls
`becomeDirector` (`:437`). **The codes are the directors' real phone numbers.**

| Constraint | Verdict | Why |
|---|---|---|
| **Memorable** | ❌ fails | The *code* is maximally memorable — it is the director's own phone number. The **ritual** is not. Nothing on screen suggests "type your phone number into the song box"; the ≥5-digit router is invisible; and the gesture appears in **no checklist, no runbook, no in-app string** (`docs/pre-mass-checklist.md:27`, `:51` say "become director" and never say how). It is pure oral tradition. They forget the gesture, not the secret. |
| **Unobservable** | ❌ fails | Ten digits, rendered unmasked at `clamp(1.7rem, 4.5vw, 2.3rem)`, weight 800, letter-spaced, white-on-dark, centered (`web/src/app.js:821` → `web/src/styles.css:620-632`), on an iPad held up in front of the choir. **One observation teaches the ritual AND the credential**, and the credential is a human's phone number that cannot be rotated without a native build. |

Three more facts that constrain every answer below:

- **The code is dual-purpose.** `becomeDirector` pipes the typed string straight into
  `setRelayPublishCode` (`PdfReaderApp.tsx:443`) → `X-Director-Code` → validated against the worker's
  `TRANSMITTER_CODES` secret. A codeless promotion path must supply a substitute credential or every
  signovivo.com follower silently 401s.
- **The mesh has no credential at all.** `startDirector` takes only the public string
  `DIRECTOR_SESSION = "1234"` (`PdfReaderApp.tsx:48`); authority is a self-minted random token,
  highest wins (`DirectorSyncModule.swift:372`, `:1546`); the session runs
  `encryptionPreference: .none` (`:125`, `:1060`) and the TLS delegate unconditionally accepts every
  peer (`:1911`). **The code is a local UI gate, nothing more.**
- **The floor is broken in three ways today.** A wrong code produces **total silence** (the numpad
  closes at `web/src/app.js:1184-1186` before native validates; native replies `role:"none"` at
  `PdfReaderApp.tsx:570` with a comment claiming a "código incorrecto" UI that does not exist). A
  failed mesh start still lights the badge (`case "error"` handles only `DIRECTOR_CONFLICT`,
  `PdfReaderApp.tsx:920-932`). A demoted director is told nothing, even though Swift already wrote
  the correct Spanish sentence at `DirectorSyncModule.swift:1546` and nobody reads it.

---

## 2. Correction to a claim in the distribution plan

`choir-pdf-distribution-plan.md:57-58` says MCSession caps at 8 peers total, "zero headroom at 8
iPads." **Wrong.** The director lazily allocates up to **two** sessions of 7 followers each —
14 followers (`DirectorSyncModule.swift:45`, `:47`, `:121`). An 8-iPad fleet uses 7 of 14. No design
here needs to work around a capacity ceiling.

---

## 3. The decision inputs (Miguel, 2026-08-01)

1. **Threat: mischief** — a choir member or kid who watched, not an adversary.
2. **Role never moves mid-Mass** — same iPad every time.
3. **One permanent director, forever**: Braulio Figueroa. Nobody else is allowed to direct. He
   keeps Apple hardware indefinitely; his device is an **iPad Pro 12.9" 2nd gen, iOS 17.7.11**
   (`iPad7,1`/`iPad7,2` — Wi-Fi/Cellular variants). Nobody else in the choir has or will have
   that model.
4. **Every iPad is personally owned** — no MDM, no shared devices, each has its owner's passcode.
5. **Scope directive: do not engineer for more than ~6 months out.**

---

## 4. THE DESIGN — «DIRIGIR»

### 4.1 What Braulio experiences

Open the app. A visible, labeled **«DIRIGIR»** button sits in the control cluster. Tap it.
Directing. **One tap, nothing typed, nothing remembered** — the affordance replaces memory, and a
shoulder-surfer learns only that Braulio's iPad has a button theirs doesn't.

### 4.2 Mechanism

- **Native shell**, at boot: read `utsname.machine`. If it is `iPad7,1` or `iPad7,2` (baking both
  variants removes any need to verify which one his device reports), inject
  `window.__SIGNO_VIVO_DIRECTOR_DEVICE = true` alongside the existing preload flags
  (`PdfReaderApp.tsx` `preloadScript`).
- **Web UI**: when that flag is set, render the DIRIGIR control. On every other device the flag is
  absent and the button does not exist — there is nothing to discover.
- **Tap** posts the existing bridge message shape; native runs the **same `becomeDirector` flow the
  code path uses today**, supplying `STANDARD_DIRECTOR_CODES.values().next()` internally so relay
  auth (§1, dual-purpose fact) is untouched. No new auth surface, no worker change, no mesh change.
- **Confirm dialog only when a live director heartbeat exists** (`LIVE_DIRECTOR_WINDOW_MS` logic
  already present) — i.e. only during Miguel's testing. The normal case is tap → directing.
- **Why one tap, not zero** (auto-direct on launch was considered and rejected): Braulio opening
  the app at home on a Tuesday must not drag every signovivo.com follower through his practice
  session. The tap is intent.

### 4.3 Fallback ladder — "his iPad is dead / at home / flat"

1. **The current code path survives byte-for-byte** on every device: ♪ numpad, ≥5-digit code. It
   stops being something Braulio must remember and becomes the emergency lane — **laminated card
   in his gig bag** (and Miguel's wallet). Written-down beats memorized, and against a
   mischief-level threat a card in a personal bag is adequate.
2. **Miguel is super-admin** — his codes work from any device, any time (`SUPER_ADMIN_CODES`).
3. Total loss of both → the pre-app protocol: the director calls page numbers out loud.

The build-371 outage class (code rejected → **no director all night**) shrinks: the primary path
no longer involves code validation at all.

### 4.4 Ride-along floor repairs (small, and they serve this design directly)

From §1's "broken floor" facts — each is a one-liner-to-small fix in the same build:

- Wrong code → show *"Código incorrecto"* instead of silence (the comment at `PdfReaderApp.tsx:570`
  already claims this exists; make it true).
- Failed mesh start → don't light the director badge (`case "error"` handling).
- Demotion → surface the Spanish sentence Swift already writes at `DirectorSyncModule.swift:1546`.

### 4.5 Cost and risk

~40 native lines + one web control + the repairs above. No new pods (the pod set is unchanged, so
`release.sh`'s Manifest guard is unaffected). Verification is self-evident: at practice, the
button either appears on Braulio's iPad or it doesn't — and must NOT appear on any other device.
If his iPad is ever replaced, the button vanishes; the gig-bag card bridges until the next build
adds the new model string.

---

## 5. Explicitly rejected

| Option | Why not |
|---|---|
| A better/rotating code | Wrong primitive — memorable vs unobservable are opposites for typed secrets (§0) |
| Auto-direct on launch | Tuesday-at-home practice drags live followers (§4.2) |
| Touch ID / `LocalAuthentication` | His personal iPad's lock screen already gates who can open the app; a second lock behind a lock, for a mischief threat. (Also: his 2017 iPad has Touch ID not Face ID; unenrolled-biometry falls back to the passcode anyway.) |
| Dashboard-designation (out-of-band) | Needs internet to set + deepens the single-operator dependency; overkill for one permanent director |
| Hand-me-down `iPad7,x` guard, re-designation without a build | Real hazards on a 2+ year horizon, non-issues within the 6-month scope directive |

---

## 6. What ATRIL was, for the record

The 13-agent pass — run before decision inputs 2–4 landed — recommended **ATRIL**: 2–3
pre-designated iPads showing a `DIRIGIR` pill (localStorage designation set once by Miguel),
printed rotating codes in a sacristy binder + directors' wallets as the offline floor, and
emphasis driven by director-snapshot recency. Its red team contributed three findings that
still stand regardless of design and are tracked as open risks: the fleet-wide relay
transmitter code cannot be revoked without a build; **the mesh has no credential at all**
(§1) so nothing here defends against a genuinely adversarial actor — accepted, threat is
mischief; and the web-PWA iPad can never direct — accepted, the director is never on it.
ATRIL's multi-device machinery is superseded by decision input 3 (exactly one director,
forever, on one known device).
