# SignoVivo — Pre-Mass / Pre-Practice Checklist

> The operator ritual. Two parts: the **rollout** (a new build reaches the group **only on a practice
> day**, proven on one canary iPad first) and the **at-the-room** check (every device reads ready during
> the 12:00–12:15 setup window). Print this and keep it with your gear.
>
> **The one hard rule:** never push a new build on a Mass day (Sun / Thu). New builds go out **Wed / Sat
> practice only**, with wifi and low stakes. A change near Mass is the exact hazard that caused past
> outages.

---

## A. Rolling out a new build (Wed/Sat, at home, with wifi)

1. **CI is green.** The PR that made the change shows a green ✓ (typecheck + safe tests + boot smoke).
   A red check means the bundle risks a bad boot — do **not** proceed.
2. **Deploy to STAGING, not prod:**
   ```bash
   STAGING=1 bash scripts/release.sh
   ```
   This builds the web bundle and deploys it to the isolated preview branch. It does **not** bump the
   version, build native, or touch signovivo.com / TestFlight.
3. **Canary-walk on ONE device — the oldest iPad** (worst case first), pointed at staging
   (`signovivo.com?env=staging`, or the printed preview URL):
   - ☐ App opens to the reader — a real page renders, **not a blank/white screen**.
   - ☐ Page-turn is snappy (< ~⅓ second warm).
   - ☐ Become director on the canary → a 2nd device follows the page over the mesh.
   - ☐ **Restart test:** force-quit the director iPad mid-page and reopen → it must NOT silently drop the
     whole group to page 1/2, and (once the M7 build lands) it should offer "¿Continuar como director?".
   - ☐ Follower ⟳ re-syncs.
   - **Any box fails → STOP.** The group is still on the old build; nothing reached them. Fix, re-stage,
     re-walk.
4. **Only after the canary is all-green, promote to prod:**
   ```bash
   bash scripts/release.sh          # full: bump + web (signovivo.com) + native IPA
   ```
   Then upload the IPA to TestFlight (`open -a Transporter ~/Desktop/SignoVivo-<N>.ipa` → Deliver).
5. **Tell the group** (Wed/Sat, while wifi is up) to update: iPads take the TestFlight update; phones
   reload signovivo.com. Everyone should be on the **same version** before practice.
6. **Have the rollback line ready** before Mass, in a note:
   ```bash
   bash scripts/rollback-web.sh     # shows deployments + the exact revert step (read-only)
   ```

## B. At the room, during setup (12:00–12:15)

For **each** device as it arrives:
- ☐ App is open and showing a page (no blank screen, no error).
- ☐ Same **version** as the others (bottom-right build label; once DIAGNÓSTICO lands, it shows LISTO/NO
  LISTO at a glance).
- ☐ The director's iPad is in director mode and its page-turns reach the followers over the mesh.
- ☐ A follower that's off can ⟳ back to the director.
- Battery ok; iPad on its stand.

If a device is on the wrong version or won't sync: pull it aside, and (if wifi is reachable) update it;
otherwise use a known-good spare. **Do not** start troubleshooting a broken build during Mass — that's
what the Wed/Sat canary walk is for.

## C. If something breaks after you've shipped

- **Web (signovivo.com):** `bash scripts/rollback-web.sh` → follow path A (dashboard one-click) or B
  (rebuild a known-good commit). Verify in a private/incognito tab.
- **iPad (native):** if a new TestFlight build is bad, simply don't install it — the canary should have
  caught it. Devices keep the last-installed build.
- **Never** deploy a fix during Mass. Fall back to the last-known-good and fix it at the next practice.

---

_Note: the automated helpers referenced here land across milestones — `?env=staging` + `STAGING=1`
(M1, done); `?selftest` card + rollback helper (M1); the DIAGNÓSTICO LISTO/NO-LISTO screen + the
director resume-prompt (later native builds). Until each lands, do the equivalent step by hand. See
`docs/major-update-2026-07.md` and `docs/implementation-log.md`._
