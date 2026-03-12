# Apple Guideline Mapping (Legal / Rights)

Date: 2026-02-19  
Owner: Miguel Cazares (App Owner)

Source baseline: Apple App Review Guidelines, section 5.2 (Intellectual Property), including 5.2.2 and 5.2.3.

## 1) Guideline-to-Control Mapping

### GM-01 - Guideline 5.2.2 (Third-Party Sites/Services)
- App Review risk: Rejection if the app accesses/monetizes/displays third-party content without specific permission under service terms.
- Control statement: Mixterious only enables third-party source/service paths that are contractually authorized.
- Evidence required: Executed authorization from each third-party service used in production, with permitted use scope that covers this app.
- Owner: Miguel Cazares
- Submission status: BLOCKED until evidence is attached.

### GM-02 - Guideline 5.2.3 (Audio/Video Downloading)
- App Review risk: Rejection if app can save/convert/download third-party media without explicit authorization.
- Control statement: Download/save/convert flows are allowed only for sources with explicit written authorization.
- Evidence required: Contract clauses or rights letters explicitly authorizing media download/conversion for app processing and delivery.
- Owner: Miguel Cazares
- Submission status: BLOCKED until evidence is attached.

### GM-03 - Guideline 5.2.1 (General third-party protected material)
- App Review risk: Rejection if copyrighted works/metadata are used without permission.
- Control statement: Catalog rights ledger documents chain-of-title for master and composition/lyric usage.
- Evidence required: Rights ledger + executed licenses covering master, composition, and lyric display/sync.
- Owner: Miguel Cazares
- Submission status: BLOCKED until ledger is complete.

### GM-04 - Guideline 2.3.1/2.3.9 (accurate metadata + rights in materials)
- App Review risk: Rejection if App Store metadata overstates rights posture or uses unlicensed assets.
- Control statement: Metadata and review notes must match documented rights scope exactly.
- Evidence required: Metadata legal QA checklist + sign-off and archive of final App Store text/screenshots.
- Owner: Miguel Cazares
- Submission status: IN PROGRESS.

## 2) Feature Exposure vs Legal Risk

### Source search/result picker (third-party discovery)
- Risk class: High (5.2.2)
- Control: Restrict to explicitly authorized services only.
- Evidence required: Service authorization agreement + allowed-endpoint matrix.

### Server/client media retrieval and upload flow
- Risk class: Critical (5.2.3)
- Control: Demonstrate explicit source authorization for any download/save/convert behavior.
- Evidence required: Explicit authorization language + technical flow mapping showing only authorized paths in release config.

### Karaoke output generation (lyrics + audio/video sync)
- Risk class: High (5.2.1 + 5.2.3)
- Control: Confirm derivative, sync, and lyric display rights for included catalog.
- Evidence required: Master/publishing/lyrics rights package with territory/platform scope.

### Save/share generated video
- Risk class: High (5.2.3 downstream use)
- Control: Terms must constrain user use to granted scope and prohibit infringement.
- Evidence required: Published Terms of Use + upstream rights permitting user output distribution at stated scope.

## 3) App Review Notes Draft (Guideline-Focused)

> We reviewed Mixterious against App Review Guidelines 5.2.2 and 5.2.3. The app is submitted under an authorized usage model (Option 1), and we maintain documentary evidence for third-party service authorization and explicit media download/processing rights.
>
> For verification, see our attached rights package and evidence index. Any source/service not covered by explicit authorization is excluded from release scope.

Evidence required: App Review attachment list includes rights index and executed legal artifacts referenced in the checklist.

## 4) Decision Rule

- PASS condition: GM-01 through GM-03 each marked READY with linked documentary proof.
- BLOCK condition: Any claim without documentary proof remains BLOCKED and submission should not proceed.
- Evidence required: Signed pre-submit legal gate record in `release/appstore/status-tab1.md`.
