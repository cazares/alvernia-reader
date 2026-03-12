# Mixterious Rights Model (Option 1)

Date: 2026-02-19  
Owner: Miguel Cazares (App Owner)  
Scope: iOS App Store submission readiness for Mixterious

## 1) Selected Model

- Selected model: **Option 1 - licensed/authorized usage model**.
- Operational meaning: Mixterious only ships features that use third-party music/content when Mixterious can produce documentary evidence of authorization for access, download, processing, lyric display, and end-user output use.
- Rejection condition: If any required authorization is missing for a source/service/catalog, that source/service/catalog is out of scope for release.
- Evidence required: Signed internal rights decision memo (App Owner + Legal sign-off) that explicitly selects Option 1 and lists in-scope sources.

## 2) Rights Chain and Required Permissions

### RM-01 Source-service authorization (access/use under service terms)
- Claim: Mixterious is permitted to access and use each integrated third-party source/service in the way the app operates.
- Evidence required: Executed partner/API agreement or explicit written permission from each source/service owner, including scope for mobile app use.

### RM-02 Audio/video retrieval authorization (download/save/convert)
- Claim: Mixterious is explicitly authorized to save, convert, or download media from each third-party source used by the app.
- Evidence required: Contract clause granting download/format-conversion rights, mapped by source/service and retained in the submission evidence pack.

### RM-03 Master recording rights
- Claim: Mixterious has rights to process and redistribute outputs derived from underlying master recordings.
- Evidence required: Executed master-use licenses (or direct owner permissions) covering processing and output generation.

### RM-04 Composition/lyrics rights
- Claim: Mixterious has rights to reproduce, display, and time-sync lyric/composition content used in generated karaoke outputs.
- Evidence required: Publisher/composer license terms (or collecting-society coverage evidence) that permit lyric display and synchronization to generated media.

### RM-05 Derivative/output rights
- Claim: Mixterious is authorized to create derivative outputs (stem-separated mixes, karaoke renders, and downloadable MP4 outputs).
- Evidence required: Contract language that explicitly permits derivative processing and user-deliverable output artifacts.

### RM-06 Territory/platform/time scope
- Claim: Rights scope covers Apple App Store distribution territories, iOS platform use, and current release period.
- Evidence required: Rights matrix (territory, platform, start/end dates, excluded territories) signed by legal owner.

### RM-07 End-user grant limits
- Claim: End users receive only the usage rights Mixterious is authorized to grant (personal, non-commercial unless a broader grant is documented).
- Evidence required: Terms of Use + upstream license terms showing sublicensing/user-grant boundaries.

### RM-08 Infringement response
- Claim: Mixterious can receive, evaluate, and act on IP complaints in a defined timeframe.
- Evidence required: Published DMCA/copyright policy, designated notice channel, and takedown runbook with response SLA.

## 3) App Review Notes Draft (Copy/Paste)

Use this in App Store Connect "App Review Information":

> Mixterious uses an authorized rights model (Option 1). The app only operates on music/content for which we hold explicit authorization to access third-party services and to process/download media used in karaoke output generation.
>
> We can provide documentary evidence for:
> 1) service/source authorization,
> 2) audio/video download and conversion authorization,
> 3) master and composition/lyrics rights,
> 4) derivative output and end-user usage scope.
>
> Verification package location: `release/appstore/legal/` (rights model, guideline mapping, evidence checklist, terms, privacy, copyright/DMCA policy) and executed agreements in our legal evidence repository referenced by the checklist.
>
> Primary legal contact for verification requests: Miguel Cazares (App Owner) and designated legal reviewer listed in the evidence index.

Evidence required: Screenshot/PDF export of submitted App Review Notes plus attachment index showing uploaded evidence files.

## 4) Submission Gate

Tab 1 can move from BLOCKED to PASS only when all conditions below are met:

1. Each RM claim above has evidence collected and indexed.
2. Rights matrix confirms no out-of-scope source/service is reachable in production build.
3. App Review Notes text is pasted in App Store Connect and references evidence artifacts.
4. Legal reviewer confirms evidence authenticity and completeness.

Evidence required: Completed checklist in `release/appstore/legal/rights-evidence-checklist.md` with zero open blockers.
