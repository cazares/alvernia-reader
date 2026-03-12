# Mixterious Privacy Policy (Draft for App Store Submission)

Effective date: 2026-02-19  
Owner: Miguel Cazares (App Owner)

## 1) What This Policy Covers

- This policy describes personal and device data processed by Mixterious iOS app and backend APIs.
- Evidence required: Public policy URL linked from App Store Connect and in-app entry point.

## 2) Data We Process

### A) Data users provide directly
- Song/query text submitted to create karaoke jobs.
- Optional uploaded audio files (when upload flow is used).
- Feedback form data: free-text feedback, device model, iOS version, app version/build, client logs, and client context.
- Evidence required: API schema (`/jobs`, `/jobs/upload`, `/feedback`) and request logs showing received fields.

### B) Data generated during app operation
- Job identifiers, status, stage/timing, and output URLs.
- Rating-prompt state keys (device key + aliases) and prompt actions.
- Backend operational logs including request metadata and client IP.
- Evidence required: Backend persistence paths and log format verification (`jobs_state`, rating state, feedback JSONL/logs).

### C) Data stored on device
- Local history and settings (for job history and prompt state behavior).
- Device key material in secure/local storage for rating prompt deduplication.
- Evidence required: Client storage implementation review (AsyncStorage/SecureStore usage).

## 3) How We Use Data

- To process karaoke generation requests and deliver outputs.
- To troubleshoot failures and improve reliability/performance.
- To limit repetitive rating prompts and respond to user feedback.
- To enforce legal and abuse controls.
- Evidence required: Feature-to-data mapping and engineering sign-off that each field has a documented purpose.

## 4) Legal Basis / Authorization Basis

- Mixterious processes data to perform requested services, maintain service security, and satisfy legal obligations.
- Evidence required: Legal basis matrix reviewed by counsel for target jurisdictions.

## 5) Sharing and Disclosure

- Mixterious shares data with infrastructure providers required to host and deliver app functionality.
- Feedback submissions may be delivered through configured email infrastructure for support handling.
- Mixterious may disclose data when required by law or to protect rights/safety.
- Evidence required: Vendor list, DPA/contract records, and data-flow diagram.

## 6) Data Retention

- Job and operational records are retained as needed for processing, support, abuse prevention, and legal compliance.
- Rating state and feedback records are persisted by backend services until pruned or deleted per retention controls.
- Evidence required: Written retention schedule (field-level or dataset-level), automated purge settings, and retention owner assignment.

## 7) User Rights and Requests

- Users may request access, correction, or deletion of personal data by contacting legal/support.
- Mixterious will verify request identity before action.
- Evidence required: Documented DSAR workflow, SLA targets, and response templates.

## 8) Security

- Mixterious applies technical and organizational controls appropriate to risk, including restricted access and operational logging.
- Evidence required: Security control inventory and incident response contacts.

## 9) Children

- Mixterious is not directed to children under 13.
- Evidence required: Age-appropriateness review and App Store age rating consistency check.

## 10) International Transfers

- If data is processed outside a user's jurisdiction, Mixterious applies appropriate contractual or legal safeguards.
- Evidence required: Hosting-region map and transfer mechanism records (if applicable).

## 11) Contact

Privacy contact:
- Name: Miguel Cazares
- Email: privacy@mixterious.app (replace with final monitored inbox before submission)

Evidence required: Active monitored privacy mailbox with assigned owner and escalation backup.

## 12) App Review Readiness Notes

- This draft requires final retention values, vendor list, and contact mailbox verification before App Store submission.
- Evidence required: Completed `rights-evidence-checklist.md` items for privacy/vendor/retention sections.
