# Mixterioso

Mixterioso is a karaoke generation project with:

- a Python backend pipeline under `scripts/` and `karaoapi/`
- a mobile app under `karaoapp/`
- local tests under `tests/` and `karaoapp/e2e/`

## Local Setup

1. Create and activate a virtual environment.
2. Install Python dependencies:
   - `pip install -r requirements.txt`
3. Run tests:
   - `pytest`

For app work:

1. `cd karaoapp`
2. `npm install`
3. `npm test`

## Repository Notes

- CI workflows have been removed.
- External cloud deployment automation has been removed.
- This repository is now configured for local development and validation only.
