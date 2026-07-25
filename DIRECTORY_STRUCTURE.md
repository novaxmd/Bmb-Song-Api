# Bmb Song API Directory Structure

This repository is intentionally small and centered around a single FastAPI service file.

## Top-level layout

```text
Bmb-Song-api/
├── app.py                  # Main FastAPI application (routes, middleware, integrations, caching)
├── requirements.txt        # Python dependencies
├── Dockerfile              # Container build/runtime definition
├── README.md               # Top-level developer/deployment guide
├── FEATURES.md             # Provided features vs explicitly unsupported features
├── DIRECTORY_STRUCTURE.md  # This file
└── .env.example            # Environment variable template
```

## app.py organization (logical sections)

`app.py` contains grouped sections for:

- Environment + middleware setup
- FastAPI app initialization
- Caching and helper utilities
- Integration clients (ytmusicapi, Apple Music, Deezer, Spotify)
- Route handlers grouped by tags:
  - `meta`
  - `search`
  - `discovery`
  - `song`
  - `artist`
  - `album`
  - `playlist`
  - `podcast`
  - `user`

## Notes for contributors

- There is currently **no multi-folder app/routes/services split**; all logic is in `app.py`.
- If you refactor into modules later, keep route behavior and endpoint contracts backward compatible.
- Use `README.md` for deployment/usage, and `FEATURES.md` to avoid feature expectation mismatch.
