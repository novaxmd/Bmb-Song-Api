# Bmb-Song API Features

This document clearly defines what Bmb-Song API provides and what it does **not** provide.

## ✅ Features Provided

| Area | Provided |
|---|---|
| Search | `/search`, `/search_suggestions` for music discovery |
| Song data | `/song/{video_id}`, `/video_info/{video_id}` |
| Streaming | `/stream/{video_id}` returns playable stream information |
| Playback context | `/now_playing/{video_id}`, `/related_songs/{video_id}`, `/upnext/{video_id}` |
| Catalog entities | Artist, album, playlist, podcast endpoints |
| Discovery | `/home`, `/charts`, `/trending`, `/genres`, `/mood_categories`, `/mood_playlists/{params}`, `/explore` |
| Extra sources | Apple Music, Deezer, Spotify trending endpoints |
| API schema/docs | `/docs`, `/redoc`, `/openapi.json` (protected by Basic Auth using `DOCS_PASSWORD`) |
| Runtime ops | `/health`, cache endpoints |
| Deployment | Local Python run + Docker support |

## ❌ Features NOT Provided

| Area | Not Provided |
|---|---|
| Authentication | No user login/signup/logout auth flow |
| Account management | No user profiles, password reset, sessions |
| Private per-user data | No private favorites/library persistence for authenticated users |
| Paid/subscription access | No paid music entitlement/account billing features |
| DRM/private media unlock | No protected-content bypass or private account data handling |

## Public/Stateless Warning

All business API endpoints are intended to be **public/stateless** in this repository.

That means:
- Requests are processed without user identity context
- No built-in user ownership model for private data
- Integrators should add their own auth/data layers externally if needed
