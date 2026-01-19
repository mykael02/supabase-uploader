# Supabase Uploader

## Setup
1) npm i
2) npm init -y
3) npm i @supabase/supabase-js mime-types
4) npm i dotenv
5) Copy `.env.example` -> `.env` and fill in:
   - SUPABASE_URL
   - SUPABASE_SERVICE_ROLE_KEY
   - LOCAL_ROOT

## Dry run (no uploads)
Set `DRY_RUN=true` in `.env`, then:
- `npm run list`
- `npm run upload`

This writes a log with status DRY_RUN for each file that would upload.

## Real upload
Set `DRY_RUN=false` in `.env`, then:
- `npm run upload`

## Resume
- `npm run resume`

Retries only FAIL and SKIP from the last upload log.


