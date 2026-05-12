# ShadoWare

ShadoWare is a local HTTP service that automates a Chromium-based browser, captures page HTML, and discovers media-related URLs (for example `.m3u8`, `.mpd`, manifest, and playlist URLs).

It supports two execution engines:

- **`extension` mode (default):** uses a temporary Chrome extension to run actions and capture network URLs.
- **`cdp` mode:** uses `chromedp` directly via the Chrome DevTools Protocol.

## Requirements

- Go **1.26+**
- A Chromium-based browser installed:
  - Microsoft Edge
  - Brave
  - Google Chrome

## Run

```bash
go run .
```

The server listens on:

```text
http://localhost:8080
```

## Build and release workflow

GitHub Actions workflow: `.github/workflows/release.yml`

- Push a tag like `v1.0.0` to automatically build and publish a release.
- Or run **Build and Release** manually from Actions and provide a `tag`.
- Release assets include binaries for:
  - Windows (`windows-amd64`)
  - Linux (`linux-amd64`)
  - macOS (`macos-amd64`, `macos-arm64`)

## API

### Health check

`GET /health`

Response:

```json
{"status":"ok"}
```

### Execute task

`POST /execute`

Request body:

```json
{
  "url": "https://example.com",
  "mode": "extension",
  "wait_ms": 3000,
  "local_storage": {
    "token": "abc123"
  },
  "actions": [
    {"type": "wait_ready", "selector": "body", "wait_ms": 5000},
    {"type": "click", "selector": "button.play"},
    {"type": "wait", "wait_ms": 2000}
  ],
  "debug": false,
  "include_headers": false,
  "stream": false
}
```

Request fields:

- `url` (**required**): valid `http/https` URL.
- `mode` (optional): `"extension"` (default) or `"cdp"`.
- `wait_ms`: extra wait after actions complete (`0..15000`).
- `local_storage`: key/value map written before reload.
- `actions`: ordered browser actions.
- `debug`: when `true`, includes all captured URLs in `all_urls`.
- `include_headers`: when `true`, includes response headers for discovered `.m3u8` URLs in `m3u8_headers`.
- `stream`: when `true`, response is NDJSON event stream.

Response body:

```json
{
  "content": "<html>...</html>",
  "m3u8_urls": ["https://cdn.example.com/master.m3u8"],
  "all_urls": ["https://..."],
  "m3u8_headers": [
    {
      "url": "https://cdn.example.com/master.m3u8",
      "method": "HEAD",
      "status": 200,
      "headers": {
        "Content-Type": "application/vnd.apple.mpegurl"
      }
    }
  ],
  "error": ""
}
```

- `content`: captured HTML.
- `m3u8_urls`: deduplicated media-candidate URLs.
- `all_urls`: only populated when `debug=true`.
- `m3u8_headers`: populated when `include_headers=true` (or `debug=true`) and `.m3u8` URLs are found.
- `error`: present if execution fails.

## Supported actions

- `wait` / `sleep` (`wait_ms`)
- `wait_ready` (`selector`, optional `wait_ms`)
- `click` (`selector` or `x` + `y`)
- `double_click` (`selector` or `x` + `y`)
- `scroll` (`delta_x` or `delta_y`)
- `send_keys` / `type` (`selector`, `text`)
- `evaluate` / `eval` (`script`)

Validation rules:

- `actions[].wait_ms` for `wait/sleep`: `0..30000`
- unsupported action types are rejected

## Streaming mode

When `stream=true`, `POST /execute` returns `application/x-ndjson` with events such as:

- `{"type":"url","url":"...","is_media":true}`
- `{"type":"done","response":{...final TaskResponse...}}`

Use `curl -N` to keep the stream open.

## Notes and behavior

- Max concurrent tasks: **5**
- Per-request timeout: **120 seconds**
- CORS is enabled (`*`)
- Internal extension endpoints:
  - `GET /extension-command`
  - `POST /extension-capture`
  - `POST /extension-result`
- A parent-process watchdog shuts down the server if the parent process exits.
