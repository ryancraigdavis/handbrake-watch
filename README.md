# hbwatch

A small single-binary CLI that watches folders and auto-transcodes new media
files with HandBrakeCLI, then moves the originals aside. Drop a file in a
folder, walk away, come back to a transcoded file (and an optional phone
notification that it's done).

Built for a Mac mini running 24/7 against a NAS, and for Ubuntu Linux.

## How it works

1. A file appears in a watched folder (poll-based, so it works over a NAS).
2. Events go quiet (debounce) and the file's size stops changing (stabilize),
   so a half-copied file is never encoded.
3. A job is queued with that folder's preset.
4. A worker runs `HandBrakeCLI --preset-import-file <preset> -Z "<PresetName>"
   -i <input> -o <output>.hbtmp`, reading the actual preset name from the JSON.
5. On a verified success (exit ok + output exists + size ≥ min + no error in
   output), the temp file is renamed into place and the original is moved to
   the originals folder.
6. Queue state is persisted, so a restart resumes cleanly. Optional ntfy.sh
   notifications fire on failure, per item, and when the queue empties.

## Prerequisites

Install HandBrakeCLI and verify it:

```bash
# macOS
brew install handbrake                 # the HandBrakeCLI binary
brew install --cask handbrake-app      # optional: the GUI (for exporting presets)

# Ubuntu
sudo apt install handbrake-cli

HandBrakeCLI --version
```

## Build

```bash
cargo build --release
# binary at target/release/hbwatch
```

## Configure

```bash
mkdir -p ~/.config/hbwatch/presets
cp config.example.toml ~/.config/hbwatch/config.toml
# edit it: set your watch/output/originals folders and a notification topic
```

Create a preset in the HandBrake GUI, then **Presets → Manage Presets →
Options → Export All User Presets** (or export a single preset). Point a
folder's `preset_file` at it, or drop it in `preset_dir` as
`<folder name>.json` (convention mode). Folder-structured exports are handled
automatically — hbwatch reads the real `PresetName` from inside the JSON.

> **Folder layout rule:** keep each `output_dir` and `originals_dir` *outside*
> every `watch_dir`, or hbwatch will refuse to start (it would re-encode its
> own output forever).

## Run

```bash
hbwatch --config ~/.config/hbwatch/config.toml check   # validate + print plan
hbwatch --config ~/.config/hbwatch/config.toml run     # start the daemon
```

`check` prints the resolved plan — confirm each folder's resolved `-Z` preset
name is what you expect before running for real.

### Progress display

When `run` is attached to a terminal, hbwatch shows live progress bars: an
overall "batch X/Y" bar, a current-film bar with percent/ETA (folded across
HandBrake's encode passes), and one line per watched folder (idle / N queued /
encoding). When stdout is not a terminal (e.g. under launchd/systemd) it falls
back to plain structured logs, so service logs stay clean.

## Run as a service

- **macOS (launchd):** see `service/com.user.hbwatch.plist`.
- **Ubuntu (systemd):** see `service/hbwatch.service`.

Both files contain install instructions in comments. Ensure the NAS is mounted
before the service starts (the systemd unit shows how with
`RequiresMountsFor`). On a Linux box, enable `loginctl enable-linger` so a
user service survives logout.

## Notifications (ntfy.sh)

Set `[notifications]` in the config. Subscribe to your topic in the ntfy app or
at `https://ntfy.sh/<your-topic>`. Three independent toggles:

- `on_failure` — an encode failed
- `on_item_complete` — a single file finished
- `on_queue_drain` — the queue went empty

## Status

Phase 0 (MVP): poll watching, debounce + size-stabilization, folder-structured
preset parsing, serial queue with JSON persistence + resume, encode-to-temp +
verified success, original-move with cross-device fallback, startup
reconciliation scan, and ntfy notifications.

Phase 1 (durability): capped retries with a delay between attempts, permanent
failures moved to `<originals_dir>/_failed/` with a `.error.txt` sidecar, and a
periodic reconciliation re-scan that also recovers from a NAS mount dropping and
reconnecting.

Progress (Phase 2 partial): `--json` progress parsing and TTY-aware indicatif
progress bars (overall + current film + per-folder), with `WorkDone` error
checking folded into success detection.

Not yet (later phases): `status` subcommand, parallel workers,
`install`/`uninstall` subcommands, config hot-reload.

### Failure handling

A failed encode is retried up to `max_attempts` times (waiting `retry_delay_secs`
between tries, which rides out transient NAS blips). If it still fails, the
original is moved to `<originals_dir>/_failed/` with a `.error.txt` explaining
why, so it stops eating restarts, and a notification fires (if enabled). The
`retry_after` timestamp is persisted, so retry state survives a restart.
