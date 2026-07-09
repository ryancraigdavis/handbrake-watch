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

## Install

From a clone (puts `hbwatch` on your PATH at `~/.cargo/bin`):

```bash
cargo install --path .
```

Or build it and place it yourself:

```bash
cargo build --release
sudo cp target/release/hbwatch /usr/local/bin/
```

hbwatch shells out to `HandBrakeCLI` at runtime, so install that separately
(see Prerequisites above). Verify with `hbwatch --version`.

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

## Status from your phone

Enable the built-in status server in `[server]`, bind it to your LAN
(`bind = "0.0.0.0"`), set a `token`, and open it on your phone:

```
http://<mini-ip>:9000/?token=your-secret
```

You get a live, auto-refreshing page: current film + progress/ETA, queue depth,
and per-folder status. **Do not expose the port to the internet** — bind it to
your LAN and reach it over your home VPN when you're away. It also serves
`/status.json` (machine-readable) and `/metrics` (Prometheus format, if you ever
want Grafana graphs/alerting).

From a terminal:

```bash
hbwatch status    # queries the running daemon; falls back to the queue file if offline
```

## Run as a service

Install the service for your OS (writes the unit with real paths and prints the
enable steps):

```bash
hbwatch install     # launchd on macOS, systemd --user on Linux
hbwatch uninstall
```

On Linux, run `loginctl enable-linger "$USER"` (the install output reminds you)
so the user service survives logout. The templates in `service/` are also
available if you prefer to install by hand.

### Running against a NAS

Two things matter:

**Use `watch_mode = "poll"`.** Native filesystem events (FSEvents on macOS,
inotify on Linux) are unreliable-to-nonexistent over SMB/NFS — they often never
fire for changes on a network share. Poll mode stats the folders on
`poll_interval_secs` instead. Only use `"native"` when the watch folders are on
a local disk.

**Mount timing is handled.** hbwatch refuses to start if a `watch_dir` doesn't
exist — which is exactly the case when the share isn't mounted yet. That's
deliberate: it will *not* create your `output_dir`/`originals_dir` on an empty
mountpoint and quietly encode to the local boot disk. Because both service
managers restart it (`KeepAlive` on launchd, `Restart=always` on systemd), it
retries until the share appears and then runs normally. On systemd you can also
add `RequiresMountsFor=/mnt/nas` to the unit. Once running, a mount that drops
and comes back is picked up by the periodic re-scan.

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

Phase 3 (remote visibility + deployment): an optional embedded status server
(live phone dashboard + `/status.json` + Prometheus `/metrics`), a `status`
subcommand, and `install`/`uninstall` for the launchd/systemd service.

Not yet (later phases): parallel workers, config hot-reload.

### Failure handling

A failed encode is retried up to `max_attempts` times (waiting `retry_delay_secs`
between tries, which rides out transient NAS blips). If it still fails, the
original is moved to `<originals_dir>/_failed/` with a `.error.txt` explaining
why, so it stops eating restarts, and a notification fires (if enabled). The
`retry_after` timestamp is persisted, so retry state survives a restart.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
