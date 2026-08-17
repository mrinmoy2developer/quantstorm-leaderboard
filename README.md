# quantstorm-open-ladder

A live, open, self-hosted round-robin leaderboard for **Divided Oracle**
(QuantStorm 2026's Round 1 game) — lets participants practice their bots
against each other outside the official graded tournament.

Live instance: https://quantstorm.mrinmoy.org/

## What this is

- `server.py` — the ladder server. Runs the authoritative, **unmodified**
  game engine, holds a websocket RPC channel per connected participant, and
  serves the public/private dashboards. Round-robins every connected
  strategy against every other one automatically.
- `matchup.py` — the participant client. Connects to a ladder server,
  runs your locally-chosen strategy inside the game's own process
  isolation, and answers the server's per-turn requests. **Your bot's
  source code never leaves your machine** — only the decisions it returns
  (a bid, a quote, accept/counter, transform yes/no) go over the wire.
- `protocol.py` — the shared wire format between the two (Obs/Contract
  serialization; no game logic of its own).
- `quantstorm.nginx.conf` — a reference nginx reverse-proxy config for
  running the server behind a domain with TLS.

## What this is *not*

This is **not** the official QuantStorm tournament infrastructure, and not
affiliated with or endorsed by the competition organizers. It's community
tooling for practice matches between rounds.

## Dependency: the game itself

This repo deliberately does **not** bundle the game engine
(`engine.py`, `game_config.py`, `sandbox.py`, `policy.py`, `limits.py`,
`bot_loader.py`) — that's the competition organizers' code, already public
at **https://github.com/vishwasmiddha/quantstorm-ps**, and it's not this
project's to redistribute a copy of. To run either `server.py` or
`matchup.py`, copy those six files from that repo into this directory (or
point `PYTHONPATH` at a checkout of it) first.

## Running the client (most participants want this)

```bash
pip install aiohttp
# copy engine.py, game_config.py, sandbox.py, policy.py, limits.py,
# bot_loader.py from quantstorm-ps into this directory, then:
python matchup.py --strategies /path/to/your/strategies
```

There's no `--name` flag — every `strategies/*.py` is required by the
competition rulebook to open with

```python
# Name: ...
# College: ...
# Roll Number: ...
```

and `matchup.py` reads your identity straight off whichever of your files
has that filled in (majority vote across files, so a stray baseline file
with a placeholder name can't hijack it).

`matchup.py` prints a private dashboard link on connect — open it and pick
which of your strategies to enter. You can select several at once (up to
the server's concurrency cap; the rest queue automatically).

## Running your own server

```bash
pip install -r requirements.txt
# same engine-file copy step as above
python server.py --host 0.0.0.0 --port 8765
```

| Flag | Default | Meaning |
|---|---|---|
| `--n_deals` | 8 | Deals per phase per match (mirror doubles it) |
| `--matches_per_pair` | 2 | How many times each pairing plays before the scheduler stops repeating it |
| `--max_concurrent` | 3 | Matches running at once |

State (leaderboard, match history, identity registry) persists to
`leaderboard_data.json` next to the server and survives restarts. It is
**not** committed to this repo (see `.gitignore`) — it accumulates real
participants' names, colleges, and roll numbers, which is not something to
publish alongside the code.

`quantstorm.nginx.conf` is a starting point for putting a real domain and
TLS in front of it via `certbot --nginx`.

## Design notes

- **Ranking is PnL-per-match, not raw total.** A running total only
  rewards playing more, not playing better, and would let whoever's been
  connected longest coast to the top. A strategy needs a minimum number of
  completed matches (`MIN_MATCHES_FOR_RANK` in `server.py`) before it's
  trusted enough to rank on at all — below that it shows as "provisional."
- **One public row per person**, keyed on roll number, not name — the
  first `(name, college)` seen for a roll number is locked in permanently,
  so one person can't fragment across several rows by varying their
  displayed name, and two different people who happen to share a name and
  college don't get merged into one.
- **The server never sees your code.** It only ever receives per-turn
  decisions over the websocket, and only sees the same `Obs` a bot would
  legally see during play.

## License

MIT — see `LICENSE`.
