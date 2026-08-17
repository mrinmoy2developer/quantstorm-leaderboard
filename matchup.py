"""
matchup.py — participant client for the Divided Oracle open leaderboard.

Connects to the ladder server over a websocket and plays turn by turn.
YOUR BOT CODE NEVER LEAVES THIS MACHINE: the server only ever receives the
per-turn DECISIONS your bot returns (a bid dict, a quote, accept/counter,
transform yes/no), never the source of strategies/*.py. Only the FILE NAMES
in your --strategies directory are sent, as metadata, so the server's web
dashboard can offer you a picker.

Your chosen bot(s) run locally inside sandbox.py's SandboxedBot -- the same
process isolation and statelessness enforcement the real tournament uses.
You may select several of your own strategies at once on the private
dashboard; each runs as its own isolated process here, up to the server's
concurrency cap, with the rest queued until a slot frees.

There is no --name flag. RULEBOOK.md SS12 already requires every
strategies/*.py to open with

    # Name: ...
    # College: ...
    # Roll Number: ...

so your candidate identity is read straight off whichever of your own
files has that filled in, rather than asked for a second time.

Usage:
    pip install aiohttp
    python matchup.py --strategies /path/to/strategies

Then open the dashboard link this prints and pick which of your bots to
enter. The server round-robins you against everyone else who has a bot
selected, automatically, and updates the public leaderboard.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

import aiohttp

#: The one and only ladder this client talks to. Not a flag: every entrant
#: connecting to the same server is the whole point of an open leaderboard.
SERVER_URL = "wss://quantstorm.mrinmoy.org/ws"

#: Bumped whenever this file's wire behavior changes in a way worth telling
#: participants about. Purely informational: an older or missing version
#: still connects and plays exactly the same, the server just tags it
#: "stale bundle" on the dashboard so the participant knows to update.
CLIENT_VERSION = 1

# Works whether this file sits in ladder/ next to the rest of the repo (dev
# layout: engine.py etc. one directory up) or standalone, as unzipped from
# the leaderboard's "download the client" bundle (flat layout: everything in
# this same directory). Both directories are safe to add either way.
_HERE = Path(__file__).resolve().parent
for _p in (_HERE, _HERE.parent):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from bot_loader import CheckResult, check_source  # noqa: E402
from game_config import GameConfig  # noqa: E402
from sandbox import SandboxedBot  # noqa: E402

import protocol  # noqa: E402


def discover_bots(strategies_dir: Path) -> list[str]:
    if not strategies_dir.is_dir():
        return []
    return sorted(
        p.name for p in strategies_dir.glob("*.py")
        if p.name != "__init__.py"
    )


#: adaptive_bidder.py, naive_ev.py and rational.py -- the shipped baselines
#: every participant starts with -- all carry this EXACT header, roll number
#: included. It is not any real person's identity, ever, so it must never be
#: eligible to win identity detection: unlike an ordinary alphabetical-first
#: mixup (fixed by the majority vote below), keeping 2+ of these three
#: unedited in a strategies/ folder makes them an outright majority on their
#: own, and because the roll number matches too, the server's roll-number
#: canonicalizer would otherwise permanently merge every such participant
#: into one shared fake "person" -- which is worse than the bug it was
#: built to fix.
_PLACEHOLDER_NAME = "Quantstorm Reference Bot"


def read_candidate_metadata(strategies_dir: Path, bot_files: list[str]) -> dict:
    """Pull Name / College / Roll Number off the mandatory header comments.

    Every strategies/*.py is required (RULEBOOK.md SS12) to start with those
    three lines, so the folder already carries the identity a --name flag
    would otherwise ask you to retype.

    Takes a MAJORITY vote across every file that has one filled in AND isn't
    the shipped-baseline placeholder (see _PLACEHOLDER_NAME), rather than
    just the alphabetically-first hit. A real entrant's own variants of one
    submission are expected to agree on their real name, so whichever real
    name shows up on the most files wins.
    """
    hits: list[tuple[str, dict]] = []
    for filename in bot_files:
        result: CheckResult = check_source(strategies_dir / filename)
        name = result.metadata.get("Name")
        if name and name != _PLACEHOLDER_NAME:
            hits.append((filename, result))

    if not hits:
        return {}

    counts: dict[str, int] = {}
    for _, result in hits:
        name = result.metadata["Name"]
        counts[name] = counts.get(name, 0) + 1
    winner = max(counts, key=lambda n: counts[n])

    filename, result = next((f, r) for f, r in hits if r.metadata["Name"] == winner)
    return {
        "name": winner,
        "college": result.metadata.get("College", ""),
        "roll_number": result.metadata.get("Roll Number", ""),
        "source_file": filename,
    }


class RunningStrategy:
    """One selected bot, live: its own isolated worker plus this match's config."""

    def __init__(self, sandboxed: SandboxedBot, config: GameConfig):
        self.sandboxed = sandboxed
        self.config = config


class Client:
    def __init__(self, server: str, name: str, college: str, roll_number: str, strategies_dir: Path):
        self.server = server
        self.name = name
        self.college = college
        self.roll_number = roll_number
        self.strategies_dir = strategies_dir
        # filename -> RunningStrategy, one per concurrently-active slot.
        # Populated on match_start, torn down on match_end/unload_bot.
        self.running: dict[str, RunningStrategy] = {}
        self.ws: aiohttp.ClientWebSocketResponse | None = None
        self.http: aiohttp.ClientSession | None = None
        # Filenames the server asked us to load (load_bot minus unload_bot and
        # invalid results). On reconnect the new session has no slots, so we
        # re-enter exactly this set through the same select endpoint the
        # browser dashboard uses.
        self.selected: set[str] = set()

    def dashboard_url(self, dashboard_path: str) -> str:
        base = self.server.replace("wss://", "https://").replace("ws://", "http://")
        base = base.split("/ws")[0]
        return base + dashboard_path

    # ── bot lifecycle ────────────────────────────────────────

    async def handle_load_bot(self, filename: str):
        path = self.strategies_dir / filename
        if not path.is_file():
            await self.send({"type": "bot_status", "filename": filename, "status": "invalid",
                              "error": f"{filename} not found locally"})
            return
        result = check_source(path)
        blocking = result.blocking(("code",))  # security-relevant only; casual ladder
        if blocking:
            self.selected.discard(filename)
            await self.send({"type": "bot_status", "filename": filename, "status": "invalid",
                              "error": "; ".join(blocking)})
            print(f"  ✗ {filename} rejected: {'; '.join(blocking)}")
            return
        self.selected.add(filename)
        await self.send({"type": "bot_status", "filename": filename, "status": "ready"})
        print(f"  ✓ entered {filename} into the ladder")

    async def handle_unload_bot(self, filename: str):
        self.selected.discard(filename)
        entry = self.running.pop(filename, None)
        if entry is not None:
            entry.sandboxed.close()
        print(f"  ↺ {filename} withdrawn from the ladder")

    # ── match lifecycle ──────────────────────────────────────

    async def handle_match_start(self, data: dict):
        filename = data["filename"]
        overrides = data.get("overrides", {})
        config = GameConfig(**overrides)
        path = self.strategies_dir / filename
        sandboxed = SandboxedBot(path, filename, config)
        self.running[filename] = RunningStrategy(sandboxed, config)
        print(f"\n▶ {filename} vs {data.get('opponent')} starting")

    async def handle_match_end(self, data: dict):
        filename = data["filename"]
        entry = self.running.pop(filename, None)
        if entry is not None:
            entry.sandboxed.close()
        if "error" in data:
            print(f"■ {filename} vs {data.get('opponent')} ended: {data['error']}")
            return
        pnl = data.get("pnl", 0.0)
        mark = "+" if pnl >= 0 else ""
        print(f"■ {filename} vs {data.get('opponent')} finished — PnL: {mark}{pnl:.2f}")

    # ── rpc dispatch: this is where your local bot actually plays ──

    async def handle_rpc(self, data: dict):
        req_id = data["id"]
        method = data["method"]
        args = data["args"]
        try:
            value = self._call_local_bot(method, args)
            await self.send({"type": "rpc_result", "id": req_id, "ok": True, "value": value})
        except Exception as e:
            await self.send({"type": "rpc_result", "id": req_id, "ok": False, "error": str(e)})

    def _call_local_bot(self, method: str, args: dict):
        filename = args.get("filename")
        entry = self.running.get(filename)
        if entry is None:
            raise RuntimeError(f"no active match for {filename!r}")
        sandboxed, config = entry.sandboxed, entry.config

        if method == "reset":
            sandboxed.new_deal()
            sandboxed.reset(args["seat"], config, args["seed"])
            return None

        obs = protocol.deserialize_obs(args["obs"])

        if method == "bid":
            result = sandboxed.bid(obs, args["offered"])
            return {str(k): int(v) for k, v in result.items()}
        if method == "quote":
            return protocol.serialize_quote(sandboxed.quote(obs))
        if method == "respond":
            quote = tuple(args["quote"])
            result = sandboxed.respond(obs, quote, args["turn"])
            return protocol.serialize_response(result)
        if method == "use_transform":
            return bool(sandboxed.use_transform(obs))

        raise RuntimeError(f"unknown method {method!r}")

    # ── plumbing ─────────────────────────────────────────────

    async def send(self, msg: dict):
        await self.ws.send_json(msg)

    async def reselect(self, dashboard_path: str):
        if not self.selected or self.http is None:
            return
        url = self.dashboard_url(dashboard_path + "/select")
        try:
            async with self.http.post(url, json={"filenames": sorted(self.selected)}) as resp:
                await resp.json()
            print(f"  ↻ re-entered {len(self.selected)} bot(s) on reconnect")
        except Exception as e:
            print(f"! could not re-enter selected bots: {e}")

    async def run(self):
        bots = discover_bots(self.strategies_dir)
        print(f"Found {len(bots)} strategy file(s) in {self.strategies_dir}: "
              f"{', '.join(bots) if bots else '(none)'}")

        retry = 2.0
        while True:
            try:
                async with aiohttp.ClientSession() as http:
                    self.http = http
                    async with http.ws_connect(self.server, heartbeat=25.0) as ws:
                        retry = 2.0
                        self.ws = ws
                        await self.send({
                            "type": "hello", "name": self.name, "college": self.college,
                            "roll_number": self.roll_number, "client_version": CLIENT_VERSION,
                            "bots": bots,
                        })

                        async for msg in ws:
                            if msg.type != aiohttp.WSMsgType.TEXT:
                                continue
                            data = msg.json()
                            kind = data.get("type")

                            if kind == "welcome":
                                url = self.dashboard_url(data["dashboard_path"])
                                print(f"\nConnected as '{self.name}'.")
                                print(f"Pick your bot here:  {url}\n")
                                await self.reselect(data["dashboard_path"])
                            elif kind == "load_bot":
                                await self.handle_load_bot(data["filename"])
                            elif kind == "unload_bot":
                                await self.handle_unload_bot(data["filename"])
                            elif kind == "match_start":
                                await self.handle_match_start(data)
                            elif kind == "rpc":
                                await self.handle_rpc(data)
                            elif kind == "match_end":
                                await self.handle_match_end(data)
                            elif kind == "identity_note":
                                print(f"note: {data.get('message')}")
                            elif kind == "error":
                                print(f"! server error: {data.get('error')}")
            except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as e:
                print(f"! connection lost: {e}")
            finally:
                self.ws = None
                self.http = None
                for entry in self.running.values():
                    entry.sandboxed.close()
                self.running.clear()
            print(f"  ↻ reconnecting in {retry:.0f}s ...")
            await asyncio.sleep(retry)
            retry = min(retry * 2, 30.0)


def main():
    ap = argparse.ArgumentParser(description="Divided Oracle ladder client")
    ap.add_argument("--strategies", required=True,
                     help="directory containing your strategies/*.py")
    args = ap.parse_args()

    strategies_dir = Path(args.strategies).resolve()
    if not strategies_dir.is_dir():
        sys.exit(f"error: {strategies_dir} is not a directory")

    bots = discover_bots(strategies_dir)
    meta = read_candidate_metadata(strategies_dir, bots)
    if not meta:
        sys.exit(
            f"error: none of the {len(bots)} .py file(s) in {strategies_dir} have YOUR "
            f"own filled-in '# Name:' header yet -- only the shipped baseline placeholder "
            f"('{_PLACEHOLDER_NAME}') was found, if anything. RULEBOOK.md SS12 requires "
            f"every strategies/*.py to start with\n"
            f"    # Name: Your Name\n    # College: Your College\n"
            f"    # Roll Number: Your Roll Number\n"
            f"Fill that in with YOUR OWN details on at least one file, then retry -- "
            f"that's also where your leaderboard name comes from, no --name flag needed."
        )
    print(f"Identified as '{meta['name']}' ({meta['college'] or 'no college given'}) "
          f"from {meta['source_file']}")
    if not meta["roll_number"]:
        print("note: no '# Roll Number:' found — the server can't tell you apart from someone "
              "else with the same name and college without it. Fill it in when you can.")

    client = Client(SERVER_URL, meta["name"], meta["college"], meta["roll_number"], strategies_dir)
    try:
        asyncio.run(client.run())
    except KeyboardInterrupt:
        print("\nDisconnected.")


if __name__ == "__main__":
    main()
