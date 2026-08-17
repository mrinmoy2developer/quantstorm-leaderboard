"""
ladder/server.py — Divided Oracle open leaderboard server.

Runs the AUTHORITATIVE, unmodified engine.py. Bot source code never reaches
this process: each connected participant runs `matchup.py`, which plays its
locally-chosen strategies/*.py through the existing sandbox.py isolation and
only ever ships this server a per-turn DECISION (a bid dict, a quote, an
accept/counter, a transform yes/no) over a websocket. This server sees the
same Obs a bot would legally see, nothing more.

Two things happen here:
  1. A websocket RPC channel per connected player, used as the "bot" inside
     an ordinary engine.play_match() call (see RemoteBotProxy). One
     connection may run several strategies at once (see Slot below).
  2. A tiny web dashboard: a public read-only leaderboard at "/", and a
     private per-session bot-picker at "/player/<token>" whose URL is only
     ever shown to that player's own matchup.py process. The picker shows
     BOT NAMES ONLY (the metadata matchup.py reports), never code.

A player may select as many of their own strategies as they like on their
private picker. At most MAX_ACTIVE_SLOTS run at once (occupying a
SandboxedBot worker on THEIR machine each); the rest sit "queued" and
promote automatically as active slots free up. The PUBLIC leaderboard shows
one row per (name, college) -- the entrant's single best-scoring strategy --
since a person's score is not the sum or average of things they tried, it's
the best of what they actually built. The per-strategy breakdown stays
visible only on that player's own private dashboard.

Round robin runs on an auto-loop: every ROUND_INTERVAL_S the server looks at
every "ready" slot across every connection and schedules any still-missing
pairings (up to MATCHES_PER_PAIR each) as background matches.

Run:
    pip install aiohttp
    python ladder/server.py --host 0.0.0.0 --port 8765
"""

from __future__ import annotations

import argparse
import asyncio
import io
import itertools
import json
import os
import random
import sys
import time
import uuid
import zipfile
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import dataclass, field
from pathlib import Path

from aiohttp import WSMsgType, web

# Works whether this file sits in ladder/ next to the rest of the repo (dev
# layout: engine.py etc. one directory up) or standalone, as in the
# open-source ladder-only repo (flat layout: engine.py etc. copied in next
# to this file, per that repo's README). Both directories are safe to add.
_HERE = Path(__file__).resolve().parent
for _p in (_HERE, _HERE.parent):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import engine  # noqa: E402
from game_config import GameConfig  # noqa: E402

import protocol  # noqa: E402

LADDER_DIR = Path(__file__).resolve().parent
STATE_FILE = LADDER_DIR / "leaderboard_data.json"

RPC_TIMEOUT_S = 8.0          # generous network/IPC allowance, NOT the 50ms game limit
ROUND_INTERVAL_S = 15.0      # how often the auto-scheduler looks for missing pairings
MAX_ACTIVE_SLOTS = 10        # strategies one connection may run concurrently; rest queue

#: Of the concurrent match slots, this many are reserved for matches where
#: at least one side's bot has LOW_MATCH_THRESHOLD or fewer completed games
#: (the dashboard's "In queue" and provisional entrants). An established
#: match can never occupy more than max_concurrent - RESERVED_LOW_SLOTS
#: slots at once, so a qualifying newcomer always has headroom to start
#: playing regardless of how many experienced matches are queued.
RESERVED_LOW_SLOTS = 10

#: Bumped whenever matchup.py's wire behavior changes in a way worth
#: flagging to participants still running an older download -- purely
#: informational, never enforced: a mismatched or missing version (every
#: bundle downloaded before this field existed) still connects and plays
#: exactly as before, just shows a "stale bundle" tag on the dashboard.
CURRENT_CLIENT_VERSION = 1

#: Matches an entrant needs before their PnL/match is trusted enough to rank
#: on. A late joiner isn't disadvantaged by the SCHEDULE -- _schedule_missing
#: re-covers every pair every tick regardless of when they connected -- but
#: right after joining their average is just noisy: two matches means one
#: swingy result can put a mediocre bot at #1 or a strong one at the bottom.
#: Below this many matches an entrant shows up as "provisional" instead of
#: in the ranked standings.
MIN_MATCHES_FOR_RANK = 10

#: A bot at or below this match count is "low" and entitled to the reserved
#: slots above. Same figure as MIN_MATCHES_FOR_RANK: below it a strategy
#: cannot rank yet and most needs to play.
LOW_MATCH_THRESHOLD = MIN_MATCHES_FOR_RANK

#: The shipped baseline strategies (adaptive_bidder.py, naive_ev.py,
#: rational.py) all carry this exact roll number in their header comment.
#: It is never a real person's roll number, so it must never be allowed to
#: anchor an identity -- matchup.py already excludes the whole placeholder
#: name from its own identity detection, but this is the server-side
#: backstop for anyone still connecting with an older client: without it,
#: every distinct participant whose folder still resolves to this shared
#: placeholder would be permanently merged into one fake "person" by
#: canonicalize() below, which is worse than doing no canonicalization at all.
PLACEHOLDER_ROLL_NUMBER = "REF-000"


def entrant_id(player: str, filename: str) -> str:
    return f"{player}::{Path(filename).stem}"


def _match_detail(result, seat: int, powers_won: list) -> dict:
    """Per-match contract detail for one seat, for the analytics history.

    result.deals (engine.DealResult) already carries the true score and
    every contract for the whole match -- this just reduces it to what the
    charts need, attributed to the correct seat.
    """
    deals = []
    for d in result.deals:
        rounds = [
            {
                **protocol.serialize_contract(c),
                "pnl": (d.score - c.price) if c.long_seat == seat else (c.price - d.score),
            }
            for c in d.contracts
        ]
        deals.append({"score": d.score, "rounds": rounds})
    return {
        "ts": time.time(),
        "pnl": result.pnl[seat],
        "deals": deals,
        "powers_won": powers_won,
    }


# ════════════════════════════════════════════════════════════════════
#  STATE
# ════════════════════════════════════════════════════════════════════

@dataclass
class Slot:
    """One of a player's selected strategies.

    status: queued | validating | ready | invalid
    "queued" means selected but not yet occupying one of the MAX_ACTIVE_SLOTS
    concurrent runs on the player's own machine; it is promoted automatically
    when a slot frees up (see Ladder._promote_queued).
    """
    filename: str
    status: str = "queued"
    error: str | None = None
    busy: bool = False


@dataclass
class Session:
    name: str
    token: str
    ws: web.WebSocketResponse
    loop: asyncio.AbstractEventLoop
    college: str = ""
    roll_number: str = ""
    client_version: int = 0    # 0 = older bundle, predates this field entirely
    bots: list = field(default_factory=list)          # metadata only: filenames discovered locally
    slots: dict = field(default_factory=dict)          # filename -> Slot, one per selected strategy
    connected_at: float = field(default_factory=time.time)
    connected: bool = True
    pending: dict = field(default_factory=dict)         # req_id -> asyncio.Future
    _counter: itertools.count = field(default_factory=itertools.count)

    def active_slot_count(self) -> int:
        return sum(1 for s in self.slots.values() if s.status in ("validating", "ready"))

    async def send(self, msg: dict):
        if not self.connected:
            return
        try:
            await self.ws.send_json(msg)
        except (ConnectionError, RuntimeError):
            self.connected = False

    async def rpc(self, method: str, args: dict, timeout_s: float = RPC_TIMEOUT_S):
        """Await a decision from this player's matchup.py. Runs on the event loop."""
        req_id = str(next(self._counter))
        fut = self.loop.create_future()
        self.pending[req_id] = fut
        try:
            await self.send({"type": "rpc", "id": req_id, "method": method, "args": args})
            return await asyncio.wait_for(fut, timeout=timeout_s)
        finally:
            self.pending.pop(req_id, None)

    def fail_pending(self, reason: str):
        for fut in list(self.pending.values()):
            if not fut.done():
                fut.set_exception(ConnectionError(reason))


class RemoteBotProxy:
    """Stands in for a local Bot instance inside engine.play_match().

    Bound to one (session, filename) slot, for the seat that entrant plays
    the WHOLE match (0 for bot_a, 1 for bot_b -- fixed by construction in
    run_match, unaffected by mirroring, which swaps hands/roles internally
    but never the wrapper index). Every method blocks the CALLING THREAD (a
    match runs in a worker thread, see run_match) until the player's
    matchup.py answers over the websocket, by bridging into the session's
    own asyncio event loop. Any failure - timeout, disconnect, a malformed
    reply - becomes a BotTimeout, which engine.BotWrapper already knows how
    to fall back on exactly as the RULEBOOK's crash-fallback table specifies.

    Also captures which powers this seat WON, for the analytics charts.
    engine.DealResult does not carry the auction tape -- it exists only on
    the Obs objects handed to bots during play -- so this is the only place
    that can see it without touching engine.py. obs.auction_log is a
    snapshot that only grows within one deal, so the latest one seen before
    the next reset() is that deal's complete tape; reset() is called once
    per deal by the engine, which is what flushes it.
    """

    def __init__(self, session: Session, loop: asyncio.AbstractEventLoop, filename: str, seat: int):
        self.session = session
        self.loop = loop
        self.filename = filename
        self.seat = seat
        self.powers_won: list[dict] = []
        self._deal_auction_log: list[dict] = []
        # engine.BotWrapper._call() reads this via getattr(bot, "last_call_ms",
        # None) and, when present, uses it INSTEAD OF wall-clock elapsed time
        # for hard-time-limit violation counting -- see the comment at its
        # call site. Without it, the engine charges the full network round
        # trip to a real participant's machine against the 50ms hard limit,
        # which is virtually never under 50ms for a WAN connection: every
        # match was racking up 5+ violations within a deal or two and
        # forfeiting outright (250 PnL transferred), long before the bot
        # itself did anything wrong. The true per-call compute-time limit is
        # already enforced correctly on the participant's own machine, inside
        # their sandbox.py worker -- this must not be re-checked, incorrectly,
        # against network latency it has no way to distinguish from real
        # bot slowness.
        self.last_call_ms = 0.0

    def _call(self, method: str, args: dict):
        args = {**args, "filename": self.filename}
        cf = asyncio.run_coroutine_threadsafe(self.session.rpc(method, args), self.loop)
        try:
            return cf.result(timeout=RPC_TIMEOUT_S + 2.0)
        except FutureTimeoutError:
            # The client genuinely never answered within a generous
            # allowance -- this is the one case that legitimately resembles
            # "the call exceeded its time budget."
            cf.cancel()
            raise engine.BotTimeout(
                f"{self.session.name}/{self.filename}: no reply to {method}() "
                f"within {RPC_TIMEOUT_S:.0f}s"
            )
        except Exception as e:
            # The client DID answer, just with an ordinary error -- its own
            # bot crashed, failed to construct for this deal, returned
            # something malformed, etc, reported honestly as ok:false. This
            # must NOT become a BotTimeout: that also counts as a hard-time-
            # limit violation on engine.BotWrapper's own timer, and five of
            # those forfeits the WHOLE MATCH (250 PnL) over something that
            # was never about time. A plain exception gets exactly the
            # documented per-call fallback instead, which is what this
            # actually is.
            raise RuntimeError(f"{self.session.name}/{self.filename}: {method}() failed: {e}")

    def _serialize(self, obs) -> dict:
        d = protocol.serialize_obs(obs)
        if d.get("auction_log"):
            self._deal_auction_log = d["auction_log"]  # only grows within a deal; latest wins
        return d

    def flush_deal_powers(self):
        """Credit this seat's wins from the deal that just finished. Call after
        the LAST deal too (run_match does), since nothing else flushes it."""
        for entry in self._deal_auction_log:
            if entry.get("seat") == self.seat:
                self.powers_won.append({
                    "round": entry["round"], "power": entry["power"], "cost": entry["cost"],
                })
        self._deal_auction_log = []

    def reset(self, seat, config, seed):
        self.flush_deal_powers()
        self._call("reset", {"seat": seat, "seed": seed})

    def bid(self, obs, offered):
        return self._call("bid", {"obs": self._serialize(obs), "offered": list(offered)})

    def quote(self, obs):
        r = self._call("quote", {"obs": self._serialize(obs)})
        return (int(r[0]), int(r[1]))

    def respond(self, obs, quote, turn):
        r = self._call("respond", {
            "obs": self._serialize(obs),
            "quote": [int(quote[0]), int(quote[1])],
            "turn": turn,
        })
        return protocol.deserialize_response(r)

    def use_transform(self, obs):
        return bool(self._call("use_transform", {"obs": self._serialize(obs)}))


# ════════════════════════════════════════════════════════════════════
#  LEADERBOARD PERSISTENCE
# ════════════════════════════════════════════════════════════════════

#: Matches of CONTRACT-level detail (per-round PnL, S, auction wins) kept per
#: entrant for the analytics charts. A ring buffer, not full history: the
#: aggregate row (pnl/matches/wins) already carries the lifetime numbers,
#: this is only for "how am I trending / where does my edge come from"
#: charts, which don't need more than a recent window to answer.
HISTORY_WINDOW = 25


class Leaderboard:
    """Stores one row per entrant_id = "<player>::<bot filename stem>".

    A player switching (or adding) a strategy starts a fresh row rather than
    mixing two different strategies' PnL into one meaningless number. The
    PUBLIC view collapses these to one row per PERSON -- see
    standings_public() and canonicalize(). The full per-strategy breakdown is
    what a player's own private dashboard shows, via rows_for_player().
    """

    def __init__(self, path: Path):
        self.path = path
        self.entrants: dict = {}
        self.pair_counts: dict = {}
        self.match_log: list = []
        self.history: dict = {}     # entrant_id -> list of per-match detail dicts
        self.identities: dict = {}  # roll_number -> {"name", "college", "first_seen"}
        self._load()

    def _load(self):
        if self.path.exists():
            try:
                data = json.loads(self.path.read_text())
                self.entrants = data.get("entrants", {})
                self.pair_counts = data.get("pair_counts", {})
                self.match_log = data.get("match_log", [])
                self.history = data.get("history", {})
                self.identities = data.get("identities", {})
            except (json.JSONDecodeError, OSError):
                pass

    def _save(self):
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps({
            "entrants": self.entrants,
            "pair_counts": self.pair_counts,
            "match_log": self.match_log[-200:],
            "history": self.history,
            "identities": self.identities,
        }, indent=2))
        os.replace(tmp, self.path)

    def canonicalize(self, roll_number: str, name: str, college: str) -> tuple[str, str, bool]:
        """One roll number = one person = one (name, college), permanently.

        The FIRST (name, college) seen for a roll number is what sticks;
        every later connection under that same roll number is forced back
        onto it, even if this hello's own header comment says something
        different. Without this, one person could rack up several separate
        public rows just by varying the "# Name:" text across their own
        files (defeating "one row per person"), and a genuine typo or a
        deliberate impersonation attempt would both silently create a
        second identity instead of surfacing as the mismatch it is.

        Returns (canonical_name, canonical_college, mismatch) -- mismatch is
        True when this hello's own header disagreed with the one on record,
        so the caller can tell the connecting client what happened.

        A blank roll number (header present but that field left empty), or
        the shipped-baseline placeholder roll number, cannot anchor
        anything -- falls back to trusting name/college as given, same as
        before this existed.
        """
        if not roll_number or roll_number == PLACEHOLDER_ROLL_NUMBER:
            return name, college, False
        known = self.identities.get(roll_number)
        if known is None:
            self.identities[roll_number] = {
                "name": name, "college": college, "first_seen": time.time(),
            }
            self._save()
            return name, college, False
        mismatch = (known["name"] != name) or (known["college"] != college)
        return known["name"], known["college"], mismatch

    @staticmethod
    def _pair_key(a: str, b: str) -> str:
        return "|".join(sorted((a, b)))

    def games_played(self, a: str, b: str) -> int:
        return self.pair_counts.get(self._pair_key(a, b), 0)

    def record(self, id_a: str, player_a: str, college_a: str, roll_a: str, bot_a: str,
               id_b: str, player_b: str, college_b: str, roll_b: str, bot_b: str,
               pnl_a: float, pnl_b: float, warnings_a: list, warnings_b: list,
               detail_a: dict | None = None, detail_b: dict | None = None):
        for eid, player, college, roll, bot, pnl, detail in (
            (id_a, player_a, college_a, roll_a, bot_a, pnl_a, detail_a),
            (id_b, player_b, college_b, roll_b, bot_b, pnl_b, detail_b),
        ):
            row = self.entrants.setdefault(eid, {
                "player": player, "college": college, "roll_number": roll,
                "bot": bot, "pnl": 0.0,
                "matches": 0, "wins": 0, "losses": 0, "draws": 0,
                "first_seen": time.time(),
            })
            row["college"] = college or row.get("college", "")
            row["roll_number"] = roll or row.get("roll_number", "")
            row["pnl"] += pnl
            row["matches"] += 1
            row["last_seen"] = time.time()
            if pnl > 1e-6:
                row["wins"] += 1
            elif pnl < -1e-6:
                row["losses"] += 1
            else:
                row["draws"] += 1

            if detail is not None:
                buf = self.history.setdefault(eid, [])
                buf.append(detail)
                if len(buf) > HISTORY_WINDOW:
                    del buf[: len(buf) - HISTORY_WINDOW]

        key = self._pair_key(id_a, id_b)
        self.pair_counts[key] = self.pair_counts.get(key, 0) + 1

        self.match_log.append({
            "ts": time.time(), "a": id_a, "b": id_b,
            "pnl_a": round(pnl_a, 2), "pnl_b": round(pnl_b, 2),
            "warnings_a": len(warnings_a), "warnings_b": len(warnings_b),
        })
        self._save()

    @staticmethod
    def _with_avg(eid: str, row: dict) -> dict:
        matches = row["matches"]
        avg = row["pnl"] / matches if matches else 0.0
        return {"entrant_id": eid, "avg_pnl": avg, **row}

    def standings_public(self) -> tuple[list, list]:
        """Two views: (ranked, provisional).

        One row per PERSON in each -- keyed on roll_number where a row has
        one (the true unique-per-person anchor: canonicalize() already
        forces every connection under one roll number onto the same
        name/college, so this key can't fragment). Rows saved before this
        existed carry no roll_number, so they fall back to (player, college)
        rather than silently vanishing.

        Ranked on PnL/match, not raw total -- a total keeps climbing forever
        just from staying connected and playing more matches, which rewards
        volume over quality. The ranked representative is the best AVERAGE
        among strategies that have played AT LEAST MIN_MATCHES_FOR_RANK
        games -- a two-match lucky outlier must not be able to represent a
        player over a strategy with an actual track record, or the threshold
        would not be protecting anything. A player with no strategy past the
        threshold yet gets a provisional row instead, picked by match count
        (closest to qualifying), not average, which isn't trustworthy yet.
        """
        ranked_best: dict = {}
        provisional_best: dict = {}
        for eid, row in self.entrants.items():
            key = row.get("roll_number") or (row["player"], row.get("college", ""))
            enriched = self._with_avg(eid, row)
            if row["matches"] >= MIN_MATCHES_FOR_RANK:
                cur = ranked_best.get(key)
                if cur is None or enriched["avg_pnl"] > cur["avg_pnl"]:
                    ranked_best[key] = enriched
            else:
                cur = provisional_best.get(key)
                if cur is None or row["matches"] > cur["matches"]:
                    provisional_best[key] = enriched

        # A player with a qualifying strategy doesn't also need a provisional row.
        for key in ranked_best:
            provisional_best.pop(key, None)

        ranked = sorted(ranked_best.values(), key=lambda r: -r["avg_pnl"])
        provisional = sorted(provisional_best.values(), key=lambda r: -r["matches"])
        return ranked, provisional

    def rows_for_player(self, name: str) -> list:
        """Every strategy this player has ever played, for their own eyes only."""
        rows = [self._with_avg(eid, row)
                for eid, row in self.entrants.items() if row["player"] == name]
        return sorted(rows, key=lambda r: -r["avg_pnl"])

    def analytics_for(self, eid: str) -> dict:
        """Chart-ready aggregates from the recent-match window, for one strategy.

        Pre-aggregated here rather than shipping the raw per-contract log to
        the browser: HISTORY_WINDOW matches x up to 20 deals x 5 rounds is a
        few thousand points, and none of the six charts need more than what's
        computed below.
        """
        hist = self.history.get(eid, [])
        pnl_series: list = []
        round_pnls: dict = {r: [] for r in range(1, 6)}
        scatter: list = []
        power_counts: dict = {}
        power_cost: dict = {}
        cum = 0.0
        for m in hist:
            cum += m["pnl"]
            pnl_series.append({"ts": m["ts"], "cum_pnl": round(cum, 2),
                                "match_pnl": round(m["pnl"], 2)})
            for deal in m.get("deals", []):
                score = deal["score"]
                for rd in deal.get("rounds", []):
                    round_pnls[rd["round"]].append(rd["pnl"])
                    scatter.append([score, rd["pnl"]])
            for p in m.get("powers_won", []):
                power_counts[p["power"]] = power_counts.get(p["power"], 0) + 1
                power_cost[p["power"]] = power_cost.get(p["power"], 0) + p["cost"]

        round_avg = {
            r: (round(sum(v) / len(v), 3) if v else 0.0) for r, v in round_pnls.items()
        }
        # Cap points shipped to the browser; a random slice is representative
        # enough for a scatter and keeps the payload bounded regardless of
        # how many contracts HISTORY_WINDOW matches happen to contain.
        if len(scatter) > 400:
            step = len(scatter) / 400
            scatter = [scatter[int(i * step)] for i in range(400)]

        return {
            "matches_in_window": len(hist),
            "pnl_series": pnl_series,
            "round_avg": round_avg,
            "scatter": scatter,
            "power_counts": power_counts,
            "power_cost": {k: round(v, 1) for k, v in power_cost.items()},
        }


# ════════════════════════════════════════════════════════════════════
#  DOWNLOADABLE CLIENT BUNDLE
# ════════════════════════════════════════════════════════════════════
#
# The only thing participants ever fetch FROM this server is client-side
# code: matchup.py plus the handful of provided-code modules it needs to run
# your bot in an isolated local process (sandbox.py and friends). None of it
# is server-specific -- every file here is already public, handed to every
# participant as part of the competition repo -- and none of it is YOUR
# strategies/*.py, which never leaves your machine. server.py itself is
# never part of this bundle.

_CLIENT_FILES = [
    ("matchup.py", LADDER_DIR / "matchup.py"),
    ("protocol.py", LADDER_DIR / "protocol.py"),
    ("engine.py", LADDER_DIR.parent / "engine.py"),
    ("game_config.py", LADDER_DIR.parent / "game_config.py"),
    ("sandbox.py", LADDER_DIR.parent / "sandbox.py"),
    ("policy.py", LADDER_DIR.parent / "policy.py"),
    ("limits.py", LADDER_DIR.parent / "limits.py"),
    ("bot_loader.py", LADDER_DIR.parent / "bot_loader.py"),
]

_CLIENT_README = f"""Divided Oracle -- open leaderboard client
===========================================

    pip install aiohttp
    python matchup.py --strategies /path/to/your/strategies

Your bot source code never leaves this machine -- only per-turn decisions
(bids, quotes, accept/counter, transform yes/no) go over the wire. Your
chosen strategies/*.py runs locally inside sandbox.py's process isolation,
the same as the real tournament.

There is no --name flag. Every strategies/*.py is required (RULEBOOK.md
SS12) to open with

    # Name: ...
    # College: ...
    # Roll Number: ...

and matchup.py reads your leaderboard identity straight off whichever of
your files has that filled in.

matchup.py prints a private dashboard link on connect -- open it and pick
AS MANY of your bots as you like. Up to {MAX_ACTIVE_SLOTS} run at once (each
is a separate isolated process on YOUR machine); the rest queue and
activate automatically as slots free up. The server round-robins every
active strategy against everyone else's, automatically.

The public leaderboard shows one row per person: your single best-scoring
strategy. Your own private dashboard shows the full breakdown of all of
them.

matchup.py reconnects automatically (with backoff) if the connection drops
or the server restarts, and re-enters whatever you had selected -- no
manual restart needed.
"""


def build_client_zip() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for arcname, path in _CLIENT_FILES:
            zf.write(path, arcname)
        zf.writestr("README.txt", _CLIENT_README)
    return buf.getvalue()


class Ladder:
    def __init__(self, n_deals: int, max_concurrent: int, matches_per_pair: int):
        # Keyed by connection token, NOT player name: one player may run
        # several matchup.py processes at once, each entering a different
        # strategy file, so several Sessions legitimately share a name.
        # entrant_id() ("<player>::<bot>") is what actually disambiguates.
        self.sessions: dict[str, Session] = {}      # token -> Session
        self.board = Leaderboard(STATE_FILE)
        self.config = GameConfig()
        self.n_deals = n_deals
        self.matches_per_pair = matches_per_pair
        self.executor = ThreadPoolExecutor(max_workers=max_concurrent)
        self.max_concurrent = max_concurrent
        self.match_semaphore: asyncio.Semaphore | None = None
        self.high_match_semaphore: asyncio.Semaphore | None = None
        self.live_matches: dict = {}   # match_id -> {"a":..., "b":...}
        self.client_zip = build_client_zip()

    # ── websocket handling ──────────────────────────────────

    async def ws_handler(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse(heartbeat=25.0)
        await ws.prepare(request)
        session: Session | None = None
        loop = asyncio.get_running_loop()

        try:
            async for msg in ws:
                if msg.type != WSMsgType.TEXT:
                    continue
                try:
                    data = json.loads(msg.data)
                except json.JSONDecodeError:
                    continue

                kind = data.get("type")

                if kind == "hello":
                    name = str(data.get("name", "")).strip()[:40]
                    if not name:
                        await ws.send_json({"type": "error", "error": "name required"})
                        continue
                    # No kick-on-same-name: a player is welcome to run several
                    # matchup.py processes concurrently. A genuinely stale
                    # connection (crashed/restarted process) cleans itself up
                    # when its socket closes -- see the finally block below.
                    college = str(data.get("college", "")).strip()[:80]
                    roll_number = str(data.get("roll_number", "")).strip()[:40]
                    try:
                        client_version = int(data.get("client_version", 0))
                    except (TypeError, ValueError):
                        client_version = 0
                    canon_name, canon_college, mismatch = self.board.canonicalize(
                        roll_number, name, college)

                    token = uuid.uuid4().hex[:16]
                    session = Session(name=canon_name, token=token, ws=ws, loop=loop,
                                       college=canon_college, roll_number=roll_number,
                                       client_version=client_version,
                                       bots=list(data.get("bots", []))[:200])
                    self.sessions[token] = session
                    await session.send({
                        "type": "welcome", "token": token,
                        "dashboard_path": f"/player/{token}",
                    })
                    if client_version < CURRENT_CLIENT_VERSION:
                        await session.send({
                            "type": "identity_note",
                            "message": (
                                f"You're running an older matchup.py bundle — it still works "
                                f"fine, but a newer one is available from the dashboard's "
                                f"download button. You'll show up tagged \"stale bundle\" on "
                                f"the leaderboard until you update."
                            ),
                        })
                    if mismatch:
                        print(f"[identity] roll {roll_number}: header says "
                              f"'{name}' / '{college}', keeping the name on record: "
                              f"'{canon_name}' / '{canon_college}'")
                        await session.send({
                            "type": "identity_note",
                            "message": (
                                f"Your header says '{name}' ({college}), but roll number "
                                f"{roll_number} is already on record as '{canon_name}' "
                                f"({canon_college}) — using the one on record so you don't "
                                f"end up with two separate leaderboard entries. Ask the "
                                f"organizer if that's wrong."
                            ),
                        })
                    continue

                if session is None:
                    continue  # everything else requires hello first

                if kind == "bots_updated":
                    session.bots = list(data.get("bots", []))[:200]
                elif kind == "bot_status":
                    filename = data.get("filename")
                    slot = session.slots.get(filename)
                    if slot is not None:
                        slot.status = data.get("status", "none")
                        slot.error = data.get("error")
                        if slot.status == "invalid":
                            # Never going to play; free its place in line.
                            await self._promote_queued(session)
                elif kind == "rpc_result":
                    fut = session.pending.get(data.get("id"))
                    if fut and not fut.done():
                        if data.get("ok"):
                            fut.set_result(data.get("value"))
                        else:
                            fut.set_exception(RuntimeError(data.get("error", "client error")))
        finally:
            if session is not None:
                session.connected = False
                session.fail_pending("disconnected")
                self.sessions.pop(session.token, None)

        return ws

    # ── HTTP: public leaderboard ────────────────────────────

    async def state_json(self, request: web.Request) -> web.Response:
        slots_view = []
        for s in self.sessions.values():
            if not s.connected:
                continue
            for filename, slot in s.slots.items():
                slots_view.append({
                    "name": s.name, "college": s.college, "bot": filename,
                    "status": "playing" if slot.busy else slot.status,
                    "error": slot.error,
                    "stale": s.client_version < CURRENT_CLIENT_VERSION,
                    "connected_at": s.connected_at,
                    "num_active": len(s.slots),
                })

        persisted_ids = {
            r.get("roll_number") or (r["player"], r.get("college", ""))
            for r in self.board.entrants.values()
        }
        connected_ids = {
            s.roll_number or (s.name, s.college) for s in self.sessions.values() if s.connected
        }
        rejected = sum(
            1 for s in self.sessions.values() if s.connected
            for slot in s.slots.values() if slot.status == "invalid"
        )
        ranked, provisional = self.board.standings_public()

        return web.json_response({
            "standings": ranked,
            "provisional": provisional,
            "slots": slots_view,
            "recent_matches": list(reversed(self.board.match_log[-30:])),
            "n_deals": self.n_deals,
            "matches_per_pair": self.matches_per_pair,
            "max_active_slots": MAX_ACTIVE_SLOTS,
            "min_matches_for_rank": MIN_MATCHES_FOR_RANK,
            "stats": {
                "entrants": len(persisted_ids | connected_ids),
                "ranked": len(ranked),
                "in_queue": len(connected_ids - persisted_ids),
                "rejected": rejected,
            },
        })

    async def index(self, request: web.Request) -> web.Response:
        return web.Response(text=INDEX_HTML, content_type="text/html")

    async def download_client(self, request: web.Request) -> web.Response:
        return web.Response(
            body=self.client_zip,
            content_type="application/zip",
            headers={"Content-Disposition": 'attachment; filename="quantstorm-ladder-client.zip"'},
        )

    # ── HTTP: per-session bot picker ────────────────────────

    async def player_page(self, request: web.Request) -> web.Response:
        token = request.match_info["token"]
        if token not in self.sessions:
            return web.Response(text="Unknown session. Restart matchup.py.", status=404)
        return web.Response(text=PLAYER_HTML.replace("__TOKEN__", token), content_type="text/html")

    async def player_state(self, request: web.Request) -> web.Response:
        token = request.match_info["token"]
        session = self.sessions.get(token)
        if session is None:
            return web.json_response({"error": "session gone"}, status=404)
        slots = [
            {"filename": f, "status": s.status, "error": s.error, "busy": s.busy}
            for f, s in session.slots.items()
        ]
        my_scores = self.board.rows_for_player(session.name)
        for row in my_scores:
            row["analytics"] = self.board.analytics_for(row["entrant_id"])
        return web.json_response({
            "name": session.name, "college": session.college, "bots": session.bots,
            "slots": slots, "max_active_slots": MAX_ACTIVE_SLOTS,
            "my_scores": my_scores,
            "stale_bundle": session.client_version < CURRENT_CLIENT_VERSION,
        })

    async def player_select(self, request: web.Request) -> web.Response:
        """Body: {"filenames": [...]}, the FULL desired set of selected bots.

        Newly-added filenames become active if there's room under
        MAX_ACTIVE_SLOTS, else queued. Removed filenames are dropped (unless
        mid-match, which is refused rather than half-applied) and free their
        slot for the next queued one.
        """
        token = request.match_info["token"]
        session = self.sessions.get(token)
        if session is None or not session.connected:
            return web.json_response({"error": "not connected"}, status=404)

        body = await request.json()
        raw = body.get("filenames", [])
        if not isinstance(raw, list):
            return web.json_response({"error": "filenames must be a list"}, status=400)
        # Dedupe, preserve requested order (that order decides who gets an
        # active slot first), keep only bots this session actually reported.
        desired = list(dict.fromkeys(str(f) for f in raw if str(f) in session.bots))

        current = set(session.slots.keys())
        for f in current - set(desired):
            slot = session.slots[f]
            if slot.busy:
                continue  # refuse to pull a bot out from under a live match
            del session.slots[f]
            if slot.status in ("validating", "ready"):
                await session.send({"type": "unload_bot", "filename": f})

        for f in desired:
            if f in session.slots:
                continue
            if session.active_slot_count() < MAX_ACTIVE_SLOTS:
                session.slots[f] = Slot(filename=f, status="validating")
                await session.send({"type": "load_bot", "filename": f})
            else:
                session.slots[f] = Slot(filename=f, status="queued")

        # Deselecting an active slot just freed capacity a still-queued
        # filename can now take -- promote it rather than leaving it stuck
        # until the next unrelated event happens to call this.
        await self._promote_queued(session)

        return web.json_response({"ok": True})

    async def _promote_queued(self, session: Session):
        while session.active_slot_count() < MAX_ACTIVE_SLOTS:
            nxt = next((f for f, s in session.slots.items() if s.status == "queued"), None)
            if nxt is None:
                return
            session.slots[nxt].status = "validating"
            session.slots[nxt].error = None
            await session.send({"type": "load_bot", "filename": nxt})

    # ── round-robin auto-scheduler ──────────────────────────

    async def scheduler_loop(self):
        while True:
            await asyncio.sleep(ROUND_INTERVAL_S)
            try:
                await self._schedule_missing()
            except Exception as e:
                print(f"[scheduler] error: {e}")

    def _matches_so_far(self, session: Session, filename: str) -> int:
        row = self.board.entrants.get(entrant_id(session.name, filename))
        return row["matches"] if row else 0

    def _schedule_key(self, sf) -> tuple:
        matches = self._matches_so_far(*sf)
        return (0, matches) if matches == 0 else (1, matches)

    async def _schedule_missing(self):
        ready = [
            (s, f) for s in self.sessions.values() if s.connected
            for f, slot in s.slots.items() if slot.status == "ready" and not slot.busy
        ]
        # Zero-matches-first, then fewest-matches-first: a connected-and-ready
        # entrant that has never completed a game -- the dashboard's "In queue"
        # bucket -- must not wait behind every already-established pair, at
        # exactly the moment it most needs matches to reach MIN_MATCHES_FOR_RANK.
        # Sorting first means combinations() naturally produces these pairs
        # earlier, and since tasks acquire match_semaphore in the order they're
        # created, they get first crack at the available concurrency each tick.
        # Best-effort, not a hard guarantee -- a semaphore already saturated by
        # matches queued in an earlier tick still drains in that tick's order
        # first.
        ready.sort(key=self._schedule_key)
        for (sa, fa), (sb, fb) in itertools.combinations(ready, 2):
            ida, idb = entrant_id(sa.name, fa), entrant_id(sb.name, fb)
            if ida == idb:
                # Same player, same file selected twice over -- not a match.
                continue
            if self.board.games_played(ida, idb) >= self.matches_per_pair:
                continue
            asyncio.create_task(self.run_match(sa, fa, sb, fb))

    async def run_match(self, sa: Session, fa: str, sb: Session, fb: str):
        low_match = (self._matches_so_far(sa, fa) <= LOW_MATCH_THRESHOLD
                     or self._matches_so_far(sb, fb) <= LOW_MATCH_THRESHOLD)
        semaphore = self.match_semaphore if low_match else self.high_match_semaphore
        async with semaphore:
            slot_a, slot_b = sa.slots.get(fa), sb.slots.get(fb)
            if slot_a is None or slot_b is None:
                return  # deselected between scheduling and now
            if slot_a.busy or slot_b.busy or not sa.connected or not sb.connected:
                return
            slot_a.busy = slot_b.busy = True
            ida, idb = entrant_id(sa.name, fa), entrant_id(sb.name, fb)
            match_id = uuid.uuid4().hex[:8]
            self.live_matches[match_id] = {"a": ida, "b": idb, "started": time.time()}
            try:
                overrides = protocol.config_overrides(self.config)
                await sa.send({"type": "match_start", "match_id": match_id, "filename": fa,
                                "opponent": sb.name, "seat": 0, "overrides": overrides})
                await sb.send({"type": "match_start", "match_id": match_id, "filename": fb,
                                "opponent": sa.name, "seat": 1, "overrides": overrides})

                loop = asyncio.get_running_loop()
                proxy_a = RemoteBotProxy(sa, loop, fa, seat=0)
                proxy_b = RemoteBotProxy(sb, loop, fb, seat=1)
                seed = random.randrange(2 ** 32)

                result = await loop.run_in_executor(
                    self.executor, self._play_sync, proxy_a, proxy_b, seed, ida, idb,
                )
                proxy_a.flush_deal_powers()  # last deal's tape never got flushed by a next reset()
                proxy_b.flush_deal_powers()

                self.board.record(
                    ida, sa.name, sa.college, sa.roll_number, fa,
                    idb, sb.name, sb.college, sb.roll_number, fb,
                    result.pnl[0], result.pnl[1],
                    result.bot_a_warnings, result.bot_b_warnings,
                    detail_a=_match_detail(result, 0, proxy_a.powers_won),
                    detail_b=_match_detail(result, 1, proxy_b.powers_won),
                )
                await sa.send({"type": "match_end", "match_id": match_id, "filename": fa,
                                "opponent": sb.name, "pnl": result.pnl[0]})
                await sb.send({"type": "match_end", "match_id": match_id, "filename": fb,
                                "opponent": sa.name, "pnl": result.pnl[1]})
            except Exception as e:
                print(f"[match {match_id}] failed: {e}")
                for s, f, opp in ((sa, fa, sb.name), (sb, fb, sa.name)):
                    await s.send({"type": "match_end", "match_id": match_id, "filename": f,
                                   "opponent": opp, "error": str(e)})
            finally:
                self.live_matches.pop(match_id, None)
                if slot_a is not None:
                    slot_a.busy = False
                if slot_b is not None:
                    slot_b.busy = False

    def _play_sync(self, proxy_a, proxy_b, seed, name_a, name_b):
        return engine.play_match(
            lambda: proxy_a, lambda: proxy_b,
            config=self.config, seed=seed, mirror=True,
            n_deals=self.n_deals, verbose=False,
            bot_a_name=name_a, bot_b_name=name_b,
        )


# ════════════════════════════════════════════════════════════════════
#  DASHBOARD HTML (self-contained, no external assets)
# ════════════════════════════════════════════════════════════════════

_FAVICON = (
    "data:image/svg+xml,"
    "<svg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 100 100%22>"
    "<text y=%22.9em%22 font-size=%2290%22>%F0%9F%94%AE</text></svg>"
)

_BASE_STYLE = """
  :root {
    --bg: #14161c; --panel: #1b1e26; --panel-alt: #20232c; --line: #2a2e3a;
    --text: #e7e9ee; --dim: #8890a0; --dimmer: #5b6272;
    --accent: #f0c040; --gold: #f0c040; --silver: #c7cbd6; --bronze: #d38a4e;
    --pos: #5fd68a; --neg: #ff7a7a; --live: #5fd68a;
    --ready: #5fd68a; --busy: #f0c040; --idle: #5b6272; --queued: #6a7280;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0; background: var(--bg); color: var(--text);
    font: 14px/1.55 ui-sans-serif, -apple-system, "Segoe UI", sans-serif;
    -webkit-font-smoothing: antialiased;
  }
  .wrap { max-width: 1040px; margin: 0 auto; padding: 2.2rem 1.4rem 4rem; }
  a { color: var(--accent); }
  code, .mono { font: 12px/1.4 ui-monospace, SFMono-Regular, Menlo, monospace; }
  table { font-variant-numeric: tabular-nums; }
"""

INDEX_HTML = """<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Divided Oracle — Open Leaderboard</title>
<link rel="icon" href='""" + _FAVICON + """'>
<style>""" + _BASE_STYLE + """
  header { display: flex; align-items: baseline; justify-content: space-between;
           flex-wrap: wrap; gap: .6rem; margin-bottom: .3rem; }
  h1 { font-size: 1.3rem; margin: 0; letter-spacing: .01em; }
  h1 .ring { color: var(--accent); }
  .live-badge { display: inline-flex; align-items: center; gap: .4rem; font-size: .75rem;
                color: var(--dim); }
  .live-dot { width: .5rem; height: .5rem; border-radius: 50%; background: var(--live);
              box-shadow: 0 0 0 0 rgba(95,214,138,.6); animation: pulse 2s infinite; }
  @keyframes pulse {
    0%   { box-shadow: 0 0 0 0 rgba(95,214,138,.55); }
    70%  { box-shadow: 0 0 0 6px rgba(95,214,138,0); }
    100% { box-shadow: 0 0 0 0 rgba(95,214,138,0); }
  }
  .lede { color: var(--dim); font-size: .88rem; margin: .2rem 0 .7rem; max-width: 760px; }
  .lede code { background: var(--panel-alt); border: 1px solid var(--line); border-radius: 5px;
               padding: .15rem .45rem; color: var(--text); line-height: 1.8; }
  .dl-button { display: inline-flex; align-items: center; gap: .4rem; background: var(--accent);
               color: #1a1a1a; font-weight: 700; font-size: .85rem; padding: .55rem 1rem;
               border-radius: 7px; text-decoration: none; }
  .dl-button:hover { filter: brightness(1.08); }
  .gh-button { display: inline-flex; align-items: center; gap: .4rem; background: transparent;
               color: var(--text); font-weight: 600; font-size: .85rem; padding: .55rem 1rem;
               border: 1px solid var(--line); border-radius: 7px; text-decoration: none; }
  .gh-button:hover { border-color: var(--dim); background: var(--panel-alt); }
  .notice { border: 1px solid #6b5416; background: #3a2e0d; border-radius: 10px;
            padding: .9rem 1.1rem; margin: 1rem 0 1.4rem; font-size: .88rem;
            line-height: 1.6; color: #e7e9ee; }
  .notice b { color: var(--accent); }
  .notice a { color: var(--accent); text-decoration: underline; }
  .stat-row { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
              gap: .7rem; margin: 1.4rem 0; }
  details.explain { background: var(--panel); border: 1px solid var(--line); border-radius: 10px;
                     margin: 1.4rem 0; padding: .2rem 1rem; }
  details.explain summary { cursor: pointer; padding: .8rem 0; font-weight: 600; font-size: .92rem;
                             list-style: none; }
  details.explain summary::-webkit-details-marker { display: none; }
  details.explain summary::before { content: '▸ '; color: var(--accent); }
  details.explain[open] summary::before { content: '▾ '; }
  .explain-body { padding: 0 0 1rem; color: var(--dim); font-size: .87rem; line-height: 1.6; }
  .explain-body h3 { color: var(--text); font-size: .8rem; margin: 1.2rem 0 .4rem;
                      text-transform: uppercase; letter-spacing: .05em; }
  .explain-body h3:first-child { margin-top: 0; }
  .explain-body p { margin: .3rem 0; }
  .explain-body ul { margin: .3rem 0; padding-left: 1.2rem; }
  .explain-body li { margin: .35rem 0; }
  .explain-body b { color: var(--text); }
  .explain-body code { background: var(--panel-alt); border: 1px solid var(--line); border-radius: 4px;
                        padding: .05rem .35rem; color: var(--text); }
  details.submit { background: #14221a; border: 1px solid #2c5a3f; border-radius: 10px;
                   margin: 1.4rem 0; padding: .2rem 1rem; }
  details.submit summary { cursor: pointer; padding: .8rem 0; font-weight: 600; font-size: .92rem;
                            list-style: none; color: var(--pos); }
  details.submit summary::-webkit-details-marker { display: none; }
  details.submit summary::before { content: '▸ '; }
  details.submit[open] summary::before { content: '▾ '; }
  .submit-body { padding: 0 0 1rem; color: #cfe8da; font-size: .88rem; line-height: 1.7; }
  .submit-body ol { margin: .3rem 0; padding-left: 1.3rem; }
  .submit-body li { margin: .45rem 0; }
  .submit-body b { color: #e7e9ee; }
  .submit-body a { color: var(--pos); }
  .submit-body code { background: #1d2b24; border: 1px solid #2c5a3f; border-radius: 4px;
                      padding: .05rem .35rem; color: #cfe8da; }
  .stat-card { background: var(--panel); border: 1px solid var(--line); border-radius: 10px;
               padding: .8rem 1rem; }
  .stat-card .label { color: var(--dimmer); font-size: .68rem; text-transform: uppercase;
                       letter-spacing: .07em; font-weight: 600; }
  .stat-card .value { font-size: 1.6rem; font-weight: 700; margin-top: .15rem; }
  h2 { font-size: .78rem; text-transform: uppercase; letter-spacing: .07em; color: var(--dim);
       margin: 2.4rem 0 .7rem; font-weight: 600; }
  .card { background: var(--panel); border: 1px solid var(--line); border-radius: 10px;
          overflow: hidden; }
  .scroll { overflow-x: auto; }
  .scroll-tall { max-height: 420px; overflow-y: auto; }
  .scroll-tall thead th { position: sticky; top: 0; z-index: 1; }
  table { width: 100%; border-collapse: collapse; min-width: 480px; }
  th, td { padding: .6rem .9rem; text-align: right; border-bottom: 1px solid var(--line); }
  th:first-child, td:first-child, th:nth-child(2), td:nth-child(2), th:nth-child(3), td:nth-child(3)
    { text-align: left; }
  th { color: var(--dimmer); font-weight: 600; font-size: .7rem; text-transform: uppercase;
       letter-spacing: .06em; background: var(--panel-alt); }
  tbody tr:last-child td { border-bottom: none; }
  tbody tr:hover { background: var(--panel-alt); }
  .rank { font-weight: 700; color: var(--dimmer); width: 2.2rem; }
  .rank-1 .rank { color: var(--gold); } .rank-2 .rank { color: var(--silver); }
  .rank-3 .rank { color: var(--bronze); }
  .player-name { font-weight: 600; }
  .college { color: var(--dim); }
  .pnl-pos { color: var(--pos); font-weight: 600; }
  .pnl-neg { color: var(--neg); font-weight: 600; }
  .pnl-zero { color: var(--dim); }
  .status-pill { display: inline-flex; align-items: center; gap: .4rem; }
  .dot { display: inline-block; width: .5rem; height: .5rem; border-radius: 50%; }
  .dot-ready { background: var(--ready); } .dot-busy, .dot-playing { background: var(--busy); }
  .dot-idle, .dot-validating { background: var(--idle); } .dot-invalid { background: var(--neg); }
  .dot-queued { background: var(--queued); }
  .stale-badge { font-size: .65rem; text-transform: uppercase; letter-spacing: .04em;
                 padding: .1rem .45rem; border-radius: 1rem; background: rgba(240,192,64,.14);
                 color: var(--accent); border: 1px solid rgba(240,192,64,.35); cursor: help;
                 white-space: nowrap; display: inline-block; }
  #slots th:nth-child(4), #slots td:nth-child(4) { min-width: 13rem; }
  .group-row { cursor: pointer; }
  .group-row:hover { background: var(--panel-alt); }
  .chevron { display: inline-block; width: 1em; color: var(--dimmer); }
  .group-detail td.mono { padding-left: 1.6rem; }
  .group-detail:hover { background: var(--panel-alt); }
  .empty { color: var(--dimmer); text-align: center; padding: 1.6rem; font-style: italic; }
  .empty td { text-align: center; }
  footer { margin-top: 3rem; color: var(--dimmer); font-size: .78rem; }
</style></head><body><div class="wrap">

<header>
  <h1>Divided <span class="ring">Oracle</span> — Open Leaderboard</h1>
  <span class="live-badge"><span class="live-dot"></span>live, updates every few seconds</span>
</header>
<details class="submit" open>
  <summary>How to submit &amp; enter the ladder</summary>
  <div class="submit-body">
    <ol>
      <li><b>Download the client</b> — <a href="/download/matchup-client.zip">⬇ matchup-client.zip</a>
        and unzip it next to your <code>strategies/</code> folder.</li>
      <li>Make sure your strategy file opens with the mandatory header —
        <code># Name:</code>, <code># College:</code>, <code># Roll Number:</code> — filled in with
        your real details. This is also your leaderboard identity; leave it blank and the client
        won't start.</li>
      <li>Install the one dependency: <code>pip install aiohttp</code></li>
      <li>Run it:
        <code>python matchup.py --strategies /path/to/your/strategies</code></li>
      <li>Open the <b>private dashboard link it prints</b> and pick which of your bots to enter —
        only file names are shown, your code never leaves your machine.</li>
      <li>Keep <code>matchup.py</code> running — the server round-robins you against every other
        connected entrant automatically, and the leaderboard updates live.</li>
    </ol>
    <p>If the connection ever drops (server restart, wifi blip), <code>matchup.py</code> reconnects
    on its own and re-enters your selected bots — no action needed on your side.</p>
  </div>
</details>
<div class="notice">
  <b>This is not the official leaderboard.</b> It's an unofficial practice ladder built by
  me for informal matches between participants — not affiliated with or endorsed by
  the competition organizers, and your position here has no bearing on the official result.
  The official leaderboard is at
  <a href="https://divided-oracle-lb.proudcoast-aaefed12.centralindia.azurecontainerapps.io/" target="_blank" rel="noopener">divided-oracle-lb.proudcoast-aaefed12.centralindia.azurecontainerapps.io</a>.
</div>
<p class="lede">Round robin, auto-scheduled. Bot code never touches this server — only per-turn
decisions do. Score is each entrant's best PnL/match among their qualifying strategies — the
full breakdown of everything you've entered stays on your own private dashboard.</p>
<p class="lede">
  <a class="dl-button" href="/download/matchup-client.zip">⬇ Download the client (matchup.py)</a>
  <a class="gh-button" href="https://github.com/mrinmoy2developer/quantstorm-leaderboard" target="_blank" rel="noopener">↗ Open source on GitHub</a>
</p>
<p class="lede">Full setup steps are under "How to submit &amp; enter the ladder" below.</p>

<div class="stat-row" id="stats"></div>

<details class="card explain">
  <summary>How the leaderboard works</summary>
  <div class="explain-body">
    <h3>How a match is played</h3>
    <p>Each match is a real, complete game against the unmodified engine — <code id="exp-deals">…</code>
    deals plus their mirrored counterpart (mirroring cancels out who-got-dealt-what-luck), 5 rounds
    each, with the full blind TE auction, negotiation and settlement from <code>RULEBOOK.md</code>.
    Nothing is simulated or shortened. Your bot runs locally, inside the same process isolation the
    real tournament uses (<code>sandbox.py</code>) — the server only ever receives your bot's
    per-turn decisions (a bid, a quote, accept/counter, transform yes/no) over a websocket, never
    your source code.</p>

    <h3>How matches get scheduled</h3>
    <p>Fully automatic, no one runs anything by hand. Every 15 seconds the server looks at every
    strategy that's validated and currently idle, and schedules any pairing that hasn't yet played
    <code id="exp-mpp">…</code> times against each other. This runs continuously, so a newly-connected
    entrant gets scheduled against everyone else without any manual step, on either side.</p>

    <h3>What each column means</h3>
    <ul>
      <li><b>Matches</b> — total games that entry has completed.</li>
      <li><b>Avg/Match</b> — total PnL ÷ matches played. This is what determines rank, not the raw
        total, since a running total only rewards playing more, not playing better.</li>
      <li><b>Score</b> — lifetime cumulative PnL for that one strategy. Shown for reference; not the
        ranking criterion.</li>
    </ul>

    <h3>How ranking works</h3>
    <ul>
      <li><b>PnL/match, not total.</b> Two entrants with the same skill but very different match
        counts should score similarly — ranking on a running total instead would just reward
        whoever's been connected longest.</li>
      <li><b>A minimum match count first.</b> A strategy needs at least
        <code id="exp-min">…</code> completed matches before its average is trusted enough to rank
        on — otherwise one lucky or unlucky early result could put a mediocre bot at #1 or a strong
        one at the bottom. Below that threshold it shows under "Still building a track record"
        instead of in the real standings.</li>
      <li><b>One row per person.</b> The public standings show exactly one row per (name, college):
        your single best-averaging strategy among the ones that qualify. You can select as many
        strategies as you like on your own private dashboard — up to
        <code id="exp-slots">…</code> running at once, the rest queued — and see the full breakdown
        of every one of them there. Only the public board collapses it down to your best.</li>
    </ul>

    <h3>The stat cards</h3>
    <ul>
      <li><b>Entrants</b> — everyone who has ever connected.</li>
      <li><b>Ranked</b> — how many currently have a strategy past the minimum-match threshold.</li>
      <li><b>In queue</b> — connected, but hasn't completed a match yet.</li>
      <li><b>Rejected</b> — a selected file that failed the static safety check (illegal import,
        banned construct) and was never entered.</li>
    </ul>
  </div>
</details>

<h2>Standings</h2>
<p class="lede" id="rank-note"></p>
<div class="card scroll"><table id="standings">
  <thead><tr><th>#</th><th>Entrant</th><th>College</th><th>Bot</th><th>Matches</th><th>Avg/Match</th><th>Score</th></tr></thead>
  <tbody></tbody>
</table></div>

<h2>Still building a track record</h2>
<div class="card scroll"><table id="provisional">
  <thead><tr><th>Entrant</th><th>College</th><th>Bot</th><th>Matches</th><th>Avg/Match so far</th></tr></thead>
  <tbody></tbody>
</table></div>

<h2>Connected strategies</h2>
<div class="card scroll scroll-tall"><table id="slots">
  <thead><tr><th>Player</th><th>College</th><th>Bot</th><th>Status</th><th>Connected</th></tr></thead>
  <tbody></tbody>
</table></div>

<h2>Recent matches</h2>
<div class="card scroll"><table id="matches">
  <thead><tr><th>When</th><th>Entrant A</th><th>PnL</th><th>Entrant B</th><th>PnL</th></tr></thead>
  <tbody></tbody>
</table></div>

<footer>Your strategy code is never uploaded — matchup.py only ever ships per-turn decisions.
This server and client are open source:
<a href="https://github.com/mrinmoy2developer/quantstorm-leaderboard" target="_blank" rel="noopener">github.com/mrinmoy2developer/quantstorm-leaderboard</a></footer>
</div>
<script>
function fmtPnl(v){
  const c = v>1e-9?'pnl-pos':(v<-1e-9?'pnl-neg':'pnl-zero');
  const s = v>0?'+':'';
  return `<span class="${c}">${s}${v.toFixed(2)}</span>`;
}
function fmtAgo(ts){
  const s = Math.max(0, Math.floor(Date.now()/1000 - ts));
  if(s<60) return s+'s ago'; if(s<3600) return Math.floor(s/60)+'m ago';
  return Math.floor(s/3600)+'h ago';
}
function medal(i){ return i===0?'🥇':i===1?'🥈':i===2?'🥉':(i+1); }

// Connected strategies: grouped by entrant, collapsed by default. Rebuilt
// with createElement + addEventListener rather than inline onclick="" with
// interpolated names/colleges -- those are arbitrary participant text (an
// apostrophe in a college name would do exactly what broke the private
// dashboard earlier: terminate a string mid-attribute and take the rest of
// the page's script with it).
let lastSlots = [];
const expandedGroups = new Set();

function slotGroupKey(p){ return p.name + ' ' + p.college; }

function renderSlotsTable(){
  const tbody = document.querySelector('#slots tbody');
  const groups = new Map();
  lastSlots.forEach(p => {
    const key = slotGroupKey(p);
    if (!groups.has(key)) groups.set(key, {name: p.name, college: p.college, rows: []});
    groups.get(key).rows.push(p);
  });

  if (groups.size === 0) {
    tbody.innerHTML = '<tr class="empty"><td colspan=5>Nobody connected right now.</td></tr>';
    return;
  }

  tbody.innerHTML = '';
  for (const [key, g] of groups) {
    const open = expandedGroups.has(key);
    const anyStale = g.rows.some(r => r.stale);
    const anyPlaying = g.rows.some(r => r.status === 'playing');
    const anyReady = g.rows.some(r => r.status === 'ready');

    const head = document.createElement('tr');
    head.className = 'group-row';
    head.innerHTML =
      `<td class="player-name"><span class="chevron">${open ? '▾' : '▸'}</span> ${g.name}</td>`+
      `<td class="college">${g.college || '—'}</td>`+
      `<td class="mono">${g.rows.length} strateg${g.rows.length === 1 ? 'y' : 'ies'}</td>`+
      `<td>${anyPlaying ? '<span class="status-pill"><span class="dot dot-playing"></span>playing</span>' : (anyReady ? '<span class="status-pill"><span class="dot dot-ready"></span>ready</span>' : '')}`+
        `${anyStale ? ' <span class="stale-badge">stale bundle</span>' : ''}</td>`+
      `<td>${fmtAgo(Math.min(...g.rows.map(r => r.connected_at)))}</td>`;
    head.addEventListener('click', () => {
      if (expandedGroups.has(key)) expandedGroups.delete(key); else expandedGroups.add(key);
      renderSlotsTable();
    });
    tbody.appendChild(head);

    if (open) {
      g.rows.forEach(p => {
        const row = document.createElement('tr');
        row.className = 'group-detail';
        const stale = p.stale ? ' <span class="stale-badge">stale bundle</span>' : '';
        row.innerHTML =
          `<td></td><td></td><td class="mono">${p.bot}</td>`+
          `<td><span class="status-pill"><span class="dot dot-${p.status}"></span>${p.status}</span>${stale}</td>`+
          `<td>${fmtAgo(p.connected_at)}</td>`;
        tbody.appendChild(row);
      });
    }
  }
}

async function refresh(){
  const r = await fetch('/api/state'); const d = await r.json();

  document.getElementById('stats').innerHTML = [
    ['Entrants', d.stats.entrants], ['Ranked', d.stats.ranked],
    ['In queue', d.stats.in_queue], ['Rejected', d.stats.rejected],
  ].map(([label,value])=>
    `<div class="stat-card"><div class="label">${label}</div><div class="value">${value}</div></div>`
  ).join('');

  document.getElementById('exp-deals').textContent = `${d.n_deals} direct + ${d.n_deals} mirrored`;
  document.getElementById('exp-mpp').textContent = d.matches_per_pair;
  document.getElementById('exp-min').textContent = d.min_matches_for_rank;
  document.getElementById('exp-slots').textContent = d.max_active_slots;

  document.getElementById('rank-note').textContent =
    `Ranked on PnL/match, not total — a running total only rewards playing more, not playing ` +
    `better. Ranked once a strategy has played ${d.min_matches_for_rank}+ matches — otherwise ` +
    `the average is too noisy to mean much.`;

  document.querySelector('#standings tbody').innerHTML = d.standings.map((e,i)=>
    `<tr class="rank-${i+1}"><td class="rank">${medal(i)}</td>`+
    `<td class="player-name">${e.player}</td><td class="college">${e.college||'—'}</td>`+
    `<td class="mono">${e.bot}</td><td>${e.matches}</td><td>${fmtPnl(e.avg_pnl)}</td>`+
    `<td>${fmtPnl(e.pnl)}</td></tr>`
  ).join('') || '<tr class="empty"><td colspan=7>No one ranked yet — be the first to reach '+
    d.min_matches_for_rank+' matches.</td></tr>';

  document.querySelector('#provisional tbody').innerHTML = d.provisional.map(e=>
    `<tr><td class="player-name">${e.player}</td><td class="college">${e.college||'—'}</td>`+
    `<td class="mono">${e.bot}</td><td>${e.matches} / ${d.min_matches_for_rank}</td>`+
    `<td>${fmtPnl(e.avg_pnl)}</td></tr>`
  ).join('') || '<tr class="empty"><td colspan=5>Nobody in the on-ramp right now.</td></tr>';

  lastSlots = d.slots;
  renderSlotsTable();

  document.querySelector('#matches tbody').innerHTML = d.recent_matches.map(m=>
    `<tr><td>${fmtAgo(m.ts)}</td><td class="mono">${m.a}</td><td>${fmtPnl(m.pnl_a)}</td>`+
    `<td class="mono">${m.b}</td><td>${fmtPnl(m.pnl_b)}</td></tr>`
  ).join('') || '<tr class="empty"><td colspan=5>No matches yet.</td></tr>';
}
refresh(); setInterval(refresh, 3000);
</script></body></html>"""

PLAYER_HTML = """<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Pick your bots</title>
<link rel="icon" href='""" + _FAVICON + """'>
<style>""" + _BASE_STYLE + """
  .wrap { max-width: 620px; }
  h1 { font-size: 1.15rem; margin: 0 0 .3rem; }
  h2 { font-size: .78rem; text-transform: uppercase; letter-spacing: .07em; color: var(--dim);
       margin: 2rem 0 .6rem; font-weight: 600; }
  .status-line { color: var(--dim); font-size: .9rem; margin: 0 0 1rem; }
  .status-line b { color: var(--text); }
  .err { color: var(--neg); }
  .card { background: var(--panel); border: 1px solid var(--line); border-radius: 10px;
          padding: .4rem; }
  label.bot {
    display: flex; align-items: center; gap: .6rem; width: 100%;
    padding: .7rem .9rem; margin: .15rem 0; border: 1px solid transparent; border-radius: 7px;
    cursor: pointer;
  }
  label.bot:hover { background: var(--panel-alt); border-color: var(--line); }
  label.bot input { width: auto; accent-color: var(--pos); }
  label.bot .fname { font: 13px ui-monospace, SFMono-Regular, monospace; flex: 1; }
  .badge { font-size: .68rem; text-transform: uppercase; letter-spacing: .04em;
           padding: .15rem .5rem; border-radius: 1rem; background: var(--panel-alt);
           color: var(--dim); }
  .badge-ready { color: var(--pos); } .badge-invalid { color: var(--neg); }
  .badge-playing { color: var(--busy); } .badge-queued { color: var(--queued); }
  .foot { color: var(--dimmer); font-size: .8rem; margin-top: 1.2rem; }
  .empty { color: var(--dimmer); padding: 1rem; text-align: center; font-style: italic; }
  table { width: 100%; border-collapse: collapse; }
  th, td { padding: .5rem .7rem; text-align: right; border-bottom: 1px solid var(--line); }
  th:first-child, td:first-child { text-align: left; }
  th { color: var(--dimmer); font-weight: 600; font-size: .68rem; text-transform: uppercase; }
  tbody tr:last-child td { border-bottom: none; }
  .pnl-pos { color: var(--pos); font-weight: 600; } .pnl-neg { color: var(--neg); font-weight: 600; }
  .pnl-zero { color: var(--dim); }

  .analytics-head { display: flex; align-items: center; justify-content: space-between;
                     flex-wrap: wrap; gap: .5rem; margin: 2rem 0 .6rem; }
  .analytics-head h2 { margin: 0; }
  select.strategy-pick { background: var(--panel); border: 1px solid var(--line); color: var(--text);
                          font: 12px ui-monospace, SFMono-Regular, monospace; padding: .35rem .6rem;
                          border-radius: 6px; }
  .chart-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: .8rem; }
  .chart-card { background: var(--panel); border: 1px solid var(--line); border-radius: 10px;
                padding: .8rem .9rem; }
  .chart-card h4 { margin: 0 0 .5rem; font-size: .74rem; text-transform: uppercase;
                    letter-spacing: .05em; color: var(--dimmer); font-weight: 600; }
  .chart-svg { width: 100%; height: auto; overflow: visible; }
  .chart-baseline { stroke: var(--line); stroke-width: 1; }
  .chart-tick { fill: var(--dimmer); font-size: 9px; font-family: ui-monospace, monospace; }
  .chart-legend { display: flex; gap: .9rem; margin-top: .4rem; font-size: .72rem; color: var(--dim); }
  .chart-legend-item { display: inline-flex; align-items: center; gap: .3rem; }
  .chart-dot { width: .5rem; height: .5rem; border-radius: 50%; display: inline-block; }
  .chart-empty { color: var(--dimmer); font-size: .8rem; font-style: italic; padding: 1.5rem 0;
                 text-align: center; }
</style></head><body><div class="wrap">
<h1>Select which strategies to evaluate</h1>
<p class="status-line" id="status">loading…</p>
<div class="card" id="bots"></div>
<p class="foot">Only file names are shown here — your code stays on your machine. Up to
<b id="cap">4</b> run at once; the rest queue and activate automatically as slots free up.</p>

<h2>Your scores</h2>
<div class="card" style="padding:0"><table id="myscores">
  <thead><tr><th>Strategy</th><th>Matches</th><th>Avg/Match</th><th>PnL</th></tr></thead>
  <tbody></tbody>
</table></div>

<div class="analytics-head">
  <h2 style="margin-top:0">Analytics</h2>
  <select class="strategy-pick" id="chart-strategy"></select>
</div>
<div class="chart-grid" id="charts"></div>
</div>
<script>
const TOKEN = "__TOKEN__";
let selected = null;   // becomes an array once we've loaded server state once
let chartStrategy = null;  // entrant_id of the strategy the charts are showing
let myScoresCache = [];

// Fixed roles, not per-series identity: green/red always mean profit/loss
// (a diverging pair by sign), blue is the one neutral magnitude hue used
// where sign doesn't apply (auction wins, S-vs-PnL scatter).
const CHART_POS = '#5fd68a', CHART_NEG = '#ff7a7a', CHART_NEUTRAL = '#6a7280', CHART_BLUE = '#3987e5';

function fmtPnl(v){
  const c = v>1e-9?'pnl-pos':(v<-1e-9?'pnl-neg':'pnl-zero');
  const s = v>0?'+':'';
  return `<span class="${c}">${s}${v.toFixed(2)}</span>`;
}

// ── chart renderers: plain inline SVG, no dependency ──────

function lineChart(points, w, h){
  if (!points.length) return '<div class="chart-empty">No matches yet.</div>';
  const pad = {l:40, r:10, t:10, b:20};
  const ys = points.map(p=>p.cum_pnl);
  const minY = Math.min(0, ...ys), maxY = Math.max(0, ...ys);
  const range = (maxY-minY) || 1;
  const xScale = i => pad.l + (points.length<=1?0:i/(points.length-1)) * (w-pad.l-pad.r);
  const yScale = v => h-pad.b - ((v-minY)/range) * (h-pad.t-pad.b);
  const zeroY = yScale(0);
  const path = points.map((p,i)=>`${i===0?'M':'L'}${xScale(i).toFixed(1)},${yScale(p.cum_pnl).toFixed(1)}`).join(' ');
  const color = ys[ys.length-1] >= 0 ? CHART_POS : CHART_NEG;
  const dots = points.map((p,i)=>
    `<circle cx="${xScale(i).toFixed(1)}" cy="${yScale(p.cum_pnl).toFixed(1)}" r="2.5" fill="${color}">`+
    `<title>match ${i+1}: ${p.match_pnl>=0?'+':''}${p.match_pnl} (cumulative ${p.cum_pnl>=0?'+':''}${p.cum_pnl})</title></circle>`
  ).join('');
  return `<svg viewBox="0 0 ${w} ${h}" class="chart-svg" preserveAspectRatio="none">
    <line x1="${pad.l}" y1="${zeroY.toFixed(1)}" x2="${w-pad.r}" y2="${zeroY.toFixed(1)}" class="chart-baseline"/>
    <path d="${path}" fill="none" stroke="${color}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
    ${dots}
    <text x="${pad.l}" y="${h-4}" class="chart-tick">oldest</text>
    <text x="${w-pad.r}" y="${h-4}" text-anchor="end" class="chart-tick">latest</text>
  </svg>`;
}

function wldChart(row){
  const total = row.wins+row.losses+row.draws || 1;
  const segs = [['Win',row.wins,CHART_POS],['Loss',row.losses,CHART_NEG],['Draw',row.draws,CHART_NEUTRAL]];
  const w = 100, gap = 0.6;
  let x = 0;
  const bars = segs.map(([label,val])=>{
    const width = (val/total)*w;
    if (val<=0) return '';
    const [,,color] = segs.find(s=>s[0]===label);
    const bar = `<rect x="${x.toFixed(2)}" y="8" width="${Math.max(0,width-gap).toFixed(2)}" height="24" rx="3" fill="${color}">`+
      `<title>${label}: ${val} (${(val/total*100).toFixed(0)}%)</title></rect>`;
    x += width;
    return bar;
  }).join('');
  const legend = segs.map(([label,val,color])=>
    `<span class="chart-legend-item"><span class="chart-dot" style="background:${color}"></span>${label} ${val}</span>`
  ).join('');
  return `<svg viewBox="0 0 100 40" class="chart-svg" preserveAspectRatio="none">${bars}</svg>`+
    `<div class="chart-legend">${legend}</div>`;
}

function avgComparisonChart(rows, w, h){
  if (!rows.length) return '<div class="chart-empty">No matches yet.</div>';
  const rowH = h / rows.length;
  const pad = {l:110, r:46};
  const maxAbs = Math.max(1, ...rows.map(r=>Math.abs(r.avg_pnl)));
  const midX = pad.l + (w-pad.l-pad.r)/2;
  const scale = v => (v/maxAbs) * ((w-pad.l-pad.r)/2);
  const bars = rows.map((r,i)=>{
    const y = i*rowH;
    const bw = scale(r.avg_pnl);
    const color = r.avg_pnl>=0 ? CHART_POS : CHART_NEG;
    const x = bw>=0 ? midX : midX+bw;
    const isActive = r.entrant_id === chartStrategy;
    return `<text x="${pad.l-8}" y="${(y+rowH/2+3).toFixed(1)}" text-anchor="end" class="chart-tick mono" `+
      `${isActive?'font-weight="bold" fill="#e7e9ee"':''}>${r.bot.replace('.py','')}</text>`+
      `<rect x="${x.toFixed(1)}" y="${(y+rowH*0.2).toFixed(1)}" width="${Math.abs(bw).toFixed(1)}" `+
      `height="${(rowH*0.6).toFixed(1)}" rx="3" fill="${color}" opacity="${isActive?1:0.55}">`+
      `<title>${r.bot}: ${r.avg_pnl>=0?'+':''}${r.avg_pnl.toFixed(2)}/match</title></rect>`;
  }).join('');
  return `<svg viewBox="0 0 ${w} ${h}" class="chart-svg" preserveAspectRatio="none">
    <line x1="${midX.toFixed(1)}" y1="0" x2="${midX.toFixed(1)}" y2="${h}" class="chart-baseline"/>${bars}</svg>`;
}

function roundChart(roundAvg, w, h){
  const pad = {l:28, r:8, t:8, b:18};
  const vals = [1,2,3,4,5].map(r => roundAvg[String(r)] ?? roundAvg[r] ?? 0);
  if (!vals.some(v=>v!==0)) return '<div class="chart-empty">No contracts yet.</div>';
  const maxAbs = Math.max(1, ...vals.map(Math.abs));
  const barW = (w-pad.l-pad.r)/5;
  const zeroY = pad.t + (h-pad.t-pad.b)/2;
  const scale = v => (v/maxAbs) * ((h-pad.t-pad.b)/2);
  const bars = vals.map((v,i)=>{
    const bh = scale(v);
    const x = pad.l + i*barW;
    const color = v>=0 ? CHART_POS : CHART_NEG;
    const y = v>=0 ? zeroY-bh : zeroY;
    return `<rect x="${(x+barW*0.2).toFixed(1)}" y="${y.toFixed(1)}" width="${(barW*0.6).toFixed(1)}" `+
      `height="${Math.abs(bh).toFixed(1)}" rx="3" fill="${color}"><title>Round ${i+1}: `+
      `${v>=0?'+':''}${v.toFixed(2)} avg contract PnL</title></rect>`+
      `<text x="${(x+barW/2).toFixed(1)}" y="${h-4}" text-anchor="middle" class="chart-tick">R${i+1}</text>`;
  }).join('');
  return `<svg viewBox="0 0 ${w} ${h}" class="chart-svg" preserveAspectRatio="none">
    <line x1="${pad.l}" y1="${zeroY.toFixed(1)}" x2="${w-pad.r}" y2="${zeroY.toFixed(1)}" class="chart-baseline"/>${bars}</svg>`;
}

function powerChart(counts, w, h){
  const powers = ['FORESIGHT','TRICK_ROOM','SUBSTITUTE','STEALTH_ROCK','TRANSFORM'];
  const vals = powers.map(p => counts[p]||0);
  if (!vals.some(v=>v>0)) return '<div class="chart-empty">No powers won yet.</div>';
  const max = Math.max(1, ...vals);
  const pad = {l:28, r:8, t:8, b:28};
  const barW = (w-pad.l-pad.r)/5;
  const bars = powers.map((p,i)=>{
    const v = vals[i];
    const bh = (v/max)*(h-pad.t-pad.b);
    const x = pad.l+i*barW;
    return `<rect x="${(x+barW*0.15).toFixed(1)}" y="${(h-pad.b-bh).toFixed(1)}" width="${(barW*0.7).toFixed(1)}" `+
      `height="${bh.toFixed(1)}" rx="3" fill="${CHART_BLUE}"><title>${p}: won ${v}x</title></rect>`+
      `<text x="${(x+barW/2).toFixed(1)}" y="${h-16}" text-anchor="middle" class="chart-tick">${v||''}</text>`+
      `<text x="${(x+barW/2).toFixed(1)}" y="${h-4}" text-anchor="middle" class="chart-tick" `+
      `style="font-size:7px">${p.split('_')[0].slice(0,7)}</text>`;
  }).join('');
  return `<svg viewBox="0 0 ${w} ${h}" class="chart-svg" preserveAspectRatio="none">
    <line x1="${pad.l}" y1="${h-pad.b}" x2="${w-pad.r}" y2="${h-pad.b}" class="chart-baseline"/>${bars}</svg>`;
}

function scatterChart(points, w, h){
  if (!points.length) return '<div class="chart-empty">No contracts yet.</div>';
  const pad = {l:34, r:10, t:10, b:22};
  const maxAbsX = Math.max(1, ...points.map(p=>Math.abs(p[0])));
  const maxAbsY = Math.max(1, ...points.map(p=>Math.abs(p[1])));
  const xScale = x => pad.l + ((x+maxAbsX)/(2*maxAbsX)) * (w-pad.l-pad.r);
  const yScale = y => h-pad.b - ((y+maxAbsY)/(2*maxAbsY)) * (h-pad.t-pad.b);
  const zeroX = xScale(0), zeroY = yScale(0);
  const dots = points.map(([S,pnl])=>
    `<circle cx="${xScale(S).toFixed(1)}" cy="${yScale(pnl).toFixed(1)}" r="2.5" fill="${CHART_BLUE}" `+
    `opacity="0.5"><title>S=${S}, contract PnL=${pnl}</title></circle>`
  ).join('');
  return `<svg viewBox="0 0 ${w} ${h}" class="chart-svg" preserveAspectRatio="none">
    <line x1="${pad.l}" y1="${zeroY.toFixed(1)}" x2="${w-pad.r}" y2="${zeroY.toFixed(1)}" class="chart-baseline"/>
    <line x1="${zeroX.toFixed(1)}" y1="${pad.t}" x2="${zeroX.toFixed(1)}" y2="${h-pad.b}" class="chart-baseline"/>
    ${dots}
    <text x="${w-pad.r}" y="${h-6}" text-anchor="end" class="chart-tick">S →</text>
    <text x="${pad.l+3}" y="${pad.t+8}" class="chart-tick">PnL ↑</text>
  </svg>`;
}

function renderCharts(){
  const row = myScoresCache.find(r => r.entrant_id === chartStrategy);
  const el = document.getElementById('charts');
  if (!row) { el.innerHTML = '<div class="chart-empty">No matches played yet.</div>'; return; }
  const a = row.analytics;
  el.innerHTML = `
    <div class="chart-card"><h4>PnL over time — ${row.bot}</h4>${lineChart(a.pnl_series, 300, 140)}</div>
    <div class="chart-card"><h4>Win / Loss / Draw — ${row.bot}</h4>${wldChart(row)}</div>
    <div class="chart-card"><h4>Avg PnL/match — all your strategies</h4>${avgComparisonChart(myScoresCache, 300, Math.max(60, myScoresCache.length*28))}</div>
    <div class="chart-card"><h4>PnL by round — ${row.bot}</h4>${roundChart(a.round_avg, 300, 140)}</div>
    <div class="chart-card"><h4>Powers won — ${row.bot}</h4>${powerChart(a.power_counts, 300, 140)}</div>
    <div class="chart-card"><h4>Contract PnL vs true score S — ${row.bot}</h4>${scatterChart(a.scatter, 300, 160)}</div>
  `;
  const w = a.matches_in_window;
  document.querySelectorAll('.chart-card h4')[0].title = `Last ${w} match(es) in the analytics window`;
}

async function refresh(){
  const r = await fetch(`/player/${TOKEN}/state`);
  if(!r.ok){ document.getElementById('status').innerHTML = '<span class="err">Session gone — restart matchup.py.</span>'; return; }
  const d = await r.json();
  document.getElementById('cap').textContent = d.max_active_slots;

  const byFile = {}; d.slots.forEach(s => byFile[s.filename] = s);
  if (selected === null) selected = d.slots.map(s => s.filename);

  const nActive = d.slots.filter(s => s.status==='ready' || s.status==='validating').length;
  document.getElementById('status').innerHTML =
    `Connected as <b>${d.name}</b> (${d.college||'no college given'}). ` +
    `${nActive} of ${d.max_active_slots} active slots in use.` +
    (d.stale_bundle ? ' <span class="err">Running an older matchup.py — still works, but grab '+
      'the latest from the download button on the leaderboard when you can.</span>' : '');

  document.getElementById('bots').innerHTML = d.bots.map(b=>{
    const slot = byFile[b];
    const checked = selected.includes(b) ? 'checked' : '';
    let badge = '';
    if (slot) {
      const label = slot.busy ? 'playing' : slot.status;
      badge = `<span class="badge badge-${label}">${label}${slot.error?': '+slot.error:''}</span>`;
    }
    return `<label class="bot"><input type="checkbox" ${checked} ${slot&&slot.busy?'disabled':''} `+
      `onchange="toggle('${b}', this.checked)"><span class="fname">${b}</span>${badge}</label>`;
  }).join('') || '<p class="empty">No strategies found by matchup.py.</p>';

  const rows = d.my_scores || [];
  myScoresCache = rows;
  document.querySelector('#myscores tbody').innerHTML = rows.map(row =>
    `<tr><td class="mono">${row.bot}</td><td>${row.matches}</td>`+
    `<td>${fmtPnl(row.avg_pnl)}</td><td>${fmtPnl(row.pnl)}</td></tr>`
  ).join('') || '<tr><td colspan=4 class="empty">No matches played yet.</td></tr>';

  const picker = document.getElementById('chart-strategy');
  if (!rows.some(r => r.entrant_id === chartStrategy)) {
    chartStrategy = rows.length ? rows[0].entrant_id : null;
  }
  picker.innerHTML = rows.map(r =>
    `<option value="${r.entrant_id}" ${r.entrant_id===chartStrategy?'selected':''}>${r.bot}</option>`
  ).join('') || '<option>no strategies yet</option>';
  renderCharts();
}

document.getElementById('chart-strategy').addEventListener('change', e => {
  chartStrategy = e.target.value;
  renderCharts();
});

async function toggle(filename, isChecked){
  if (isChecked) { if(!selected.includes(filename)) selected.push(filename); }
  else { selected = selected.filter(f => f !== filename); }
  await fetch(`/player/${TOKEN}/select`, {method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({filenames: selected})});
  refresh();
}
refresh(); setInterval(refresh, 2000);
</script></body></html>"""


# ════════════════════════════════════════════════════════════════════
#  ENTRYPOINT
# ════════════════════════════════════════════════════════════════════

def build_app(ladder: Ladder) -> web.Application:
    app = web.Application()
    app.router.add_get("/ws", ladder.ws_handler)
    app.router.add_get("/", ladder.index)
    app.router.add_get("/download/matchup-client.zip", ladder.download_client)
    app.router.add_get("/api/state", ladder.state_json)
    app.router.add_get("/player/{token}", ladder.player_page)
    app.router.add_get("/player/{token}/state", ladder.player_state)
    app.router.add_post("/player/{token}/select", ladder.player_select)

    async def on_startup(app):
        ladder.match_semaphore = asyncio.Semaphore(ladder.max_concurrent)
        ladder.high_match_semaphore = asyncio.Semaphore(
            max(1, ladder.max_concurrent - RESERVED_LOW_SLOTS))
        app["scheduler_task"] = asyncio.create_task(ladder.scheduler_loop())

    app.on_startup.append(on_startup)
    return app


def main():
    ap = argparse.ArgumentParser(description="Divided Oracle open leaderboard server")
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--n_deals", type=int, default=8, help="deals per phase per match (mirror doubles it)")
    ap.add_argument("--max_concurrent", type=int, default=3)
    ap.add_argument("--matches_per_pair", type=int, default=2)
    args = ap.parse_args()

    ladder = Ladder(args.n_deals, args.max_concurrent, args.matches_per_pair)
    app = build_app(ladder)
    print(f"Divided Oracle ladder listening on {args.host}:{args.port}")
    print(f"  dashboard: http://{args.host}:{args.port}/")
    print(f"  websocket: ws://{args.host}:{args.port}/ws")
    web.run_app(app, host=args.host, port=args.port, print=None)


if __name__ == "__main__":
    main()
