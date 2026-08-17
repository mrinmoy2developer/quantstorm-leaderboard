"""
ladder/protocol.py — wire format shared by server.py and matchup.py.

Only ever carries PER-TURN DECISIONS and the public Obs a bot is legally
allowed to see (RULEBOOK.md §9). No bot source code, no hidden game state
(hands, score) ever appears in a message built here.
"""

from __future__ import annotations

import sys
from pathlib import Path

# game_config.py sits one level up in the dev layout (ladder/ next to the
# rest of the repo) or right alongside this file in the standalone
# downloadable client bundle (flat layout). Both are safe to add.
_HERE = Path(__file__).resolve().parent
for _p in (_HERE, _HERE.parent):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from game_config import Contract, GameConfig, Obs  # noqa: E402


def serialize_contract(c: Contract) -> dict:
    return {
        "round": c.round, "price": c.price, "long_seat": c.long_seat,
        "forced": c.forced, "forcer": c.forcer, "shift": c.shift,
        "maker_seat": c.maker_seat, "open_bid": c.open_bid, "open_ask": c.open_ask,
    }


def deserialize_contract(d: dict) -> Contract:
    return Contract(**d)


def serialize_obs(obs: Obs) -> dict:
    return {
        "seat": obs.seat,
        "round": obs.round,
        "my_revealed": list(obs.my_revealed),
        "te_mine": obs.te_mine,
        "te_theirs": obs.te_theirs,
        "spread_cap": obs.spread_cap,
        "final_cap": obs.final_cap,
        "is_maker": obs.is_maker,
        "powers_mine": sorted(obs.powers_mine),
        "powers_theirs": sorted(obs.powers_theirs),
        "auction_log": [dict(d) for d in obs.auction_log],
        "contracts": [serialize_contract(c) for c in obs.contracts],
        "foresight": list(obs.foresight),
        "n_unknown_both": obs.n_unknown_both,
        "n_turns": obs.n_turns,
    }


def deserialize_obs(d: dict) -> Obs:
    return Obs(
        seat=d["seat"],
        round=d["round"],
        my_revealed=tuple(d["my_revealed"]),
        te_mine=d["te_mine"],
        te_theirs=d["te_theirs"],
        spread_cap=d["spread_cap"],
        final_cap=d["final_cap"],
        is_maker=d["is_maker"],
        powers_mine=frozenset(d["powers_mine"]),
        powers_theirs=frozenset(d["powers_theirs"]),
        auction_log=tuple(d["auction_log"]),
        contracts=tuple(deserialize_contract(c) for c in d["contracts"]),
        foresight=tuple(d["foresight"]),
        n_unknown_both=d["n_unknown_both"],
        n_turns=d["n_turns"],
    )


def serialize_quote(q) -> list:
    bid, ask = q
    return [int(bid), int(ask)]


def serialize_response(r):
    """respond() returns a str or ('COUNTER', bid, ask)."""
    if isinstance(r, str):
        return r
    kind, nb, na = r
    return [kind, int(nb), int(na)]


def deserialize_response(v):
    if isinstance(v, str):
        return v
    kind, nb, na = v
    return (kind, int(nb), int(na))


def config_overrides(config: GameConfig) -> dict:
    return dict(config._overrides)
