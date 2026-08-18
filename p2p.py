"""Optional WebRTC signaling and tamper-evident transcript helpers.

The ladder's normal WebSocket RPC is the authoritative path.  This module is
deliberately optional: it lets two modern clients establish a direct data
channel, while the server still owns pairing and (eventually) transcript
replay.  Importing it never requires ``aiortc``; clients without that extra
package simply advertise no P2P capability and keep using the legacy path.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from typing import Awaitable, Callable

MAX_SIGNAL_BYTES = 48_000
SIGNAL_TYPES = frozenset({"offer", "answer", "candidate", "close"})


def canonical_json(value: object) -> str:
    """Stable encoding for a transcript hash chain."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def transcript_digest(previous: str, entry: dict) -> str:
    """Return the next digest for an append-only action transcript."""
    payload = f"{previous}\n{canonical_json(entry)}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def valid_signal(signal: object) -> bool:
    """Reject malformed or oversized WebRTC signaling before relaying it."""
    if not isinstance(signal, dict) or signal.get("type") not in SIGNAL_TYPES:
        return False
    try:
        return len(canonical_json(signal).encode("utf-8")) <= MAX_SIGNAL_BYTES
    except (TypeError, ValueError):
        return False


def p2p_enabled() -> bool:
    """Whether this client may opt in to direct transport for this run."""
    if os.environ.get("QUANTSTORM_ENABLE_P2P") != "1":
        return False
    try:
        import aiortc  # noqa: F401
    except ImportError:
        return False
    return True


class P2PTransport:
    """A best-effort WebRTC data channel negotiated through the ladder WS.

    It is intentionally a *side channel* for now.  Ranking continues through
    the normal server engine until signed action transcript replay is enabled
    server-side.  That means a failed direct connection cannot stall, alter,
    or fabricate a ranked result.
    """

    def __init__(self, match_id: str, send_signal: Callable[[dict], Awaitable[None]]):
        self.match_id = match_id
        self.send_signal = send_signal
        self.pc = None
        self.channel = None

    @classmethod
    async def start(cls, match_id: str, initiator: bool,
                    send_signal: Callable[[dict], Awaitable[None]]) -> "P2PTransport":
        if not p2p_enabled():
            raise RuntimeError("P2P is disabled or aiortc is not installed")
        from aiortc import RTCPeerConnection
        from aiortc.sdp import candidate_to_sdp

        self = cls(match_id, send_signal)
        self.pc = RTCPeerConnection()

        @self.pc.on("icecandidate")
        async def on_icecandidate(candidate):
            if candidate is None:
                return
            await self.send_signal({
                "type": "candidate",
                "candidate": candidate_to_sdp(candidate),
                "sdpMid": candidate.sdpMid,
                "sdpMLineIndex": candidate.sdpMLineIndex,
            })

        @self.pc.on("datachannel")
        def on_datachannel(channel):
            self.channel = channel

        if initiator:
            self.channel = self.pc.createDataChannel("quantstorm-actions", ordered=True)
            offer = await self.pc.createOffer()
            await self.pc.setLocalDescription(offer)
            await self.send_signal({"type": "offer", "sdp": offer.sdp})
        return self

    async def receive(self, signal: dict) -> None:
        """Apply one server-authorized offer, answer, or ICE candidate."""
        if self.pc is None:
            raise RuntimeError("P2P transport is closed")
        signal_type = signal.get("type")
        if signal_type == "close":
            await self.close()
            return

        if signal_type in {"offer", "answer"}:
            from aiortc import RTCSessionDescription
            await self.pc.setRemoteDescription(
                RTCSessionDescription(sdp=str(signal["sdp"]), type=signal_type))
            if signal_type == "offer":
                answer = await self.pc.createAnswer()
                await self.pc.setLocalDescription(answer)
                await self.send_signal({"type": "answer", "sdp": answer.sdp})
            return

        if signal_type == "candidate":
            from aiortc.sdp import candidate_from_sdp
            candidate = candidate_from_sdp(str(signal["candidate"]))
            candidate.sdpMid = signal.get("sdpMid")
            candidate.sdpMLineIndex = signal.get("sdpMLineIndex")
            await self.pc.addIceCandidate(candidate)

    async def close(self) -> None:
        if self.pc is not None:
            await self.pc.close()
            self.pc = None
        self.channel = None
