"""Little-endian waypoint frames for the routing.edges stand-in."""

from __future__ import annotations

import struct

from .exceptions import EngineKernelException

FORMAT: str = "<Iddd"
FRAME: struct.Struct = struct.Struct(FORMAT)


def pack_waypoint(ident: int, lat: float, lon: float, congestion: float) -> bytes:
    """Pack one waypoint.

    Layout, little-endian: id ``uint32``, latitude ``float64``, longitude
    ``float64``, congestion ``float64``.
    """
    if isinstance(ident, bool) or not isinstance(ident, int):
        raise EngineKernelException("waypoint id must be an integer")
    if ident < 0 or ident > 0xFFFFFFFF:
        raise EngineKernelException(f"waypoint id out of uint32 range: {ident}")
    try:
        return FRAME.pack(ident, float(lat), float(lon), float(congestion))
    except (struct.error, OverflowError, ValueError) as exc:
        raise EngineKernelException("waypoint pack failed") from exc


def unpack_waypoint(payload: bytes) -> dict[str, object]:
    """Unpack one waypoint. Finite values round-trip exactly."""
    if len(payload) != FRAME.size:
        raise EngineKernelException(f"waypoint length {len(payload)} != {FRAME.size}")
    try:
        ident, lat, lon, congestion = FRAME.unpack(payload)
    except struct.error as exc:
        raise EngineKernelException("waypoint unpack failed") from exc
    return {
        "id": int(ident),
        "lat": float(lat),
        "lon": float(lon),
        "congestion": float(congestion),
    }
