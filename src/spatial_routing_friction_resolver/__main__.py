"""Command-line route for a small waypoint set."""

from __future__ import annotations

import asyncio
import logging

from .engine import SpatialRoutingFrictionResolver


def _point(ident: int, lat: float, lon: float, congestion: float) -> dict[str, object]:
    return {"id": ident, "lat": lat, "lon": lon, "congestion": congestion}


def main() -> int:
    """Solve one city sample and log the path dict."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    resolver = SpatialRoutingFrictionResolver()
    records = [
        _point(1, 40.7128, -74.0060, 0.20),
        _point(9, 40.7128, -74.0060, 0.20),
        _point(2, 40.7306, -73.9352, 0.55),
        _point(4, 40.7128, -74.0060, 0.10),
        _point(3, 40.6782, -73.9442, 0.15),
    ]
    report = asyncio.run(resolver.run(records))
    logging.getLogger(__name__).info("%s", report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
