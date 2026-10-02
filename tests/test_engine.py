"""Wire, Dijkstra, and edge-case tests. No network."""

from __future__ import annotations

import asyncio
import logging
import math
import unittest

from spatial_routing_friction_resolver import (
    EngineKernelException,
    SpatialRoutingFrictionResolver,
)
from spatial_routing_friction_resolver.engine import (
    TURN_PENALTY_M,
    haversine_m,
)
from spatial_routing_friction_resolver.wire import pack_waypoint, unpack_waypoint

logging.getLogger("spatial_routing_friction_resolver").setLevel(logging.CRITICAL)


def _point(ident: int, lat: float, lon: float, congestion: float) -> dict[str, object]:
    return {"id": ident, "lat": lat, "lon": lon, "congestion": congestion}


class SpatialEngineTest(unittest.TestCase):
    def test_wire_roundtrip(self) -> None:
        payload = pack_waypoint(7, 40.5, -74.25, 0.25)
        frame = unpack_waypoint(payload)
        self.assertEqual(frame["id"], 7)
        self.assertEqual(frame["lat"], 40.5)
        self.assertEqual(frame["lon"], -74.25)
        self.assertEqual(frame["congestion"], 0.25)
        self.assertEqual(payload, pack_waypoint(7, 40.5, -74.25, 0.25))
        with self.assertRaises(EngineKernelException):
            unpack_waypoint(b"\x00\x01")

    def test_happy_path_skips_detour(self) -> None:
        report = asyncio.run(
            SpatialRoutingFrictionResolver().run(
                [
                    _point(1, 0.0, 0.0, 0.0),
                    _point(2, 0.0, 5.0, 0.0),
                    _point(3, 0.0, 0.1, 0.0),
                ]
            )
        )
        self.assertEqual(report["path"], [1, 3])
        self.assertEqual(len(report["edges"]), 1)
        self.assertGreater(float(report["total_friction_m"]), 10_000.0)
        self.assertNotIn(2, report["path"])
        edge = report["edges"][0]
        self.assertEqual(edge["from_id"], 1)
        self.assertEqual(edge["to_id"], 3)
        self.assertGreater(float(edge["distance_m"]), 0.0)

    def test_duplicate_consecutive_points_are_dropped(self) -> None:
        report = asyncio.run(
            SpatialRoutingFrictionResolver().run(
                [
                    _point(1, 40.0, -74.0, 0.0),
                    _point(9, 40.0, -74.0, 0.2),
                    _point(2, 41.0, -73.0, 0.1),
                ]
            )
        )
        self.assertEqual(report["duplicate_dropped"], 1)
        self.assertEqual(report["path"], [1, 2])
        self.assertNotIn(9, report["path"])
        self.assertTrue(
            all(float(edge["distance_m"]) > 0.0 for edge in report["edges"])
        )

    def test_zero_length_segment_is_dropped(self) -> None:
        report = asyncio.run(
            SpatialRoutingFrictionResolver().run(
                [
                    _point(1, 0.0, 0.0, 0.0),
                    _point(2, 0.0, 1.0, 0.0),
                    _point(3, 0.0, 0.0, 0.0),
                ]
            )
        )
        self.assertEqual(report["path"], [1, 2, 3])
        self.assertGreaterEqual(int(report["zero_length_dropped"]), 1)
        self.assertTrue(
            all(float(edge["distance_m"]) > 0.0 for edge in report["edges"])
        )
        self.assertTrue(
            math.isclose(
                float(report["edges"][1]["turn_penalty_m"]),
                TURN_PENALTY_M,
                rel_tol=1e-6,
                abs_tol=1e-4,
            )
        )
        distance = haversine_m(0.0, 0.0, 0.0, 1.0)
        expected = 2.0 * distance + TURN_PENALTY_M
        self.assertTrue(
            math.isclose(
                float(report["total_friction_m"]),
                expected,
                rel_tol=1e-6,
                abs_tol=1e-3,
            )
        )
        self.assertTrue(math.isfinite(float(report["total_friction_m"])))
