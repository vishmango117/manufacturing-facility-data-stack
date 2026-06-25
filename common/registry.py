"""Canonical device registry — the single source of truth for every machine.

Design principle: **one IoT stream per machine**. Each device defined here gets its
own Modbus slave unit-id, its own polling/publishing task, and its own Kafka message
key (``deviceId``). Nothing is pre-aggregated.

Two domains:

* **BMS / HVAC** — derived directly from ``samples/bms.csv`` so the simulated points,
  their metric keys, and value ranges mirror the real facility data. Enriched with
  ``machines.csv`` (``bmsTag`` / ``energyTag``) where a point maps to a master asset.
* **EMS / manufacturing machines** — Injection Moulding, CNC and Heating machines.
  These are production assets (also scheduled by MES/ERP), each emitting its own
  ``{totalEnergy, totalPower}`` energy stream.
"""

from __future__ import annotations

import csv
import json
import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional

# Container path; falls back to repo-relative when running locally.
_samples_env = os.environ.get("SAMPLES_DIR")
if _samples_env:
    SAMPLES_DIR = Path(_samples_env)
elif (Path("/samples") / "bms.csv").exists():
    SAMPLES_DIR = Path("/samples")
else:
    # Walk up from this file to find samples/
    _cand = Path(__file__).resolve().parent.parent / "samples"
    SAMPLES_DIR = _cand if _cand.exists() else Path("/samples")
    del _cand
del _samples_env


# --------------------------------------------------------------------------- #
# Data model
# --------------------------------------------------------------------------- #
@dataclass
class MetricSpec:
    """A single measured signal exposed by a device (mapped to 2 Modbus registers)."""

    key: str            # e.g. "CHWS_Temp" or "totalPower"
    lo: float           # plausible minimum (calibration)
    hi: float           # plausible maximum (calibration)
    nominal: float      # baseline value the generator oscillates around


@dataclass
class Device:
    """One physically-modelled asset = one IoT stream."""

    device_id: str                       # natural key / Kafka message key (== sample `name`)
    source: str                          # "bms" | "ems"
    equipment_type: str                  # e.g. "Chillers", "AHU", "INJECTION_MOULDING"
    building: str                        # "Building-Alpha" | ...
    unit_id: int                         # Modbus slave unit id (per source)
    topic: str                           # Kafka topic this stream is produced to
    metrics: List[MetricSpec] = field(default_factory=list)
    bms_tag: Optional[str] = None
    energy_tag: Optional[str] = None

    def register_index(self, key: str) -> int:
        """Holding-register start address for a metric (2 registers / float)."""
        for i, m in enumerate(self.metrics):
            if m.key == key:
                return i * 2
        raise KeyError(key)


# --------------------------------------------------------------------------- #
# Topic mapping
# --------------------------------------------------------------------------- #
_BMS_TOPIC_BY_TYPE = {
    "AHU": "bms.hvac.ahu",
    "Chillers": "bms.hvac.chiller",
    "Chiller MainHeader": "bms.hvac.chiller_header",
    "CoolingTower MainHeader": "bms.hvac.cooling_tower",
    "Air Compressors": "bms.hvac.air_compressor",
    "Air Coolers": "bms.hvac.air_cooler",
}

_EMS_TOPIC_BY_TYPE = {
    "INJECTION_MOULDING": "ems.machine.injection_moulding",
    "CNC": "ems.machine.cnc",
    "HEATING": "ems.machine.heating",
}

ALL_TOPICS = sorted(set(_BMS_TOPIC_BY_TYPE.values()) | set(_EMS_TOPIC_BY_TYPE.values()))


def topic_for(source: str, equipment_type: str) -> str:
    table = _BMS_TOPIC_BY_TYPE if source == "bms" else _EMS_TOPIC_BY_TYPE
    return table.get(equipment_type, f"{source}.unknown")


# --------------------------------------------------------------------------- #
# BMS devices — parsed from samples/bms.csv
# --------------------------------------------------------------------------- #
def _bms_tag_lookup() -> Dict[str, dict]:
    """Map bmsTag -> machine master row (for type/energyTag enrichment)."""
    path = SAMPLES_DIR / "machines.csv"
    out: Dict[str, dict] = {}
    if not path.exists():
        return out
    with path.open(newline="") as fh:
        for row in csv.DictReader(fh):
            tag = (row.get("bmsTag") or "").strip()
            if tag and tag != "NULL":
                out[tag] = row
    return out


def _parse_bms_samples() -> Dict[str, Device]:
    """Build BMS device catalog with per-metric ranges derived from the sample."""
    path = SAMPLES_DIR / "bms.csv"
    if not path.exists():
        return {}

    machine_by_tag = _bms_tag_lookup()
    # Accumulate observed values per (device, metric) to calibrate ranges.
    acc: Dict[str, dict] = {}
    meta: Dict[str, dict] = {}
    with path.open(newline="") as fh:
        for row in csv.DictReader(fh):
            name = row["name"].strip()
            meta[name] = {"building": row["building"].strip(),
                          "equipmentType": row["equipmentType"].strip()}
            try:
                values = json.loads(row["value"])
            except (json.JSONDecodeError, KeyError):
                continue
            store = acc.setdefault(name, {})
            for k, v in values.items():
                if not isinstance(v, (int, float)):
                    continue
                s = store.setdefault(k, {"min": v, "max": v})
                s["min"] = min(s["min"], v)
                s["max"] = max(s["max"], v)

    devices: Dict[str, Device] = {}
    unit = 1
    for name in sorted(acc):
        equip_type = meta[name]["equipmentType"]
        metrics = []
        for key, s in sorted(acc[name].items()):
            lo, hi = s["min"], s["max"]
            if lo == hi:  # widen a flat sample so the generator has room to move
                lo, hi = lo - abs(lo) * 0.05 - 0.5, hi + abs(hi) * 0.05 + 0.5
            metrics.append(MetricSpec(key=key, lo=lo, hi=hi, nominal=(lo + hi) / 2))
        master = machine_by_tag.get(name, {})
        devices[name] = Device(
            device_id=name,
            source="bms",
            equipment_type=equip_type,
            building=meta[name]["building"],
            unit_id=unit,
            topic=topic_for("bms", equip_type),
            metrics=metrics,
            bms_tag=name,
            energy_tag=(master.get("energyTag") or None),
        )
        unit += 1
    return devices


# --------------------------------------------------------------------------- #
# EMS devices — manufacturing machines (Injection Moulding / CNC / Heating)
# --------------------------------------------------------------------------- #
# (building_code, building, type, count) — generates per-building machine fleets.
_EMS_FLEET = [
    ("BA", "Building-Alpha", "INJECTION_MOULDING", 3),
    ("BA", "Building-Alpha", "CNC", 3),
    ("BA", "Building-Alpha", "HEATING", 2),
    ("BB", "Building-Beta", "INJECTION_MOULDING", 2),
    ("BB", "Building-Beta", "CNC", 2),
    ("BB", "Building-Beta", "HEATING", 1),
    ("BG", "Building-Gamma", "INJECTION_MOULDING", 2),
    ("BG", "Building-Gamma", "CNC", 2),
    ("BG", "Building-Gamma", "HEATING", 1),
]

_EMS_CODE = {"INJECTION_MOULDING": "IMM", "CNC": "CNC", "HEATING": "HEAT"}

# Nominal power envelope (kW) per machine type for calibration.
_EMS_POWER = {
    "INJECTION_MOULDING": (8.0, 140.0, 75.0),   # idle, peak (clamp+heat), nominal
    "CNC": (2.0, 35.0, 15.0),                   # idle, spindle peak, nominal
    "HEATING": (10.0, 220.0, 120.0),            # standby, full ramp, soak
}


def _build_ems_devices(start_unit: int = 1) -> Dict[str, Device]:
    devices: Dict[str, Device] = {}
    unit = start_unit
    for code, building, mtype, count in _EMS_FLEET:
        for n in range(1, count + 1):
            mid = f"{code}-{_EMS_CODE[mtype]}-{n:02d}"
            lo, hi, nom = _EMS_POWER[mtype]
            metrics = [
                MetricSpec("totalPower", lo, hi, nom),         # instantaneous kW
                MetricSpec("totalEnergy", 0.0, 1_000_000.0, 0.0),  # cumulative kWh
            ]
            devices[mid] = Device(
                device_id=mid,
                source="ems",
                equipment_type=mtype,
                building=building,
                unit_id=unit,
                topic=topic_for("ems", mtype),
                metrics=metrics,
                energy_tag=mid,
            )
            unit += 1
    return devices


# --------------------------------------------------------------------------- #
# Public accessors
# --------------------------------------------------------------------------- #
@lru_cache(maxsize=1)
def bms_devices() -> List[Device]:
    return list(_parse_bms_samples().values())


@lru_cache(maxsize=1)
def ems_devices() -> List[Device]:
    return list(_build_ems_devices().values())


def devices_for(source: str) -> List[Device]:
    return bms_devices() if source == "bms" else ems_devices()


def ems_machine_master() -> List[dict]:
    """Rows for seeding the ERP `machines` table / dim_machine."""
    rows = []
    for d in ems_devices():
        rows.append({
            "machine_id": d.device_id,
            "machine_type": d.equipment_type,
            "building": d.building,
            "energy_tag": d.energy_tag,
            "rated_power_kw": _EMS_POWER[d.equipment_type][1],
        })
    return rows


if __name__ == "__main__":  # quick local introspection
    bms, ems = bms_devices(), ems_devices()
    print(f"BMS devices: {len(bms)} | EMS machines: {len(ems)}")
    for d in bms[:3] + ems[:3]:
        print(f"  {d.source:3} unit={d.unit_id:<3} {d.device_id:<22} "
              f"{d.equipment_type:<20} -> {d.topic} "
              f"({', '.join(m.key for m in d.metrics)})")
