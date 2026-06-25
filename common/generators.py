"""Realistic value generators for the simulated field devices.

* BMS/HVAC: smooth diurnal drift within the sample-calibrated [lo, hi] envelope,
  plus small per-tick noise and occasional faults.
* EMS/manufacturing: machine-type-specific load profiles
  - Injection Moulding: periodic clamp+heat cycles (square-ish bursts).
  - CNC: stepped spindle load (discrete cutting passes).
  - Heating: slow ramp / soak / cool saw-tooth.
  Energy (kWh) is integrated from power (kW) over elapsed time.
"""

from __future__ import annotations

import math
import random
import time
from typing import Dict

from .registry import Device, MetricSpec

_SECONDS_PER_DAY = 86_400.0


def _diurnal_factor(now: float) -> float:
    """0..1 occupancy-style curve peaking mid-afternoon."""
    frac = (now % _SECONDS_PER_DAY) / _SECONDS_PER_DAY
    # peak ~15:00, trough ~03:00
    return 0.5 + 0.5 * math.sin(2 * math.pi * (frac - 0.25))


def bms_metric_value(metric: MetricSpec, now: float, *, fault: bool = False) -> float:
    """Generate one HVAC metric reading."""
    span = metric.hi - metric.lo
    base = metric.lo + span * (0.35 + 0.5 * _diurnal_factor(now))
    noise = random.uniform(-0.02, 0.02) * span
    value = base + noise
    if fault:  # transient excursion / sensor glitch
        value += random.choice([-1, 1]) * span * random.uniform(0.3, 0.6)
    return round(max(metric.lo - span * 0.1, min(metric.hi + span * 0.1, value)), 4)


def ems_power(machine_type: str, lo: float, hi: float, nominal: float, now: float) -> float:
    """Instantaneous power (kW) for a manufacturing machine."""
    diurnal = _diurnal_factor(now)            # production heavier in the day
    duty = 0.25 + 0.7 * diurnal               # fraction of time machine is loaded

    if machine_type == "INJECTION_MOULDING":
        cycle = 45.0                          # ~45s mould cycle
        phase = (now % cycle) / cycle
        loaded = phase < duty                 # clamp + inject + hold
        level = hi if loaded else lo
    elif machine_type == "CNC":
        step = 30.0                           # discrete cutting passes
        passes = [lo, nominal * 0.7, nominal, hi * 0.85]
        idx = int((now // step)) % len(passes)
        level = passes[idx] if random.random() < duty else lo
    elif machine_type == "HEATING":
        period = 600.0                        # 10-min ramp/soak/cool saw-tooth
        phase = (now % period) / period
        if phase < 0.3:                       # ramp up
            level = lo + (hi - lo) * (phase / 0.3)
        elif phase < 0.7:                     # soak
            level = nominal
        else:                                 # cool
            level = nominal - (nominal - lo) * ((phase - 0.7) / 0.3)
    else:
        level = nominal

    return round(max(0.0, level + random.uniform(-0.03, 0.03) * (hi - lo)), 4)


class EnergyIntegrator:
    """Accumulates kWh from successive kW samples (per device)."""

    def __init__(self) -> None:
        self._total_kwh: Dict[str, float] = {}
        self._last_t: Dict[str, float] = {}

    def add(self, device_id: str, power_kw: float, now: float) -> float:
        last = self._last_t.get(device_id)
        total = self._total_kwh.get(device_id, 0.0)
        if last is not None:
            total += power_kw * ((now - last) / 3600.0)
        self._total_kwh[device_id] = total
        self._last_t[device_id] = now
        return round(total, 5)
