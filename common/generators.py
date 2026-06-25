"""Realistic value generators for the simulated field devices.

BMS/HVAC: equipment-type-specific control logic — setpoint tracking, diurnal
occupancy curves, correlated metrics, and occasional sensor faults.

EMS/manufacturing: machine-type-specific load profiles (injection-moulding clamp/
heat cycles, CNC stepped spindle passes, heating ramp/soak/cool sawtooth).
Energy (kWh) is integrated from power (kW) over elapsed time.
"""

from __future__ import annotations

import math
import random
from typing import TYPE_CHECKING, Dict, List

if TYPE_CHECKING:
    from .registry import Device, MetricSpec

_SECONDS_PER_DAY = 86_400.0


# --------------------------------------------------------------------------- #
# Shared helpers
# --------------------------------------------------------------------------- #

def _diurnal_factor(now: float) -> float:
    """0..1 occupancy-style curve peaking mid-afternoon (~15:00)."""
    frac = (now % _SECONDS_PER_DAY) / _SECONDS_PER_DAY
    return 0.5 + 0.5 * math.sin(2 * math.pi * (frac - 0.25))


def _n(scale: float = 1.0) -> float:
    """Small uniform noise centred on zero."""
    return random.uniform(-scale, scale)


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


# --------------------------------------------------------------------------- #
# Generic BMS fallback (for unknown equipment types)
# --------------------------------------------------------------------------- #

def bms_metric_value(metric: "MetricSpec", now: float, *, fault: bool = False) -> float:
    """Single-metric generic generator — fallback for unrecognised equipment types."""
    span = metric.hi - metric.lo
    base = metric.lo + span * (0.35 + 0.5 * _diurnal_factor(now))
    value = base + random.uniform(-0.02, 0.02) * span
    if fault:
        value += random.choice([-1, 1]) * span * random.uniform(0.3, 0.6)
    return round(_clamp(value, metric.lo - span * 0.1, metric.hi + span * 0.1), 4)


# --------------------------------------------------------------------------- #
# BMS equipment-type-specific generators
# Each returns {metric_key: float} for every metric the type exposes.
# --------------------------------------------------------------------------- #

def _chiller_values(d: "Device", now: float, fault: bool) -> Dict[str, float]:
    """Chiller: setpoint-controlled CHWS, load-tracking CHWR, ambient condenser."""
    load = _diurnal_factor(now)

    # CHWS tightly controlled to 7 °C setpoint
    chws = 7.0 + _n(0.25)

    # CHWR: ΔT widens with load (3 °C at min → 8 °C at full load)
    delta_t = 3.0 + 5.0 * load + _n(0.15)
    if fault and random.random() < 0.4:
        delta_t *= 0.45  # delta_t_low: partial-load bypass
    chwr = chws + delta_t

    # Condenser: supply warm (27-32 °C) tracking ambient heat rejection
    cdws = 27.0 + 5.0 * load + _n(0.4)
    cdwr = cdws + 3.0 + 1.5 * load + _n(0.25)

    return {
        "Chilled_Water_Supply_Temp": round(_clamp(chws, 5.0, 12.0), 4),
        "Chilled_Water_Return_Temp": round(_clamp(chwr, chws + 1.0, 18.0), 4),
        "Condensor_Supply_Temp":     round(_clamp(cdws, 24.0, 34.0), 4),
        "Condensor_Return_Temp":     round(_clamp(cdwr, cdws + 1.0, 38.0), 4),
    }


def _ahu_values(d: "Device", now: float, fault: bool) -> Dict[str, float]:
    """AHU: supply-air setpoint control, zone-coupled return, humidity tracking."""
    load = _diurnal_factor(now)

    supply_t = 14.0 + _n(0.35)
    return_t  = 22.0 + 5.5 * load + _n(0.5)

    # Supply RH controlled ~58%; return picks up humidity from occupants
    supply_rh = 58.0 + _n(1.5)
    return_rh  = supply_rh - 2.0 + 3.0 * load + _n(1.2)

    if fault:              # fan_stall: flow collapses, supply temp drifts up
        supply_t += 3.5

    result: Dict[str, float] = {
        "Supply_Temp": round(_clamp(supply_t, 10.0, 20.0), 4),
        "Return_Temp": round(_clamp(return_t, 18.0, 32.0), 4),
        "Supply_RH":   round(_clamp(supply_rh, 45.0, 75.0), 4),
        "Return_RH":   round(_clamp(return_rh, 40.0, 72.0), 4),
    }

    # Optional Supply_Flow metric (present on some AHUs)
    for m in d.metrics:
        if m.key == "Supply_Flow":
            flow = m.nominal * (0.55 + 0.45 * load) + _n(0.02 * m.nominal)
            if fault:
                flow *= 0.18
            result["Supply_Flow"] = round(_clamp(flow, 50.0, m.nominal * 1.1), 4)
            break

    return result


def _cooling_tower_values(d: "Device", now: float, fault: bool) -> Dict[str, float]:
    """Cooling tower ambient sensor: outdoor temp anti-correlated with RH."""
    load = _diurnal_factor(now)

    temp = 22.0 + 10.0 * load + _n(0.7)
    # RH drops as temperature rises during the day
    rh   = 78.0 - 16.0 * load + _n(2.0)

    if fault:
        temp += _n(2.5)

    return {
        "Outdoor_MIT_01_Temp": round(_clamp(temp, 14.0, 40.0), 4),
        "Outdoor_MIT_01_RH":   round(_clamp(rh,   30.0, 95.0), 4),
    }


def _chiller_header_values(d: "Device", now: float, fault: bool) -> Dict[str, float]:
    """Chiller plant header: correlated CHW/CW temps, differential pressures, flow."""
    load = _diurnal_factor(now)

    chws = 7.0 + _n(0.35)
    chwr = chws + 3.0 + 4.0 * load + _n(0.3)
    cdws = 28.0 + 4.5 * load + _n(0.4)
    cdwr = cdws + 3.0 + 2.0 * load + _n(0.3)

    # Differential pressures track flow demand
    chw_dpt = 210.0 + 70.0 * load + _n(6.0)
    cw_dpt  = 175.0 + 45.0 * load + _n(5.0)

    if fault and random.random() < 0.3:
        chw_dpt *= 0.55   # pressure transient

    flow    = 420.0 + 110.0 * load + _n(12.0)
    chw_pdt = chwr - chws                  # derived delta-T across plant

    return {
        "CHWS_Temp":           round(_clamp(chws,    5.0, 12.0), 4),
        "CHWR_Temp":           round(_clamp(chwr,    chws + 1.0, 18.0), 4),
        "CDWS_Temp":           round(_clamp(cdws,    24.0, 36.0), 4),
        "CDWR_Temp":           round(_clamp(cdwr,    cdws + 1.0, 40.0), 4),
        "CHW_DPT":             round(_clamp(chw_dpt, 120.0, 320.0), 4),
        "CW_DPT":              round(_clamp(cw_dpt,  100.0, 260.0), 4),
        "consumptionFlowRate": round(_clamp(flow,    150.0, 600.0), 4),
        "CHW_PDT":             round(_clamp(chw_pdt, 1.0, 10.0), 4),
    }


def _compressor_values(d: "Device", now: float, fault: bool) -> Dict[str, float]:
    """Air compressor: staged flow (discrete step-changes as compressors start/stop)."""
    load = _diurnal_factor(now)
    nom  = next((m.nominal for m in d.metrics if m.key == "Total_Flow"), 500.0)

    # Stage index changes every 5 min; 4 stages (25/50/75/100 % capacity)
    stage = int((now // 300)) % 4
    flow  = nom * (0.25 + 0.25 * stage) * (0.65 + 0.35 * load)
    flow += _n(0.015 * nom)

    if fault:
        flow *= 0.35

    return {"Total_Flow": round(_clamp(flow, 30.0, nom * 1.15), 4)}


def _air_cooler_values(d: "Device", now: float, fault: bool) -> Dict[str, float]:
    """Air cooler: mild supply-temp drift with zone thermal load."""
    load = _diurnal_factor(now)
    t = 14.0 + 2.5 * load + _n(0.45)
    if fault:
        t += _n(2.0)
    return {"Supply_Temp": round(_clamp(t, 10.0, 22.0), 4)}


# Registry of type → generator function
_BMS_GENERATORS = {
    "Chillers":              _chiller_values,
    "AHU":                   _ahu_values,
    "CoolingTower MainHeader": _cooling_tower_values,
    "Chiller MainHeader":    _chiller_header_values,
    "Air Compressors":       _compressor_values,
    "Air Coolers":           _air_cooler_values,
}


def bms_values(d: "Device", now: float, *, fault: bool = False) -> List[float]:
    """Return values for every metric in d.metrics in metric-list order.

    Dispatches to an equipment-type-specific generator; falls back to the
    generic per-metric generator for unknown types.
    """
    gen = _BMS_GENERATORS.get(d.equipment_type)
    if gen is None:
        return [bms_metric_value(m, now, fault=fault) for m in d.metrics]

    keyed = gen(d, now, fault)
    out = []
    for m in d.metrics:
        v = keyed.get(m.key)
        if v is None:
            v = bms_metric_value(m, now, fault=fault)
        out.append(v)
    return out


# --------------------------------------------------------------------------- #
# EMS manufacturing-machine generators
# --------------------------------------------------------------------------- #

def ems_power(machine_type: str, lo: float, hi: float, nominal: float,
              now: float) -> float:
    """Instantaneous power (kW) for a manufacturing machine."""
    diurnal = _diurnal_factor(now)
    duty    = 0.25 + 0.70 * diurnal   # fraction of time actively running

    if machine_type == "INJECTION_MOULDING":
        # 45-second mould cycle: clamp → inject → hold/cool → eject/idle
        cycle = 45.0
        phase = (now % cycle) / cycle
        if phase < 0.25:
            level = hi * 0.55 + _n(0.04 * hi)        # clamp stroke
        elif phase < 0.45:
            level = hi + _n(0.03 * hi)                # inject (peak demand)
        elif phase < 0.75:
            level = nominal * 0.75 + _n(0.04 * nominal)  # hold pressure / cool
        else:
            level = lo * 1.2 + _n(0.08 * lo)         # eject / mould open
        level *= (0.55 + 0.45 * duty)

    elif machine_type == "CNC":
        # 30-second step cycle: 6 discrete cutting passes
        step   = 30.0
        passes = [lo, nominal * 0.60, nominal * 0.85, hi * 0.90, nominal * 0.70, lo * 1.5]
        idx    = int((now // step)) % len(passes)
        if random.random() < duty:
            level = passes[idx] + _n(0.04 * (hi - lo))
        else:
            level = lo

    elif machine_type == "HEATING":
        # 10-minute sawtooth: ramp → soak → over-shoot correction → cool
        period = 600.0
        phase  = (now % period) / period
        if phase < 0.25:
            level = lo + (hi - lo) * (phase / 0.25)       # ramp up
        elif phase < 0.60:
            level = nominal + _n(0.025 * nominal)           # soak at setpoint
        elif phase < 0.75:
            blend = (phase - 0.60) / 0.15
            level = hi - (hi - nominal * 1.1) * blend      # brief overshoot → correct
        else:
            blend = (phase - 0.75) / 0.25
            level = nominal * 1.1 - (nominal * 1.1 - lo) * blend  # ramp down
    else:
        level = nominal

    return round(max(0.0, level + random.uniform(-0.015, 0.015) * (hi - lo)), 4)


# --------------------------------------------------------------------------- #
# Energy accumulator
# --------------------------------------------------------------------------- #

class EnergyIntegrator:
    """Accumulates kWh from successive kW samples (per device)."""

    def __init__(self) -> None:
        self._total_kwh: Dict[str, float] = {}
        self._last_t: Dict[str, float] = {}

    def add(self, device_id: str, power_kw: float, now: float) -> float:
        last  = self._last_t.get(device_id)
        total = self._total_kwh.get(device_id, 0.0)
        if last is not None:
            total += power_kw * ((now - last) / 3600.0)
        self._total_kwh[device_id] = total
        self._last_t[device_id]    = now
        return round(total, 5)
