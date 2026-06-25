"""Equipment-type profiles for BMS/EMS simulators.

Each profile defines the metric keys, setpoints, and operational logic that a
real-world device of that type would exhibit.  The generators in
``common.generators`` read these profiles to produce realistic, equipment-specific
values instead of a single generic diurnal curve.

Profile fields:
    metrics: ordered list of (key, unit, setpoint, nominal_delta) tuples.
    setpoint: the control target the device tries to maintain.
    nominal_delta: expected delta between supply and return (e.g. chiller ΔT = 5°C).
    diurnal_weight: how strongly the diurnal occupancy curve affects this type
                    (chillers follow building load closely → 0.9; cooling tower
                     follows ambient → 0.6; air cooler minimal → 0.2).
    fault_types: list of fault behaviours the generator should occasionally inject.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Literal

# --------------------------------------------------------------------------- #
# Fault taxonomy
# --------------------------------------------------------------------------- #
FaultType = Literal[
    "sensor_drift",   # gradual offset (thermometer slowly wrong)
    "sensor_glitch",  # sudden spike/drop then recovery
    "stuck_value",    # value stops changing for a few ticks
    "delta_t_low",    # chiller ΔT drops below normal (partial load)
    "fan_stall",      # AHU airflow collapses
    "compressor_cycle",  # compressor short-cycles
]

# --------------------------------------------------------------------------- #
# Profile dataclass
# --------------------------------------------------------------------------- #
@dataclass
class EquipmentProfile:
    """Operational profile for one equipment type."""

    metrics: List[str]                          # ordered metric keys
    units: Dict[str, str]                       # metric → unit
    setpoints: Dict[str, float]                 # metric → nominal setpoint
    nominal_delta: float = 0.0                  # supply→return delta (e.g. 5°C)
    diurnal_weight: float = 0.5                 # 0 = no diurnal, 1 = full diurnal
    ambient_weight: float = 0.0                 # 0 = no ambient dependence, 1 = full
    fault_types: List[FaultType] = field(default_factory=list)


# --------------------------------------------------------------------------- #
# BMS profiles — one per equipment type from samples/bms.csv
# --------------------------------------------------------------------------- #
_BMS_PROFILES: Dict[str, EquipmentProfile] = {
    # ----- Chillers -------------------------------------------------------- #
    # 4 metrics: CW supply/return, condenser supply/return.
    # Chiller maintains ~7°C CHWS; return rises with load (~14-16°C).
    # Condenser rejects heat: supply ~28-30°C, return ~31-33°C.
    "Chillers": EquipmentProfile(
        metrics=[
            "Chilled_Water_Supply_Temp",
            "Chilled_Water_Return_Temp",
            "Condensor_Supply_Temp",
            "Condensor_Return_Temp",
        ],
        units={
            "Chilled_Water_Supply_Temp": "°C",
            "Chilled_Water_Return_Temp": "°C",
            "Condensor_Supply_Temp": "°C",
            "Condensor_Return_Temp": "°C",
        },
        setpoints={
            "Chilled_Water_Supply_Temp": 7.0,
            "Chilled_Water_Return_Temp": 14.0,
            "Condensor_Supply_Temp": 29.0,
            "Condensor_Return_Temp": 32.0,
        },
        nominal_delta=7.0,          # CHW ΔT = 7°C (return - supply)
        diurnal_weight=0.9,
        fault_types=["sensor_drift", "delta_t_low", "compressor_cycle"],
    ),

    # ----- AHU ------------------------------------------------------------- #
    # 5 metrics: supply/return temp + RH, optional supply flow.
    # AHU maintains ~14°C supply air; return follows zone load (24-30°C).
    # Humidity controlled to ~55-60% RH at supply.
    "AHU": EquipmentProfile(
        metrics=[
            "Supply_Temp",
            "Return_Temp",
            "Supply_RH",
            "Return_RH",
            "Supply_Flow",
        ],
        units={
            "Supply_Temp": "°C",
            "Return_Temp": "°C",
            "Supply_RH": "%",
            "Return_RH": "%",
            "Supply_Flow": "L/s",
        },
        setpoints={
            "Supply_Temp": 14.0,
            "Return_Temp": 25.0,
            "Supply_RH": 58.0,
            "Return_RH": 55.0,
            "Supply_Flow": 1000.0,     # nominal L/s (varies by AHU size)
        },
        nominal_delta=11.0,         # return - supply temp ≈ 11°C
        diurnal_weight=0.85,
        fault_types=["sensor_drift", "fan_stall"],
    ),

    # ----- Cooling Tower MainHeader ---------------------------------------- #
    # 2 metrics: outdoor dry-bulb temp + RH.
    # Cooling tower rejects heat to ambient; performance tracks outdoor temp.
    "CoolingTower MainHeader": EquipmentProfile(
        metrics=[
            "Outdoor_MIT_01_Temp",
            "Outdoor_MIT_01_RH",
        ],
        units={
            "Outdoor_MIT_01_Temp": "°C",
            "Outdoor_MIT_01_RH": "%",
        },
        setpoints={
            "Outdoor_MIT_01_Temp": 30.0,    # typical design ambient
            "Outdoor_MIT_01_RH": 65.0,
        },
        diurnal_weight=0.3,             # outdoor temp changes slowly
        ambient_weight=1.0,
        fault_types=["sensor_drift"],
    ),

    # ----- Chiller MainHeader ---------------------------------------------- #
    # 8+ metrics: CHWS/CHWR, CDWS/CDWR, CHW differential pressure,
    # condenser water delta, flow rate, header temps.
    "Chiller MainHeader": EquipmentProfile(
        metrics=[
            "CHWS_Temp",
            "CHWR_Temp",
            "CDWS_Temp",
            "CDWR_Temp",
            "CHW_DPT",
            "CW_DPT",
            "consumptionFlowRate",
            "CHW_PDT",
        ],
        units={
            "CHWS_Temp": "°C",
            "CHWR_Temp": "°C",
            "CDWS_Temp": "°C",
            "CDWR_Temp": "°C",
            "CHW_DPT": "kPa",
            "CW_DPT": "kPa",
            "consumptionFlowRate": "L/s",
            "CHW_PDT": "°C",
        },
        setpoints={
            "CHWS_Temp": 7.0,
            "CHWR_Temp": 12.0,
            "CDWS_Temp": 30.0,
            "CDWR_Temp": 35.0,
            "CHW_DPT": 250.0,
            "CW_DPT": 200.0,
            "consumptionFlowRate": 500.0,
            "CHW_PDT": 5.0,
        },
        nominal_delta=5.0,          # CHWR - CHWS = 5°C
        diurnal_weight=0.85,
        fault_types=["sensor_drift", "delta_t_low"],
    ),

    # ----- Air Compressors ------------------------------------------------- #
    # 1 metric: total system flow (m³/min).
    # Compressor flow follows plant demand; relatively stable with step changes.
    "Air Compressors": EquipmentProfile(
        metrics=["Total_Flow"],
        units={"Total_Flow": "m³/min"},
        setpoints={"Total_Flow": 500.0},
        diurnal_weight=0.6,
        fault_types=["sensor_drift"],
    ),

    # ----- Air Coolers ----------------------------------------------------- #
    # 1 metric: supply air temperature.
    # Air cooler maintains ~14°C supply; return varies with zone load.
    "Air Coolers": EquipmentProfile(
        metrics=["Supply_Temp"],
        units={"Supply_Temp": "°C"},
        setpoints={"Supply_Temp": 14.0},
        diurnal_weight=0.5,
        fault_types=["sensor_drift"],
    ),
}


def get_profile(equipment_type: str) -> EquipmentProfile | None:
    """Return the profile for an equipment type, or None if unknown."""
    return _BMS_PROFILES.get(equipment_type)


def get_all_bms_profiles() -> Dict[str, EquipmentProfile]:
    """Return all BMS equipment profiles."""
    return dict(_BMS_PROFILES)


# --------------------------------------------------------------------------- #
# EMS profiles — manufacturing machines
# --------------------------------------------------------------------------- #
@dataclass
class EMSMachineProfile:
    """Profile for a manufacturing machine type."""

    machine_type: str
    power_envelope: tuple[float, float, float]  # (idle, peak, nominal) kW
    cycle_seconds: float                        # characteristic cycle time
    duty_pattern: str                           # "square", "stepped", "sawtooth"
    energy_start_kwh: float = 0.0               # starting cumulative energy


_EMS_PROFILES: Dict[str, EMSMachineProfile] = {
    "INJECTION_MOULDING": EMSMachineProfile(
        machine_type="INJECTION_MOULDING",
        power_envelope=(8.0, 140.0, 75.0),
        cycle_seconds=45.0,
        duty_pattern="square",
    ),
    "CNC": EMSMachineProfile(
        machine_type="CNC",
        power_envelope=(2.0, 35.0, 15.0),
        cycle_seconds=30.0,
        duty_pattern="stepped",
    ),
    "HEATING": EMSMachineProfile(
        machine_type="HEATING",
        power_envelope=(10.0, 220.0, 120.0),
        cycle_seconds=600.0,
        duty_pattern="sawtooth",
    ),
}


def get_ems_profile(machine_type: str) -> EMSMachineProfile | None:
    """Return the EMS profile for a machine type, or None if unknown."""
    return _EMS_PROFILES.get(machine_type)


def get_all_ems_profiles() -> Dict[str, EMSMachineProfile]:
    """Return all EMS machine profiles."""
    return dict(_EMS_PROFILES)
